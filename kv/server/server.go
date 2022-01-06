package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.GetResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, getLockErr := txn.GetLock(req.Key)
	if getLockErr != nil {
		return &kvrpcpb.GetResponse{}, getLockErr
	}
	if lock != nil {
		if lock.Ts < txn.StartTS {
			return &kvrpcpb.GetResponse{
				Error: &kvrpcpb.KeyError{
					Locked: lock.Info(req.Key),
				},
			}, nil
		}
	}
	value, getValueErr := txn.GetValue(req.Key)
	if getValueErr != nil {
		return nil, getValueErr
	}
	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
			Value:    value,
		}, nil
	}
	return &kvrpcpb.GetResponse{
		NotFound: false,
		Value:    value,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.PrewriteResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	writes := req.Mutations

	// check write conflicts
	errors := make([]*kvrpcpb.KeyError, 0)
	for _, write := range writes {
		recentWrite, writeTs, _ := txn.MostRecentWrite(write.Key)
		if writeTs < txn.StartTS || recentWrite == nil {
			continue
		} else {
			errors = append(errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: recentWrite.StartTS,
					Key:        write.Key,
				},
			})
		}
	}
	if len(errors) != 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: errors,
		}, nil
	}

	// check lock free
	lockErrors := make([]*kvrpcpb.KeyError, 0)
	for _, write := range writes {
		lock, _ := txn.GetLock(write.Key)

		if lock != nil {
			if lock.Ts < txn.StartTS {
				lockErrors = append(lockErrors, &kvrpcpb.KeyError{
					Locked: lock.Info(write.Key),
				})
			}
		}
	}
	if len(lockErrors) != 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: lockErrors,
		}, nil
	}

	// add lock
	for _, write := range writes {
		txn.PutValue(write.Key, write.Value)
		txn.PutLock(write.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(write.GetOp()),
		})
	}
	server.storage.Write(req.Context, txn.Writes())
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.CommitResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, getLockErr := txn.GetLock(key)
		if getLockErr != nil {
			return &kvrpcpb.CommitResponse{}, getLockErr
		}
		if lock == nil {
			return &kvrpcpb.CommitResponse{}, nil
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, nil
		}

	}
	server.Latches.AcquireLatches(req.Keys)
	for _, key := range req.Keys {

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindPut,
		})
		txn.DeleteLock(key)
	}
	server.storage.Write(req.Context, txn.Writes())
	server.Latches.ReleaseLatches(req.Keys)

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.ScanResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	iter := mvcc.NewScanner(req.StartKey, txn)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for i := req.Limit; i > 0; i-- {
		key, value, _ := iter.Next()
		if key == nil {
			break
		} else {
			if value != nil {
				kvs = append(kvs, &kvrpcpb.KvPair{
					Key:   key,
					Value: value,
				})
			}
		}
	}
	return &kvrpcpb.ScanResponse{
		Pairs: kvs,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.CheckTxnStatusResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return &kvrpcpb.CheckTxnStatusResponse{}, err
	}
	//主键上的锁不存在
	if lock == nil {
		// 事务已被提交
		currentWrite, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_NoAction,
			}, err
		}
		if currentWrite != nil {
			if currentWrite.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.CheckTxnStatusResponse{
					CommitVersion: commitTs,
					Action:        kvrpcpb.Action_NoAction,
				}, nil
			} else {
				return &kvrpcpb.CheckTxnStatusResponse{}, nil
			}
		} else {
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			server.storage.Write(req.Context, txn.Writes())
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_LockNotExistRollback,
			}, nil
		}

	} else {
		t1 := lock.Ts + lock.Ttl
		print(t1 < req.CurrentTs)
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			server.storage.Write(req.Context, txn.Writes())
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_TTLExpireRollback,
			}, nil
		} else {
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_NoAction,
			}, nil
		}
	}
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.BatchRollbackResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		currentWrite, _, _ := txn.CurrentWrite(key)
		if currentWrite != nil {
			if currentWrite.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.BatchRollbackResponse{}, nil
			} else {
				return &kvrpcpb.BatchRollbackResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "true",
					},
				}, nil
			}
		}

		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)

		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: txn.StartTS,
		})

	}
	server.storage.Write(req.Context, txn.Writes())
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, getReaderErr := server.storage.Reader(req.Context)
	if getReaderErr != nil {
		return &kvrpcpb.ResolveLockResponse{}, getReaderErr
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	it := txn.Reader.IterCF(engine_util.CfLock)
	keys := make([][]byte, 0)
	for it.Valid() {
		lockItem := it.Item()
		lockKey := lockItem.Key()
		lockValue, _ := lockItem.Value()
		lockObject, _ := mvcc.ParseLock(lockValue)
		if lockObject.Ts == txn.StartTS {
			keys = append(keys, lockKey)
		}
		it.Next()
	}

	if req.CommitVersion == 0 {
		resp, _ := server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		return &kvrpcpb.ResolveLockResponse{
			Error:       resp.Error,
			RegionError: resp.RegionError,
		}, nil
	} else {
		resp, _ := server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		return &kvrpcpb.ResolveLockResponse{
			Error:       resp.Error,
			RegionError: resp.RegionError,
		}, nil
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
