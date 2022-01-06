package standalone_storage

import (
	"log"
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config // 配置文件
	KvDB *badger.DB     // 底层数据库
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf, KvDB: nil}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.conf.DBPath
	opts := badger.DefaultOptions
	opts.Dir = dbPath
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	kvDB, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.KvDB = kvDB
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.KvDB.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.conf.DBPath); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.KvDB.NewTransaction(false)
	return NewStandaloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.KvDB, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.KvDB, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
