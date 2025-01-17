package server

import (
	"context"
	"log"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err == nil && value == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	} else if err == nil && value != nil {
		return &kvrpcpb.RawGetResponse{
			Value: value,
		}, nil
	} else {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}

}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{}
	modify.Data = storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{}
	modify.Data = storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	cf := req.Cf
	limit := req.Limit

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}
	iter := reader.IterCF(cf)
	kvPairs := make([](*kvrpcpb.KvPair), 0)

	for i := 0; i < int(limit) && iter.Valid(); i++ {
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			log.Println(err)
		} else {
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
		}
		iter.Next()
	}
	iter.Close()

	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}, nil
}
