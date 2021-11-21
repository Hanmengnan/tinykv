package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	Txn *badger.Txn
}

func NewStandaloneStorageReader(txn *badger.Txn) *StandaloneStorageReader {
	return &StandaloneStorageReader{Txn: txn}
}

func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.Txn)
	iter.Rewind()
	return iter
}

func (r *StandaloneStorageReader) Close() {
	r.Txn.Discard()
}
