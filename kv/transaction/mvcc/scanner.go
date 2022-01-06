package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	it  engine_util.DBIterator
	txn *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	cfIter := txn.Reader.IterCF(engine_util.CfWrite)
	cfIter.Seek(startKey)
	return &Scanner{
		it:  cfIter,
		txn: txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.it.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.it.Valid() {
		key, time, writeKind := scan.extractWrite()
		if writeKind == WriteKindDelete {
			oldKey := key
			for bytes.Equal(key, oldKey) {
				scan.it.Next()
				key, time, writeKind = scan.extractWrite()
			}
		}
		for time > scan.txn.StartTS {
			scan.it.Next()
			key, time, writeKind = scan.extractWrite()
		}

		value, _ := scan.txn.GetValue(key)
		scan.it.Next()
		return key, value, nil
	}
	return nil, nil, nil
}

// extract key,time,writekind
func (scan *Scanner) extractWrite() ([]byte, uint64, WriteKind) {
	writeItem := scan.it.Item()
	writeKey := writeItem.Key()
	writeValue, _ := writeItem.Value()
	writeObject, _ := ParseWrite(writeValue)
	writeKind := writeObject.Kind
	key := DecodeUserKey(writeKey)
	time := decodeTimestamp(writeKey)
	return key, time, writeKind
}
