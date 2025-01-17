package mvcc

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	fullKey := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   fullKey,
			Cf:    engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lock, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		resLock, err := ParseLock(lock)
		if err != nil {
			return nil, err
		}
		return resLock, nil
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Cf:    engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}
func (txn *MvccTxn) getWrite(key []byte, ts uint64) (*Write, uint64, error) {
	// Your Code Here (4A).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	fullKey := EncodeKey(key, ts)
	writeIter.Seek(fullKey)
	if !writeIter.Valid() {
		return nil, 0, nil
	}
	writeItem := writeIter.Item()
	// ensure the iterator points to the write with the same userkey
	writeKey := writeItem.Key()
	userKey := DecodeUserKey(writeKey)
	commitTs := decodeTimestamp(writeKey)
	if !bytes.Equal(userKey, key) {
		return nil, 0, nil
	}
	data, getValueErr := writeItem.Value()
	if getValueErr != nil {
		return nil, 0, getValueErr
	}
	write, parseWriteErr := ParseWrite(data)
	if parseWriteErr != nil {
		return nil, 0, parseWriteErr
	}

	return write, commitTs, nil
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	recentWrite, _, errRecent := txn.getWrite(key, txn.StartTS)
	if errRecent != nil || recentWrite == nil {
		return nil, errRecent
	}

	if recentWrite.Kind == WriteKindDelete {
		return nil, nil
	}
	targetTs := recentWrite.StartTS
	targetKey := EncodeKey(key, targetTs)
	defaultIter := txn.Reader.IterCF(engine_util.CfDefault)
	defaultIter.Seek(targetKey)
	if defaultIter.Valid() {
		defaultItem := defaultIter.Item()
		userKey := DecodeUserKey(defaultItem.Key())
		if !bytes.Equal(userKey, key) {
			return nil, nil
		}
		value, err := defaultItem.Value()
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	fullKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   fullKey,
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	})

}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	fullKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: fullKey,
			Cf:  engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	for writeIter.Valid() {
		writeItem := writeIter.Item()
		writeValue, errGetValue := writeItem.Value()
		if errGetValue != nil {
			return nil, 0, errGetValue
		}
		writeObject, errParseWrite := ParseWrite(writeValue)
		if errParseWrite != nil {
			return nil, 0, errParseWrite
		}
		writeKey := writeItem.Key()
		user_key := DecodeUserKey(writeKey)
		commit_ts := decodeTimestamp(writeKey)
		if bytes.Equal(key, user_key) && writeObject.StartTS == txn.StartTS {
			return writeObject, commit_ts, nil
		}
		writeIter.Next()
	}
	writeIter.Close()
	return nil, 0, nil

}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	return txn.getWrite(key, math.MaxUint64)
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
