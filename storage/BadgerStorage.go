package storage

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
)

const (
	SET = "SET"
	DEL = "DEL"
)

type StorageFSM struct {
	db *badger.DB
}

func NewStorageFSM(db *badger.DB) *StorageFSM {
	return &StorageFSM{
		db: db,
	}
}

func (sf StorageFSM) Set(key []byte, value []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key is invalid")
	}
	if value == nil || len(value) == 0 {
		return errors.New("value is invalid")
	}
	return sf.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (sf StorageFSM) Get(key []byte) ([]byte, error) {
	if key == nil || len(key) == 0 {
		return nil, errors.New("key is invalid")
	}

	var data []byte
	err := sf.db.View(func(txn *badger.Txn) error {
		d, err := txn.Get(key)
		if err != nil {
			return err
		}

		data, err = d.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (sf StorageFSM) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key is invalid")
	}
	return sf.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
