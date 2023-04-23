package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	storage_internal "github.com/diasYuri/zendb/storage"
)

func main() {
	badgerOptions := badger.DefaultOptions("data")
	badgerDb, _ := badger.Open(badgerOptions)

	storage := storage_internal.NewStorageFSM(badgerDb)

	//_ = storage.Set([]byte("pessoa-1"), []byte("Timtim"))
	//_ = storage.Set([]byte("pessoa-2"), []byte("Coutinho"))

	data, _ := storage.Get([]byte("pessoa-1"))
	fmt.Println(string(data))
}
