package engine

import (
	"log"

	"github.com/dgraph-io/badger"
)

// Implementation of the "Storage" interface
type BadgerStorage struct {
	db *badger.DB
}

var (
	gst BadgerStorage
)

func storageInit() {
	gst = BadgerStorage{}
	// Open the database under /tmp/badger
	// Automatically create it if not exists
	gdb, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	gst.db = gdb
	// Remember to call db.Close()
}
