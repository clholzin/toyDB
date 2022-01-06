package db

import (
	"fmt"
)

//TODO: log.Version

const (
	// ErrNotFound is returned on read when data is not present
	ErrNotFound        = "Key not found"
	ErrNothingToCommit = "Nothing To Commit"
)

const (
	//Added flag for log action
	Added = iota
	//Updated flag for log action
	Updated
	//Deleted flag for log action
	Deleted
)

// Data for primary storage
type Data map[string]string

// Log data for tmp storage
type Log struct {
	Value   string
	Action  int
	Version int
	Prev    *Log
}

// TmpData store
type TmpData map[string]*Log

// Storage to contain primary store and session tmp store
type Storage struct {
	data    Data
	tmp     map[string]TmpData
	version int
}

type DataBaser interface {
	Get(transkey, key string) (string, error)
	Set(transkey, key, value string)
	Delete(transkey, key string)
	Commit(transkey string) error
	Version() int
	Abort(transkey string)
	Incr() int
}

func NewTransaction(db DataBaser, transkey string) DataBaser {
	db.Incr()
	tmp := make(TmpData)
	store := db.(*Storage)
	store.tmp[transkey] = tmp
	return store
}

func NewStorage() DataBaser {
	store := make(Data)
	storage := &Storage{}
	storage.tmp = make(map[string]TmpData)
	storage.data = store
	storage.version = 0
	return storage
}

// Get on store or current transaction store
func (store *Storage) Get(transkey, key string) (string, error) {
	if log, ok := store.tmp[transkey][key]; ok {
		if log.Action != Deleted {
			return log.Value, nil
		}
	} else if val, ok := store.data[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("%s: %s", ErrNotFound, key)
}

func (store *Storage) Set(transkey, key, value string) {
	nextlog := &Log{}
	if _, ok := store.tmp[transkey]; !ok {
		return
	}
	if log, ok := store.tmp[transkey][key]; ok {
		if log.Version != store.version {
			nextlog.Prev = log
		}
		nextlog.Action = Updated
	} else {
		nextlog.Action = Added
	}
	nextlog.Value = value
	nextlog.Version = store.version
	store.tmp[transkey][key] = nextlog
}

func (store *Storage) Delete(transkey, key string) {
	nextlog := &Log{}
	if _, ok := store.tmp[transkey]; !ok {
		return
	}

	log, tmpok := store.tmp[transkey][key]
	_, primaryok := store.data[key]
	if !primaryok && !tmpok {
		return
	}
	if tmpok {
		if log.Version != store.version {
			nextlog.Prev = log
		}
	}
	nextlog.Action = Deleted
	nextlog.Version = store.version
	store.tmp[transkey][key] = nextlog
}

func (store *Storage) decr() {
	if store.version > 0 {
		store.version--
	}
}

func (store *Storage) Commit(transkey string) error {
	defer store.decr()
	return store.commitPrimary(transkey)
}

func (store *Storage) commitPrimary(transkey string) error {
	commitCount := len(store.tmp[transkey])
	if commitCount == 0 {
		return fmt.Errorf(ErrNothingToCommit)
	}
	for key, log := range store.tmp[transkey] {
		if log.Action == Updated || log.Action == Added {
			store.data[key] = log.Value
		} else if log.Action == Deleted {
			delete(store.data, key)
			delete(store.tmp[transkey], key)
		}
	}
	store.tmp[transkey] = make(TmpData)
	return nil
}

func (store *Storage) Abort(transkey string) {
	defer store.decr()
	delete(store.tmp, transkey)
}

func (store *Storage) Version() int {
	return store.version
}

func (store *Storage) Incr() int {
	store.version++
	return store.version
}
