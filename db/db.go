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
	tmp     TmpData
	version int
}

type DataBaser interface {
	Get(key string) (string, error)
	Set(key, value string)
	Delete(key string)
	Commit() error
	Version() int
	Abort()
	Incr() int
}

func NewTransaction(db DataBaser) DataBaser {
	db.Incr()
	return db
}

func NewStorage() DataBaser {
	store := make(Data)
	tmp := make(TmpData)
	storage := &Storage{}
	storage.tmp = tmp
	storage.data = store
	storage.version = 0
	return storage
}

// Get on store or current transaction store
func (store *Storage) Get(key string) (string, error) {
	if log, ok := store.tmp[key]; ok {
		if log.Action != Deleted {
			return log.Value, nil
		}
	} else if val, ok := store.data[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("%s: %s", ErrNotFound, key)
}

func (store *Storage) Set(key, value string) {
	nextlog := &Log{}
	if log, ok := store.tmp[key]; ok {
		if log.Version != store.version {
			nextlog.Prev = log
		}
		nextlog.Action = Updated
	} else {
		nextlog.Action = Added
	}
	nextlog.Value = value
	nextlog.Version = store.version
	store.tmp[key] = nextlog
	if store.version == 0 {
		store.commitPrimary()
	}
}

func (store *Storage) Delete(key string) {
	nextlog := &Log{}
	log, tmpok := store.tmp[key]
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
	store.tmp[key] = nextlog
	if store.version == 0 {
		store.commitPrimary()
	}
}

func (store *Storage) decr() {
	if store.version > 0 {
		store.version--
	}
}

func (store *Storage) Commit() error {
	isNextPrimary := (store.version - 1) <= 0

	defer store.decr()

	if !isNextPrimary {
		return store.commitParent()
	}
	return store.commitPrimary()
}

func (store *Storage) commitParent() error {
	count := 0
	for key, log := range store.tmp {
		if store.version == log.Version {
			count++
			log.Version--
			prev := log.Prev
			if prev != nil {
				log.Prev = prev.Prev
			}
			store.tmp[key] = log
		}
	}
	if count == 0 {
		return fmt.Errorf(ErrNothingToCommit)
	}
	return nil
}

func (store *Storage) commitPrimary() error {
	commitCount := len(store.tmp)
	if commitCount == 0 {
		return fmt.Errorf(ErrNothingToCommit)
	}
	for key, log := range store.tmp {
		if log.Action == Updated || log.Action == Added {
			store.data[key] = log.Value
		} else if log.Action == Deleted {
			delete(store.data, key)
			delete(store.tmp, key)
		}
	}
	store.tmp = make(TmpData)
	return nil
}

func (store *Storage) Abort() {
	defer store.decr()

	for key, log := range store.tmp {
		if store.version == log.Version {
			prev := log.Prev
			if prev != nil {
				store.tmp[key] = prev
			} else {
				delete(store.tmp, key)
			}
		}
	}
}

func (store *Storage) Version() int {
	return store.version
}

func (store *Storage) Incr() int {
	store.version++
	return store.version
}
