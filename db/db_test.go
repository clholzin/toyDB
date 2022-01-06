// +build/unit
package db

import "testing"

func TestDbSetup(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	expectErr := ErrNotFound + ": apples"
	if _, err := db.Get("apples"); err.Error() != expectErr {
		t.Fail()
	}
}

func TestDbSetGetDelete(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	db.Set("apple", "seed")

	db.Set("peach", "pit")
	if val, err := db.Get("peach"); err != nil && val != "pit" {
		t.Fail()
	}
	db.Delete("peach")
	if _, err := db.Get("peach"); err == nil {
		t.Fail()
	}
	err := db.Commit()
	if err == nil {
		t.Fail()
	}
	db.Delete("peach")
	if _, err := db.Get("apple"); err != nil {
		t.Fail()
	}
}

func TestDbTransactionAbort(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	db.Set("apple", "seed")
	db.Set("peach", "pit")
	if val, err := db.Get("peach"); err != nil && val != "pit" {
		t.Fail()
	}
	trans := NewTransaction(db)
	switch trans.(type) {
	case DataBaser:
		if trans.Version() != 1 {
			t.Fail()
		}
		break
	default:
		t.Fail()
	}

	trans.Set("peach", "slice")
	trans2 := NewTransaction(db)
	trans2.Set("peach", "pie")

	trans2.Set("grapes", "ofwrath")
	trans2.Delete("apple")
	if val, err := trans.Get("peach"); err != nil && val != "slice" {
		t.Fail()
	}
	trans2.Abort()
	if db.Version() != trans2.Version() {
		t.Fail()
	}

	if val, err := db.Get("peach"); err != nil && val != "pit" {
		t.Fail()
	}
	if val, err := db.Get("apple"); err != nil && val != "seed" {
		t.Fail()
	}
}

func TestDbTransactionCommit(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	db.Set("peach", "pit")
	db.Set("apple", "seeds")

	if val, err := db.Get("peach"); err != nil && val != "pit" {
		t.Fail()
	}
	trans1 := NewTransaction(db)
	switch trans1.(type) {
	case DataBaser:
		if trans1.Version() != 1 {
			t.Fail()
		}
		break
	default:
		t.Fail()
	}

	trans1.Set("peach", "slice")
	trans1.Delete("apple")
	if val, err := trans1.Get("peach"); err != nil && val != "slice" {
		t.Fail()
	}

	if _, err := trans1.Get("apple"); err == nil {
		t.Fail()
	}

	trans2 := NewTransaction(trans1)
	trans2.Set("apple", "sauce")
	if val, err := trans2.Get("peach"); err != nil && val != "sauce" {
		t.Fail()
	}
	trans2.Commit()
	if val, err := trans1.Get("apple"); err != nil && val != "sauce" {
		t.Fail()
	}
	trans1.Delete("apple")
	trans1.Commit()
	if db.Version() != trans1.Version() {
		t.Fail()
	}

	if val, err := db.Get("peach"); err != nil && val != "slice" {
		t.Fail()
	}

}

func TestDbDelete(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	db = NewTransaction(db)

	db.Set("apple", "seed")

	db.Set("peach", "pit")
	if val, err := db.Get("peach"); err != nil && val != "pit" {
		t.Fail()
	}
	db = NewTransaction(db)
	db.Delete("peach")
	if _, err := db.Get("peach"); err == nil {
		t.Fail()
	}
	err := db.Commit()
	if err != nil {
		t.Fail()
	}
	db.Delete("peach")
	if _, err := db.Get("apple"); err != nil {
		t.Fail()
	}
}

func TestDbCommitParent(t *testing.T) {
	db := NewStorage()
	if db.Version() != 0 {
		t.Fail()
	}
	db = NewTransaction(db)
	db = NewTransaction(db)
	err := db.Commit()
	if err == nil {
		t.Fail()
	}
}
