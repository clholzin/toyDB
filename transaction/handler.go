package transaction

import (
	"fmt"
	"net/http"
	"toyDB/db"
)

func Save(store db.DataBaser) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		trans := db.NewTransaction(store)
		fmt.Fprintf(w, "go got it %d\n", trans.Version())
	}
}
