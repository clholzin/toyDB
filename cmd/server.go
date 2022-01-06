package cmd

import (
	"net/http"
	"toyDB/db"
	"toyDB/transaction"

	"github.com/gorilla/mux"
)

func Server() {

	repository := db.NewStorage()
	r := mux.NewRouter()
	r.HandleFunc("/", transaction.Save(repository)).Methods("POST")
	http.ListenAndServe(":8080", nil)
}
