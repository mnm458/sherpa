package main

import (
	"fmt"
	"io"
	"net/http"
)

func ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func (app *application) handleBinance(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	fmt.Printf("Received body: %s\n", body)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Received binance signal POST request"))

}

func (app *application) handleBybit(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	fmt.Printf("Received body: %s\n", body)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Received binance signal POST request"))
}
