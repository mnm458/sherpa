package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/mnm458/sherpa/pkg/exchange"
)

func ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func (app *application) testBinance(w http.ResponseWriter, r *http.Request) {
	err := app.binanceHandler.Process(exchange.BinanceSignal{
		Contract: "BTCUSDT-4HR",
		Type:     "open",
		Action:   "long",
	})
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)

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
