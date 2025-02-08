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

func (app *application) testBybit(w http.ResponseWriter, r *http.Request) {
	err := app.ExchangeHandler.Process(exchange.BybitSignal{
		Category:    "linear",
		OrderType:   "Limit",
		Side:        "Sell",
		Symbol:      "BTCUSDT",
		Leverage:    5,
		PositionIdx: 0,
		TP:          0.001,
		SL:          0.0019,
	})
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)

}

func (app *application) testBinance(w http.ResponseWriter, r *http.Request) {
	err := app.ExchangeHandler.Process(exchange.BinanceSignal{
		Symbol:   "BTCUSDT",
		Type:     "open",
		Action:   "BUY",
		Leverage: 2,
		TP:       0.01,
		SL:       -0.019,
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
