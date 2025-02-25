package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mnm458/sherpa/pkg/exchange"

	"github.com/mnm458/sherpa/pkg/types"
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
		SL:          0.019,
	})
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)

}

func (app *application) testBinance(w http.ResponseWriter, r *http.Request) {
	err := app.ExchangeHandler.Process(types.BinanceSignal{
		Symbol:   "BTCUSDT",
		Type:     "LIMIT",
		Action:   "Sell",
		Leverage: 5,
		TP:       0.001,
		SL:       0.019,
	})
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)

}

func (app *application) HandleSignal(w http.ResponseWriter, r *http.Request) {
	var signal exchange.Signal
	err := json.NewDecoder(r.Body).Decode(&signal)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = app.ExchangeHandler.Process(signal)
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)
}

func (app *application) TriggerReentryBybit(w http.ResponseWriter, r *http.Request) {

}
