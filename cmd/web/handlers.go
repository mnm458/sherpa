package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	switch app.ExchangeID {
	case exchange.BYBIT_EXCHANGE_ID:
		var signal exchange.BybitSignal
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
	case exchange.BINANCE_EXCHANGE_ID:

		var signal types.BinanceSignal
		err := json.NewDecoder(r.Body).Decode(&signal)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Printf("Received signal: Symbol: %s, Type: %s, Action: %s, Leverage: %d, TP: %.5f, SL: %.5f\n",
			signal.Symbol, signal.Type, signal.Action, signal.Leverage, signal.TP, signal.SL)
		bodyBytes, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Reset for later decoding
		fmt.Printf("Raw request body: %s\n", string(bodyBytes))
		err = app.ExchangeHandler.Process(signal)

	}
}

func (app *application) AdhocMarketOrder(w http.ResponseWriter, r *http.Request) {

	bh, _ := app.ExchangeHandler.(*exchange.BinanceHandler)

	err := bh.CreateMarketOrder()
	if err != nil {
		fmt.Println(err)
	}
	w.WriteHeader(http.StatusOK)
}
