package main

import (
	"encoding/json"
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
		app.logger.Error("test bybit order failed", "error", err)
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
		app.logger.Error("test binance order failed", "error", err)
	}
	w.WriteHeader(http.StatusOK)
}

func (app *application) HandleSignal(w http.ResponseWriter, r *http.Request) {
	switch app.ExchangeID {
	case exchange.BYBIT_EXCHANGE_ID:
		var signal exchange.BybitSignal
		if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		app.logger.Info("signal received",
			"exchange", "bybit",
			"symbol", signal.Symbol,
			"side", signal.Side,
			"leverage", signal.Leverage,
			"tp", signal.TP,
			"sl", signal.SL,
		)
		if err := app.ExchangeHandler.Process(signal); err != nil {
			app.logger.Error("signal processing failed", "error", err)
		}
		w.WriteHeader(http.StatusOK)

	case exchange.BINANCE_EXCHANGE_ID:
		var signal types.BinanceSignal
		if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		app.logger.Info("signal received",
			"exchange", "binance",
			"symbol", signal.Symbol,
			"type", signal.Type,
			"action", signal.Action,
			"leverage", signal.Leverage,
			"tp", signal.TP,
			"sl", signal.SL,
		)
		if err := app.ExchangeHandler.Process(signal); err != nil {
			app.logger.Error("signal processing failed", "error", err)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (app *application) AdhocMarketOrder(w http.ResponseWriter, r *http.Request) {
	bh, _ := app.ExchangeHandler.(*exchange.BinanceHandler)
	if err := bh.CreateMarketOrder(); err != nil {
		app.logger.Error("adhoc market order failed", "error", err)
	}
	w.WriteHeader(http.StatusOK)
}
