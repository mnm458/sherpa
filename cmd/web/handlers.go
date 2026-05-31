package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

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

// HandleSignal processes an inbound TradingView / webhook signal.
// Returns 429 if a signal is already in flight (M-1), 500 if processing fails (C-1).
func (app *application) HandleSignal(w http.ResponseWriter, r *http.Request) {
	// M-1: reject concurrent signals — only one order pipeline at a time.
	if !atomic.CompareAndSwapInt32(&app.signalInFlight, 0, 1) {
		app.logger.Warn("signal rejected — another signal is already in progress")
		http.Error(w, "signal already in progress", http.StatusTooManyRequests)
		return
	}
	defer atomic.StoreInt32(&app.signalInFlight, 0)

	app.stateMu.Lock()
	app.lastSignalAt = time.Now()
	app.stateMu.Unlock()

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
		// C-1: propagate errors to caller rather than silently swallowing them.
		if err := app.ExchangeHandler.Process(signal); err != nil {
			app.logger.Error("signal processing failed", "error", err)
			http.Error(w, "signal processing failed", http.StatusInternalServerError)
			return
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
			http.Error(w, "signal processing failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (app *application) AdhocMarketOrder(w http.ResponseWriter, r *http.Request) {
	bh, ok := app.ExchangeHandler.(*exchange.BinanceHandler)
	if !ok {
		http.Error(w, "adhoc market orders only supported for Binance", http.StatusBadRequest)
		return
	}
	var req struct {
		Symbol   string `json:"symbol"`
		Side     string `json:"side"`
		Quantity string `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := bh.CreateMarketOrder(req.Symbol, req.Side, req.Quantity); err != nil {
		app.logger.Error("adhoc market order failed", "error", err)
		http.Error(w, "adhoc market order failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Status returns a full system state snapshot as JSON.
// HTTP 200 = healthy, HTTP 503 = unhealthy (WebSocket down when re-entry is on).
func (app *application) Status(w http.ResponseWriter, r *http.Request) {
	app.stateMu.RLock()
	wsConnected := app.wsConnected
	wsAuthenticated := app.wsAuthenticated
	wsLastMsgAt := app.wsLastMsgAt
	wsLastPingAt := app.wsLastPingAt
	currOrder := app.CurrByMainOrder
	lastSignalAt := app.lastSignalAt
	app.stateMu.RUnlock()

	hasPosition := currOrder.Symbol != ""
	inFlight := atomic.LoadInt32(&app.signalInFlight) == 1

	// Healthy means: if re-entry is on, the WebSocket must be connected and authenticated.
	// If re-entry is off, the HTTP server being up is enough.
	wsHealthy := !app.reEntrySwitchOn || (wsConnected && wsAuthenticated)

	type wsStatus struct {
		Connected     bool   `json:"connected"`
		Authenticated bool   `json:"authenticated"`
		LastMessageAt string `json:"lastMessageAt,omitempty"`
		LastPingAt    string `json:"lastPingAt,omitempty"`
	}

	type positionStatus struct {
		HasPosition bool    `json:"hasPosition"`
		Symbol      string  `json:"symbol,omitempty"`
		Side        string  `json:"side,omitempty"`
		Quantity    float64 `json:"quantity,omitempty"`
		EntryPrice  float64 `json:"entryPrice,omitempty"`
		TakeProfit  float64 `json:"takeProfit,omitempty"`
		StopLoss    float64 `json:"stopLoss,omitempty"`
		Leverage    int64   `json:"leverage,omitempty"`
	}

	type statusResponse struct {
		Healthy        bool           `json:"healthy"`
		Exchange       string         `json:"exchange"`
		Environment    string         `json:"environment"`
		Uptime         string         `json:"uptime"`
		ReEntryOn      bool           `json:"reEntryOn"`
		SignalInFlight bool           `json:"signalInFlight"`
		WebSocket      wsStatus       `json:"websocket"`
		Position       positionStatus `json:"currentPosition"`
		LastSignalAt   string         `json:"lastSignalAt,omitempty"`
	}

	ws := wsStatus{
		Connected:     wsConnected,
		Authenticated: wsAuthenticated,
		LastMessageAt: formatSydney(wsLastMsgAt),
		LastPingAt:    formatSydney(wsLastPingAt),
	}

	pos := positionStatus{HasPosition: hasPosition}
	if hasPosition {
		pos.Symbol = currOrder.Symbol
		pos.Side = currOrder.Side
		pos.Quantity = currOrder.Quantity
		pos.EntryPrice = currOrder.Price
		pos.TakeProfit = currOrder.TakeProfit
		pos.StopLoss = currOrder.StopLoss
		pos.Leverage = currOrder.Leverage
	}

	resp := statusResponse{
		Healthy:        wsHealthy,
		Exchange:       app.ActiveExchange,
		Environment:    app.environment,
		Uptime:         time.Since(app.startedAt).Round(time.Second).String(),
		ReEntryOn:      app.reEntrySwitchOn,
		SignalInFlight: inFlight,
		WebSocket:      ws,
		Position:       pos,
		LastSignalAt:   formatSydney(lastSignalAt),
	}

	w.Header().Set("Content-Type", "application/json")
	if !wsHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}
