package main

import (
	"net/http"

	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /ping", http.HandlerFunc(ping))
	mux.Handle("POST /test", http.HandlerFunc(app.testBybit))
	mux.Handle("POST /test-binance", http.HandlerFunc(app.testBinance))
	mux.Handle("POST /handle-signal/binance", http.HandlerFunc(app.handleBinance))
	mux.Handle("POST /handle-signal/bybit", http.HandlerFunc(app.handleBybit))
	standard := alice.New(app.recoverPanic, app.logRequest, commonHeaders)
	return standard.Then(mux)
}
