package main

import (
	"net/http"

	"github.com/justinas/alice"
)

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /ping", http.HandlerFunc(ping))
	mux.Handle("POST /test", http.HandlerFunc(app.testBybit))
	mux.Handle("POST /handle-signal", http.HandlerFunc(app.HandleSignal))
	mux.Handle("POST /test-binance", http.HandlerFunc(app.testBinance))
	standard := alice.New(app.recoverPanic, app.logRequest, commonHeaders)
	return standard.Then(mux)
}
