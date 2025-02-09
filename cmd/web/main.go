package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/mnm458/sherpa/internal/logger"
)

func main() {
	logger := logger.Init()
	ctx := context.Background()

	var addr = flag.String("addr", ":4000", "HTTP network address")
	exchangeName := flag.String("exchange", "", "Exchange name")
	stage := flag.String("stage", "", "Stag environment")

	flag.Parse()

	if *exchangeName == "" || *stage == "" {
		log.Fatal("exchange and stage args are required to start server")
	}

	app := NewApplication(ctx, *exchangeName, *stage, logger)
	if app == nil {
		panic("app init failed")
	}
	if *exchangeName == "bybit" {
		go func() {
			app.WSByConnect(app.wsURL, app.ExchangeHandler)
		}()
	} else if *exchangeName == "binance" {
		go func() {

			app.WSBiConnect(app.ExchangeHandler)
		}()
	}
	server := &http.Server{
		Addr:         *addr,
		Handler:      app.routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	app.logger.Info("starting server", slog.Any("addr", *addr))
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			app.logger.Error("server listen error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()
	go app.ListenForByOrderUpdates()
	go app.ListenForBiOrderUpdates()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown: ", err)
	}
	os.Exit(0)
}
