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
	app := NewApplication(ctx, logger)
	// if err != nil {
	// 	logger.Error("Failed to initalise application", "error", err)
	// 	os.Exit(1)
	// }
	var addr = flag.String("addr", ":4000", "HTTP network address")
	flag.Parse()

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
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown: ", err)
	}
	os.Exit(1)
}
