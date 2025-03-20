package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mnm458/sherpa/internal/logger"
	"github.com/mnm458/sherpa/pkg/types"
)

type Config struct {
	Addr          string
	Exchange      string
	Environment   types.Environment
	Logger        *slog.Logger
	ReEntrySwitch bool
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Logger = logger.Init()

	app := NewApplication(ctx, cfg)
	if app == nil {
		log.Fatal("app initialization failed")
	}

	// Start exchange-specific services
	go func() {
		if cfg.ReEntrySwitch {
			var err error
			switch cfg.Exchange {
			case types.EXCHANGE_BYBIT:
				go app.ListenForByOrderUpdates(ctx)
				app.WSByConnect(app.wsURL, app.ExchangeHandler)
			case types.EXCHANGE_BINANCE:
				go app.ListenForBiOrderUpdates(ctx)
				err = app.WSBiConnect(ctx, app.ExchangeHandler)
			}
			if err != nil {
				app.logger.Error("websocket error", slog.String("error", err.Error()))
				cancel()
			}
		}
	}()

	server := &http.Server{
		Addr:         cfg.Addr,
		Handler:      app.routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
		ErrorLog:     slog.NewLogLogger(cfg.Logger.Handler(), slog.LevelError),
	}

	go func() {
		app.logger.Info("starting server", slog.Any("addr", cfg.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			app.logger.Error("server error", slog.String("error", err.Error()))
			cancel()
		}
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	app.logger.Info("shutting down server...")
	app.closeAllWebSockets()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		app.logger.Error("server forced to shutdown", slog.String("error", err.Error()))
	}

	// Give some time for cleanup
	time.Sleep(2 * time.Second)
	app.logger.Info("server shutdown complete")
}

func parseFlags() (Config, error) {
	addr := flag.String("addr", ":4000", "HTTP network address")
	exchange := flag.String("exchange", "", "Exchange name")
	env := flag.String("env", "", "Environment (TEST/PROD)")
	reEntrySwitch := flag.Bool("reEntrySwitch", true, "ReEntrySwitch") //by default keep the switch on

	flag.Parse()

	if *exchange == "" || *env == "" {
		return Config{}, fmt.Errorf("exchange and environment flags are required")
	}

	exchangeUpper := strings.ToUpper(*exchange)
	switch exchangeUpper {
	case types.EXCHANGE_BYBIT, types.EXCHANGE_BINANCE:
		// valid
	default:
		return Config{}, fmt.Errorf("invalid exchange: %s (must be BYBIT or BINANCE)", *exchange)
	}

	// Convert to uppercase and validate environment
	envUpper := strings.ToUpper(*env)
	var environment types.Environment

	switch envUpper {
	case string(types.TEST):
		environment = types.TEST
	case string(types.PROD):
		environment = types.PROD
	default:
		return Config{}, fmt.Errorf("invalid environment: %s (must be TEST or PROD)", *env)
	}

	return Config{
		Addr:          *addr,
		Exchange:      exchangeUpper,
		Environment:   environment,
		ReEntrySwitch: *reEntrySwitch,
	}, nil
}
