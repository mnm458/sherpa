package main

import (
	"context"
	"log/slog"

	"github.com/mnm458/sherpa/pkg/exchange"
)

type application struct {
	ctx            context.Context
	logger         *slog.Logger
	binanceHandler exchange.ExchangeStrategy
}

func NewApplication(ctx context.Context, logger *slog.Logger) *application {
	bh, err := exchange.NewExchangeHandler("binance", "ak", "secret")
	if err != nil {
		return nil
	}
	return &application{
		ctx:            ctx,
		logger:         logger,
		binanceHandler: bh}
}
