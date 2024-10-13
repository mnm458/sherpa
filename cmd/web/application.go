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
	bh, err := exchange.NewExchangeHandler("binance", "23af38855c0e69da01b26eebdbdcfedf7c055d0c577302808127416afa5db684", "eeb2a16c3758aca84752bcbd64617bd58073c80228ce67caf2ff22a0b7dfe933")
	if err != nil {
		return nil
	}
	return &application{
		ctx:            ctx,
		logger:         logger,
		binanceHandler: bh}
}
