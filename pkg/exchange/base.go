package exchange

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mnm458/sherpa/pkg/types"
)

type Signal interface {
	GetSymbol() string
	GetLeverage() int64
}

const (
	EQUITY_PERCENTAGE = 0.95
)

type ExchangeStrategy interface {
	Process(Signal) error
}

func NewExchangeHandler(ctx context.Context, exchangeType string, apiKey string, secret string, stage types.Environment, logger *slog.Logger) (ExchangeStrategy, error) {
	switch exchangeType {
	case "binance":
		//TODO: add stage to this handler
		return NewBinanceHandler(apiKey, secret, logger), nil
	case "bybit":
		return NewBybitHandler(ctx, apiKey, secret, stage, logger), nil
	default:
		return nil, errors.New("unsupported exchange")
	}
}
