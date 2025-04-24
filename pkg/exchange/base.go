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
	EQUITY_PERCENTAGE = 0.97
)

type ExchangeStrategy interface {
	Process(Signal) error
}

func NewExchangeHandler(ctx context.Context, exchangeType string, apiKey string, secret string, stage types.Environment, byOrderChan chan types.ByMainOrder, biOrderChan chan types.BiSubmittedOrders, logger *slog.Logger) (ExchangeStrategy, error) {
	switch exchangeType {
	case types.EXCHANGE_BINANCE:
		//TODO: add stage to this handler
		return NewBinanceHandler(ctx, apiKey, secret, biOrderChan, logger), nil
	case types.EXCHANGE_BYBIT:
		return NewBybitHandler(ctx, apiKey, secret, stage, byOrderChan, logger), nil
	default:
		return nil, errors.New("unsupported exchange")
	}
}
