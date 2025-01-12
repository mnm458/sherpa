package exchange

import (
	"errors"
	"log/slog"
)

type Signal interface {
	GetType() string
	GetAction() string
	GetSymbol() string
	GetLeverage() int64
}

const (
	EQUITY_PERCENTAGE = 0.95
)

type ExchangeStrategy interface {
	Process(Signal) error
}

func NewExchangeHandler(exchangeType string, apiKey string, secret string, logger *slog.Logger) (ExchangeStrategy, error) {
	switch exchangeType {
	case "binance":
		return NewBinanceHandler(apiKey, secret, logger), nil
	case "bybit":
		return NewBybitHandler(apiKey, secret), nil
	default:
		return nil, errors.New("unsupported exchange")
	}
}
