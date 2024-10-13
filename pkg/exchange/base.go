package exchange

import "errors"

type Signal interface {
	GetType() string
	GetAction() string
}

type ExchangeStrategy interface {
	Process(Signal) error
}

func NewExchangeHandler(exchangeType string, apiKey string, secret string) (ExchangeStrategy, error) {
	switch exchangeType {
	case "binance":
		return NewBinanceHandler(apiKey, secret), nil
	case "bybit":
		return NewBybitHandler(apiKey, secret), nil
	default:
		return nil, errors.New("unsupported exchange")
	}
}
