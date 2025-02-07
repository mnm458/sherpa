package exchange

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/mnm458/sherpa/pkg/types"
	bybitClib "github.com/wuhewuhe/bybit.go.api"
)

type BybitHandler struct {
	client *bybitClib.Client
	// websockClient *bybitClib.WebSocket
	ctx    context.Context
	logger *slog.Logger
}

type BybitSignal struct {
	Category    string
	Symbol      string
	Side        string
	PositionIdx int32
	OrderType   string
	Quantity    float64
	Price       float64
	TimeInForce string
	TP          float64
	SL          float64
	IsLeverage  int8
	TPOderType  string
	SLOrderType string
}

func (bs BybitSignal) GetSymbol() string {
	return bs.Symbol
}

func (bs BybitSignal) GetLeverage() int64 {
	return 0
}

func (b BybitSignal) String() string {
	return fmt.Sprintf("--[Bybit signal]--\nCategory:%s\nSymbol:%s\nSide:%s\nPosition Idx:%d\nOrderType:%s\nQuantity:%.2f\nPrice:%.2f\nTime In Force:%s\nTP:%.2f\nSL:%.2f\nIs Leverage:%d\nTP Order Type:%s\nSL Order Type:%s\n",
		b.Category,
		b.Symbol,
		b.Side,
		b.PositionIdx,
		b.OrderType,
		b.Quantity,
		b.Price,
		b.TimeInForce,
		b.TP,
		b.SL,
		b.IsLeverage,
		b.TPOderType,
		b.SLOrderType)
}

const (
	BYBIT_BASE_URL_TEST = ""
	BYBIT_BASE_URL_PROD = ""
)

func NewBybitHandler(ctx context.Context, apiKey string, secret string, stage types.Environment, logger *slog.Logger) *BybitHandler {
	handler := &BybitHandler{
		ctx:    ctx,
		logger: logger,
	}
	switch stage {
	case types.PROD:
		handler.client = bybitClib.NewBybitHttpClient(apiKey, secret, bybitClib.WithBaseURL(BYBIT_BASE_URL_PROD))
	case types.TEST:
		handler.client = bybitClib.NewBybitHttpClient(apiKey, secret, bybitClib.WithBaseURL(BYBIT_BASE_URL_TEST))
	}
	return handler
}

func (bh *BybitHandler) Validate() error {
	return nil
}

func (bh *BybitHandler) Process(s Signal) error {
	bybitSignal, ok := s.(BybitSignal)
	if ok {
		bh.logger.Info("[BybitHandler] processing signal", "signal", bybitSignal.String())
	}
	return nil
}
