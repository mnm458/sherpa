package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/mnm458/sherpa/pkg/types"
	bybitClib "github.com/wuhewuhe/bybit.go.api"
)

type BybitHandler struct {
	client *bybitClib.Client
	// websockClient *bybitClib.WebSocket
	ctx    context.Context
	logger *slog.Logger
}

type ServerResponse struct {
	Result struct {
		List []struct {
			TotalAvailableBalance string `json:"totalAvailableBalance"`
			LastPrice             string `json:"lastPrice"`
		} `json:"list"`
	} `json:"result"`
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
	BYBIT_BASE_URL_TEST = "https://api-testnet.bybit.com"
	BYBIT_BASE_URL_PROD = "https://api.bybit.com"
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

func (bh *BybitHandler) Validate(s *BybitSignal) error {
	if s.Symbol == "" || s.Side == "" {
		return errInvalidSignal
	}
	if s.Side != "Buy" && s.Side != "Sell" {
		return errInvalidSide
	}
	return nil
}

func (bh *BybitHandler) Process(s Signal) error {
	bs, ok := s.(BybitSignal)
	if ok {
		bh.logger.Info("[BybitHandler] processing signal",
			"category", bs.Category,
			"symbol", bs.Symbol,
			"side", bs.Side,
			"positionIdx", bs.PositionIdx,
			"orderType", bs.OrderType,
			"quantity", bs.Quantity,
			"price", bs.Price,
			"timeInForce", bs.TimeInForce,
			"tp", bs.TP,
			"sl", bs.SL,
			"isLeverage", bs.IsLeverage,
			"tpOrderType", bs.TPOderType,
			"slOrderType", bs.SLOrderType,
		)
	} else {
		bh.logger.Error("Failed to case signal to Bybit Signal")
	}
	if validateErr := bh.Validate(&bs); validateErr != nil {
		bh.logger.Error("[BybitHandler] Failed to validate signal", "error", validateErr)
		return validateErr
	}
	balance, balanceErr := bh.GetWalletBalance()
	if balanceErr != nil {
		bh.logger.Error("[BybitHandler] Failed to get balance", "error", balanceErr)
		return balanceErr
	}

	price, priceErr := bh.GetCurrPrice(bs.Category, bs.Symbol)
	if priceErr != nil {
		bh.logger.Error("[BybitHandler] Failed to get price", "error", balanceErr)
		return priceErr
	}
	_, _ = balance, price
	return nil
	// qty, qtyErr := bh.calculateQuantity(balance)

}

func (bh *BybitHandler) GetCurrPrice(category string, symbol string) (float64, error) {
	params := map[string]interface{}{
		"category": category,
		"symbol":   symbol,
	}
	res, err := bh.client.NewUtaBybitServiceWithParams(params).GetMarketTickers(bh.ctx)
	if err != nil {
		return 0, err
	}
	if res.RetCode != 0 || res.RetMsg != "OK" {
		return 0, errInvalidServerResp
	}

	jsonData, marshErr := json.Marshal(res)
	if marshErr != nil {
		return 0, err
	}

	var serverResp ServerResponse
	if err := json.Unmarshal(jsonData, &serverResp); err != nil {
		return 0, errPriceRespUnmarshalFailure
	}
	price, priceErr := strconv.ParseFloat(serverResp.Result.List[0].LastPrice, 64)
	if priceErr != nil {
		return 0, errPriceRespUnmarshalFailure
	}
	bh.logger.Info("[BybitHandler] price received", "price", price)
	return price, nil
}

func (bh *BybitHandler) GetWalletBalance() (float64, error) {
	params := map[string]interface{}{"accountType": "UNIFIED", "coin": "USDT"}
	res, err := bh.client.NewUtaBybitServiceWithParams(params).GetAccountWallet(bh.ctx)
	if err != nil {
		return 0, err
	}
	if res.RetCode != 0 || res.RetMsg != "OK" {
		return 0, errInvalidServerResp
	}
	jsonData, err := json.Marshal(res)
	if err != nil {
		return 0, fmt.Errorf("error marshaling response to JSON: %w", err)
	}

	var serverResp ServerResponse
	if err := json.Unmarshal(jsonData, &serverResp); err != nil {
		return 0, errWalletRespUnmarshalFailure
	}

	totalBlanace, err := strconv.ParseFloat(serverResp.Result.List[0].TotalAvailableBalance, 64)
	if err != nil {
		return 0, err
	}
	bh.logger.Info("[BybitHandler] total balance received", "balance", totalBlanace)
	return totalBlanace, nil
}

// func (bh *BybitHandler) placeOrder(s BybitSignal) error {
// 	params := map[string]interface{}{
// 		"category":    s.Category,
// 		"symbol":      s.Symbol,
// 		"side":        s.Side,
// 		"positionIdx": s.PositionIdx,
// 	}
// 	_ = params
// 	return nil
// }
