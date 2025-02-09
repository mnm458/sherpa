package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/mnm458/sherpa/pkg/types"
	"github.com/mnm458/sherpa/pkg/util"
	bybitClib "github.com/wuhewuhe/bybit.go.api"
)

type BybitHandler struct {
	client        *bybitClib.Client
	ctx           context.Context
	logger        *slog.Logger
	mainOrderChan chan types.ByMainOrder
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
	Leverage    int64
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

func NewBybitHandler(ctx context.Context, apiKey string, secret string, stage types.Environment, mainOrderChan chan types.ByMainOrder, logger *slog.Logger) *BybitHandler {
	handler := &BybitHandler{
		ctx:           ctx,
		logger:        logger,
		mainOrderChan: mainOrderChan,
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
		bh.logger.Error("[BybitHandler] Failed to get price", "error", priceErr)
		return priceErr
	}

	qty := bh.calculateQuantity(balance, price, int32(bs.Leverage))

	levErr := bh.setLeverage(&bs)
	if levErr != nil {
		return levErr
	}

	finalPrice, tpPrice, slPrice, precision, calcErr := bh.calculateTPSLPrice(price, &bs)
	if calcErr != nil {
		bh.logger.Error("[BybitHandler] Failed calculate final prices", "error", calcErr)
		return calcErr
	}

	orderID, orderErr := bh.PlaceOrder(bs.Category, bs.Symbol, bs.Side, bs.OrderType, qty, finalPrice, tpPrice, slPrice, precision)
	if orderErr != nil {
		return orderErr
	}

	bh.logger.Info("[BybitHandler] Order placed successfull", "orderID", orderID)

	return nil
}

func (bh *BybitHandler) setLeverage(signal *BybitSignal) error {
	leverageStr := strconv.FormatInt(signal.Leverage, 10)
	params := map[string]interface{}{
		"category":     signal.Category,
		"symbol":       signal.Symbol,
		"buyLeverage":  leverageStr,
		"sellLeverage": leverageStr,
	}
	res, err := bh.client.NewUtaBybitServiceWithParams(params).SetPositionLeverage(bh.ctx)
	if err != nil {
		return err
	}

	if res.RetCode != 0 || res.RetMsg != "OK" {
		if res.RetCode == 110043 {
			return nil
		}
		jsonData, _ := json.Marshal(res)

		bh.logger.Error("[BybitHandler] failed to set leverage", "jsondata", string(jsonData))
		return errors.New("failed to set leverage")
	}
	return nil
}

func (bh *BybitHandler) PlaceOrder(category string, symbol string, side string, orderType string, quantity float64, finalPrice float64, tpPrice float64, slPrice float64, precision int64) (string, error) {
	qtyStr := strconv.FormatFloat(quantity, 'f', 3, 64)
	priceStr := strconv.FormatFloat(finalPrice, 'f', int(precision), 64)
	tpStr := strconv.FormatFloat(tpPrice, 'f', int(precision), 64)
	slStr := strconv.FormatFloat(slPrice, 'f', int(precision), 64)
	params := map[string]interface{}{
		"category":   category,
		"symbol":     symbol,
		"side":       side,
		"orderType":  orderType,
		"qty":        qtyStr,
		"price":      priceStr,
		"takeProfit": tpStr,
		"stopLoss":   slStr,
	}
	fmt.Println("PARAMS: ", params)
	res, orderErr := bh.client.NewUtaBybitServiceWithParams(params).PlaceOrder(bh.ctx)
	if orderErr != nil {
		return "", orderErr
	}
	var serverResp types.ByBitOrderResponse
	if res.RetCode != 0 || res.RetMsg != "OK" {
		jsonData, _ := json.Marshal(res)

		bh.logger.Error("[BybitHandler] failed to place order", "jsondata", string(jsonData))
		return "", errors.New("failed to place order")
	}
	jsonData, marshErr := json.Marshal(res)
	if marshErr != nil {
		return "", marshErr
	}

	if unmarshalErr := json.Unmarshal(jsonData, &serverResp); unmarshalErr != nil {
		return "", unmarshalErr
	}
	bh.mainOrderChan <- types.ByMainOrder{
		Category:   category,
		Symbol:     symbol,
		Side:       side,
		OrderType:  orderType,
		Quantity:   quantity,
		Price:      finalPrice,
		TakeProfit: tpPrice,
		StopLoss:   slPrice,
		Precision:  precision,
	}
	return serverResp.Result.OrderId, nil

}

func (bh *BybitHandler) calculateTPSLPrice(price float64, signal *BybitSignal) (float64, float64, float64, int64, error) {
	var tpPrice float64
	var slPrice float64
	switch signal.Side {
	case "Buy":
		tpPrice = price * (1 + signal.TP)
		slPrice = price * (1 - signal.SL)
	case "Sell":
		tpPrice = price * (1 - signal.TP)
		slPrice = price * (1 + signal.SL)
	default:
		return 0, 0, 0, 0, errUnsupportedSide
	}
	params := map[string]interface{}{"category": signal.Category, "symbol": signal.Symbol}
	res, resErr := bh.client.NewClassicalBybitServiceWithParams(params).GetInstrumentInfo(bh.ctx)
	if resErr != nil {
		return 0, 0, 0, 0, resErr
	}
	serverResp, extractErr := util.ExtractResponse(res)
	if extractErr != nil {
		return 0, 0, 0, 0, extractErr
	}
	precision, precErr := strconv.ParseInt(serverResp.Result.List[0].PriceScale, 10, 64)
	if precErr != nil {
		return 0, 0, 0, 0, precErr
	}
	return util.RoundToDecimals(price, precision), util.RoundToDecimals(tpPrice, precision), util.RoundToDecimals(slPrice, precision), precision, nil

}

func (bh *BybitHandler) calculateQuantity(balance float64, price float64, leverage int32) float64 {
	availBalance := EQUITY_PERCENTAGE * balance
	result := math.Floor(((float64(leverage)*availBalance)/price)*1000) / 1000
	return result
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
	serverResp, extractErr := util.ExtractResponse(res)
	if extractErr != nil {
		return 0, extractErr
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
	var serverResp types.BybitBalanceResponse
	if res.RetCode != 0 || res.RetMsg != "OK" {
		return 0, errors.New("failed to get balance")
	}
	jsonData, marshErr := json.Marshal(res)
	if marshErr != nil {
		return 0, marshErr
	}

	if unmarshalErr := json.Unmarshal(jsonData, &serverResp); unmarshalErr != nil {
		return 0, unmarshalErr
	}

	totalBlanace, err := strconv.ParseFloat(serverResp.Result.List[0].Coin[0].WalletBalance, 64)
	if err != nil {
		return 0, err
	}
	bh.logger.Info("[BybitHandler] total balance received", "balance", totalBlanace)
	return totalBlanace, nil
}
