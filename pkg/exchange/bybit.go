package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/mnm458/sherpa/pkg/types"
	"github.com/mnm458/sherpa/pkg/util"
	bybitClib "github.com/wuhewuhe/bybit.go.api"
)

// instrumentInfo caches the symbol-level constants that don't change at runtime.
type instrumentInfo struct {
	pricePrecision int64
	qtyStep        float64
}

type BybitHandler struct {
	client        *bybitClib.Client
	ctx           context.Context
	logger        *slog.Logger
	mainOrderChan chan types.ByMainOrder
	instrMu       sync.RWMutex
	instrCache    map[string]instrumentInfo
}

type BybitSignal struct {
	Category    string  `json:"category"`
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`
	PositionIdx int32   `json:"position_idx"`
	OrderType   string  `json:"order_type"`
	Quantity    float64 `json:"quantity"`
	Price       float64 `json:"price"`
	TimeInForce string  `json:"time_in_force"`
	TP          float64 `json:"tp"`
	SL          float64 `json:"sl"`
	IsLeverage  int8    `json:"is_leverage"`
	TPOderType  string  `json:"tp_order_type"`
	SLOrderType string  `json:"sl_order_type"`
	Leverage    int64   `json:"leverage"`
}

func (bs BybitSignal) GetSymbol() string {
	return bs.Symbol
}

func (bs BybitSignal) GetLeverage() int64 {
	return bs.Leverage
}

func (b BybitSignal) String() string {
	return fmt.Sprintf("--[Bybit signal]--\nCategory:%s\nSymbol:%s\nSide:%s\nPosition Idx:%d\nOrderType:%s\nQuantity:%.2f\nPrice:%.2f\nTime In Force:%s\nTP:%.4f\nSL:%.4f\nIs Leverage:%d\nTP Order Type:%s\nSL Order Type:%s\n",
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
		instrCache:    make(map[string]instrumentInfo),
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
	if !ok {
		bh.logger.Error("[BybitHandler] failed to cast signal to BybitSignal")
		return errInvalidSignal
	}
	bh.logger.Info("[BybitHandler] processing signal",
		"category", bs.Category,
		"symbol", bs.Symbol,
		"side", bs.Side,
		"positionIdx", bs.PositionIdx,
		"orderType", bs.OrderType,
		"timeInForce", bs.TimeInForce,
		"tp", bs.TP,
		"sl", bs.SL,
		"leverage", bs.Leverage,
	)

	// 1. Validate signal fields.
	if err := bh.Validate(&bs); err != nil {
		bh.logger.Error("[BybitHandler] signal validation failed", "error", err)
		return err
	}

	// 2. Set leverage BEFORE calculating quantity (C-4) so the margin
	// requirement is correct when we size the position.
	if err := bh.setLeverage(&bs); err != nil {
		return err
	}

	// 3. Get available balance (H-1: use availableToWithdraw, not total equity).
	balance, err := bh.GetWalletBalance()
	if err != nil {
		bh.logger.Error("[BybitHandler] failed to get balance", "error", err)
		return err
	}
	// 4. Guard: reject if there is nothing to trade with (C-5).
	if balance <= 0 {
		return errors.New("insufficient available balance")
	}

	// 5. Get current market price.
	price, err := bh.GetCurrPrice(bs.Category, bs.Symbol)
	if err != nil {
		bh.logger.Error("[BybitHandler] failed to get price", "error", err)
		return err
	}

	// 6. Calculate raw quantity.
	qty := bh.calculateQuantity(balance, price, int32(bs.Leverage))

	// 7. Fetch (or reuse cached) instrument info for price precision and step size (H-6).
	info, err := bh.getOrCacheInstrumentInfo(bs.Category, bs.Symbol)
	if err != nil {
		bh.logger.Error("[BybitHandler] failed to get instrument info", "error", err)
		return err
	}

	// 8. Round quantity DOWN to the nearest step size (H-3).
	qty = roundToStep(qty, info.qtyStep)
	if qty <= 0 {
		return errors.New("calculated quantity rounds to zero — check balance and leverage")
	}

	// 9. Round TP/SL prices.
	finalPrice, tpPrice, slPrice := bh.calcPrices(price, &bs, info.pricePrecision)

	// 10. Place the order.
	order := types.ByMainOrder{
		Category:    bs.Category,
		Symbol:      bs.Symbol,
		Side:        bs.Side,
		OrderType:   bs.OrderType,
		Quantity:    qty,
		Price:       finalPrice,
		TakeProfit:  tpPrice,
		StopLoss:    slPrice,
		Precision:   info.pricePrecision,
		QtyStep:     info.qtyStep,
		TPPct:       bs.TP,
		SLPct:       bs.SL,
		Leverage:    bs.Leverage,
		PositionIdx: bs.PositionIdx,
	}
	orderID, err := bh.placeOrder(order)
	if err != nil {
		return err
	}

	bh.logger.Info("[BybitHandler] order placed successfully", "orderID", orderID)
	return nil
}

// ReEnter places a fresh order using stored TP/SL percentages and leverage but
// fetches a live price and recalculates quantity from the current balance (C-2, C-3).
func (bh *BybitHandler) ReEnter(currOrder types.ByMainOrder) error {
	bh.logger.Info("[BybitHandler] initiating re-entry", "symbol", currOrder.Symbol, "side", currOrder.Side)

	price, err := bh.GetCurrPrice(currOrder.Category, currOrder.Symbol)
	if err != nil {
		return fmt.Errorf("re-entry: price fetch failed: %w", err)
	}

	balance, err := bh.GetWalletBalance()
	if err != nil {
		return fmt.Errorf("re-entry: balance fetch failed: %w", err)
	}
	if balance <= 0 {
		return errors.New("re-entry: zero available balance")
	}

	qty := bh.calculateQuantity(balance, price, int32(currOrder.Leverage))
	qty = roundToStep(qty, currOrder.QtyStep)
	if qty <= 0 {
		return errors.New("re-entry: quantity rounds to zero")
	}

	var tp, sl float64
	switch currOrder.Side {
	case "Buy":
		tp = price * (1 + currOrder.TPPct)
		sl = price * (1 - currOrder.SLPct)
	case "Sell":
		tp = price * (1 - currOrder.TPPct)
		sl = price * (1 + currOrder.SLPct)
	default:
		return errUnsupportedSide
	}

	order := types.ByMainOrder{
		Category:    currOrder.Category,
		Symbol:      currOrder.Symbol,
		Side:        currOrder.Side,
		OrderType:   currOrder.OrderType,
		Quantity:    qty,
		Price:       util.RoundToDecimals(price, currOrder.Precision),
		TakeProfit:  util.RoundToDecimals(tp, currOrder.Precision),
		StopLoss:    util.RoundToDecimals(sl, currOrder.Precision),
		Precision:   currOrder.Precision,
		QtyStep:     currOrder.QtyStep,
		TPPct:       currOrder.TPPct,
		SLPct:       currOrder.SLPct,
		Leverage:    currOrder.Leverage,
		PositionIdx: currOrder.PositionIdx,
	}

	orderID, err := bh.placeOrder(order)
	if err != nil {
		return fmt.Errorf("re-entry: order placement failed: %w", err)
	}

	bh.logger.Info("[BybitHandler] re-entry order placed",
		"orderID", orderID,
		"symbol", order.Symbol,
		"side", order.Side,
		"price", order.Price,
		"qty", order.Quantity,
	)
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
		// 110043 = leverage not modified (already set to this value) — not an error (M-2).
		if res.RetCode == 110043 {
			bh.logger.Debug("[BybitHandler] leverage already set, skipping", "leverage", signal.Leverage)
			return nil
		}
		jsonData, _ := json.Marshal(res)
		bh.logger.Error("[BybitHandler] failed to set leverage", "jsondata", string(jsonData))
		return errors.New("failed to set leverage")
	}
	return nil
}

// placeOrder formats and submits a single order then writes the complete
// ByMainOrder (with all re-entry fields populated) to the state channel.
func (bh *BybitHandler) placeOrder(order types.ByMainOrder) (string, error) {
	qtyDecimals := countDecimals(order.QtyStep)
	qtyStr := strconv.FormatFloat(order.Quantity, 'f', qtyDecimals, 64)
	priceStr := strconv.FormatFloat(order.Price, 'f', int(order.Precision), 64)
	tpStr := strconv.FormatFloat(order.TakeProfit, 'f', int(order.Precision), 64)
	slStr := strconv.FormatFloat(order.StopLoss, 'f', int(order.Precision), 64)

	params := map[string]interface{}{
		"category":    order.Category,
		"symbol":      order.Symbol,
		"side":        order.Side,
		"orderType":   order.OrderType,
		"qty":         qtyStr,
		"price":       priceStr,
		"takeProfit":  tpStr,
		"stopLoss":    slStr,
		"positionIdx": order.PositionIdx, // H-2
		"timeInForce": "GTC",             // H-4
	}
	bh.logger.Debug("[BybitHandler] placing order",
		"symbol", order.Symbol,
		"side", order.Side,
		"qty", qtyStr,
		"price", priceStr,
		"tp", tpStr,
		"sl", slStr,
	)

	res, err := bh.client.NewUtaBybitServiceWithParams(params).PlaceOrder(bh.ctx)
	if err != nil {
		return "", err
	}
	if res.RetCode != 0 || res.RetMsg != "OK" {
		jsonData, _ := json.Marshal(res)
		bh.logger.Error("[BybitHandler] failed to place order", "jsondata", string(jsonData))
		return "", errors.New("failed to place order")
	}

	jsonData, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	var serverResp types.ByBitOrderResponse
	if err := json.Unmarshal(jsonData, &serverResp); err != nil {
		return "", err
	}

	bh.logger.Debug("[BybitHandler] sending order to channel", "symbol", order.Symbol, "side", order.Side)
	bh.mainOrderChan <- order
	return serverResp.Result.OrderId, nil
}

// calcPrices computes rounded entry, TP and SL prices from a live price and
// the signal's fractional TP/SL offsets. No API calls.
func (bh *BybitHandler) calcPrices(price float64, signal *BybitSignal, precision int64) (float64, float64, float64) {
	var tp, sl float64
	switch signal.Side {
	case "Buy":
		tp = price * (1 + signal.TP)
		sl = price * (1 - signal.SL)
	case "Sell":
		tp = price * (1 - signal.TP)
		sl = price * (1 + signal.SL)
	}
	return util.RoundToDecimals(price, precision),
		util.RoundToDecimals(tp, precision),
		util.RoundToDecimals(sl, precision)
}

// getOrCacheInstrumentInfo returns instrument info for a symbol, fetching from
// the API only on the first call per symbol (H-6).
func (bh *BybitHandler) getOrCacheInstrumentInfo(category, symbol string) (instrumentInfo, error) {
	bh.instrMu.RLock()
	if info, ok := bh.instrCache[symbol]; ok {
		bh.instrMu.RUnlock()
		return info, nil
	}
	bh.instrMu.RUnlock()

	params := map[string]interface{}{"category": category, "symbol": symbol}
	res, err := bh.client.NewClassicalBybitServiceWithParams(params).GetInstrumentInfo(bh.ctx)
	if err != nil {
		return instrumentInfo{}, err
	}
	serverResp, err := util.ExtractResponse(res)
	if err != nil {
		return instrumentInfo{}, err
	}

	precision, err := strconv.ParseInt(serverResp.Result.List[0].PriceScale, 10, 64)
	if err != nil {
		return instrumentInfo{}, fmt.Errorf("parse priceScale: %w", err)
	}

	qtyStep, err := strconv.ParseFloat(serverResp.Result.List[0].LotSizeFilter.QtyStep, 64)
	if err != nil {
		return instrumentInfo{}, fmt.Errorf("parse qtyStep: %w", err)
	}

	info := instrumentInfo{pricePrecision: precision, qtyStep: qtyStep}

	bh.instrMu.Lock()
	bh.instrCache[symbol] = info
	bh.instrMu.Unlock()

	bh.logger.Info("[BybitHandler] instrument info cached",
		"symbol", symbol,
		"pricePrecision", precision,
		"qtyStep", qtyStep,
	)
	return info, nil
}

func (bh *BybitHandler) calculateQuantity(balance float64, price float64, leverage int32) float64 {
	availBalance := EQUITY_PERCENTAGE * balance
	return math.Floor(((float64(leverage)*availBalance)/price)*1000) / 1000
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
	serverResp, err := util.ExtractResponse(res)
	if err != nil {
		return 0, err
	}

	price, err := strconv.ParseFloat(serverResp.Result.List[0].LastPrice, 64)
	if err != nil {
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
		return 0, errors.New("failed to get balance")
	}

	var serverResp types.BybitBalanceResponse
	jsonData, err := json.Marshal(res)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(jsonData, &serverResp); err != nil {
		return 0, err
	}

	// Use availableToWithdraw (available margin) in preference to walletBalance
	// (total equity including unrealised PnL) — H-1.
	// Bybit UTA omits availableToWithdraw ("") when there are no open positions;
	// in that case walletBalance equals the tradeable amount, so use it as a fallback.
	availStr := serverResp.Result.List[0].Coin[0].AvailableToWithdraw
	if availStr == "" {
		availStr = serverResp.Result.List[0].Coin[0].WalletBalance
	}
	if availStr == "" {
		availStr = "0"
	}
	available, err := strconv.ParseFloat(availStr, 64)
	if err != nil {
		return 0, fmt.Errorf("parse availableToWithdraw: %w", err)
	}
	bh.logger.Info("[BybitHandler] available balance", "balance", available)
	return available, nil
}

// countDecimals returns the number of decimal places in f (e.g. 0.001 → 3).
func countDecimals(f float64) int {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		return len(s) - dot - 1
	}
	return 0
}

// roundToStep floors qty to the nearest multiple of step (lot-size rounding).
func roundToStep(qty, step float64) float64 {
	if step <= 0 {
		return qty
	}
	return math.Floor(qty/step) * step
}
