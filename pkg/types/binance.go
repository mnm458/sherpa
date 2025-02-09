package types

import (
	"github.com/adshao/go-binance/v2/futures"
)

const LISTEN_KEY_EXPIRED_EVENT = "listenKeyExpired"

type Order struct {
	AvgPrice            string `json:"avgPrice"`
	ClientOrderId       string `json:"clientOrderId"`
	CumQuote            string `json:"cumQuote"`
	ExecutedQty         string `json:"executedQty"`
	OrderId             int64  `json:"orderId"`
	OrigQty             string `json:"origQty"`
	OrigType            string `json:"origType"`
	Price               string `json:"price"`
	ReduceOnly          bool   `json:"reduceOnly"`
	Side                string `json:"side"`
	PositionSide        string `json:"positionSide"`
	Status              string `json:"status"`
	StopPrice           string `json:"stopPrice"`
	ClosePosition       bool   `json:"closePosition"`
	Symbol              string `json:"symbol"`
	Time                int64  `json:"time"`
	TimeInForce         string `json:"timeInForce"`
	Type                string `json:"type"`
	ActivatePrice       string `json:"activatePrice"`
	PriceRate           string `json:"priceRate"`
	UpdateTime          int64  `json:"updateTime"`
	WorkingType         string `json:"workingType"`
	PriceProtect        bool   `json:"priceProtect"`
	PriceMatch          string `json:"priceMatch"`
	SelfTradePrevention string `json:"selfTradePreventionMode"`
	GoodTillDate        int64  `json:"goodTillDate"`
}

type OpenOrder struct {
	Symbol        string            // BIGTIMEUSDT
	Side          futures.SideType  // string // SELL
	Type          futures.OrderType // string                   // LIMIT, MARKET
	Quantity      string            // 464
	Price         string            // 0.2151
	StopPrice     string
	WorkingType   futures.WorkingType
	ClosePosition string
	TimeInForce   futures.TimeInForceType
	ReduceOnly    bool
}

type OrderStatusChecker interface {
	GetOrder(symbol string, orderId int64) (*Order, error)
}

type OrderPlacer interface {
	PlaceReentryOrders(symbol string, side string, price float64) error
}

type ReentryConditions struct {
	StopPrice float64
	Side      string
	Symbol    string
}

// MarketStreamChecker interface for checking reentry conditions
type MarketStreamChecker interface {
	CheckReentryConditions(price float64, conditions ReentryConditions) bool
}

type OrderTradeUpdate struct {
	Symbol        string `json:"s"`
	ClientOrderID string `json:"c"`
	Side          string `json:"S"`
	OrderType     string `json:"o"`
	TimeInForce   string `json:"f"`
	Quantity      string `json:"q"`
	Price         string `json:"p"`
	AvgPrice      string `json:"ap"`
	StopPrice     string `json:"sp"`
	CurrentStatus string `json:"X"`
	OrderID       int64  `json:"i"`
	Timestamp     int64  `json:"T"`
	IsReduceOnly  bool   `json:"R"`
	WorkingType   string `json:"wt"`
	PositionSide  string `json:"ps"`
}

type BiSubmittedOrders struct {
	MainOrder *futures.Order
	TPOrder   *futures.Order
	SLOrder   *futures.Order
}
