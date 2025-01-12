package types

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

type OrderStatusChecker interface {
	GetOrder(symbol string, orderId int64) (*Order, error)
}
