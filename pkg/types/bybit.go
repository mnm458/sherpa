package types

import "errors"

// BybitServerResponse is a generic envelope shared by the market-tickers
// and instrument-info endpoints. Unused fields from a given endpoint are
// simply zero-valued by the JSON decoder.
type BybitServerResponse struct {
	Result struct {
		List []struct {
			LastPrice  string `json:"lastPrice"`
			PriceScale string `json:"priceScale"`
			LotSizeFilter struct {
				QtyStep string `json:"qtyStep"`
			} `json:"lotSizeFilter"`
		} `json:"list"`
	} `json:"result"`
}

type BybitBalanceResponse struct {
	Result struct {
		List []struct {
			Coin []struct {
				WalletBalance       string `json:"walletBalance"`
				AvailableToWithdraw string `json:"availableToWithdraw"`
			} `json:"coin"`
		} `json:"list"`
	} `json:"result"`
}

type ByBitOrderResponse struct {
	Result struct {
		OrderId string `json:"orderId"`
	} `json:"result"`
	Time int64 `json:"time"`
}

var ErrInvalidServerResp = errors.New("invalid wallet balance response")

// ByMainOrder holds all state needed to re-enter the same trade at a fresh price.
type ByMainOrder struct {
	Category    string
	Symbol      string
	Side        string
	OrderType   string
	Quantity    float64
	Price       float64
	TakeProfit  float64
	StopLoss    float64
	Precision   int64
	QtyStep     float64 // lot-size step for quantity precision
	TPPct       float64 // TP as fraction of price (e.g. 0.001 = 0.1%)
	SLPct       float64 // SL as fraction of price
	Leverage    int64
	PositionIdx int32
}
