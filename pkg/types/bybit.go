package types

import "errors"

type BybitServerResponse struct {
	Result struct {
		List []struct {
			WalletBalance string `json:"walletBalance"`
			LastPrice     string `json:"lastPrice"`
			PriceScale    string `json:"priceScale"`
		} `json:"list"`
	} `json:"result"`
}

type BybitBalanceResponse struct {
	Result struct {
		List []struct {
			Coin []struct {
				WalletBalance string `json:"walletBalance"`
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

type MainOrder struct {
	Category   string
	Symbol     string
	Side       string
	OrderType  string
	Quantity   float64
	Price      float64
	TakeProfit float64
	StopLoss   float64
	Precision  int64
}
