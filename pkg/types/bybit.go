package types

import "errors"

type BybitServerResponse struct {
	Result struct {
		List []struct {
			TotalAvailableBalance string `json:"totalAvailableBalance"`
			LastPrice             string `json:"lastPrice"`
			PriceScale            string `json:"priceScale"`
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
