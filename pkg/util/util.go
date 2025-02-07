package util

import (
	"encoding/json"
	"math"

	"github.com/mnm458/sherpa/pkg/types"
	bybitClib "github.com/wuhewuhe/bybit.go.api"
)

type PositionRiskResp struct {
	Leverage string `json:"leverage"`
}

func ExtractResponse(res *bybitClib.ServerResponse) (*types.BybitServerResponse, error) {
	var serverResp types.BybitServerResponse
	if res.RetCode != 0 || res.RetMsg != "OK" {
		return &serverResp, types.ErrInvalidServerResp
	}
	jsonData, marshErr := json.Marshal(res)
	if marshErr != nil {
		return &serverResp, marshErr
	}

	if err := json.Unmarshal(jsonData, &serverResp); err != nil {
		return &serverResp, err
	}
	return &serverResp, nil
}

func RoundToDecimals(num float64, decimals int64) float64 {
	multiplier := math.Pow10(int(decimals))
	return math.Round(num*multiplier) / multiplier
}
