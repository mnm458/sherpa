package exchange

import (
	"log/slog"

	"github.com/mnm458/sherpa/pkg/types"
	bybitCLib "github.com/wuhewuhe/bybit.go.api"
)

type BybitSignal struct{}

type BybitHandler struct {
	BaseURL string
}

func NewBybitHandler(apiKey string, secret string, stage types.Environment, logger *slog.Logger) *BybitHandler {
	client := bybitCLib.NewBybitHttpClient(apiKey, secret, bybitCLib.WithBaseURL("base"))
	_ = client
	return &BybitHandler{}
}

func (bh *BybitHandler) Validate() error {
	return nil
}

func (bh *BybitHandler) Process(s Signal) error {
	return nil
}
