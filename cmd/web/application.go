package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

const (
	BYBIT_API_KEY_TEST    = "BYBIT_API_KEY_TEST"
	BYBIT_SECRET_TEST     = "BYBIT_SECRET_TEST"
	BINANCE_API_KEY_TEST  = "BINANCE_API_KEY_TEST"
	BINANCE_SECRET_TEST   = "BINANCE_SECRET_TEST"
	BYBIT_API_KEY_PROD    = "BYBIT_API_KEY_PROD"
	BYBIT_SECRET_PROD     = "BYBIT_SECRET_PROD"
	BINANCE_API_KEY_PROD  = "BINANCE_API_KEY_PROD"
	BINANCE_SECRET_PROD   = "BINANCE_SECRET_PROD"
	BYBIT_BASE_URL_TEST   = "BYBIT_BASE_URL_TEST"
	BINANCE_BASE_URL_TEST = "BINANCE_BASE_URL_TEST"
	BYBIT_BASE_URL_PROD   = "BYBIT_BASE_URL_PROD"
	BINANCE_BASE_URL_PROD = "BINANCE_BASE_URL_PROD"
	BYBIT_WS_PRIVATE_PROD = "BYBIT_WS_PRIVATE_PROD"
)

type application struct {
	ctx             context.Context
	logger          *slog.Logger
	ExchangeHandler exchange.ExchangeStrategy
	wsURL           string
	apiKey          string
	secret          string
	CurrMainOrder   types.MainOrder
	TpOrderId       string
	SlOrderId       string
	MainOrderChan   chan types.MainOrder
}

func NewApplication(ctx context.Context, exchangeName string, stage string, logger *slog.Logger) *application {
	var apiKey string
	var secret string
	var wsUrl string
	mainOrderChan := make(chan types.MainOrder)
	exchangeName = strings.ToLower(exchangeName)
	stage = strings.ToUpper(stage)
	var environment types.Environment
	if stage == "TEST" {
		environment = types.TEST
	} else if stage == "PROD" {
		environment = types.PROD
	} else {
		panic("invalid stage environment")
	}

	err := godotenv.Load(".env")
	if err != nil {
		panic("cannot load env file")
	}
	switch exchangeName {
	case "binance":
		switch environment {
		case types.TEST:
			apiKey = os.Getenv(BINANCE_API_KEY_TEST)
			secret = os.Getenv(BINANCE_SECRET_TEST)
		case types.PROD:
			apiKey = os.Getenv(BINANCE_API_KEY_PROD)
			secret = os.Getenv(BINANCE_SECRET_PROD)
		default:
			panic("unsupported stage env")
		}
	case "bybit":
		switch environment {
		case types.TEST:
			apiKey = os.Getenv(BYBIT_API_KEY_TEST)
			secret = os.Getenv(BYBIT_SECRET_TEST)
		case types.PROD:
			apiKey = os.Getenv(BYBIT_API_KEY_PROD)
			secret = os.Getenv(BYBIT_SECRET_PROD)
			wsUrl = os.Getenv(BYBIT_WS_PRIVATE_PROD)
		default:
			panic("unsupported stage env")
		}
	default:
		panic("unsupported exchange")
	}

	if apiKey == "" || secret == "" {
		panic("invalid credentials")
	}

	eh, err := exchange.NewExchangeHandler(ctx, exchangeName, apiKey, secret, environment, mainOrderChan, logger)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	return &application{
		ctx:             ctx,
		logger:          logger,
		wsURL:           wsUrl,
		apiKey:          apiKey,
		secret:          secret,
		MainOrderChan:   mainOrderChan,
		ExchangeHandler: eh}
}

func (a *application) ListenForOrderUpdates() {
	go func() {
		for order := range a.MainOrderChan {
			fmt.Println("got a new main order ======>", order)
			a.CurrMainOrder = order
		}
	}()
}
