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
	BYBIT_API_KEY_TEST      = "BYBIT_API_KEY_TEST"
	BYBIT_SECRET_TEST       = "BYBIT_SECRET_TEST"
	BINANCE_API_KEY_TEST    = "BINANCE_API_KEY_TEST"
	BINANCE_SECRET_TEST     = "BINANCE_SECRET_TEST"
	BYBIT_API_KEY_PROD      = "BYBIT_API_KEY_PROD"
	BYBIT_SECRET_PROD       = "BYBIT_SECRET_PROD"
	BINANCE_API_KEY_PROD    = "BINANCE_API_KEY_PROD"
	BINANCE_SECRET_PROD     = "BINANCE_SECRET_PROD"
	BYBIT_BASE_URL_TEST     = "BYBIT_BASE_URL_TEST"
	BINANCE_BASE_URL_TEST   = "BINANCE_BASE_URL_TEST"
	BYBIT_BASE_URL_PROD     = "BYBIT_BASE_URL_PROD"
	BINANCE_BASE_URL_PROD   = "BINANCE_BASE_URL_PROD"
	BYBIT_WS_PRIVATE_PROD   = "BYBIT_WS_PRIVATE_PROD"
	ACTIVE_EXCHANGE_BINANCE = "BINANCE"
	ACTIVE_EXCHANGE_BYBIT   = "BYBIT"
)

type application struct {
	ctx              context.Context
	logger           *slog.Logger
	ExchangeHandler  exchange.ExchangeStrategy
	wsURL            string
	apiKey           string
	secret           string
	CurrByMainOrder  types.ByMainOrder
	CurrBiMainOrders types.BiSubmittedOrders
	BiTPStopPrice    float64
	BiSLStopPrice    float64
	TpOrderId        string
	SlOrderId        string
	ByOrdersChan     chan types.ByMainOrder
	BiOrdersChan     chan types.BiSubmittedOrders
	ActiveExchange   string
}

func NewApplication(ctx context.Context, exchangeName string, stage string, logger *slog.Logger) *application {
	var apiKey string
	var secret string
	var wsUrl string
	bySubmittedOrderChan := make(chan types.ByMainOrder)
	biSubmittedOrderChan := make(chan types.BiSubmittedOrders)
	exchangeName = strings.ToLower(exchangeName)
	stage = strings.ToUpper(stage)
	var environment types.Environment
	var activeExchange string
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
		activeExchange = ACTIVE_EXCHANGE_BINANCE
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
		activeExchange = ACTIVE_EXCHANGE_BYBIT
	default:
		panic("unsupported exchange")
	}

	if apiKey == "" || secret == "" {
		panic("invalid credentials")
	}

	eh, err := exchange.NewExchangeHandler(ctx, exchangeName, apiKey, secret, environment, bySubmittedOrderChan, biSubmittedOrderChan, logger)
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
		ByOrdersChan:    bySubmittedOrderChan,
		BiOrdersChan:    biSubmittedOrderChan,
		ActiveExchange:  activeExchange,
		ExchangeHandler: eh}
}

func (a *application) ListenForByOrderUpdates() {
	for order := range a.ByOrdersChan {
		fmt.Println("got a bybit main order ======>", order)
		a.CurrByMainOrder = order
	}
}

func (a *application) ListenForBiOrderUpdates() {
	for order := range a.BiOrdersChan {
		fmt.Println("got a new binance orders ======>", order)
		a.CurrBiMainOrders = order
	}
}
