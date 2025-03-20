package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

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
	wsStopChannels   map[string]chan struct{}
	wsMutex          sync.Mutex
	ExchangeID       int32
}

func NewApplication(ctx context.Context, cfg Config) *application {
	var apiKey string
	var secret string
	var wsUrl string
	bySubmittedOrderChan := make(chan types.ByMainOrder)
	biSubmittedOrderChan := make(chan types.BiSubmittedOrders)

	err := godotenv.Load(".env")
	if err != nil {
		panic("cannot load env file")
	}
	fmt.Println("EXCHANFGW", cfg.Exchange)
	switch cfg.Exchange {
	case types.EXCHANGE_BINANCE:
		switch cfg.Environment {
		case types.TEST:
			apiKey = os.Getenv(BINANCE_API_KEY_TEST)
			secret = os.Getenv(BINANCE_SECRET_TEST)
		case types.PROD:
			apiKey = os.Getenv(BINANCE_API_KEY_PROD)
			secret = os.Getenv(BINANCE_SECRET_PROD)
		default:
			panic("unsupported stage env")
		}
	case types.EXCHANGE_BYBIT:
		switch cfg.Environment {
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

	eh, err := exchange.NewExchangeHandler(ctx, cfg.Exchange, apiKey, secret, cfg.Environment, bySubmittedOrderChan, biSubmittedOrderChan, cfg.Logger)
	if err != nil {
		cfg.Logger.Error(err.Error())
		return nil
	}
	return &application{
		ctx:             ctx,
		logger:          cfg.Logger,
		wsURL:           wsUrl,
		apiKey:          apiKey,
		secret:          secret,
		ByOrdersChan:    bySubmittedOrderChan,
		BiOrdersChan:    biSubmittedOrderChan,
		ActiveExchange:  cfg.Exchange,
		ExchangeHandler: eh}
}

func (a *application) ListenForByOrderUpdates(ctx context.Context) {
	a.logger.Info("starting bybit order updates listener")

	// Add a debug message when the function starts
	fmt.Printf("Debug: Starting listener. Channel address: %p\n", a.ByOrdersChan)

	for order := range a.ByOrdersChan {
		fmt.Println("got a bybit main order ======>", order)
		a.CurrByMainOrder = order
	}

	a.logger.Info("bybit order updates listener stopped")
}

func (a *application) ListenForBiOrderUpdates(ctx context.Context) {
	a.logger.Info("starting binance order updates listener")

	for order := range a.BiOrdersChan {
		fmt.Println("got a bybit main order ======>", order)
		a.CurrBiMainOrders = order
	}
	a.logger.Info("binance order updates listener stopped")
}

func (a *application) closeAllWebSockets() {
	a.wsMutex.Lock()
	defer a.wsMutex.Unlock()

	for _, stopCh := range a.wsStopChannels {
		if stopCh != nil {
			close(stopCh)
		}
	}
}
