package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
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
	ctx                  context.Context
	logger               *slog.Logger
	ExchangeHandler      exchange.ExchangeStrategy
	wsURL                string
	apiKey               string
	secret               string
	CurrByMainOrder      types.ByMainOrder
	CurrBiMainOrders     types.BiSubmittedOrders
	BiTPStopPrice        float64
	BiSLStopPrice        float64
	TpOrderId            string
	SlOrderId            string
	ByOrdersChan         chan types.ByMainOrder
	BiOrdersChan         chan types.BiSubmittedOrders
	ActiveExchange       string
	wsStopChannels       map[string]chan struct{}
	wsMutex              sync.Mutex
	ExchangeID           int32
	wsManager            *WebSocketManager
	priceStreamCancel    context.CancelFunc
	shouldProcessReentry bool
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
	cfg.Logger.Info("initialising exchange handler", "exchange", cfg.Exchange)
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

	for order := range a.ByOrdersChan {
		a.logger.Info("bybit main order received", "symbol", order.Symbol, "side", order.Side, "qty", order.Quantity, "price", order.Price)
		a.CurrByMainOrder = order
	}

	a.logger.Info("bybit order updates listener stopped")
}

func (a *application) ListenForBiOrderUpdates(ctx context.Context) {
	a.logger.Info("starting binance order updates listener")

	for order := range a.BiOrdersChan {
		a.logger.Info("binance main order received", "symbol", order.Signal.Symbol, "action", order.Signal.Action)
		a.CurrBiMainOrders = order
		tpPrice, _ := strconv.ParseFloat(order.TPOrder.Price, 64)
		slPrice, _ := strconv.ParseFloat(order.SLOrder.Price, 64)
		a.BiTPStopPrice = tpPrice
		a.BiSLStopPrice = slPrice
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
