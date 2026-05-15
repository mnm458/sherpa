package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/gorilla/websocket"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

func (a *application) SetupWebSockets(ctx context.Context, eh exchange.ExchangeStrategy) error {
	bh, ok := eh.(*exchange.BinanceHandler)
	if !ok {
		return fmt.Errorf("exchange is not a BinanceHandler")
	}
	if a.wsManager == nil {
		a.wsManager = NewWebSocketManager(a.logger)
	}

	//Setup user data strategy
	userDataStrategy := &UserDataStrategy{
		listenKey:        bh.ListenKey,
		refreshListenKey: func() (string, error) { return a.wsBiReissueListenKey(bh) },
		handler:          a.createWSHandler(bh),
		logger:           a.logger,
	}

	return a.wsManager.Connect(ctx, "user-data", userDataStrategy)
}

func (a *application) createWSHandler(bh *exchange.BinanceHandler) func(*futures.WsUserDataEvent) {
	return func(event *futures.WsUserDataEvent) {
		jsonBytes, err := json.MarshalIndent(event, "", "  ")
		if err != nil {
			a.logger.Error("JSON marshal error", "error", err)
			return
		}

		var parsedEvent futures.WsUserDataEvent
		if err := json.Unmarshal(jsonBytes, &parsedEvent); err != nil {
			a.logger.Error("JSON unmarshal error", "error", err)
			return
		}
		a.logOrderUpdate(parsedEvent)
		if parsedEvent.Event == types.LISTEN_KEY_EXPIRED_EVENT {
			a.logger.Info("listen key expired, creating a new one")
			a.wsBiReissueListenKey(bh)
		}
		if a.CurrBiMainOrders.TPOrder != nil && parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status != futures.OrderStatusTypeFilled && parsedEvent.OrderTradeUpdate.ClientOrderID == a.CurrBiMainOrders.TPOrder.ClientOrderID {
			a.shouldProcessReentry = false
		}
		if a.CurrBiMainOrders.MainOrder != nil && a.CurrBiMainOrders.TPOrder != nil && a.CurrBiMainOrders.SLOrder != nil {
			if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderTradeUpdate.ClientOrderID == a.CurrBiMainOrders.TPOrder.ClientOrderID {
				a.logger.Info("TP filled — initiating re-entry", "clientOrderID", a.CurrBiMainOrders.TPOrder.ClientOrderID)
				a.shouldProcessReentry = true
				a.startReentry(bh)
			} else if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderID == a.CurrBiMainOrders.SLOrder.OrderID {
				a.logger.Info("SL triggered — clearing position state", "orderID", parsedEvent.OrderID)
				a.CurrBiMainOrders = types.BiSubmittedOrders{}
			}
		}

	}
}

func (a *application) createPriceWsHandler(bh *exchange.BinanceHandler) func(*futures.WsMarkPriceEvent) {
	return func(event *futures.WsMarkPriceEvent) {
		if a.shouldProcessReentry {
			newPriceStr := event.MarkPrice
			newPrice, err := strconv.ParseFloat(newPriceStr, 64)
			if err != nil {
				a.logger.Error("failed to parse mark price", "error", err)
				return
			}
			a.logger.Debug("--- Price update ---",
				"currentPrice", newPrice,
				"TPThreshold", 0.9995*a.BiTPStopPrice,
				"SLThreshold", a.BiSLStopPrice,
				"TPThresholdUpper", 1.0005*a.BiTPStopPrice,
				"conditionMet",
				(newPrice <= 0.9995*a.BiTPStopPrice && newPrice >= a.BiSLStopPrice) ||
					(newPrice >= 1.0005*a.BiTPStopPrice && newPrice <= a.BiSLStopPrice))

			if (newPrice <= 0.9995*a.BiTPStopPrice && newPrice >= a.BiSLStopPrice) || (newPrice >= 1.0005*a.BiTPStopPrice && newPrice <= a.BiSLStopPrice) {
				totBalance, err := bh.GetAccountBalance()
				if err != nil {
					a.logger.Error("re-entry: failed to get balance", "error", err)
					return
				}
				priceToFloat, _ := strconv.ParseFloat(a.CurrBiMainOrders.MainOrder.Price, 64)
				qty := bh.GetFinalQty(totBalance, a.CurrBiMainOrders.Signal.Leverage, priceToFloat)
				a.logger.Info("re-entry: quantity calculated", "qty", qty)

				cancelErr := bh.CancelAllOpenOrders(a.CurrBiMainOrders.Signal.Symbol)
				if cancelErr != nil {
					a.logger.Error("re-entry: failed to cancel open orders", "error", cancelErr)
				}
				bh.ExecuteBatchOrder(&a.CurrBiMainOrders.Signal, priceToFloat, qty, a.CurrBiMainOrders.StepSize, a.CurrBiMainOrders.TickSize)
				a.shouldProcessReentry = false
				if a.priceStreamCancel != nil {
					a.priceStreamCancel()
				}
			}
		}
	}

}

func (a *application) startReentry(bh *exchange.BinanceHandler) {
	symbol := a.CurrBiMainOrders.MainOrder.Symbol

	// Setup mark price strategy
	markPriceStrategy := &MarkPriceStrategy{
		symbol:  symbol,
		handler: a.createPriceWsHandler(bh),
		logger:  a.logger,
	}

	// Create context with cancel for this connection
	priceCtx, cancel := context.WithCancel(bh.Ctx)
	a.priceStreamCancel = cancel

	// Connect with mark price strategy
	if err := a.wsManager.Connect(priceCtx, "mark-price-"+symbol, markPriceStrategy); err != nil {
		a.logger.Error("failed to connect to mark price WebSocket", "symbol", symbol, "error", err)
	}

	// priceStreamHandler := a.createPriceWsHandler(bh)
	// errHandler := a.reEntryCreateErrorHandler()
	// doneCh, stopCh, err := futures.WsMarkPriceServe(a.CurrBiMainOrders.MainOrder.Symbol, priceStreamHandler, errHandler)
	// _ = doneCh
	// _ = stopCh
	// if err != nil {
	// 	fmt.Println("ERRROR: ", err)
	// }
}

func (a *application) reEntryCreateErrorHandler() func(error) {
	return func(err error) {
		a.logger.Error("re-entry WebSocket error", "error", err)
	}
}
func (a *application) logOrderUpdate(event futures.WsUserDataEvent) {
	var printer struct {
		eventName     string
		orderId       int64
		clientOrderID string
		orderSide     string
		orderType     string
		orderStatus   string
	}

	if event.Event == futures.UserDataEventTypeOrderTradeUpdate {
		printer.eventName = string(event.Event)
		printer.clientOrderID = event.OrderTradeUpdate.ClientOrderID
		printer.orderSide = string(event.Side)
		printer.orderType = string(event.OrderTradeUpdate.Type)
		printer.orderStatus = string(event.OrderTradeUpdate.Status)
		printer.orderId = event.OrderID
		// Option 1: Log individual fields
		a.logger.Info("Binance order update",
			"eventName", printer.eventName,
			"orderID", printer.orderId,
			"clientOrderID", printer.clientOrderID,
			"orderSide", printer.orderSide,
			"orderType", printer.orderType,
			"orderStatus", printer.orderStatus,
		)
	}

}

func (a *application) wsBiReissueListenKey(h *exchange.BinanceHandler) (string, error) {
	listenKey, err := h.Client.NewStartUserStreamService().Do(h.Ctx)
	if err != nil {
		return "", err
	}
	return listenKey, nil
}

type WebSocketStrategy interface {
	GetEndpoint() string
	HandleMessage(message []byte) error
	HandleError(err error)
}

type WebSocketManager struct {
	connections map[string]*websocket.Conn
	strategies  map[string]WebSocketStrategy
	logger      *slog.Logger
	mu          sync.RWMutex
}

func NewWebSocketManager(logger *slog.Logger) *WebSocketManager {
	return &WebSocketManager{
		connections: make(map[string]*websocket.Conn),
		strategies:  make(map[string]WebSocketStrategy),
		logger:      logger,
	}
}

// Connect establishes a WebSocket connection using the given strategy
func (m *WebSocketManager) Connect(ctx context.Context, strategyName string, strategy WebSocketStrategy) error {
	m.mu.Lock()
	m.strategies[strategyName] = strategy
	m.mu.Unlock()

	return m.connectWithRetry(ctx, strategyName)
}

func (m *WebSocketManager) connectWithRetry(ctx context.Context, name string) error {
	m.mu.RLock()
	strategy := m.strategies[name]
	m.mu.RUnlock()

	// Establish connection
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(strategy.GetEndpoint(), nil)
	if err != nil {
		m.logger.Error("WebSocket connection failed", "name", name, "error", err)
		// Retry connection after delay
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				if err := m.connectWithRetry(ctx, name); err != nil {
					m.logger.Error("WebSocket reconnection failed", "name", name, "error", err)
				}
			}
		}()
		return err
	}
	// Set up ping handler
	conn.SetPingHandler(func(data string) error {
		m.logger.Debug("received ping, sending pong", "name", name)
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	m.mu.Lock()
	m.connections[name] = conn
	m.mu.Unlock()

	// Start reading messages
	go m.readMessages(ctx, name, conn)

	return nil
}

func (m *WebSocketManager) readMessages(ctx context.Context, name string, conn *websocket.Conn) {
	defer func() {
		conn.Close()
		m.mu.Lock()
		delete(m.connections, name)
		m.mu.Unlock()
	}()

	m.mu.RLock()
	strategy := m.strategies[name]
	m.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				strategy.HandleError(err)
				// Try to reconnect
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(2 * time.Second):
						m.connectWithRetry(ctx, name)
					}
				}()
				return
			}

			if err := strategy.HandleMessage(message); err != nil {
				m.logger.Error("message handling error", "name", name, "error", err)
				if err.Error() == "listen key refreshed, need to reconnect" {
					// Reconnect with new listen key
					go func() {
						conn.Close()
						m.connectWithRetry(ctx, name)
					}()
					return
				}
			}
		}
	}
}

// Disconnect closes a WebSocket connection
func (m *WebSocketManager) Disconnect(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, exists := m.connections[name]
	if !exists {
		return nil
	}

	err := conn.Close()
	delete(m.connections, name)
	return err
}

// DisconnectAll closes all WebSocket connections
func (m *WebSocketManager) DisconnectAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, conn := range m.connections {
		conn.Close()
		delete(m.connections, name)
	}
}

type UserDataStrategy struct {
	listenKey        string
	refreshListenKey func() (string, error)
	handler          func(*futures.WsUserDataEvent)
	logger           *slog.Logger
}

func (s *UserDataStrategy) GetEndpoint() string {
	return fmt.Sprintf("wss://fstream.binance.com/ws/%s", s.listenKey)
}

func (s *UserDataStrategy) HandleMessage(message []byte) error {
	var event futures.WsUserDataEvent
	if err := json.Unmarshal(message, &event); err != nil {
		return err
	}

	if event.Event == types.LISTEN_KEY_EXPIRED_EVENT {
		s.logger.Info("listen key expired, refreshing")
		newListenKey, err := s.refreshListenKey()
		if err != nil {
			s.logger.Error("failed to refresh listen key", "error", err)
			return err
		}
		s.listenKey = newListenKey
		s.logger.Info("listen key refreshed")
		return fmt.Errorf("listen key refreshed, need to reconnect")
	}
	s.handler(&event)
	return nil
}

func (s *UserDataStrategy) HandleError(err error) {
	s.logger.Error("user data WebSocket error", "error", err)
}

type MarkPriceStrategy struct {
	symbol  string
	handler func(*futures.WsMarkPriceEvent)
	logger  *slog.Logger
}

func (s *MarkPriceStrategy) GetEndpoint() string {
	return fmt.Sprintf("wss://fstream.binance.com/ws/%s@markPrice", strings.ToLower(s.symbol))
}

func (s *MarkPriceStrategy) HandleMessage(message []byte) error {
	var event futures.WsMarkPriceEvent
	if err := json.Unmarshal(message, &event); err != nil {
		return err
	}
	s.handler(&event)
	return nil
}

func (s *MarkPriceStrategy) HandleError(err error) {
	s.logger.Error("mark price WebSocket error", "error", err)
}
