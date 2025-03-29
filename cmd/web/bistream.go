package main

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

// func (a *application) WSBiConnect(ctx context.Context, eh exchange.ExchangeStrategy) error {
// 	bh, ok := eh.(*exchange.BinanceHandler)
// 	if !ok {
// 		return fmt.Errorf("incorrect handler passed for ws connect")
// 	}

// 	// Start WebSocket connection
// 	doneCh, stopCh, err := a.startWebSocketConnection(bh)
// 	if err != nil {
// 		return fmt.Errorf("failed to start websocket: %w", err)
// 	}
// 	defer close(stopCh)

// 	// Start keepalive service
// 	keepaliveDone := a.startKeepaliveService(ctx, bh)
// 	defer func() {
// 		// Wait for keepalive to finish when we exit
// 		<-keepaliveDone
// 	}()

// 	// Wait for either context cancellation or websocket closure
// 	select {
// 	case <-ctx.Done():
// 		close(stopCh)
// 		return ctx.Err()
// 	case <-doneCh:
// 		return fmt.Errorf("websocket connection closed")
// 	}
// }

func (a *application) WSBiConnect(ctx context.Context, eh exchange.ExchangeStrategy) error {
	bh, ok := eh.(*exchange.BinanceHandler)
	if !ok {
		return fmt.Errorf("incorrect handler passed for ws connect")
	}

	// Store the WebSocket channels at the application level
	var doneCh chan struct{}
	var stopCh chan struct{}
	var wsErr error

	// Use a mutex to protect access to WebSocket channels
	var wsMutex sync.Mutex

	// Create a reconnection function
	reconnect := func() error {
		wsMutex.Lock()
		defer wsMutex.Unlock()

		// Close old channels if they exist
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}

		// Reissue listen key before reconnection
		if err := a.wsBiReissueListenKey(bh); err != nil {
			return fmt.Errorf("failed to reissue listen key: %w", err)
		}

		a.logger.Info("starting new websocket connection", "listenKey", bh.ListenKey)

		// Create WebSocket handler with additional logging
		wsHandler := func(event *futures.WsUserDataEvent) {
			a.logger.Debug("received websocket event", "event", event.Event)
			a.createWSHandler(bh)(event)
		}

		// Create error handler with additional logging
		errHandler := func(err error) {
			a.logger.Error("websocket error", "error", err)
			wsErr = err
			fmt.Println("wsErr: ", wsErr.Error())
		}

		// Start new connection
		newDoneCh, newStopCh, err := futures.WsUserDataServe(bh.ListenKey, wsHandler, errHandler)
		if err != nil {
			return fmt.Errorf("websocket serve error: %w", err)
		}

		doneCh = newDoneCh
		stopCh = newStopCh
		return nil
	}

	// Initial connection
	if err := reconnect(); err != nil {
		return err
	}

	// Start ping ticker (every 30 seconds to prevent timeout)
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	// Start keepalive service with robust error handling (for listen key renewal)
	keepaliveTicker := time.NewTicker(30 * time.Minute) // More frequent than Binance's 60-minute expiry
	defer keepaliveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			wsMutex.Lock()
			if stopCh != nil {
				close(stopCh)
			}
			wsMutex.Unlock()
			return ctx.Err()

		case <-doneCh:
			a.logger.Warn("websocket connection closed, attempting to reconnect")

			// Sleep before reconnection to avoid hammering the server
			time.Sleep(3 * time.Second)

			if err := reconnect(); err != nil {
				return fmt.Errorf("failed to reconnect: %w", err)
			}

		case <-pingTicker.C:
			// This is the ping mechanism to keep the WebSocket connection alive
			a.logger.Debug("sending ping to keep connection alive")

			// The ping is implemented at the WebSocket protocol level
			// Since we don't have direct access to the underlying websocket.Conn in the futures library,
			// we'll use an empty data channel message as a heartbeat
			// This won't actually send data but indicates activity on our connection

			// Check if connection is still active before attempting anything
			wsMutex.Lock()
			if stopCh == nil {
				// Connection is not active, try reconnecting
				wsMutex.Unlock()
				if err := reconnect(); err != nil {
					a.logger.Error("failed to reconnect during ping cycle", "error", err)
				}
				continue
			}
			wsMutex.Unlock()

		case <-keepaliveTicker.C:
			a.logger.Debug("sending keepalive for listen key")

			wsMutex.Lock()
			currentListenKey := bh.ListenKey
			wsMutex.Unlock()

			// Send keepalive without holding the lock
			err := bh.Client.NewKeepaliveUserStreamService().
				ListenKey(currentListenKey).
				Do(ctx)

			if err != nil {
				a.logger.Error("keepalive error", "error", err)

				// If keepalive fails, try reconnecting
				wsMutex.Lock()
				if stopCh != nil {
					close(stopCh)
					stopCh = nil
				}
				wsMutex.Unlock()

				if err := reconnect(); err != nil {
					return fmt.Errorf("failed to reconnect after keepalive failure: %w", err)
				}
			}
		}
	}
}

func (a *application) sendCustomPing(bh *exchange.BinanceHandler) {
	// We could implement a custom ping here if Binance provides an API endpoint for this
	// For now, we'll use the empty cycle in the main loop to indicate connection activity
	a.logger.Debug("ping cycle executed")
}

// Modified function for websocket error handling
func (a *application) createErrorHandler() func(error) {
	return func(err error) {
		a.logger.Error("WebSocket error:", "error", err, "stack", debug.Stack())
	}
}

func (a *application) startWebSocketConnection(bh *exchange.BinanceHandler) (chan struct{}, chan struct{}, error) {
	wsHandler := a.createWSHandler(bh)
	errHandler := a.createErrorHandler()

	doneCh, stopCh, err := futures.WsUserDataServe(bh.ListenKey, wsHandler, errHandler)
	if err != nil {
		return nil, nil, fmt.Errorf("websocket serve error: %w", err)
	}

	return doneCh, stopCh, nil
}

func (a *application) startKeepaliveService(ctx context.Context, bh *exchange.BinanceHandler) chan struct{} {
	ticker := time.NewTicker(50 * time.Minute)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		defer close(done)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := bh.Client.NewKeepaliveUserStreamService().
					ListenKey(bh.ListenKey).
					Do(ctx); err != nil {
					a.logger.Error("keepalive error", "error", err)
				}
			}
		}
	}()

	return done
}

// func (a *application) WSBiConnect(ctx context.Context, eh exchange.ExchangeStrategy) error {
// 	bh, ok := eh.(*exchange.BinanceHandler)
// 	if !ok {
// 		panic("incorrect handler passed for ws connect")
// 	}

// 	// Start WebSocket connection
// 	a.startWebSocketConnection(bh)

// 	// Start keepalive service
// 	a.startKeepaliveService(bh)
// 	return nil
// }

// func (a *application) startWebSocketConnection(bh *exchange.BinanceHandler) {
// 	wsHandler := a.createWSHandler(bh)
// 	errHandler := a.createErrorHandler()

// 	go func() {
// 		_, _, err := futures.WsUserDataServe(bh.ListenKey, wsHandler, errHandler)
// 		if err != nil {
// 			a.logger.Error("WebSocket serve error:", "error", err)
// 		}
// 	}()
// }

func (a *application) createWSHandler(bh *exchange.BinanceHandler) func(*futures.WsUserDataEvent) {
	return func(event *futures.WsUserDataEvent) {
		jsonBytes, err := json.MarshalIndent(event, "", "  ")
		if err != nil {
			a.logger.Error("JSON marshal error:", "error", err)
			return
		}

		var parsedEvent futures.WsUserDataEvent
		if err := json.Unmarshal(jsonBytes, &parsedEvent); err != nil {
			a.logger.Error("JSON unmarshal error:", "error", err)
			return
		}
		a.logOrderUpdate(parsedEvent)
		if parsedEvent.Event == types.LISTEN_KEY_EXPIRED_EVENT {
			a.logger.Info("Listen key expired, creating a new one")
			a.wsBiReissueListenKey(bh)
		}
		if a.CurrBiMainOrders.MainOrder != nil && a.CurrBiMainOrders.TPOrder != nil && a.CurrBiMainOrders.SLOrder != nil {
			fmt.Println("tp order details: ", a.CurrBiMainOrders.TPOrder.ClientOrderID)
			if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderTradeUpdate.ClientOrderID == a.CurrBiMainOrders.TPOrder.ClientOrderID {
				fmt.Println("====reentry starting now====")
				a.startReentry(bh)
			} else if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderID == a.CurrBiMainOrders.SLOrder.OrderID {
				a.CurrBiMainOrders = types.BiSubmittedOrders{}
			}
		}

	}
}

func (a *application) createPriceWsHandler(bh *exchange.BinanceHandler) func(*futures.WsMarkPriceEvent) {
	return func(event *futures.WsMarkPriceEvent) {
		newPriceStr := event.MarkPrice
		newPrice, err := strconv.ParseFloat(newPriceStr, 64)
		if err != nil {
			fmt.Println("PRICE COVNERSION ERR: ", err)
			return
		}
		fmt.Println("Price update ---->>", event.MarkPrice)

		if (newPrice <= 0.9995*a.BiTPStopPrice && newPrice >= a.BiSLStopPrice) || (newPrice >= 1.0005*a.BiTPStopPrice && newPrice <= a.BiSLStopPrice) {
			totBalance, err := bh.GetAccountBalance()
			if err != nil {
				a.logger.Error("Failed to get balance", "error", err)
				return
			}
			priceToFloat, _ := strconv.ParseFloat(a.CurrBiMainOrders.MainOrder.Price, 64)
			qty := bh.GetFinalQty(totBalance, a.CurrBiMainOrders.Signal.Leverage, priceToFloat)
			a.logger.Info("[BinanceHandler] final quantity calculated", "qty", qty)

			cancelErr := bh.CancelAllOpenOrders(a.CurrBiMainOrders.Signal.Symbol)
			if cancelErr != nil {
				a.logger.Error("[BinanceHandler] fialed to cancel open orders", "error", cancelErr)
			}
			bh.ExecuteBatchOrder(&a.CurrBiMainOrders.Signal, priceToFloat, qty, a.CurrBiMainOrders.StepSize, a.CurrBiMainOrders.TickSize)
		}
	}

}

func (a *application) startReentry(bh *exchange.BinanceHandler) {
	priceStreamHandler := a.createPriceWsHandler(bh)
	errHandler := a.reEntryCreateErrorHandler()
	doneCh, stopCh, err := futures.WsMarkPriceServe(a.CurrBiMainOrders.MainOrder.Symbol, priceStreamHandler, errHandler)
	_ = doneCh
	_ = stopCh
	if err != nil {
		fmt.Println("ERRROR: ", err)
	}
}

func (a *application) reEntryCreateErrorHandler() func(error) {
	return func(err error) {
		a.logger.Error("Reentry webSocket error:", "error", err)
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
		a.logger.Info("Received order update:",
			"eventName", printer.eventName,
			"orderID", printer.orderId,
			"clientOrderID", printer.clientOrderID,
			"orderSide", printer.orderSide,
			"orderType", printer.orderType,
			"orderStatus", printer.orderStatus,
		)
	}

}

// func (a *application) startKeepaliveService(bh *exchange.BinanceHandler) {
// 	ticker := time.NewTicker(50 * time.Minute)

// 	go func() {
// 		for range ticker.C {
// 			if err := bh.Client.NewKeepaliveUserStreamService().
// 				ListenKey(bh.ListenKey).
// 				Do(bh.Ctx); err != nil {
// 				a.logger.Error("Keepalive error:", "error", err)
// 			}
// 		}
// 	}()
// }

func (a *application) wsBiReissueListenKey(h *exchange.BinanceHandler) error {
	listenKey, err := h.Client.NewStartUserStreamService().Do(h.Ctx)
	if err != nil {
		return err
	}
	h.ListenKey = listenKey
	return nil
}
