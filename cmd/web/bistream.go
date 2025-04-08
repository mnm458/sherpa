package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/gorilla/websocket"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

func (a *application) WSBiConnectTest(ctx context.Context, eh exchange.ExchangeStrategy) error {
	bh, ok := eh.(*exchange.BinanceHandler)
	if !ok {
		return fmt.Errorf("incorrect handler passed for ws connect")
	}
	// Create a cancellable context
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {

		if err := a.wsBiReissueListenKey(bh); err != nil {
			a.logger.Error("Failed to get listen key", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}
		a.logger.Info("Connecting to Binance WebSocket", "listenKey", bh.ListenKey, "time", time.Now())
		wsURL := fmt.Sprintf("wss://fstream.binance.com/ws/%s", bh.ListenKey)

		// Use gorilla/websocket
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		}
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			a.logger.Error("WebSocket connection failed", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}
		conn.SetPingHandler(func(data string) error {
			a.logger.Debug("Received ping, sending pong", "time", time.Now())
			return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
		})
		// Start listen key keepalive
		keepaliveTicker := time.NewTicker(30 * time.Minute)
		defer keepaliveTicker.Stop()

		// Create channel for this connection's lifecycle
		connDone := make(chan struct{})

		go func() {
			defer close(connDone)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					a.logger.Error("WebSocket read error", "error", err)
					return
				}

				// Parse the message
				var event futures.WsUserDataEvent
				if err := json.Unmarshal(message, &event); err != nil {
					a.logger.Error("JSON unmarshal error", "error", err)
					continue
				}

				// Process event in a non-blocking way
				go a.createWSHandler(bh)(&event)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return ctx.Err()
			case <-connDone:
				a.logger.Warn("WebSocket connection closed, reconnecting...")
				time.Sleep(2 * time.Second)
			case <-keepaliveTicker.C:
				a.logger.Debug("Sending listen key keepalive", "time", time.Now())
				if err := bh.Client.NewKeepaliveUserStreamService().ListenKey(bh.ListenKey).Do(streamCtx); err != nil {
					a.logger.Error("Keepalive failed", "error", err)
					conn.Close()
					break
				}
				continue
			}
			break
		}
	}

	// func (a *application) createRawWsHandler(bh *exchange.BinanceHandler) func(*custom.Event){
	// 	return func(event *custom.Event){
	// 		a.logger.Debug("Received raw event", "event", event)
	// 	}
	// }

	// for {
	// 	// Check if parent context is done
	// 	select {
	// 	case <-ctx.Done():
	// 		return ctx.Err()
	// 	default:
	// 	}

	// 	// Get/refresh listen key
	// 	if err := a.wsBiReissueListenKey(bh); err != nil {
	// 		a.logger.Error("Failed to get listen key", "error", err)
	// 		time.Sleep(5 * time.Second)
	// 		continue
	// 	}

	// 	// Connect to WebSocket
	// 	a.logger.Info("Connecting to Binance WebSocket", "listenKey", bh.ListenKey)
	// 	wsURL := fmt.Sprintf("wss://fstream.binance.com/ws/%s", bh.ListenKey)

	// 	// Use gorilla/websocket
	// 	dialer := websocket.Dialer{
	// 		HandshakeTimeout: 10 * time.Second,
	// 	}
	// 	conn, _, err := dialer.Dial(wsURL, nil)
	// 	if err != nil {
	// 		a.logger.Error("WebSocket connection failed", "error", err)
	// 		time.Sleep(5 * time.Second)
	// 		continue
	// 	}

	// 	// Set up proper ping/pong handling
	// 	conn.SetPingHandler(func(data string) error {
	// 		a.logger.Debug("Received ping, sending pong")
	// 		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	// 	})

	// 	// Start listen key keepalive
	// 	keepaliveTicker := time.NewTicker(30 * time.Minute)
	// 	defer keepaliveTicker.Stop()

	// 	// Create channel for this connection's lifecycle
	// 	connDone := make(chan struct{})

	// 	// Start reading messages
	// 	go func() {
	// 		defer close(connDone)
	// 		defer conn.Close()

	// 		for {
	// 			_, message, err := conn.ReadMessage()
	// 			if err != nil {
	// 				a.logger.Error("WebSocket read error", "error", err)
	// 				return
	// 			}

	// 			// Parse the message
	// 			var event futures.WsUserDataEvent
	// 			if err := json.Unmarshal(message, &event); err != nil {
	// 				a.logger.Error("JSON unmarshal error", "error", err)
	// 				continue
	// 			}

	// 			// Process event in a non-blocking way
	// 			go a.createWSHandler(bh)(&event)
	// 		}
	// 	}()

	// 	// Wait for either context cancellation, connection close, or keepalive
	// 	select {
	// 	case <-ctx.Done():
	// 		conn.Close()
	// 		return ctx.Err()

	// 	case <-connDone:
	// 		a.logger.Warn("WebSocket connection closed, reconnecting...")
	// 		time.Sleep(2 * time.Second)

	// 	case <-keepaliveTicker.C:
	// 		a.logger.Debug("Sending listen key keepalive")
	// 		if err := bh.Client.NewKeepaliveUserStreamService().
	// 			ListenKey(bh.ListenKey).
	// 			Do(streamCtx); err != nil {
	// 			a.logger.Error("Keepalive failed", "error", err)
	// 			conn.Close()
	// 			time.Sleep(2 * time.Second)
	// 		}
	// 	}
	// }
}

func (a *application) WSBiConnect(ctx context.Context, eh exchange.ExchangeStrategy) error {
	fmt.Println("Current time:", time.Now())
	bh, ok := eh.(*exchange.BinanceHandler)
	if !ok {
		return fmt.Errorf("incorrect handler passed for ws connect")
	}

	// Create a context that can be cancelled for each connection attempt
	for {
		// Check if main context is done
		select {
		case <-ctx.Done():
			fmt.Println("MAIN CONTEXT DONE")
			return ctx.Err()
		default:
			// Continue with connection attempt
		}

		// Get or refresh listen key
		if err := a.wsBiReissueListenKey(bh); err != nil {
			a.logger.Error("[WSBiConnect] failed to get listen key", "error", err)
			time.Sleep(5 * time.Second) // Wait before retry
			continue
		}

		a.logger.Info("[WSBiConnect] starting websocket connection", "listenKey", bh.ListenKey)

		// Define event handler
		wsHandler := func(event *futures.WsUserDataEvent) {
			a.logger.Debug("[WSBiConnect] received websocket event", "event", event.Event)
			a.createWSHandler(bh)(event)
		}

		// Define error handler
		errHandler := func(err error) {
			fmt.Println("Failure time:", time.Now())
			a.logger.Error("[WSBiConnect] websocket error", "error", err)
			// Error will trigger reconnection via doneCh
		}

		// Start keepalive ticker
		keepaliveTicker := time.NewTicker(30 * time.Minute)
		defer keepaliveTicker.Stop()

		// Establish WebSocket connection
		doneCh, stopCh, err := futures.WsUserDataServe(bh.ListenKey, wsHandler, errHandler)
		if err != nil {
			a.logger.Error("[WSBiConnect] failed to start websocket", "error", err)
			time.Sleep(5 * time.Second) // Wait before retry
			continue
		}

		activeTicker := time.NewTicker(1 * time.Minute)
		defer activeTicker.Stop()

		go func() {
			pingTicker := time.NewTicker(30 * time.Second)
			defer pingTicker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-pingTicker.C:
					// Send a simple application-level request every 30 seconds
					// This is different from the listen key keepalive
					a.logger.Debug("[WSBiConnect] sending application ping")
					err := bh.Client.NewPingService().Do(ctx)
					if err != nil {
						a.logger.Error("[WSBiConnect] application ping failed", "error", err)
					}
				}
			}
		}()

		go func() {
			activityTicker := time.NewTicker(10 * time.Second)
			defer activityTicker.Stop()

			lastActivityTime := time.Now()

			for {
				select {
				case <-ctx.Done():
					return
				case <-activityTicker.C:
					elapsed := time.Since(lastActivityTime)
					a.logger.Debug("[WSBiConnect] connection status",
						"seconds_since_activity", elapsed.Seconds())
					if elapsed > 55*time.Second {
						a.logger.Warn("[WSBiConnect] potential inactivity timeout approaching",
							"seconds_inactive", elapsed.Seconds())
					}
					lastActivityTime = time.Now() // Reset for monitoring purposes
				}
			}
		}()
		// Start a goroutine for keepalive
		keepaliveDone := make(chan struct{})
		go func() {
			defer close(keepaliveDone)
			for {
				select {
				case <-ctx.Done():
					return
				case <-keepaliveTicker.C:
					a.logger.Debug("[WSBiConnect] sending keepalive")
					if err := bh.Client.NewKeepaliveUserStreamService().
						ListenKey(bh.ListenKey).
						Do(ctx); err != nil {
						a.logger.Error("[WSBiConnect] keepalive failed", "error", err)
						// Close the connection to trigger reconnect
						if stopCh != nil {
							close(stopCh)
						}
						return
					}
				}
			}
		}()

		// Wait for connection to close or context to be cancelled
		select {
		case <-ctx.Done():
			if stopCh != nil {
				close(stopCh)
			}
			<-keepaliveDone // Wait for keepalive goroutine to exit
			return ctx.Err()
		case <-doneCh:
			// Connection closed, wait for keepalive to finish
			if stopCh != nil {
				close(stopCh)
			}
			<-keepaliveDone
			a.logger.Warn("[WSBiConnect] connection closed, reconnecting...")
			time.Sleep(2 * time.Second) // Small delay before reconnect
		}
	}
}

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

func (a *application) wsBiReissueListenKey(h *exchange.BinanceHandler) error {
	listenKey, err := h.Client.NewStartUserStreamService().Do(h.Ctx)
	if err != nil {
		return err
	}
	h.ListenKey = listenKey
	return nil
}
