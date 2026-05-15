package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

type OrderUpdate struct {
	OrderID    string `json:"orderId"`
	Status     string `json:"orderStatus"`
	Side       string `json:"side"`
	CreateType string `json:"createType"`
}

// bybitWSMessage is the envelope for every Bybit WebSocket message.
// Op is set for control messages (auth, subscribe, pong).
// Topic is set for data messages (order updates).
type bybitWSMessage struct {
	Op      string          `json:"op"`
	Success bool            `json:"success"`
	RetMsg  string          `json:"ret_msg"`
	Topic   string          `json:"topic"`
	Data    json.RawMessage `json:"data"`
}

const (
	CREATE_TYPE_MAIN = "CreateByUser"
	CREATE_TYPE_TP   = "CreateByTakeProfit"
	CREATE_TYPE_SL   = "CreateByStopLoss"
	STATUS_FILLED    = "Filled"
	STATUS_TRIGGERED = "Triggered"
)

func (a *application) WSByConnect(wsUrl string, eh exchange.ExchangeStrategy) {
	bybitHandler, ok := eh.(*exchange.BybitHandler)
	if !ok {
		a.logger.Error("failed to cast to BybitHandler")
		os.Exit(1)
	}

	for {
		if err := a.bybitDial(wsUrl, bybitHandler); err != nil {
			a.logger.Error("Bybit WebSocket disconnected, retrying in 5s", "error", err)
		}
		time.Sleep(5 * time.Second)
	}
}

// bybitDial establishes one WebSocket session. Returns when the connection closes.
func (a *application) bybitDial(wsUrl string, handler *exchange.BybitHandler) error {
	c, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		a.stateMu.Lock()
		a.wsConnected = false
		a.wsAuthenticated = false
		a.stateMu.Unlock()
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		c.Close()
		a.stateMu.Lock()
		a.wsConnected = false
		a.wsAuthenticated = false
		a.stateMu.Unlock()
		a.logger.Info("Bybit WebSocket connection closed")
	}()

	a.stateMu.Lock()
	a.wsConnected = true
	a.stateMu.Unlock()
	a.logger.Info("Bybit WebSocket connected")

	a.sendAuth(c)
	a.sendSubscription(c, "order")

	// Ping ticker — stopped cleanly when this function returns.
	stopPing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-ticker.C:
				if err := c.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					a.logger.Error("failed to send ping", "error", err)
					return
				}
				a.stateMu.Lock()
				a.wsLastPingAt = time.Now()
				a.stateMu.Unlock()
				a.logger.Debug("ping sent")
			}
		}
	}()
	defer close(stopPing)

	// Set an initial read deadline; reset on every received message.
	c.SetReadDeadline(time.Now().Add(60 * time.Second))

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		// Reset deadline on any activity.
		c.SetReadDeadline(time.Now().Add(60 * time.Second))

		a.stateMu.Lock()
		a.wsLastMsgAt = time.Now()
		a.stateMu.Unlock()

		a.handleBybitMessage(string(message), handler)
	}
}

// handleBybitMessage routes an incoming message based on its type.
func (a *application) handleBybitMessage(message string, handler *exchange.BybitHandler) {
	// Bybit sends a plain "pong" text frame in response to our "ping".
	if message == "pong" {
		return
	}

	var msg bybitWSMessage
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		a.logger.Error("failed to parse Bybit message", "error", err, "raw", message)
		return
	}

	switch {
	case msg.Op == "auth":
		a.stateMu.Lock()
		a.wsAuthenticated = msg.Success
		a.stateMu.Unlock()
		if msg.Success {
			a.logger.Info("Bybit WebSocket authenticated")
		} else {
			a.logger.Error("Bybit WebSocket auth failed", "msg", msg.RetMsg)
		}

	case msg.Op == "subscribe":
		if msg.Success {
			a.logger.Info("Bybit WebSocket subscription confirmed")
		} else {
			a.logger.Error("Bybit WebSocket subscription failed", "msg", msg.RetMsg)
		}

	case msg.Op == "pong":
		// JSON pong frame — nothing to do.

	case msg.Topic == "order":
		a.receive(msg.Data, handler)

	default:
		a.logger.Debug("Bybit unhandled message", "op", msg.Op, "topic", msg.Topic)
	}
}

func (a *application) sendAuth(c *websocket.Conn) {
	expires := time.Now().UnixNano()/1e6 + 10000
	val := fmt.Sprintf("GET/realtime%d", expires)

	mac := hmac.New(sha256.New, []byte(a.secret))
	mac.Write([]byte(val))
	signature := hex.EncodeToString(mac.Sum(nil))

	authMessage := map[string]interface{}{
		"op":   "auth",
		"args": []interface{}{a.apiKey, expires, signature},
	}

	message, err := json.Marshal(authMessage)
	if err != nil {
		a.logger.Error("failed to marshal auth message", "error", err)
		return
	}
	if err = c.WriteMessage(websocket.TextMessage, message); err != nil {
		a.logger.Error("failed to send auth message", "error", err)
	}
}

func (a *application) sendSubscription(c *websocket.Conn, topic string) {
	subMessage := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{topic},
	}

	message, err := json.Marshal(subMessage)
	if err != nil {
		a.logger.Error("failed to marshal subscription message", "error", err)
		return
	}
	if err = c.WriteMessage(websocket.TextMessage, message); err != nil {
		a.logger.Error("failed to send subscription message", "error", err)
		return
	}
	a.logger.Info("Bybit WebSocket subscribed", "topic", topic)
}

func (a *application) receive(data json.RawMessage, handler *exchange.BybitHandler) {
	var orders []OrderUpdate
	if err := json.Unmarshal(data, &orders); err != nil {
		a.logger.Error("failed to unmarshal Bybit order data", "error", err)
		return
	}

	for _, order := range orders {
		a.logger.Info("Bybit order update",
			"orderID", order.OrderID,
			"status", order.Status,
			"side", order.Side,
			"createType", order.CreateType,
		)

		a.stateMu.RLock()
		currOrder := a.CurrByMainOrder
		a.stateMu.RUnlock()

		switch order.CreateType {
		case CREATE_TYPE_TP:
			if order.Status == STATUS_FILLED {
				a.logger.Info("TP filled — initiating re-entry",
					"orderID", order.OrderID,
					"symbol", currOrder.Symbol,
				)
				if err := handler.ReEnter(currOrder); err != nil {
					a.logger.Error("re-entry failed", "error", err, "symbol", currOrder.Symbol)
				}
			}
		case CREATE_TYPE_SL:
			if order.Status == STATUS_TRIGGERED {
				a.logger.Info("stop loss triggered — clearing position state",
					"orderID", order.OrderID,
					"symbol", currOrder.Symbol,
				)
				a.stateMu.Lock()
				a.CurrByMainOrder = types.ByMainOrder{}
				a.stateMu.Unlock()
			}
		}
	}
}
