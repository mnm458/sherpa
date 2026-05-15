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

type OrderResponse struct {
	Data []OrderUpdate `json:"data"`
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
	c, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		a.logger.Error("failed to connect to Bybit WebSocket", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	a.logger.Info("Bybit WebSocket connected")
	a.onOpen(c)

	ticker := time.NewTicker(20 * time.Second)
	go func() {
		for range ticker.C {
			if err := c.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				a.logger.Error("failed to send ping", "error", err)
			} else {
				a.logger.Debug("ping sent")
			}
		}
	}()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			a.logger.Error("failed to read Bybit WebSocket message", "error", err)
			return
		}
		a.receive(string(message), bybitHandler)
	}
}

func (a *application) onOpen(c *websocket.Conn) {
	a.logger.Info("Bybit WebSocket opened, authenticating")
	a.sendAuth(c)
	a.sendSubscription(c, "order")
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

func (a *application) receive(message string, handler *exchange.BybitHandler) {
	var orderResp OrderResponse
	if err := json.Unmarshal([]byte(message), &orderResp); err != nil {
		a.logger.Error("failed to unmarshal Bybit order update", "error", err)
		return
	}

	for _, order := range orderResp.Data {
		a.logger.Info("Bybit order update",
			"orderID", order.OrderID,
			"status", order.Status,
			"side", order.Side,
			"createType", order.CreateType,
		)

		switch order.CreateType {
		case CREATE_TYPE_TP:
			if order.Status == STATUS_FILLED {
				a.logger.Info("TP filled — initiating re-entry",
					"orderID", order.OrderID,
					"symbol", a.CurrByMainOrder.Symbol,
				)
				handler.PlaceOrder(
					a.CurrByMainOrder.Category,
					a.CurrByMainOrder.Symbol,
					a.CurrByMainOrder.Side,
					a.CurrByMainOrder.OrderType,
					a.CurrByMainOrder.Quantity,
					a.CurrByMainOrder.Price,
					a.CurrByMainOrder.TakeProfit,
					a.CurrByMainOrder.StopLoss,
					a.CurrByMainOrder.Precision)
			}
		case CREATE_TYPE_SL:
			if order.Status == STATUS_TRIGGERED {
				a.logger.Info("stop loss triggered — clearing position state",
					"orderID", order.OrderID,
					"symbol", a.CurrByMainOrder.Symbol,
				)
				a.CurrByMainOrder = types.ByMainOrder{}
			}
		}
	}
}
