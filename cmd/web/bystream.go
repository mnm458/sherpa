package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

func (a *application) WSConnect(wsUrl string, eh exchange.ExchangeStrategy) {
	bybitHandler, ok := eh.(*exchange.BybitHandler)
	if !ok {
		fmt.Println("FAILED TO CAST TO BYBIT HANDLER")
	}
	address := wsUrl
	c, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer c.Close()

	fmt.Println("Connected.")
	a.onOpen(c)

	ticker := time.NewTicker(20 * time.Second)
	go func() {
		for range ticker.C {
			err := c.WriteMessage(websocket.TextMessage, []byte("ping"))
			if err != nil {
				log.Println("Failed to send ping:", err)
			}
			fmt.Println("Ping sent.")
		}
	}()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("Failed to read message:", err)
			return
		}
		a.receive(string(message), bybitHandler)
	}
}

func (a *application) onOpen(c *websocket.Conn) {
	fmt.Println("Opened.")
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
		log.Println("Failed to marshal auth message:", err)
		return
	}
	err = c.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("Failed to send auth message:", err)
		return
	}
}

func (a *application) sendSubscription(c *websocket.Conn, topic string) {
	subMessage := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{topic},
	}

	message, err := json.Marshal(subMessage)
	if err != nil {
		log.Println("Failed to marshal subscription message:", err)
		return
	}
	err = c.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("Failed to send subscription message:", err)
		return
	}
	fmt.Println("Subscription sent for topic:", topic)
}

func (a *application) receive(message string, handler *exchange.BybitHandler) {
	fmt.Println(strings.Repeat("-", 50))
	fmt.Println("|               RECEIVED ORDER UPDATE                |")
	fmt.Println(strings.Repeat("-", 50))

	var orderResp OrderResponse
	err := json.Unmarshal([]byte(message), &orderResp)
	if err != nil {
		log.Println("Failed to unmarshal order:", err)
		return
	}

	for _, order := range orderResp.Data {
		switch order.CreateType {
		case CREATE_TYPE_TP:
			if order.Status == STATUS_FILLED {
				fmt.Println(strings.Repeat("-", 50))
				fmt.Println("|               INITIATING ORDER REENTRY                |")
				fmt.Println(strings.Repeat("-", 50))
				handler.PlaceOrder(
					a.CurrMainOrder.Category,
					a.CurrMainOrder.Symbol,
					a.CurrMainOrder.Side,
					a.CurrMainOrder.OrderType,
					a.CurrMainOrder.Quantity,
					a.CurrMainOrder.Price,
					a.CurrMainOrder.TakeProfit,
					a.CurrMainOrder.StopLoss,
					a.CurrMainOrder.Precision)
			}
		case CREATE_TYPE_SL:
			if order.Status == STATUS_TRIGGERED {
				fmt.Println(strings.Repeat("-", 50))
				fmt.Println("|               STOP LOSS HIT                |")
				fmt.Println(strings.Repeat("-", 50))
				a.CurrMainOrder = types.MainOrder{}
			}
		}
		fmt.Println(strings.Repeat("-", 50))
		fmt.Printf("| OrderID    | %s\n", order.OrderID)
		fmt.Printf("| Status     | %s\n", order.Status)
		fmt.Printf("| Side       | %s\n", order.Side)
		fmt.Printf("| CreateType | %s\n", order.CreateType)
		fmt.Println(strings.Repeat("-", 50))
		fmt.Println()
	}
}
