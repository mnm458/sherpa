package fstream

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mnm458/sherpa/pkg/types"
)

type MarkPriceData struct {
	EventType   string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	MarkPrice   string `json:"p"`
	IndexPrice  string `json:"i"`
	SettlePrice string `json:"P"`
	FundingRate string `json:"r"`
	NextFunding int64  `json:"T"`
}

type ReentryConditions struct {
	stopPrice float64
	side      string // "BUY" or "SELL"
	symbol    string
}

type MarketStreamHandler struct {
	wsConn         *websocket.Conn
	symbol         string
	isRunning      bool
	logger         *slog.Logger
	pingTicker     *time.Ticker
	connectedSince time.Time
	onPrice        func(string, float64)
	onIndexPrice   func(symbol string, price float64) // Callback for index price updates
	ReentryChecker func(price float64, conditions types.ReentryConditions) bool
}

func NewMarketStreamHandler(symbol string, logger *slog.Logger, onPrice func(string, float64)) *MarketStreamHandler {
	return &MarketStreamHandler{
		symbol:    strings.ToLower(symbol),
		logger:    logger,
		isRunning: true,
		onPrice:   onPrice,
		ReentryChecker: func(price float64, conditions types.ReentryConditions) bool {
			if conditions.Side == "BUY" {
				stopCondition := price >= conditions.StopPrice
				tpCondition := price <= conditions.StopPrice*0.9
				return stopCondition && tpCondition
			} else {
				stopCondition := price <= conditions.StopPrice
				tpCondition := price >= conditions.StopPrice*1.1
				return stopCondition && tpCondition
			}
		},
	}
}
func (msh *MarketStreamHandler) connectWebSocket() error {
	// Connect to 1-second mark price stream
	u := url.URL{
		Scheme: "wss",
		Host:   "fstream.binance.com",
		Path:   fmt.Sprintf("/ws/%s@markPrice@1s", msh.symbol),
	}

	c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	msh.logger.Info("[connectWebSocket]", "resp", resp)

	msh.wsConn = c
	msh.connectedSince = time.Now()

	// Start ping ticker
	msh.pingTicker = time.NewTicker(3 * time.Minute)
	go msh.pingHandler()

	return nil
}

func (msh *MarketStreamHandler) pingHandler() {
	for range msh.pingTicker.C {
		if err := msh.wsConn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
			msh.logger.Error("Failed to send ping", "error", err)
			return
		}
	}
}

func (msh *MarketStreamHandler) readMessages() error {
	for {
		_, message, err := msh.wsConn.ReadMessage()
		if err != nil {
			return fmt.Errorf("error reading message: %w", err)
		}

		// Process the message
		msh.handleMessage(message)
	}
}
func (msh *MarketStreamHandler) handleMessage(message []byte) {
	var data MarkPriceData
	if err := json.Unmarshal(message, &data); err != nil {
		msh.logger.Error("Failed to unmarshal message", "error", err, "message", string(message))
		return
	}

	// Convert mark price string to float64
	markPrice, err := strconv.ParseFloat(data.MarkPrice, 64)
	if err != nil {
		msh.logger.Error("Failed to parse mark price", "error", err, "price", data.MarkPrice)
		return
	}

	// Log every price update with debug level
	msh.logger.Debug("Received mark price update",
		"symbol", data.Symbol,
		"price", markPrice,
		"time", time.Unix(0, data.EventTime*int64(time.Millisecond)))

	// Call the callback with the price
	if msh.onPrice != nil {
		msh.onPrice(data.Symbol, markPrice)
	}
}
func (msh *MarketStreamHandler) Start() {
	for msh.isRunning {
		if err := msh.connectWebSocket(); err != nil {
			msh.logger.Error("Failed to connect to WebSocket", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if err := msh.readMessages(); err != nil {
			msh.logger.Error("WebSocket error", "error", err)
			msh.wsConn.Close()
			time.Sleep(5 * time.Second)
		}
	}
}

func (msh *MarketStreamHandler) Stop() {
	msh.isRunning = false
	if msh.pingTicker != nil {
		msh.pingTicker.Stop()
	}
	if msh.wsConn != nil {
		msh.wsConn.Close()
	}
}

func (msh *MarketStreamHandler) CheckReentryConditions(price float64, conditions types.ReentryConditions) bool {
	if msh.ReentryChecker != nil {
		return msh.ReentryChecker(price, conditions)
	}
	return false
}
