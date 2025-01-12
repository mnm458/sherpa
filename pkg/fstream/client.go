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

type MarketStreamHandler struct {
	wsConn         *websocket.Conn
	symbol         string
	isRunning      bool
	logger         *slog.Logger
	pingTicker     *time.Ticker
	connectedSince time.Time
	onIndexPrice   func(symbol string, price float64) // Callback for index price updates
}

func NewMarketStreamHandler(symbol string, logger *slog.Logger, onIndexPrice func(string, float64)) *MarketStreamHandler {
	return &MarketStreamHandler{
		symbol:       strings.ToLower(symbol),
		logger:       logger,
		isRunning:    true,
		onIndexPrice: onIndexPrice,
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

	// Convert index price string to float64
	indexPrice, err := strconv.ParseFloat(data.IndexPrice, 64)
	if err != nil {
		msh.logger.Error("Failed to parse index price", "error", err, "price", data.IndexPrice)
		return
	}

	// Call the callback with the index price
	if msh.onIndexPrice != nil {
		msh.onIndexPrice(data.Symbol, indexPrice)
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
