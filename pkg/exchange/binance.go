package exchange

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	BASE = "https://testnet.binancefuture.com"
)

type BinanceSignal struct {
	Symbol   string
	Type     string
	Action   string
	Leverage int64
	TP       float64
	SL       float64
}
type ReentryState struct {
	Symbol       string
	EntryPrice   float64
	Leverage     int64
	TP           float64
	SL           float64
	IsActive     bool
	OriginalSide string
}

type WSMessage struct {
	Stream string `json:"stream"`
	Data   struct {
		E string `json:"e"` // Event type
		S string `json:"s"` // Symbol
		P string `json:"p"` // Price
		T int64  `json:"T"` // Trade time
	} `json:"data"`
}

type BinanceAPIError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type PriceResp struct {
	Price float64 `json:"price"`
}

type BalanceResp struct {
	Asset   string `json:"asset"`
	Balance string `json:"balance"`
}

type USDTBalance struct {
	USDTBalance float64
}

func (bs BinanceSignal) GetType() string {
	return bs.Type
}

func (bs BinanceSignal) GetAction() string {
	return bs.Action
}
func (bs BinanceSignal) GetSymbol() string {
	return bs.Symbol
}

func (bs BinanceSignal) GetLeverage() int64 {
	return bs.Leverage
}

type BinanceHandler struct {
	apiKey         string
	secret         string
	client         *http.Client
	wsConn         *websocket.Conn
	reentryMutex   sync.Mutex
	reentryState   map[string]*ReentryState
	isRunning      bool
	doneCh         chan struct{}
	pingTicker     *time.Ticker
	lastPongTime   time.Time
	authenticated  bool
	connectedSince time.Time
}

func NewBinanceHandler(apiKey string, secret string) *BinanceHandler {
	return &BinanceHandler{
		apiKey:       apiKey,
		secret:       secret,
		client:       &http.Client{},
		reentryState: make(map[string]*ReentryState),
		doneCh:       make(chan struct{}),
	}
}

func (bh *BinanceHandler) Process(s Signal) error {
	binanceSignal, ok := s.(BinanceSignal)
	if !ok {
		return errInvalidSignal
	}
	if err := bh.Validate(&binanceSignal); err != nil {
		return err
	}

	price, err := bh.FetchCurrPrice(s.GetSymbol())
	if err != nil {
		return err
	}

	totBalance, err := bh.GetAccountBalance()
	if err != nil {
		return err
	}
	fmt.Println("TOTAL BALANCE: ", totBalance)
	fmt.Println("Price", price)
	availBalance := EQUITY_PERCENTAGE * totBalance
	fmt.Println("Avail BALANCE: ", availBalance)
	leverage := float64(s.GetLeverage())
	fmt.Println("leverage:", leverage)
	quantity := (availBalance * leverage) / price

	fmt.Println("QUANTITY before", quantity)
	validatedQuantity, err := bh.ValidateQuantity(binanceSignal.Symbol, quantity)
	if err != nil {
		return err
	}
	fmt.Println("QUANTITY after", validatedQuantity)

	err = bh.CancelAllOpenOrders(binanceSignal.Symbol)
	if err != nil {
		return err
	}

	err = bh.ExecuteMainOrder(&binanceSignal, price, validatedQuantity)
	if err != nil {
		return err
	}

	err = bh.ExecuteTPOrder(&binanceSignal, price, validatedQuantity)
	if err != nil {
		return err
	}

	err = bh.ExecuteSLOrder(&binanceSignal, price, validatedQuantity)
	if err != nil {
		return err
	}

	err = bh.StartReentryMonitoring(binanceSignal.Symbol, price, binanceSignal.Leverage, binanceSignal.TP, binanceSignal.SL, binanceSignal.Action)
	if err != nil {
		log.Printf("Failed to start re-entry monitoring for %s: %v", binanceSignal.Symbol, err)
		// Consider whether you want to return this error or just log it
		// return err
	}

	// Set up a timeout for the re-entry monitoring
	go func() {
		// Adjust the timeout duration as needed
		time.Sleep(24 * time.Hour)
		bh.StopReentryMonitoring(binanceSignal.Symbol)
		log.Printf("Re-entry monitoring for %s stopped after timeout", binanceSignal.Symbol)
	}()

	return nil

	// if err = bh.ExecuteTPOrder(&binanceSignal, price, quantity); err != nil {
	// 	return err
	// }

	// if err = bh.ExecuteSLOrder(&binanceSignal, price, quantity); err != nil {
	// 	return err
	// }
	// // if err := bh.PrepareOrder(&binanceSignal); err != nil {
	// // 	return err
	// // }

	// if err = bh.ExecuteOrder(); err != nil {

	// 	return err
	// }

}
func (bh *BinanceHandler) sendRequest(method, endpoint string, params url.Values, needsAuth bool) ([]byte, error) {
	fullURL := BASE + endpoint

	if needsAuth {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		params.Set("timestamp", timestamp)

		// Generate signature from params without including the signature itself
		signature := bh.generateSignature(params.Encode())

		// Add signature to params after generating the signature
		params.Set("signature", signature)
	}

	var req *http.Request
	var err error

	switch method {
	case "GET":
		fullURL += "?" + params.Encode()
		req, err = http.NewRequest(method, fullURL, nil)
	case "POST", "DELETE":
		req, err = http.NewRequest(method, fullURL, strings.NewReader(params.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", method)
	}

	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	if needsAuth {
		req.Header.Set("X-MBX-APIKEY", bh.apiKey)
	}

	resp, err := bh.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiError BinanceAPIError
		if err := json.Unmarshal(body, &apiError); err == nil {
			return nil, fmt.Errorf("binance API error: %d - %s", apiError.Code, apiError.Msg)
		}
		return nil, fmt.Errorf("HTTP error: %d - %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func (bh *BinanceHandler) Validate(s *BinanceSignal) error {
	err := bh.handleLeverage(s.GetSymbol(), s.GetLeverage())
	if err != nil {
		return err
	}
	return nil
}

// handleLeverage returns an error. This method set the leverage for the account. It is not order specific
func (bh *BinanceHandler) handleLeverage(symbol string, leverage int64) error {
	params := url.Values{}
	params.Add("symbol", symbol)
	params.Add("leverage", strconv.FormatInt(leverage, 10))

	body, err := bh.sendRequest("POST", "/fapi/v1/leverage", params, true)
	if err != nil {
		return err
	}

	fmt.Println("Leverage Response:", string(body))
	return nil
}

func (bh *BinanceHandler) generateSignature(queryString string) string {
	h := hmac.New(sha256.New, []byte(bh.secret))
	h.Write([]byte(queryString))
	return hex.EncodeToString(h.Sum(nil))
}

func (bh *BinanceHandler) FetchCurrPrice(symbol string) (float64, error) {
	params := url.Values{}
	params.Add("symbol", symbol)

	body, err := bh.sendRequest("GET", "/fapi/v1/ticker/price", params, false)
	if err != nil {
		return 0, err
	}

	var priceResp struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
		Time   int64  `json:"time"`
	}

	err = json.Unmarshal(body, &priceResp)
	if err != nil {
		return 0, fmt.Errorf("error unmarshaling response for CP: %v", err)
	}

	price, err := strconv.ParseFloat(priceResp.Price, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing price: %v", err)
	}

	return price, nil
}

func (bh *BinanceHandler) ExecuteMainOrder(s *BinanceSignal, price float64, quantity string) error {
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", s.Action)
	params.Add("type", "LIMIT")
	params.Add("timeInForce", "GTC")
	params.Add("quantity", quantity)
	params.Add("price", strconv.FormatFloat(price, 'f', 2, 64))

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := bh.generateSignature(queryString)

	// Construct the full URL with the query string and signature
	fullURL := BASE + endpoint

	reqBody := queryString + "&signature=" + signature

	req, err := http.NewRequest("POST", fullURL, strings.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("error generating request: %v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := bh.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	// Print the response
	fmt.Println("Main Order Response:", string(body))
	return nil
}

func (bh *BinanceHandler) GetAccountBalance() (float64, error) {
	endpoint := "/fapi/v2/balance"
	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())

	// Create the query string
	queryString := fmt.Sprintf("timestamp=%s", timestamp)

	// Create the signature
	signature := bh.generateSignature(queryString)

	// Construct the full URL
	fullURL := fmt.Sprintf("%s%s?%s&signature=%s", BASE, endpoint, queryString, signature)

	// Create and send the request
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("X-MBX-APIKEY", bh.apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Read and return the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading response: %w", err)
	}

	usdtBalance, err := GetUSDTBalance(body)
	if err != nil {
		return 0, err
	}

	fmt.Println("USDT BALANCE: ", usdtBalance)

	return usdtBalance, nil
}

func (u *USDTBalance) UnmarshalJSON(data []byte) error {
	var balances []BalanceResp
	if err := json.Unmarshal(data, &balances); err != nil {
		return err
	}

	for _, balance := range balances {
		if balance.Asset == "USDT" {
			val, err := strconv.ParseFloat(balance.Balance, 64)
			if err != nil {
				return err
			}
			u.USDTBalance = val
			return nil
		}
	}

	return fmt.Errorf("USDT balance not found")
}

// In your main function or wherever you're handling the response:
func GetUSDTBalance(body []byte) (float64, error) {
	var usdtBalance USDTBalance
	err := json.Unmarshal(body, &usdtBalance)
	if err != nil {
		return 0, fmt.Errorf("error unmarshaling USDT balance: %w", err)
	}

	return usdtBalance.USDTBalance, nil
}

func (bh *BinanceHandler) ExecuteTPOrder(s *BinanceSignal, price float64, quantity string) error {
	var tpPrice float64
	if s.Action == "BUY" {
		tpPrice = (s.TP + 1.0) * price
	} else {
		tpPrice = (1.0 - s.TP) * price
	}
	var tpAction string
	switch s.Action {
	case "BUY":
		tpAction = "SELL"
	case "SELL":
		tpAction = "BUY"
	}
	fmt.Println("TP PRICE: ", tpPrice, "after conv: ", strconv.FormatFloat(tpPrice, 'f', 1, 64))
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", tpAction)
	params.Add("type", "TAKE_PROFIT")
	params.Add("timeInForce", "GTC")
	params.Add("reduceOnly", strconv.FormatBool(true))
	params.Add("quantity", quantity)
	params.Add("price", strconv.FormatFloat(tpPrice, 'f', 1, 64))
	params.Add("stopPrice", strconv.FormatFloat(tpPrice, 'f', 1, 64))

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := bh.generateSignature(queryString)

	// Construct the full URL with the query string and signature
	fullURL := BASE + endpoint

	reqBody := queryString + "&signature=" + signature

	req, err := http.NewRequest("POST", fullURL, strings.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("error generating request: %v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := bh.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	// Print the response
	fmt.Println("TP Order Response:", string(body))
	return nil
}

func (bh *BinanceHandler) ExecuteSLOrder(s *BinanceSignal, price float64, quantity string) error {
	var slPrice float64
	if s.Action == "BUY" {
		slPrice = (1.0 - s.TP) * price
	} else {
		slPrice = (s.TP + 1.0) * price
	}
	var slAction string
	switch s.Action {
	case "BUY":
		slAction = "SELL"
	case "SELL":
		slAction = "BUY"
	}
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", slAction)
	params.Add("timeInForce", "GTC")
	params.Add("type", "STOP")
	params.Add("reduceOnly", strconv.FormatBool(true))
	params.Add("quantity", quantity)
	params.Add("price", strconv.FormatFloat(slPrice, 'f', 1, 64))
	params.Add("stopPrice", strconv.FormatFloat(slPrice, 'f', 1, 64))

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := bh.generateSignature(queryString)

	// Construct the full URL with the query string and signature
	fullURL := BASE + endpoint

	reqBody := queryString + "&signature=" + signature

	req, err := http.NewRequest("POST", fullURL, strings.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("error generating request: %v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := bh.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	// Print the response
	fmt.Println("SL Order Response:", string(body))
	return nil
}

func (bh *BinanceHandler) ValidateQuantity(symbol string, quantity float64) (string, error) {
	endpoint := "/fapi/v1/exchangeInfo"

	// Send request to get exchange info
	body, err := bh.sendRequest("GET", endpoint, nil, false)
	if err != nil {
		return "", fmt.Errorf("error fetching exchange info: %w", err)
	}

	// Parse the response
	var exchangeInfo struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Filters []struct {
				FilterType string `json:"filterType"`
				MinQty     string `json:"minQty"`
				MaxQty     string `json:"maxQty"`
				StepSize   string `json:"stepSize"`
				TickSize   string `json:"tickSize"`
			} `json:"filters"`
		} `json:"symbols"`
	}

	if err := json.Unmarshal(body, &exchangeInfo); err != nil {
		return "", fmt.Errorf("error unmarshaling exchange info: %w", err)
	}

	// Find the symbol and its lot size filter
	var lotSizeFilter struct {
		MinQty   float64
		MaxQty   float64
		StepSize float64
	}

	for _, s := range exchangeInfo.Symbols {
		if s.Symbol == symbol {

			for _, filter := range s.Filters {
				fmt.Println("TICK SIZE IS: ", filter.TickSize)
				if filter.FilterType == "LOT_SIZE" {
					lotSizeFilter.MinQty, _ = strconv.ParseFloat(filter.MinQty, 64)
					lotSizeFilter.MaxQty, _ = strconv.ParseFloat(filter.MaxQty, 64)
					lotSizeFilter.StepSize, _ = strconv.ParseFloat(filter.StepSize, 64)
					break
				}
			}
			break
		}
	}

	round := func(x, unit float64) float64 {
		return math.Round(x/unit) * unit
	}

	// Validate the quantity
	if quantity < lotSizeFilter.MinQty {
		return "", fmt.Errorf("quantity %f is less than minimum quantity %f", quantity, lotSizeFilter.MinQty)
	}
	if quantity > lotSizeFilter.MaxQty {
		return "", fmt.Errorf("quantity %f is greater than maximum quantity %f", quantity, lotSizeFilter.MaxQty)
	}

	// Adjust quantity to valid step size
	validatedQuantity := round(quantity-lotSizeFilter.MinQty, lotSizeFilter.StepSize) + lotSizeFilter.MinQty

	// Ensure the quantity is within bounds after adjustment
	if validatedQuantity > lotSizeFilter.MaxQty {
		validatedQuantity = lotSizeFilter.MaxQty
	}

	remainder := validatedQuantity - lotSizeFilter.MinQty
	steps := math.Round(remainder / lotSizeFilter.StepSize)
	expectedQuantity := lotSizeFilter.MinQty + (steps * lotSizeFilter.StepSize)

	// Use a relative tolerance
	relativeTolerance := 1e-9 // This is a very small relative difference, 0.0000001%
	if math.Abs(validatedQuantity-expectedQuantity) > math.Max(lotSizeFilter.StepSize*relativeTolerance, 1e-8) {
		return "", fmt.Errorf("unable to adjust quantity to meet lot size requirements")
	}

	// If we've made it here, the quantity is valid
	precision := int(math.Ceil(-math.Log10(lotSizeFilter.StepSize)))
	return strconv.FormatFloat(validatedQuantity, 'f', precision, 64), nil
}

func (bh *BinanceHandler) CancelAllOpenOrders(symbol string) error {
	endpoint := "/fapi/v1/allOpenOrders"

	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())

	// Create the query string including the symbol
	queryString := fmt.Sprintf("symbol=%s&timestamp=%s", symbol, timestamp)

	// Create the signature
	signature := bh.generateSignature(queryString)

	// Construct the full URL
	fullURL := fmt.Sprintf("%s%s?%s&signature=%s", BASE, endpoint, queryString, signature)

	// Create and send the request
	req, err := http.NewRequest("DELETE", fullURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("X-MBX-APIKEY", bh.apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Read and return the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %w", err)
	}

	// Parse the response
	var response struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("error unmarshaling response: %w", err)
	}

	// Check for error response
	if response.Code < 0 {
		return fmt.Errorf("API error: %d - %s", response.Code, response.Msg)
	}

	return nil
}

func (bh *BinanceHandler) StartReentryMonitoring(symbol string, entryPrice float64, leverage int64, tp, sl float64, side string) error {
	bh.reentryMutex.Lock()
	defer bh.reentryMutex.Unlock()

	if bh.wsConn == nil {
		if err := bh.connectWebSocket(); err != nil {
			return err
		}
	}

	bh.reentryState[symbol] = &ReentryState{
		Symbol:       symbol,
		EntryPrice:   entryPrice,
		Leverage:     leverage,
		TP:           tp,
		SL:           sl,
		IsActive:     true,
		OriginalSide: side,
	}

	if !bh.isRunning {
		bh.isRunning = true
		go bh.websocketHandler()
	}

	// Subscribe to the symbol's trade stream
	subscribeMsg := fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s@trade"], "id": 1}`, strings.ToLower(symbol))
	if err := bh.wsConn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		return fmt.Errorf("failed to subscribe to trade stream: %w", err)
	}

	return nil
}

func (bh *BinanceHandler) connectWebSocket() error {
	u := url.URL{Scheme: "wss", Host: "testnet.binancefuture.com", Path: "/ws-fapi/v1"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	bh.wsConn = c
	bh.connectedSince = time.Now()
	bh.authenticated = false

	// Start ping ticker
	bh.pingTicker = time.NewTicker(3 * time.Minute)
	go bh.pingHandler()

	return nil
}

func (bh *BinanceHandler) pingHandler() {
	for {
		select {
		case <-bh.pingTicker.C:
			if err := bh.wsConn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Println("Failed to send ping:", err)
				return
			}
		case <-bh.doneCh:
			return
		}
	}
}

func (bh *BinanceHandler) websocketHandler() {
	for bh.isRunning {
		if err := bh.connectWebSocket(); err != nil {
			log.Println("Failed to connect to WebSocket:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if err := bh.authenticate(); err != nil {
			log.Println("Failed to authenticate WebSocket connection:", err)
			bh.wsConn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		if err := bh.readMessages(); err != nil {
			log.Println("WebSocket error:", err)
			bh.wsConn.Close()
			time.Sleep(5 * time.Second)
		}
	}
}
func (bh *BinanceHandler) generateEd25519Signature(payload string) string {
	seed, err := hex.DecodeString(bh.secret)
	if err != nil {
		log.Printf("Error decoding secret key: %v", err)
		return ""
	}

	if len(seed) != ed25519.SeedSize {
		log.Printf("Invalid seed length. Expected %d, got %d", ed25519.SeedSize, len(seed))
		return ""
	}

	// Generate the full private key from the seed
	privateKey := ed25519.NewKeyFromSeed(seed)

	// Sign the payload
	signature := ed25519.Sign(privateKey, []byte(payload))
	return hex.EncodeToString(signature)
}

func (bh *BinanceHandler) authenticate() error {
	timestamp := time.Now().UnixMilli()

	// Construct the payload string
	// Sort parameters alphabetically: apiKey, timestamp
	payload := fmt.Sprintf("apiKey=%s&timestamp=%d", bh.apiKey, timestamp)

	// Generate the signature
	signature := bh.generateEd25519Signature(payload)

	authMsg := struct {
		ID     string `json:"id"`
		Method string `json:"method"`
		Params struct {
			APIKey    string `json:"apiKey"`
			Signature string `json:"signature"`
			Timestamp int64  `json:"timestamp"`
		} `json:"params"`
	}{
		ID:     "auth",
		Method: "session.logon",
		Params: struct {
			APIKey    string `json:"apiKey"`
			Signature string `json:"signature"`
			Timestamp int64  `json:"timestamp"`
		}{
			APIKey:    bh.apiKey,
			Signature: signature,
			Timestamp: timestamp,
		},
	}

	jsonMsg, _ := json.Marshal(authMsg)
	log.Printf("Sending authentication message: %s", string(jsonMsg))

	if err := bh.wsConn.WriteJSON(authMsg); err != nil {
		return fmt.Errorf("failed to send authentication message: %w", err)
	}

	_, msg, err := bh.wsConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read authentication response: %w", err)
	}

	log.Printf("Received authentication response: %s", string(msg))

	var response struct {
		ID     string `json:"id"`
		Status int    `json:"status"`
		Result struct {
			APIKey          string `json:"apiKey"`
			AuthorizedSince int64  `json:"authorizedSince"`
		} `json:"result"`
		Error *struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		} `json:"error,omitempty"`
	}

	if err := json.Unmarshal(msg, &response); err != nil {
		return fmt.Errorf("failed to parse authentication response: %w", err)
	}

	if response.Status != 200 {
		if response.Error != nil {
			return fmt.Errorf("authentication failed: code=%d, msg=%s", response.Error.Code, response.Error.Msg)
		}
		return fmt.Errorf("authentication failed: %s", string(msg))
	}

	bh.authenticated = true
	return nil
}

func (bh *BinanceHandler) readMessages() error {
	for {
		select {
		case <-bh.doneCh:
			return nil
		default:
			_, message, err := bh.wsConn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					return nil
				}
				return err
			}

			if bh.wsConn.PongHandler() != nil {
				bh.lastPongTime = time.Now()
			}

			var wsMsg WSMessage
			if err := json.Unmarshal(message, &wsMsg); err != nil {
				log.Println("Failed to unmarshal WebSocket message:", err)
				continue
			}

			if wsMsg.Data.E == "trade" {
				bh.checkAndPlaceReentryOrder(wsMsg.Data.S, wsMsg.Data.P)
			}
		}
	}
}

func (bh *BinanceHandler) handleWebSocketMessages() {
	for {
		_, message, err := bh.wsConn.ReadMessage()
		if err != nil {
			log.Println("WebSocket read error:", err)
			return
		}

		var wsMsg WSMessage
		if err := json.Unmarshal(message, &wsMsg); err != nil {
			log.Println("Failed to unmarshal WebSocket message:", err)
			continue
		}

		if wsMsg.Data.E == "trade" {
			bh.checkAndPlaceReentryOrder(wsMsg.Data.S, wsMsg.Data.P)
		}
	}
}

func (bh *BinanceHandler) checkAndPlaceReentryOrder(symbol, priceStr string) {
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		log.Println("Failed to parse price:", err)
		return
	}

	bh.reentryMutex.Lock()
	defer bh.reentryMutex.Unlock()

	state, exists := bh.reentryState[symbol]
	if !exists || !state.IsActive {
		return
	}

	// Check if price has crossed the entry price
	if (state.OriginalSide == "BUY" && price <= state.EntryPrice) ||
		(state.OriginalSide == "SELL" && price >= state.EntryPrice) {
		// Place re-entry order
		if err := bh.placeReentryOrder(state); err != nil {
			log.Println("Failed to place re-entry order:", err)
			return
		}
		// Deactivate re-entry for this symbol
		state.IsActive = false
	}
}

func (bh *BinanceHandler) placeReentryOrder(state *ReentryState) error {
	// Place main order
	if err := bh.ExecuteMainOrder(&BinanceSignal{
		Symbol:   state.Symbol,
		Action:   state.OriginalSide,
		Leverage: state.Leverage,
		TP:       state.TP,
		SL:       state.SL,
	}, state.EntryPrice, ""); err != nil {
		return fmt.Errorf("failed to place main re-entry order: %w", err)
	}

	// Place TP order
	if err := bh.ExecuteTPOrder(&BinanceSignal{
		Symbol:   state.Symbol,
		Action:   state.OriginalSide,
		Leverage: state.Leverage,
		TP:       state.TP,
		SL:       state.SL,
	}, state.EntryPrice, ""); err != nil {
		return fmt.Errorf("failed to place TP re-entry order: %w", err)
	}

	// Place SL order
	if err := bh.ExecuteSLOrder(&BinanceSignal{
		Symbol:   state.Symbol,
		Action:   state.OriginalSide,
		Leverage: state.Leverage,
		TP:       state.TP,
		SL:       state.SL,
	}, state.EntryPrice, ""); err != nil {
		return fmt.Errorf("failed to place SL re-entry order: %w", err)
	}

	return nil
}

// Add this method to stop monitoring for re-entry
func (bh *BinanceHandler) StopReentryMonitoring(symbol string) {
	bh.reentryMutex.Lock()
	defer bh.reentryMutex.Unlock()

	if state, exists := bh.reentryState[symbol]; exists {
		state.IsActive = false
	}

	// Unsubscribe from the symbol's trade stream
	unsubscribeMsg := fmt.Sprintf(`{"method": "UNSUBSCRIBE", "params": ["%s@trade"], "id": 1}`, strings.ToLower(symbol))
	if err := bh.wsConn.WriteMessage(websocket.TextMessage, []byte(unsubscribeMsg)); err != nil {
		log.Println("Failed to unsubscribe from trade stream:", err)
	}
}

// Don't forget to close the WebSocket connection when you're done
func (bh *BinanceHandler) CloseWebSocket() {
	bh.reentryMutex.Lock()
	defer bh.reentryMutex.Unlock()
	bh.isRunning = false
	if bh.doneCh != nil {
		close(bh.doneCh)
	}
	if bh.pingTicker != nil {
		bh.pingTicker.Stop()
	}
	if bh.wsConn != nil {
		bh.wsConn.Close()
	}
}
