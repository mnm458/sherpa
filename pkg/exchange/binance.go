package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2"
	futures "github.com/adshao/go-binance/v2/futures"
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

type OrderResponse struct {
	OrderId int64 `json:"orderId"`
}
type BinanceAPIError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type PriceResp struct {
	Price float64 `json:"price"`
}

type BalanceResp struct {
	Asset              string `json:"asset"`
	WalletBalance      string `json:"walletBalance"`      // Modified
	CrossWalletBalance string `json:"crossWalletBalance"` // Added
	Balance            string `json:"balance"`            // Kept for compatibility
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
func (b BinanceSignal) String() string {
	return fmt.Sprintf("Symbol: %s, Type: %s, Action: %s, Leverage: %d, TP: %.2f, SL: %.2f",
		b.Symbol, b.Type, b.Action, b.Leverage, b.TP, b.SL)
}

type BinanceHandler struct {
	ctx    context.Context
	apiKey string
	secret string
	logger *slog.Logger
	client *futures.Client
}

func NewBinanceHandler(ctx context.Context, apiKey string, secret string, logger *slog.Logger) *BinanceHandler {
	client := binance.NewFuturesClient(apiKey, secret)
	return &BinanceHandler{
		ctx:    ctx,
		apiKey: apiKey,
		secret: secret,
		client: client,
		logger: logger,
	}
}

func (bh *BinanceHandler) Process(s Signal) error {
	binanceSignal, ok := s.(BinanceSignal)
	if ok {
		bh.logger.Info("[BinanceHandler] processing signal", "signal", binanceSignal.String())
	} else {
		bh.logger.Error("[BinanceHandler] failed to cast signal to BinanceSignal")
		return errInvalidSignal
	}
	levErr := bh.handleLeverage(binanceSignal.Symbol, binanceSignal.Leverage)
	if levErr != nil {
		bh.logger.Error("[BinanceHandler] failed to set leverage", binanceSignal.Leverage)
	}

	price, err := bh.FetchCurrPrice(s.GetSymbol())
	if err != nil {
		bh.logger.Error("Failed to fetch price", "error", err)
		return err
	}
	_ = price

	// totBalance, err := bh.GetAccountBalance()
	// if err != nil {
	// 	bh.logger.Error("Failed to get balance", "error", err)
	// 	return err
	// }

	// availBalance := EQUITY_PERCENTAGE * totBalance
	// leverage := float64(s.GetLeverage())
	// //TODO: FLOOR 3DP
	// quantity := (availBalance * leverage) / price
	// bh.logger.Info("checkpoint 1", "Balance", totBalance, "Price", price, "Available Balance", availBalance, "Quantity", quantity)
	// validatedQuantity, err := bh.ValidateQuantity(binanceSignal.Symbol, quantity)
	// if err != nil {
	// 	bh.logger.Error("Failed to validate quantity", "error", err)
	// 	return err
	// }

	// err = bh.CancelAllOpenOrders(binanceSignal.Symbol)
	// if err != nil {
	// 	bh.logger.Error("Failed to cancel all orders", "error", err)
	// 	return err
	// }

	// err = bh.ExecuteMainOrder(&binanceSignal, price, validatedQuantity)
	// if err != nil {
	// 	bh.logger.Error("Failed to execute main order", "error", err)
	// 	return err
	// }

	// err = bh.ExecuteTPOrder(&binanceSignal, price, validatedQuantity)
	// if err != nil {
	// 	bh.logger.Error("Failed to execute TP order", "error", err)
	// 	return err
	// }

	// err = bh.ExecuteSLOrder(&binanceSignal, price, validatedQuantity)
	// if err != nil {
	// 	bh.logger.Error("Failed to execute SL order", "error", err)
	// 	return err
	// }
	return nil
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

// handleLeverage returns an error. This method set the leverage for the account. It is not order specific
func (bh *BinanceHandler) handleLeverage(symbol string, leverage int64) error {
	res, err := bh.client.NewChangeLeverageService().Symbol(symbol).Leverage(int(leverage)).Do(bh.ctx)
	if err != nil {
		bh.logger.Error("[BinanceHandler] failed to set leverage, err=%v", err)
		return err
	}
	if res.Leverage != int(leverage) {
		bh.logger.Error("[BinanceHandler] failed to set correct leverage")
		return errIncorrectLeverageSet
	}
	return nil
}

func (bh *BinanceHandler) generateSignature(queryString string) string {
	h := hmac.New(sha256.New, []byte(bh.secret))
	h.Write([]byte(queryString))
	return hex.EncodeToString(h.Sum(nil))
}

func (bh *BinanceHandler) FetchCurrPrice(symbol string) (float64, error) {
	res, err := bh.client.NewPremiumIndexService().Symbol(symbol).Do(bh.ctx)
	if err != nil {
		return 0, err
	}
	fmt.Println("FETCH PRICE RES: ", res)
	return 0, nil
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

	var orderResp OrderResponse
	if err := json.Unmarshal(body, &orderResp); err != nil {
		return fmt.Errorf("error parsing order response: %w", err)
	}
	bh.logger.Info("Main order placed",
		"orderId", orderResp.OrderId,
		"symbol", s.Symbol,
		"side", s.Action,
	)
	bh.executedOrders[0] = orderResp.OrderId
	return nil
}

// In your main function or wherever you're handling the response:
func GetUSDTBalance(body []byte) (float64, error) {
	var balances []BalanceResp
	if err := json.Unmarshal(body, &balances); err != nil {
		return 0, fmt.Errorf("error unmarshaling balance array: %w", err)
	}

	// Find USDT balance
	for _, balance := range balances {
		if balance.Asset == "USDT" {
			// Try crossWalletBalance first, then walletBalance, then balance
			balanceStr := balance.CrossWalletBalance
			if balanceStr == "" {
				balanceStr = balance.WalletBalance
			}
			if balanceStr == "" {
				balanceStr = balance.Balance
			}

			val, err := strconv.ParseFloat(balanceStr, 64)
			if err != nil {
				return 0, fmt.Errorf("error parsing USDT balance: %w", err)
			}
			return val, nil
		}
	}

	return 0, fmt.Errorf("USDT balance not found in response")
}

// func (bh *BinanceHandler) ExecuteTPOrder(s *BinanceSignal, price float64, quantity string) error {
// 	var tpPrice float64
// 	if s.Action == "BUY" {
// 		tpPrice = (s.TP + 1.0) * price
// 	} else {
// 		tpPrice = (1.0 - s.TP) * price
// 	}
// 	var tpAction string
// 	switch s.Action {
// 	case "BUY":
// 		tpAction = "SELL"
// 	case "SELL":
// 		tpAction = "BUY"
// 	}
// 	fmt.Println("TP PRICE: ", tpPrice, "after conv: ", strconv.FormatFloat(tpPrice, 'f', 1, 64))
// 	endpoint := "/fapi/v1/order"
// 	params := url.Values{}
// 	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
// 	params.Add("timestamp", timestamp)
// 	params.Add("symbol", s.Symbol)
// 	params.Add("side", tpAction)
// 	params.Add("type", "TAKE_PROFIT")
// 	params.Add("timeInForce", "GTC")
// 	params.Add("reduceOnly", strconv.FormatBool(true))
// 	params.Add("quantity", quantity)
// 	params.Add("price", strconv.FormatFloat(tpPrice, 'f', 1, 64))
// 	params.Add("stopPrice", strconv.FormatFloat(tpPrice, 'f', 1, 64))

// 	// Create the query string without the signature
// 	queryString := params.Encode()

// 	// Generate the signature
// 	signature := bh.generateSignature(queryString)

// 	// Construct the full URL with the query string and signature
// 	fullURL := BASE + endpoint

// 	reqBody := queryString + "&signature=" + signature

// 	req, err := http.NewRequest("POST", fullURL, strings.NewReader(reqBody))
// 	if err != nil {
// 		return fmt.Errorf("error generating request: %v", err)
// 	}

// 	// Add necessary headers
// 	req.Header.Add("X-MBX-APIKEY", bh.apiKey)
// 	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

// 	resp, err := bh.client.Do(req)
// 	if err != nil {
// 		return fmt.Errorf("error sending request: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	// Read the response body
// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return fmt.Errorf("error reading response: %v", err)
// 	}

// 	var orderResp OrderResponse
// 	if err := json.Unmarshal(body, &orderResp); err != nil {
// 		return fmt.Errorf("error parsing order response: %w", err)
// 	}
// 	bh.logger.Info("TP order placed",
// 		"orderId", orderResp.OrderId,
// 		"symbol", s.Symbol,
// 		"side", s.Action,
// 	)
// 	bh.executedOrders[1] = orderResp.OrderId
// 	// Print the response
// 	fmt.Println("TP Order Response:", string(body))
// 	return nil
// }

// func (bh *BinanceHandler) ExecuteSLOrder(s *BinanceSignal, price float64, quantity string) error {
// 	var slPrice float64
// 	if s.Action == "BUY" {
// 		slPrice = (1.0 - s.TP) * price
// 	} else {
// 		slPrice = (s.TP + 1.0) * price
// 	}
// 	var slAction string
// 	switch s.Action {
// 	case "BUY":
// 		slAction = "SELL"
// 	case "SELL":
// 		slAction = "BUY"
// 	}
// 	endpoint := "/fapi/v1/order"
// 	params := url.Values{}
// 	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
// 	params.Add("timestamp", timestamp)
// 	params.Add("symbol", s.Symbol)
// 	params.Add("side", slAction)
// 	params.Add("timeInForce", "GTC")
// 	params.Add("type", "STOP")
// 	params.Add("reduceOnly", strconv.FormatBool(true))
// 	params.Add("quantity", quantity)
// 	params.Add("price", strconv.FormatFloat(slPrice, 'f', 1, 64))
// 	params.Add("stopPrice", strconv.FormatFloat(slPrice, 'f', 1, 64))

// 	// Create the query string without the signature
// 	queryString := params.Encode()

// 	// Generate the signature
// 	signature := bh.generateSignature(queryString)

// 	// Construct the full URL with the query string and signature
// 	fullURL := BASE + endpoint

// 	reqBody := queryString + "&signature=" + signature

// 	req, err := http.NewRequest("POST", fullURL, strings.NewReader(reqBody))
// 	if err != nil {
// 		return fmt.Errorf("error generating request: %v", err)
// 	}

// 	// Add necessary headers
// 	req.Header.Add("X-MBX-APIKEY", bh.apiKey)
// 	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

// 	resp, err := bh.client.Do(req)
// 	if err != nil {
// 		return fmt.Errorf("error sending request: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	// Read the response body
// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return fmt.Errorf("error reading response: %v", err)
// 	}
// 	var orderResp OrderResponse
// 	if err := json.Unmarshal(body, &orderResp); err != nil {
// 		return fmt.Errorf("error parsing order response: %w", err)
// 	}
// 	bh.logger.Info("SL order placed",
// 		"orderId", orderResp.OrderId,
// 		"symbol", s.Symbol,
// 		"side", s.Action,
// 	)
// 	bh.executedOrders[2] = orderResp.OrderId
// 	// Print the response
// 	fmt.Println("SL Order Response:", string(body))
// 	return nil
// }

// func (bh *BinanceHandler) GetOrder(symbol string, orderId int64) (*types.Order, error) {
// 	endpoint := "/fapi/v1/order"

// 	// Create parameters
// 	params := url.Values{}
// 	params.Add("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
// 	params.Add("symbol", symbol)
// 	params.Add("orderId", strconv.FormatInt(orderId, 10))

// 	// Generate signature
// 	queryString := params.Encode()
// 	signature := bh.generateSignature(queryString)

// 	// Construct full URL with query string and signature
// 	fullURL := BASE + endpoint + "?" + queryString + "&signature=" + signature

// 	// Create request
// 	req, err := http.NewRequest("GET", fullURL, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("error creating request: %w", err)
// 	}

// 	// Add headers
// 	req.Header.Add("X-MBX-APIKEY", bh.apiKey)

// 	// Send request
// 	resp, err := bh.client.Do(req)
// 	if err != nil {
// 		return nil, fmt.Errorf("error sending request: %w", err)
// 	}
// 	defer resp.Body.Close()

// 	// Read response body
// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return nil, fmt.Errorf("error reading response: %w", err)
// 	}

// 	// Check for error status
// 	if resp.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("API error: %s", string(body))
// 	}

// 	// Parse response
// 	var order types.Order
// 	if err := json.Unmarshal(body, &order); err != nil {
// 		return nil, fmt.Errorf("error parsing response: %w", err)
// 	}

// 	return &order, nil
// }

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
