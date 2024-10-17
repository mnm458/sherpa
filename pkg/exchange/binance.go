package exchange

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	apiKey string
	secret string
	client *http.Client
}

func NewBinanceHandler(apiKey string, secret string) *BinanceHandler {
	return &BinanceHandler{
		apiKey: apiKey,
		secret: secret,
		client: &http.Client{},
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
	params.Add("price", strconv.FormatFloat(price, 'f', 4, 64))

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
		tpPrice = (s.TP + 1) * price
	} else {
		tpPrice = (1 - s.TP) * price
	}
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", s.Action)
	params.Add("type", "TAKE_PROFIT")
	params.Add("timeInForce", "GTC")
	params.Add("reduceOnly", strconv.FormatBool(true))
	params.Add("quantity", quantity)
	params.Add("price", strconv.FormatFloat(tpPrice, 'f', 2, 64))
	params.Add("stopPrice", strconv.FormatFloat(tpPrice, 'f', 2, 64))

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
		slPrice = (1 - s.TP) * price
		slPrice = (s.TP + 1) * price
	} else {
		slPrice = (s.TP + 1) * price
	}
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", s.Action)
	params.Add("timeInForce", "GTC")
	params.Add("type", "TAKE_PROFIT")
	params.Add("reduceOnly", strconv.FormatBool(true))
	params.Add("quantity", quantity)
	params.Add("price", strconv.FormatFloat(slPrice, 'f', 2, 64))
	params.Add("stopPrice", strconv.FormatFloat(slPrice, 'f', 2, 64))

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
