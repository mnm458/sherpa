package exchange

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type BinanceSignal struct {
	Symbol   string
	Type     string
	Action   string
	Leverage int64
}

type PriceResp struct {
	Price float64 `json:"price"`
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
	signal *BinanceSignal
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

const BASE = "https://testnet.binancefuture.com"

func (bh *BinanceHandler) Validate(s *BinanceSignal) error {
	// tsignal := bh.signal
	// if tsignal.Type != TYPE_OPEN {
	// 	return errInvalidSignal
	// }
	err := bh.handleLeverage()
	if err != nil {
		return err
	}
	return nil
}

func (bh *BinanceHandler) handleLeverage() error {
	endpoint := "/fapi/v1/leverage"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", "BTCUSDT")
	params.Add("leverage", "20")

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := generateSignature(queryString, bh.secret)

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
	fmt.Println("Response:", string(body))
	return nil
}

func generateSignature(queryString, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(queryString))
	return hex.EncodeToString(h.Sum(nil))
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

	err = bh.PrepareOrder(&binanceSignal, price, quantity)

	// if err = bh.ExecuteMainOrder(&binanceSignal, price, quantity); err != nil {
	// 	return err
	// }

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

func (bh *BinanceHandler) FetchCurrPrice(symbol string) (float64, error) {
	endpoint := "/fapi/v1/ticker/price"
	params := url.Values{}
	fmt.Println("symbol: ", symbol)
	params.Add("symbol", symbol)

	// Create the query string
	queryString := params.Encode()

	// Construct the full URL
	fullURL := BASE + endpoint + "?" + queryString

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return 0, fmt.Errorf("error generating request: %v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)

	resp, err := bh.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading response: %v", err)
	}
	fmt.Println("resp bytes", string(respBytes))
	var priceResp struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}

	err = json.Unmarshal(respBytes, &priceResp)
	if err != nil {
		return 0, fmt.Errorf("error unmarshaling response: %v", err)
	}
	fmt.Println("Price resp: ", priceResp)
	price, err := strconv.ParseFloat(priceResp.Price, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing price: %v", err)
	}

	return price, nil
}

func (bh *BinanceHandler) PrepareOrder(s *BinanceSignal, price float64, quantity float64) error {
	fmt.Println("quantity is: ", quantity)
	endpoint := "/fapi/v1/order"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", s.Symbol)
	params.Add("side", s.Action)
	params.Add("type", "TAKE_PROFIT")
	params.Add("positionSide", "BOTH") // Assuming one-way mode, change if using Hedge Mode
	params.Add("timeInForce", "GTC")
	params.Add("quantity", strconv.FormatFloat(quantity, 'E', -1, 64)) // Replace with actual quantity
	params.Add("price", "35000")                                       // Replace with actual take profit price
	params.Add("stopPrice", "34900")                                   // Replace with actual trigger price
	params.Add("workingType", "CONTRACT_PRICE")
	params.Add("priceProtect", "FALSE")

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := generateSignature(queryString, bh.secret)

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
	fmt.Println("Order Response:", string(body))
	return nil
}

func (bh *BinanceHandler) ExecuteOrder() error {

	return nil
}

// func (bh *BinanceHandler) ExecuteMainOrder(s *BinanceSignal, price float64, quantity float64) error {
// 	//main order
// 	// symbol -> BTCUSDT, type -> limit, side -> buy, quantity -> 0.02, price -> price, timestamp, signature
// 	endpoint := "/fapi/v1/order"
// 	params := url.Values{}
// 	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
// 	params.Add("timestamp", timestamp)
// 	params.Add("symbol", s.Symbol)
// 	params.Add("price", strconv.FormatFloat(price, 'E', -1, 64))
// 	params.Add("quantity")
// }

func (bh *BinanceHandler) GetAccountBalance() (float64, error) {
	endpoint := "/fapi/v2/balance"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := generateSignature(queryString, bh.secret)

	// Construct the full URL with the query string and signature
	fullURL := BASE + endpoint + "?" + queryString + "&signature=" + signature

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return 0, fmt.Errorf("error generating request: %v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)

	resp, err := bh.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading response: %v", err)
	}

	var balances []struct {
		Asset   string `json:"asset"`
		Balance string `json:"balance"`
	}
	err = json.Unmarshal(body, &balances)
	if err != nil {
		return 0, fmt.Errorf("error unmarshaling response: %v", err)
	}

	var totalBalance float64
	for _, balance := range balances {
		balanceFloat, err := strconv.ParseFloat(balance.Balance, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing balance for %s: %v", balance.Asset, err)
		}
		totalBalance += balanceFloat
	}

	return totalBalance, nil
}

func (bh *BinanceHandler) ExecuteTPOrder(s *BinanceSignal, price float64, quantity float64) error {
	return nil
}

func (bh *BinanceHandler) ExecuteSLOrder(s *BinanceSignal, price float64, quantity float64) error {
	return nil
}

// EQUITY PERCENTAGE -> 98%
// RISK ->
