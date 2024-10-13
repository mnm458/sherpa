package exchange

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type BinanceSignal struct {
	Contract string
	Type     string
	Action   string
	Leverage float32
}

func (bs BinanceSignal) GetType() string {
	return bs.Type
}

func (bs BinanceSignal) GetAction() string {
	return bs.Action
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
	endpoint := "/fapi/v2/positionRisk"
	params := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Add("timestamp", timestamp)
	params.Add("symbol", "BTCUSDT") // Note: Changed from "BTC/USDT" to "BTCUSDT"

	// Create the query string without the signature
	queryString := params.Encode()

	// Generate the signature
	signature := generateSignature(queryString, bh.secret)

	// Construct the full URL with the query string and signature
	fullURL := BASE + endpoint + "?" + queryString + "&signature=" + signature

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return fmt.Errorf("error generating request, err:%v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", bh.apiKey)

	resp, err := bh.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request, err:%v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response, err:%v", err)
	}

	// Print the response
	fmt.Println("Response:", string(body))
	return nil

	//get position info for symbol
	// if nil or error then set leverage
	// otherwise compare leverage with the current one on the signal
	// set accordingly if needed

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

	if err := bh.PrepareOrder(); err != nil {
		return err
	}

	if err := bh.ExecuteOrder(); err != nil {
		return err
	}
	return nil

}

func (bh *BinanceHandler) PrepareOrder() error {
	return nil
}

func (bh *BinanceHandler) ExecuteOrder() error {
	return nil
}
