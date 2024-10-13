package exchange

import (
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
	tsignal := bh.signal
	if tsignal.Type != TYPE_OPEN {
		return errInvalidSignal
	}
	err := bh.handleLeverage()
	if err != nil {
		return err
	}
	return nil
}

func (bh *BinanceHandler) handleLeverage() error {
	endpoint := "/fapi/v2/positionRisk"
	params := url.Values{}
	params.Add("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	fullURL := BASE + endpoint + "?" + params.Encode()

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return fmt.Errorf("error generating request, err:%v", err)
	}

	// Add necessary headers
	req.Header.Add("X-MBX-APIKEY", "YOUR_API_KEY_HERE")

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
