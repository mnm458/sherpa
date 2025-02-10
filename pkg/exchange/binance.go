package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/mnm458/sherpa/pkg/types"
)

type BinanceHandler struct {
	Ctx                 context.Context
	apiKey              string
	secret              string
	logger              *slog.Logger
	Client              *futures.Client
	ListenKey           string
	submittedOrdersChan chan types.BiSubmittedOrders
}

func NewBinanceHandler(ctx context.Context, apiKey string, secret string, submittedOrdersChan chan types.BiSubmittedOrders, logger *slog.Logger) *BinanceHandler {
	fmt.Println("API KEY + SECRET", apiKey, secret)
	client := futures.NewClient(apiKey, secret)
	listenKey, err := client.NewStartUserStreamService().Do(ctx)
	if err != nil {
		panic("failed to create listen key")
	}

	// wsHandler := func(event *futures.WsUserDataEvent) {
	// 	jsonBytes, err := json.MarshalIndent(event, "", "  ")
	// 	if err != nil {
	// 		fmt.Println("error:", err)
	// 		return
	// 	}

	// 	var parsedEvent futures.WsUserDataEvent
	// 	if err := json.Unmarshal(jsonBytes, &parsedEvent); err != nil {
	// 		fmt.Println("error:", err)
	// 		return
	// 	}
	// 	fmt.Printf("Parsed event: %+v\n", parsedEvent)
	// }

	// errHandler := func(err error) {
	// 	fmt.Println("WebSocket error:", err)
	// }

	// go func() {
	// 	_, _, err := futures.WsUserDataServe(listenKey, wsHandler, errHandler)
	// 	if err != nil {
	// 		fmt.Println("WebSocket serve error:", err)
	// 	}
	// }()
	return &BinanceHandler{
		Ctx:                 ctx,
		apiKey:              apiKey,
		secret:              secret,
		Client:              client,
		logger:              logger,
		ListenKey:           listenKey,
		submittedOrdersChan: submittedOrdersChan,
	}
}

func (bh *BinanceHandler) Process(s Signal) error {
	binanceSignal, ok := s.(types.BinanceSignal)
	if ok {
		bh.logger.Info("[BinanceHandler] processing signal", "signal", binanceSignal.String())
	} else {
		bh.logger.Error("[BinanceHandler] failed to cast signal to BinanceSignal")
		return errInvalidSignal
	}
	levErr := bh.handleLeverage(binanceSignal.Symbol, binanceSignal.Leverage)
	if levErr != nil {
		bh.logger.Error("[BinanceHandler] failed to set leverage", "leverage", binanceSignal.Leverage)
	}

	price, err := bh.FetchCurrPrice(s.GetSymbol())
	if err != nil {
		bh.logger.Error("Failed to fetch price", "error", err)
		return err
	}

	totBalance, err := bh.GetAccountBalance()
	if err != nil {
		bh.logger.Error("Failed to get balance", "error", err)
		return err
	}
	bh.logger.Info("[BinanceHandler] account balance calculated", "balance", totBalance)

	qty := bh.GetFinalQty(totBalance, binanceSignal.Leverage, price)
	bh.logger.Info("[BinanceHandler] final quantity calculated", "qty", qty)

	cancelErr := bh.CancelAllOpenOrders(binanceSignal.Symbol)
	if cancelErr != nil {
		bh.logger.Error("[BinanceHandler] fialed to cancel open orders", "error", cancelErr)
	}
	bh.logger.Info("[BinanceHandler] all open orders cancelled")

	stepSize, tickSize, err := bh.getPricePrecisionAndTickSize()
	if err != nil {
		bh.logger.Error("[BinanceHandler] failed to get precision", "error", err)
	}
	bh.logger.Info("[BinanceHandler] precision calculated", "stepSize", stepSize, "tick size", tickSize)

	mainErr := bh.ExecuteBatchOrder(&binanceSignal, price, qty, stepSize, tickSize)
	if mainErr != nil {
		bh.logger.Error("[BinanceHandler] order execution failed", "error", mainErr)
	}
	// validatedQuantity, err := bh.ValidateQuantity(binanceSignal.Symbol, quantity)
	// if err != nil {
	// 	bh.logger.Error("Failed to validate quantity", "error", err)
	// 	return err
	// }
	return nil
}

func (bh *BinanceHandler) getPricePrecisionAndTickSize() (float64, float64, error) {
	var tickSize float64
	var stepSize float64
	exInfo, err := bh.Client.NewExchangeInfoService().Do(bh.Ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, symbol := range exInfo.Symbols {
		if symbol.Symbol == BTCUSDT {
			for _, filter := range symbol.Filters {
				if filter["filterType"] == "PRICE_FILTER" {
					tickSizef, err := strconv.ParseFloat(filter["tickSize"].(string), 64)
					if err != nil {
						return 0, 0, err
					}
					tickSize = tickSizef
				}
				if filter["filterType"] == "LOT_SIZE" {
					stepSizef, err := strconv.ParseFloat(filter["stepSize"].(string), 64)
					if err != nil {
						return 0, 0, err
					}
					stepSize = stepSizef
				}
				if tickSize != 0 && stepSize != 0 {
					return stepSize, tickSize, nil
				}
			}
		}
	}
	return 0, 0, fmt.Errorf("symbol or price filter not found")
}
func countDecimalPlaces(tickSize float64) int {
	str := strconv.FormatFloat(tickSize, 'f', -1, 64)
	if i := strings.IndexByte(str, '.'); i > -1 {
		return len(str) - i - 1
	}
	return 0
}

func (bh *BinanceHandler) ExecuteBatchOrder(s *types.BinanceSignal, price float64, qty float64, stepSize float64, tickSize float64) error {

	orders := make([]*futures.CreateOrderService, 0, 3)
	finalPrice := math.Round(price/tickSize) * tickSize
	decimals := countDecimalPlaces(tickSize)
	priceStr := strconv.FormatFloat(finalPrice, 'f', decimals, 64)
	fmt.Println("PRICE  MAIN:", priceStr)
	finalQty := math.Round(qty/stepSize) * stepSize
	qtyDecimals := countDecimalPlaces(stepSize)
	qtyStr := strconv.FormatFloat(finalQty, 'f', qtyDecimals, 64)
	fmt.Println("Quantity: ", qtyStr)
	var mOrder types.OpenOrder
	if strings.ToUpper(s.Action) == "BUY" {
		mOrder.Side = futures.SideTypeBuy
	} else {
		mOrder.Side = futures.SideTypeSell
	}
	mOrder.Symbol = s.Symbol
	mOrder.Type = futures.OrderType(strings.ToUpper(s.Type)) // OrderTypeLimit OrderTypeMarket
	mOrder.Price = priceStr
	mOrder.TimeInForce = futures.TimeInForceTypeGTC
	mOrder.Quantity = qtyStr
	mOrderService := bh.CreateOrderLimitMarket(mOrder)
	orders = append(orders, mOrderService)

	var tpOrder types.OpenOrder
	var tpslSide futures.SideType
	if strings.ToUpper(s.Action) == "BUY" {
		tpslSide = futures.SideTypeSell
	} else {
		tpslSide = futures.SideTypeBuy
	}

	tpPrice, slPrice, calcErr := bh.calculateTPSLPrice(price, s.Action, s.TP, s.SL)
	if calcErr != nil {
		return calcErr
	}
	tpPriceFinal := math.Round(tpPrice/tickSize) * tickSize
	slPriceFinal := math.Round(slPrice/tickSize) * tickSize
	tpPriceStr := strconv.FormatFloat(tpPriceFinal, 'f', decimals, 64)
	slPriceStr := strconv.FormatFloat(slPriceFinal, 'f', decimals, 64)
	fmt.Println("PRICE  TP:", tpPriceStr)
	fmt.Println("PRICE  SL:", slPriceStr)
	tpOrder.Symbol = s.Symbol
	tpOrder.Side = futures.SideType(tpslSide) // SideTypeBuy SideTypeSell
	tpOrder.Type = futures.OrderType("TAKE_PROFIT")
	tpOrder.Price = tpPriceStr
	tpOrder.ReduceOnly = true
	tpOrder.TimeInForce = futures.TimeInForceTypeGTC
	tpOrder.StopPrice = tpPriceStr
	tpOrder.Quantity = qtyStr
	tpOrderService := bh.CreateOrderLimitMarket(tpOrder)
	orders = append(orders, tpOrderService)

	var slOrder types.OpenOrder
	slOrder.Symbol = s.Symbol
	slOrder.Side = futures.SideType(tpslSide) // SideTypeBuy SideTypeSell
	slOrder.Type = futures.OrderType("STOP")  // OrderTypeLimit OrderTypeMarket
	slOrder.Price = slPriceStr
	slOrder.TimeInForce = futures.TimeInForceTypeGTC
	slOrder.StopPrice = slPriceStr
	slOrder.Quantity = qtyStr
	slOrder.ReduceOnly = true
	slOrderService := bh.CreateOrderLimitMarket(slOrder)
	orders = append(orders, slOrderService)

	res, err := bh.Client.NewCreateBatchOrdersService().OrderList(orders).Do(bh.Ctx)
	if err != nil {
		return err
	}
	for _, e := range res.Errors {
		if e != nil {
			return e
		}
	}
	bh.logger.Info("Orders placed successfully", "res", res)
	var submittedOrders types.BiSubmittedOrders
	submittedOrders.Signal = *s
	submittedOrders.StepSize = stepSize
	submittedOrders.TickSize = tickSize
	for _, order := range res.Orders {
		if order.Type == futures.OrderType(strings.ToUpper(s.Type)) {
			submittedOrders.MainOrder = order
		} else if order.Type == futures.OrderTypeTakeProfit {
			submittedOrders.TPOrder = order
		} else if order.Type == futures.OrderTypeStop {
			submittedOrders.SLOrder = order
		}
	}

	bh.submittedOrdersChan <- submittedOrders
	return nil
}

func (bh *BinanceHandler) calculateTPSLPrice(price float64, action string, tp float64, sl float64) (float64, float64, error) {
	var tpPrice float64
	var slPrice float64
	switch strings.ToUpper(action) {
	case "BUY":
		tpPrice = price * (1 + tp)
		slPrice = price * (1 - sl)
	case "SELL":
		tpPrice = price * (1 - tp)
		slPrice = price * (1 + sl)
	default:
		return 0, 0, errUnsupportedSide
	}
	return tpPrice, slPrice, nil
}

func (bh *BinanceHandler) CreateOrderLimitMarket(args types.OpenOrder) *futures.CreateOrderService {
	order := bh.Client.NewCreateOrderService()
	if args.Symbol != "" {
		order = order.Symbol(args.Symbol)
	}

	if args.Side != "" {
		order = order.Side(args.Side)
	}

	if args.Type != "" {
		order = order.Type(args.Type)
	}

	if args.Quantity != "" {
		order = order.Quantity(args.Quantity)
	}

	if args.Price != "" {
		order = order.Price(args.Price)
	}

	if args.WorkingType != "" {
		order = order.WorkingType(args.WorkingType)
	}
	if args.StopPrice != "" {
		order = order.StopPrice(args.StopPrice)
	}

	order = order.TimeInForce(args.TimeInForce)
	order = order.ReduceOnly(args.ReduceOnly)

	if args.ClosePosition != "" {
		var v bool
		v = false
		if args.ClosePosition == "true" {
			v = true
		}
		order = order.ClosePosition(v)
	}
	return order
}

func (bh *BinanceHandler) CancelAllOpenOrders(symbol string) error {
	return bh.Client.NewCancelAllOpenOrdersService().Symbol(symbol).Do(bh.Ctx)
}

func (bh *BinanceHandler) GetFinalQty(totalBalance float64, leverage int64, price float64) float64 {
	availBalance := EQUITY_PERCENTAGE * totalBalance  // Available balance with risk management
	notionalValue := availBalance * float64(leverage) // Total position value with leverage
	return notionalValue / price                      // Convert to quantity in base asset
}

// handleLeverage returns an error. This method set the leverage for the account. It is not order specific
func (bh *BinanceHandler) handleLeverage(symbol string, leverage int64) error {
	fmt.Println("client credentials:", bh.Client.APIKey, bh.Client.SecretKey, bh.Client.BaseURL)
	res, err := bh.Client.NewChangeLeverageService().Symbol(symbol).Leverage(int(leverage)).Do(bh.Ctx)
	if err != nil {
		bh.logger.Error("[BinanceHandler] failed to set leverage", "error", err)
		return err
	}
	if res.Leverage != int(leverage) {
		bh.logger.Error("[BinanceHandler] failed to set correct leverage")
		return errIncorrectLeverageSet
	}
	return nil
}

func (bh *BinanceHandler) FetchCurrPrice(symbol string) (float64, error) {
	res, err := bh.Client.NewPremiumIndexService().Symbol(symbol).Do(bh.Ctx)
	if err != nil {
		return 0, err
	}
	for _, r := range res {
		jsonData, err := json.Marshal(r)
		if err != nil {
			fmt.Printf("Error marshaling to JSON: %v\n", err)
			continue
		}
		fmt.Println(string(jsonData))
	}
	markPrice, convErr := strconv.ParseFloat(res[0].MarkPrice, 64)
	if convErr != nil {
		return 0, convErr
	}
	return markPrice, nil
}

func (bh *BinanceHandler) GetAccountBalance() (float64, error) {
	res, err := bh.Client.NewGetAccountService().Do(bh.Ctx)
	if err != nil {
		return 0, err
	}
	bal, balErr := strconv.ParseFloat(res.AvailableBalance, 64)
	if balErr != nil {
		return 0, balErr
	}

	return bal, nil
}
