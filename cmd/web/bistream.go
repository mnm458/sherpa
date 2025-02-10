package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

func (a *application) WSBiConnect(eh exchange.ExchangeStrategy) {
	bh, ok := eh.(*exchange.BinanceHandler)
	if !ok {
		panic("incorrect handler passed for ws connect")
	}

	// Start WebSocket connection
	a.startWebSocketConnection(bh)

	// Start keepalive service
	a.startKeepaliveService(bh)
}

func (a *application) startWebSocketConnection(bh *exchange.BinanceHandler) {
	wsHandler := a.createWSHandler(bh)
	errHandler := a.createErrorHandler()

	go func() {
		_, _, err := futures.WsUserDataServe(bh.ListenKey, wsHandler, errHandler)
		if err != nil {
			a.logger.Error("WebSocket serve error:", "error", err)
		}
	}()
}

func (a *application) createWSHandler(bh *exchange.BinanceHandler) func(*futures.WsUserDataEvent) {
	return func(event *futures.WsUserDataEvent) {
		jsonBytes, err := json.MarshalIndent(event, "", "  ")
		if err != nil {
			a.logger.Error("JSON marshal error:", "error", err)
			return
		}

		var parsedEvent futures.WsUserDataEvent
		if err := json.Unmarshal(jsonBytes, &parsedEvent); err != nil {
			a.logger.Error("JSON unmarshal error:", "error", err)
			return
		}

		if parsedEvent.Event == types.LISTEN_KEY_EXPIRED_EVENT {
			a.logger.Info("Listen key expired, creating a new one")
			a.wsBiReissueListenKey(bh)
			if a.CurrBiMainOrders.MainOrder != nil && a.CurrBiMainOrders.TPOrder != nil && a.CurrBiMainOrders.SLOrder != nil {
				if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderID == a.CurrBiMainOrders.TPOrder.OrderID {
					a.startReentry(bh)
				} else if parsedEvent.Event == types.ORDER_TRADE_UPDATE_EVENT && parsedEvent.OrderTradeUpdate.Status == futures.OrderStatusTypeFilled && parsedEvent.OrderID == a.CurrBiMainOrders.SLOrder.OrderID {
					a.CurrBiMainOrders = types.BiSubmittedOrders{}
				}
			}

			a.logOrderUpdate(parsedEvent)
		}
	}
}

func (a *application) createPriceWsHandler(bh *exchange.BinanceHandler) func(*futures.WsMarkPriceEvent) {
	return func(event *futures.WsMarkPriceEvent) {
		newPriceStr := event.MarkPrice
		newPrice, err := strconv.ParseFloat(newPriceStr, 64)
		if err != nil {
			fmt.Println("PRICE COVNERSION ERR: ", err)
			return
		}
		fmt.Println("Price update ---->>", event.MarkPrice)

		if (newPrice <= 0.9*a.BiTPStopPrice && newPrice >= a.BiSLStopPrice) || (newPrice >= 1.1*a.BiTPStopPrice && newPrice <= a.BiSLStopPrice) {
			totBalance, err := bh.GetAccountBalance()
			if err != nil {
				a.logger.Error("Failed to get balance", "error", err)
				return
			}
			priceToFloat, _ := strconv.ParseFloat(a.CurrBiMainOrders.MainOrder.Price, 64)
			qty := bh.GetFinalQty(totBalance, a.CurrBiMainOrders.Signal.Leverage, priceToFloat)
			a.logger.Info("[BinanceHandler] final quantity calculated", "qty", qty)

			cancelErr := bh.CancelAllOpenOrders(a.CurrBiMainOrders.Signal.Symbol)
			if cancelErr != nil {
				a.logger.Error("[BinanceHandler] fialed to cancel open orders", "error", cancelErr)
			}
			bh.ExecuteBatchOrder(&a.CurrBiMainOrders.Signal, priceToFloat, qty, a.CurrBiMainOrders.StepSize, a.CurrBiMainOrders.TickSize)
		}
	}

}

func (a *application) startReentry(bh *exchange.BinanceHandler) {
	priceStreamHandler := a.createPriceWsHandler(bh)
	errHandler := a.createErrorHandler()
	doneCh, stopCh, err := futures.WsMarkPriceServe(a.CurrBiMainOrders.MainOrder.Symbol, priceStreamHandler, errHandler)
	_ = doneCh
	_ = stopCh
	if err != nil {
		fmt.Println("ERRROR: ", err)
	}
}

func (a *application) createErrorHandler() func(error) {
	return func(err error) {
		a.logger.Error("WebSocket error:", "error", err)
	}
}

func (a *application) logOrderUpdate(event futures.WsUserDataEvent) {
	border := strings.Repeat("-", 50)
	a.logger.Info(border)
	a.logger.Info("|               RECEIVED ORDER UPDATE                |")
	a.logger.Info(border)
	a.logger.Info("Parsed event:", "event", event)
	a.logger.Info(border)
}

func (a *application) startKeepaliveService(bh *exchange.BinanceHandler) {
	ticker := time.NewTicker(50 * time.Minute)

	go func() {
		for range ticker.C {
			if err := bh.Client.NewKeepaliveUserStreamService().
				ListenKey(bh.ListenKey).
				Do(bh.Ctx); err != nil {
				a.logger.Error("Keepalive error:", "error", err)
			}
		}
	}()
}

func (a *application) wsBiReissueListenKey(h *exchange.BinanceHandler) error {
	listenKey, err := h.Client.NewStartUserStreamService().Do(h.Ctx)
	if err != nil {
		return err
	}
	h.ListenKey = listenKey
	return nil
}
