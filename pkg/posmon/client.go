package posmon

import (
	"log/slog"
	"time"

	"github.com/mnm458/sherpa/pkg/types"
)

// MarketStreamChecker interface for checking reentry conditions

type PositionMonitor struct {
	orderChecker    types.OrderStatusChecker
	marketStream    types.MarketStreamChecker // Changed from pointer to interface
	symbol          string
	entryOrderId    int64
	tpOrderId       int64
	slOrderId       int64
	originalQty     string
	currentPrice    float64
	side            string
	ticker          *time.Ticker
	done            chan struct{}
	logger          *slog.Logger
	onPositionClose func(outcome string)
}

func NewPositionMonitor(
	orderChecker types.OrderStatusChecker,
	marketStream types.MarketStreamChecker,
	symbol string,
	entryOrderID, tpOrderID, slOrderID int64,
	originalQty string,
	side string,
	currentPrice float64,
	logger *slog.Logger,
	onPositionClose func(outcome string),
) *PositionMonitor {
	return &PositionMonitor{
		orderChecker:    orderChecker,
		marketStream:    marketStream,
		symbol:          symbol,
		entryOrderId:    entryOrderID,
		tpOrderId:       tpOrderID,
		slOrderId:       slOrderID,
		originalQty:     originalQty,
		side:            side,
		currentPrice:    currentPrice,
		ticker:          time.NewTicker(10 * time.Second),
		done:            make(chan struct{}),
		logger:          logger,
		onPositionClose: onPositionClose,
	}
}
func (pm *PositionMonitor) UpdateCurrentPrice(price float64) {
	pm.currentPrice = price
}

func (pm *PositionMonitor) isOrderFilled(orderId int64) (bool, error) {
	order, err := pm.orderChecker.GetOrder(pm.symbol, orderId)
	if err != nil {
		return false, err
	}
	return order.Status == "FILLED" && order.ExecutedQty == pm.originalQty, nil
}

func (pm *PositionMonitor) Start() {
	go func() {
		// First wait for entry order to be filled
		for {
			select {
			case <-pm.done:
				return
			case <-pm.ticker.C:
				filled, err := pm.isOrderFilled(pm.entryOrderId)
				if err != nil {
					pm.logger.Error("Error checking entry order", "error", err)
					continue
				}

				if filled {
					pm.logger.Info("Entry order filled, starting outcome monitoring",
						"orderId", pm.entryOrderId,
						"symbol", pm.symbol)
					// Entry order is filled, start monitoring TP/SL
					pm.monitorOutcome()
					return
				}
			}
		}
	}()
}

func (pm *PositionMonitor) monitorOutcome() {
	pm.logger.Info("Starting outcome monitoring",
		"symbol", pm.symbol,
		"side", pm.side,
		"currentPrice", pm.currentPrice)

	for {
		select {
		case <-pm.done:
			return
		case <-pm.ticker.C:
			// Check TP order
			tpFilled, err := pm.isOrderFilled(pm.tpOrderId)
			if err != nil {
				pm.logger.Error("Error checking TP order", "error", err)
				continue
			}

			// Log TP check result
			pm.logger.Debug("Checking TP order",
				"orderId", pm.tpOrderId,
				"filled", tpFilled)

			if tpFilled {
				pm.logger.Info("TP order filled - Position won",
					"orderId", pm.tpOrderId,
					"symbol", pm.symbol,
					"side", pm.side)
				if pm.onPositionClose != nil {
					pm.onPositionClose("Win")
				}
				continue
			}

			// Check SL order
			slFilled, err := pm.isOrderFilled(pm.slOrderId)
			if err != nil {
				pm.logger.Error("Error checking SL order", "error", err)
				continue
			}

			// Log SL check result
			pm.logger.Debug("Checking SL order",
				"orderId", pm.slOrderId,
				"filled", slFilled)

			if slFilled {
				pm.logger.Info("SL order filled - Position lost",
					"orderId", pm.slOrderId,
					"symbol", pm.symbol,
					"side", pm.side)
				if pm.onPositionClose != nil {
					pm.onPositionClose("Loss")
				}
				pm.Stop()
				return
			}
		}
	}
}

func (pm *PositionMonitor) Stop() {
	close(pm.done)
	pm.ticker.Stop()
}
