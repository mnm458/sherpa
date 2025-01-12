package posmon

import (
	"log/slog"
	"time"

	"github.com/mnm458/sherpa/pkg/types"
)

type Order struct {
	Status      string `json:"status"`
	ExecutedQty string `json:"executedQty"`
}

type PositionMonitor struct {
	orderChecker    types.OrderStatusChecker // Interface instead of *BinanceHandler
	symbol          string
	entryOrderId    int64
	tpOrderId       int64
	slOrderId       int64
	originalQty     string
	ticker          *time.Ticker
	done            chan struct{}
	logger          *slog.Logger
	onPositionClose func(outcome string)
}

func NewPositionMonitor(orderChecker types.OrderStatusChecker, symbol string, entryOrderID, tpOrderID, slOrderID int64, originalQty string, logger *slog.Logger, onPositionClose func(outcome string)) *PositionMonitor {
	return &PositionMonitor{
		orderChecker:    orderChecker,
		symbol:          symbol,
		entryOrderId:    entryOrderID,
		tpOrderId:       tpOrderID,
		slOrderId:       slOrderID,
		originalQty:     originalQty,
		ticker:          time.NewTicker(10 * time.Second),
		done:            make(chan struct{}),
		logger:          logger,
		onPositionClose: onPositionClose,
	}
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
					// Entry order is filled, start monitoring TP/SL
					pm.monitorOutcome()
					return
				}
			}
		}
	}()
}

func (pm *PositionMonitor) monitorOutcome() {
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

			if tpFilled {
				if pm.onPositionClose != nil {
					pm.onPositionClose("Win")
				}
				pm.Stop()
				return
			}

			// Check SL order
			slFilled, err := pm.isOrderFilled(pm.slOrderId)
			if err != nil {
				pm.logger.Error("Error checking SL order", "error", err)
				continue
			}

			if slFilled {
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
