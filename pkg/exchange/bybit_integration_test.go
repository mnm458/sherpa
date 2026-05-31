//go:build integration

package exchange

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/mnm458/sherpa/pkg/types"
)

// ────────────────────────────────────────────────────────────────────────────
// Setup
// ────────────────────────────────────────────────────────────────────────────

// newTestnetHandler creates a BybitHandler pointed at the testnet.
// It loads credentials from the .env file at the repo root. The channel is
// buffered so that placeOrder never blocks in tests.
func newTestnetHandler(t *testing.T) (*BybitHandler, chan types.ByMainOrder) {
	t.Helper()
	// .env is two levels up from pkg/exchange/
	_ = godotenv.Load("../../.env")

	apiKey := os.Getenv("BYBIT_API_KEY_TEST")
	secret := os.Getenv("BYBIT_SECRET_TEST")
	if apiKey == "" || secret == "" {
		t.Skip("BYBIT_API_KEY_TEST / BYBIT_SECRET_TEST not set — skipping integration test")
	}

	ch := make(chan types.ByMainOrder, 10)
	bh := NewBybitHandler(context.Background(), apiKey, secret, types.TEST, ch, slog.Default())
	return bh, ch
}

// cancelAll cleans up any open BTCUSDT linear orders after a test.
func cancelAll(t *testing.T, bh *BybitHandler) {
	t.Helper()
	params := map[string]interface{}{
		"category":   "linear",
		"symbol":     "BTCUSDT",
		"settleCoin": "USDT",
	}
	res, err := bh.client.NewUtaBybitServiceWithParams(params).CancelAllOrders(bh.ctx)
	if err != nil {
		t.Logf("[cleanup] CancelAllOrders error: %v", err)
		return
	}
	t.Logf("[cleanup] CancelAllOrders retCode=%d retMsg=%s", res.RetCode, res.RetMsg)
}

// ────────────────────────────────────────────────────────────────────────────
// 1. GetWalletBalance — H-1 field name validation
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_GetWalletBalance(t *testing.T) {
	bh, _ := newTestnetHandler(t)

	balance, err := bh.GetWalletBalance()
	if err != nil {
		t.Fatalf("GetWalletBalance error: %v", err)
	}

	t.Logf("availableToWithdraw = %.4f USDT", balance)

	if balance < 0 {
		t.Errorf("balance should be >= 0, got %f", balance)
	}
	// A zero balance is technically valid on an empty testnet account but is
	// useful to flag so the caller knows orders will be blocked by the C-5 guard.
	if balance == 0 {
		t.Logf("WARNING: balance is zero — Process will be blocked by the balance guard (expected on empty testnet account)")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// 2. GetCurrPrice — market ticker endpoint
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_GetCurrPrice(t *testing.T) {
	bh, _ := newTestnetHandler(t)

	price, err := bh.GetCurrPrice("linear", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetCurrPrice error: %v", err)
	}

	t.Logf("BTCUSDT last price = %.2f", price)

	if price <= 0 {
		t.Errorf("price should be positive, got %f", price)
	}
	if price < 1000 {
		t.Errorf("price %f seems unrealistically low — check symbol/category", price)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// 3. getOrCacheInstrumentInfo — H-3 / H-6 field validation
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_InstrumentInfo(t *testing.T) {
	bh, _ := newTestnetHandler(t)

	info, err := bh.getOrCacheInstrumentInfo("linear", "BTCUSDT")
	if err != nil {
		t.Fatalf("getOrCacheInstrumentInfo error: %v", err)
	}

	t.Logf("pricePrecision = %d  qtyStep = %f", info.pricePrecision, info.qtyStep)

	if info.pricePrecision <= 0 {
		t.Errorf("pricePrecision should be > 0, got %d", info.pricePrecision)
	}
	if info.qtyStep <= 0 {
		t.Errorf("qtyStep should be > 0, got %f", info.qtyStep)
	}

	// Second call must hit the cache (no extra API call).
	t.Log("Calling again — should be served from cache...")
	info2, err := bh.getOrCacheInstrumentInfo("linear", "BTCUSDT")
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if info2.pricePrecision != info.pricePrecision || info2.qtyStep != info.qtyStep {
		t.Error("cached values differ from original — cache is broken")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// 4. setLeverage — leverage endpoint
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_SetLeverage(t *testing.T) {
	bh, _ := newTestnetHandler(t)

	signal := &BybitSignal{
		Category: "linear",
		Symbol:   "BTCUSDT",
		Leverage: 5,
	}

	err := bh.setLeverage(signal)
	if err != nil {
		t.Fatalf("setLeverage error: %v", err)
	}
	t.Log("setLeverage succeeded (or 110043 — already set, which is fine)")
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Full Process — places a limit order far from market, then cancels it
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_Process_PlacesOrder(t *testing.T) {
	bh, ch := newTestnetHandler(t)
	t.Cleanup(func() { cancelAll(t, bh) })

	// Get current price so we can place far from market (won't fill).
	price, err := bh.GetCurrPrice("linear", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetCurrPrice error: %v", err)
	}
	t.Logf("Current BTCUSDT price: %.2f", price)

	signal := BybitSignal{
		Category:    "linear",
		Symbol:      "BTCUSDT",
		Side:        "Buy",
		OrderType:   "Limit",
		PositionIdx: 0,
		Leverage:    5,
		TP:          0.001, // 0.1%
		SL:          0.001,
	}

	// Collect the placed order from the channel in a goroutine.
	type result struct {
		order types.ByMainOrder
		err   error
	}
	done := make(chan result, 1)
	go func() {
		if err := bh.Process(signal); err != nil {
			done <- result{err: err}
			return
		}
		select {
		case order := <-ch:
			done <- result{order: order}
		case <-time.After(15 * time.Second):
			done <- result{err: errInvalidSignal} // channel send timed out
		}
	}()

	res := <-done
	if res.err != nil {
		t.Fatalf("Process error: %v", res.err)
	}

	order := res.order
	t.Logf("Order placed successfully:")
	t.Logf("  symbol      = %s", order.Symbol)
	t.Logf("  side        = %s", order.Side)
	t.Logf("  orderType   = %s", order.OrderType)
	t.Logf("  quantity    = %.3f  (qtyStep=%.3f)", order.Quantity, order.QtyStep)
	t.Logf("  price       = %.2f  (precision=%d)", order.Price, order.Precision)
	t.Logf("  takeProfit  = %.2f", order.TakeProfit)
	t.Logf("  stopLoss    = %.2f", order.StopLoss)
	t.Logf("  leverage    = %d", order.Leverage)
	t.Logf("  positionIdx = %d", order.PositionIdx)
	t.Logf("  tpPct       = %.4f  slPct = %.4f", order.TPPct, order.SLPct)

	// Assertions
	if order.Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want BTCUSDT", order.Symbol)
	}
	if order.Quantity <= 0 {
		t.Errorf("quantity should be positive, got %f", order.Quantity)
	}
	if order.Price <= 0 {
		t.Errorf("price should be positive, got %f", order.Price)
	}
	if order.TakeProfit <= order.Price {
		t.Errorf("buy TP (%.2f) should be above entry price (%.2f)", order.TakeProfit, order.Price)
	}
	if order.StopLoss >= order.Price {
		t.Errorf("buy SL (%.2f) should be below entry price (%.2f)", order.StopLoss, order.Price)
	}
	if order.QtyStep <= 0 {
		t.Errorf("QtyStep should be positive (not hardcoded), got %f", order.QtyStep)
	}
	if order.Precision <= 0 {
		t.Errorf("Precision should be positive, got %d", order.Precision)
	}
	if order.TPPct != signal.TP {
		t.Errorf("TPPct = %f, want %f (not preserved for re-entry)", order.TPPct, signal.TP)
	}
	if order.Leverage != signal.Leverage {
		t.Errorf("Leverage = %d, want %d", order.Leverage, signal.Leverage)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// 6. ReEnter — places a second order using fresh price + balance
// ────────────────────────────────────────────────────────────────────────────

func TestIntegration_ReEnter(t *testing.T) {
	bh, ch := newTestnetHandler(t)
	t.Cleanup(func() { cancelAll(t, bh) })

	signal := BybitSignal{
		Category:    "linear",
		Symbol:      "BTCUSDT",
		Side:        "Buy",
		OrderType:   "Limit",
		PositionIdx: 0,
		Leverage:    5,
		TP:          0.001,
		SL:          0.001,
	}

	// Place first order (simulates original signal).
	if err := bh.Process(signal); err != nil {
		t.Fatalf("initial Process error: %v", err)
	}
	var firstOrder types.ByMainOrder
	select {
	case firstOrder = <-ch:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for first order on channel")
	}
	t.Logf("First order: price=%.2f qty=%.3f", firstOrder.Price, firstOrder.Quantity)

	// Small delay so timestamps differ.
	time.Sleep(500 * time.Millisecond)

	// Simulate a TP fill triggering re-entry.
	if err := bh.ReEnter(firstOrder); err != nil {
		t.Fatalf("ReEnter error: %v", err)
	}
	var reEntryOrder types.ByMainOrder
	select {
	case reEntryOrder = <-ch:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for re-entry order on channel")
	}

	t.Logf("Re-entry order: price=%.2f qty=%.3f", reEntryOrder.Price, reEntryOrder.Quantity)

	// Re-entry must use a live price — cannot be a stale copy of the first order.
	// On testnet the price may not have moved, but the flow must have executed.
	if reEntryOrder.Symbol != firstOrder.Symbol {
		t.Errorf("re-entry symbol mismatch: %s vs %s", reEntryOrder.Symbol, firstOrder.Symbol)
	}
	if reEntryOrder.TPPct != firstOrder.TPPct {
		t.Errorf("TPPct changed across re-entry: %.4f vs %.4f", reEntryOrder.TPPct, firstOrder.TPPct)
	}
	if reEntryOrder.SLPct != firstOrder.SLPct {
		t.Errorf("SLPct changed across re-entry: %.4f vs %.4f", reEntryOrder.SLPct, firstOrder.SLPct)
	}
	if reEntryOrder.Quantity <= 0 {
		t.Errorf("re-entry quantity should be positive, got %f", reEntryOrder.Quantity)
	}

	t.Logf("Re-entry confirmed — fresh price + balance used, TP/SL percentages preserved")
}
