package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/mnm458/sherpa/pkg/types"
)

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

// streamApp builds a minimal application suitable for bystream tests.
// No exchange client is required — handler is left nil for paths that don't
// call ReEnter.
func streamApp() *application {
	return &application{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// ────────────────────────────────────────────────────────────────────────────
// handleBybitMessage — control-message routing
// ────────────────────────────────────────────────────────────────────────────

func TestHandleBybitMessage_TextPong(t *testing.T) {
	a := streamApp()
	// A plain "pong" string must return immediately without panicking.
	a.handleBybitMessage("pong", nil)
	// If we reach here without panic, the test passes.
}

func TestHandleBybitMessage_JSONPong(t *testing.T) {
	a := streamApp()
	a.handleBybitMessage(`{"op":"pong"}`, nil)
}

func TestHandleBybitMessage_InvalidJSON_Logged(t *testing.T) {
	a := streamApp()
	// Must not panic on invalid JSON — the error is logged internally.
	a.handleBybitMessage(`{not valid json`, nil)
}

func TestHandleBybitMessage_UnknownOp_NoStateChange(t *testing.T) {
	a := streamApp()
	a.handleBybitMessage(`{"op":"someUnknownOp"}`, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.wsAuthenticated {
		t.Error("wsAuthenticated should remain false for unknown op")
	}
}

func TestHandleBybitMessage_AuthSuccess(t *testing.T) {
	a := streamApp()
	a.handleBybitMessage(`{"op":"auth","success":true}`, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if !a.wsAuthenticated {
		t.Error("expected wsAuthenticated=true after successful auth")
	}
}

func TestHandleBybitMessage_AuthFailed(t *testing.T) {
	a := streamApp()
	// Prime to true so we can verify it flips back to false.
	a.stateMu.Lock()
	a.wsAuthenticated = true
	a.stateMu.Unlock()

	a.handleBybitMessage(`{"op":"auth","success":false,"ret_msg":"Invalid signature"}`, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.wsAuthenticated {
		t.Error("expected wsAuthenticated=false after failed auth")
	}
}

func TestHandleBybitMessage_SubscribeSuccess_NoStateChange(t *testing.T) {
	a := streamApp()
	a.handleBybitMessage(`{"op":"subscribe","success":true}`, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	// A subscribe ack must not touch wsAuthenticated.
	if a.wsAuthenticated {
		t.Error("subscribe ack must not set wsAuthenticated")
	}
}

func TestHandleBybitMessage_SubscribeFailed_NoStateChange(t *testing.T) {
	a := streamApp()
	a.handleBybitMessage(`{"op":"subscribe","success":false,"ret_msg":"not authorized"}`, nil)
}

// ────────────────────────────────────────────────────────────────────────────
// receive — order update routing
// ────────────────────────────────────────────────────────────────────────────

func TestReceive_SLTriggered_ClearsCurrByMainOrder(t *testing.T) {
	// A StopLoss Triggered event must reset CurrByMainOrder to zero value.
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT", Side: "Buy", Leverage: 5}
	a.stateMu.Unlock()

	orders := []OrderUpdate{
		{
			OrderID:    "sl-order-id",
			Status:     STATUS_TRIGGERED,
			Side:       "Sell",
			CreateType: CREATE_TYPE_SL,
		},
	}
	data, _ := json.Marshal(orders)

	// nil handler is safe here — SL path does not call handler.ReEnter.
	a.receive(data, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "" {
		t.Errorf("expected CurrByMainOrder to be cleared, got Symbol=%q", a.CurrByMainOrder.Symbol)
	}
}

func TestReceive_SLNotTriggered_NoStateChange(t *testing.T) {
	// A StopLoss that is NOT "Triggered" must not clear the position.
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT"}
	a.stateMu.Unlock()

	orders := []OrderUpdate{
		{CreateType: CREATE_TYPE_SL, Status: "PartiallyFilled"},
	}
	data, _ := json.Marshal(orders)
	a.receive(data, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "BTCUSDT" {
		t.Error("CurrByMainOrder should not be cleared for non-Triggered SL")
	}
}

func TestReceive_UnknownCreateType_NoStateChange(t *testing.T) {
	// An order with an unrecognised createType must be a no-op.
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT"}
	a.stateMu.Unlock()

	orders := []OrderUpdate{
		{CreateType: "CreateBySomeOtherMechanism", Status: STATUS_FILLED},
	}
	data, _ := json.Marshal(orders)
	a.receive(data, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "BTCUSDT" {
		t.Error("unexpected state change for unknown createType")
	}
}

func TestReceive_InvalidJSON_NoStateChange(t *testing.T) {
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT"}
	a.stateMu.Unlock()

	a.receive([]byte(`[{invalid json}]`), nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "BTCUSDT" {
		t.Error("CurrByMainOrder must not change on invalid order JSON")
	}
}

func TestReceive_EmptyOrderList_NoStateChange(t *testing.T) {
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT"}
	a.stateMu.Unlock()

	a.receive([]byte(`[]`), nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "BTCUSDT" {
		t.Error("empty order list must not change state")
	}
}

func TestReceive_MultipleOrders_SLClearsState(t *testing.T) {
	// When multiple orders arrive in one batch, the SL triggered one must clear state.
	a := streamApp()
	a.stateMu.Lock()
	a.CurrByMainOrder = types.ByMainOrder{Symbol: "BTCUSDT"}
	a.stateMu.Unlock()

	orders := []OrderUpdate{
		{CreateType: "CreateBySomeOtherMechanism", Status: STATUS_FILLED},
		{CreateType: CREATE_TYPE_SL, Status: STATUS_TRIGGERED},
	}
	data, _ := json.Marshal(orders)
	a.receive(data, nil)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.CurrByMainOrder.Symbol != "" {
		t.Error("SL triggered in batch must clear position state")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// OrderUpdate constants
// ────────────────────────────────────────────────────────────────────────────

func TestOrderUpdateConstants(t *testing.T) {
	// Guard against typos in the string constants — these must match Bybit's API.
	if CREATE_TYPE_MAIN != "CreateByUser" {
		t.Errorf("CREATE_TYPE_MAIN = %q, want \"CreateByUser\"", CREATE_TYPE_MAIN)
	}
	if CREATE_TYPE_TP != "CreateByTakeProfit" {
		t.Errorf("CREATE_TYPE_TP = %q, want \"CreateByTakeProfit\"", CREATE_TYPE_TP)
	}
	if CREATE_TYPE_SL != "CreateByStopLoss" {
		t.Errorf("CREATE_TYPE_SL = %q, want \"CreateByStopLoss\"", CREATE_TYPE_SL)
	}
	if STATUS_FILLED != "Filled" {
		t.Errorf("STATUS_FILLED = %q, want \"Filled\"", STATUS_FILLED)
	}
	if STATUS_TRIGGERED != "Triggered" {
		t.Errorf("STATUS_TRIGGERED = %q, want \"Triggered\"", STATUS_TRIGGERED)
	}
}
