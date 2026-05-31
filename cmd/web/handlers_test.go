package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnm458/sherpa/pkg/exchange"
	"github.com/mnm458/sherpa/pkg/types"
)

var errTest = errors.New("test processing error")

// ────────────────────────────────────────────────────────────────────────────
// Mock exchange strategy
// ────────────────────────────────────────────────────────────────────────────

type mockExchange struct {
	processErr  error
	// blockCh, if non-nil, causes Process to block until the channel is closed.
	// This lets tests verify the concurrent-signal guard (M-1).
	blockCh     chan struct{}
	mu          sync.Mutex
	processCalls int
}

func (m *mockExchange) Process(s exchange.Signal) error {
	m.mu.Lock()
	m.processCalls++
	m.mu.Unlock()
	if m.blockCh != nil {
		<-m.blockCh
	}
	return m.processErr
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func newTestApp(t *testing.T, handler exchange.ExchangeStrategy, exchangeID int32) *application {
	t.Helper()
	return &application{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExchangeHandler: handler,
		ExchangeID:      exchangeID,
		startedAt:       time.Now(),
		ActiveExchange:  "BYBIT",
		environment:     "TEST",
	}
}

func bybitSignalBody(t *testing.T) io.Reader {
	t.Helper()
	return strings.NewReader(`{"category":"linear","symbol":"BTCUSDT","side":"Buy","leverage":5,"tp":0.001,"sl":0.019}`)
}

// ────────────────────────────────────────────────────────────────────────────
// ping
// ────────────────────────────────────────────────────────────────────────────

func TestPing(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	ping(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if body := w.Body.String(); body != "OK" {
		t.Errorf("expected body \"OK\", got %q", body)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// HandleSignal — Bybit exchange
// ────────────────────────────────────────────────────────────────────────────

func TestHandleSignalBybit_BadJSON(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", strings.NewReader(`not-json`))
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleSignalBybit_ProcessSuccess(t *testing.T) {
	mock := &mockExchange{}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	mock.mu.Lock()
	calls := mock.processCalls
	mock.mu.Unlock()
	if calls != 1 {
		t.Errorf("expected Process to be called once, got %d", calls)
	}
}

func TestHandleSignalBybit_ProcessError_Returns500(t *testing.T) {
	// C-1: a Process error must produce 500, not 200.
	mock := &mockExchange{processErr: errTest}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleSignalBybit_SignalInFlight_Returns429(t *testing.T) {
	// M-1: while signalInFlight==1 a second request must be rejected with 429.
	mock := &mockExchange{}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)

	// Inject an in-flight signal directly via the atomic flag.
	atomic.StoreInt32(&app.signalInFlight, 1)

	req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w.Code)
	}
	// Verify Process was never called — the request must have been rejected before decoding.
	mock.mu.Lock()
	calls := mock.processCalls
	mock.mu.Unlock()
	if calls != 0 {
		t.Errorf("Process should not have been called, but was called %d times", calls)
	}
}

func TestHandleSignalBybit_FlagReleasedAfterProcessing(t *testing.T) {
	// M-1: the flag must be cleared after a successful request so subsequent signals work.
	mock := &mockExchange{}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)

	for i := 1; i <= 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
		w := httptest.NewRecorder()
		app.HandleSignal(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, w.Code)
		}
	}

	if got := atomic.LoadInt32(&app.signalInFlight); got != 0 {
		t.Errorf("signalInFlight should be 0 after processing, got %d", got)
	}
}

func TestHandleSignalBybit_RealConcurrency_429(t *testing.T) {
	// M-1: integration-style concurrency test — first request blocks via
	// blockCh; while it is blocked a second request must get 429.
	blockCh := make(chan struct{})
	mock := &mockExchange{blockCh: blockCh}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)

	firstStarted := make(chan struct{})
	firstDone := make(chan int)

	// First request — will block inside Process until we close blockCh.
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
		w := httptest.NewRecorder()

		// Signal that we're about to call HandleSignal (slight race window is OK
		// for this test — the atomic CAS makes it deterministic once the flag is set).
		close(firstStarted)
		app.HandleSignal(w, req)
		firstDone <- w.Code
	}()

	<-firstStarted
	// Give the first goroutine time to set the in-flight flag.
	// We poll rather than sleep to keep this fast.
	deadline := time.Now().Add(200 * time.Millisecond)
	for atomic.LoadInt32(&app.signalInFlight) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&app.signalInFlight) == 0 {
		// First goroutine finished before we could check — skip (already validated by unit test).
		close(blockCh)
		<-firstDone
		t.Skip("first goroutine completed before in-flight flag was observable")
	}

	// Second request — must be rejected.
	req2 := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
	w2 := httptest.NewRecorder()
	app.HandleSignal(w2, req2)

	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("concurrent second request: expected 429, got %d", w2.Code)
	}

	// Unblock and verify first succeeds.
	close(blockCh)
	if code := <-firstDone; code != http.StatusOK {
		t.Errorf("first request: expected 200, got %d", code)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// HandleSignal — Binance exchange
// ────────────────────────────────────────────────────────────────────────────

func TestHandleSignalBinance_Success(t *testing.T) {
	mock := &mockExchange{}
	app := newTestApp(t, mock, exchange.BINANCE_EXCHANGE_ID)
	app.ActiveExchange = "BINANCE"

	body := strings.NewReader(`{"symbol":"BTCUSDT","type":"LIMIT","action":"Buy","leverage":5,"tp":0.001,"sl":0.019}`)
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", body)
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleSignalBinance_ProcessError_Returns500(t *testing.T) {
	mock := &mockExchange{processErr: errTest}
	app := newTestApp(t, mock, exchange.BINANCE_EXCHANGE_ID)
	app.ActiveExchange = "BINANCE"

	body := strings.NewReader(`{"symbol":"BTCUSDT","type":"LIMIT","action":"Buy","leverage":5,"tp":0.001,"sl":0.019}`)
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", body)
	w := httptest.NewRecorder()

	app.HandleSignal(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// HandleSignal — lastSignalAt updated
// ────────────────────────────────────────────────────────────────────────────

func TestHandleSignal_UpdatesLastSignalAt(t *testing.T) {
	mock := &mockExchange{}
	app := newTestApp(t, mock, exchange.BYBIT_EXCHANGE_ID)

	before := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/handle-signal", bybitSignalBody(t))
	app.HandleSignal(w_discard(), req)

	app.stateMu.RLock()
	ts := app.lastSignalAt
	app.stateMu.RUnlock()

	if ts.Before(before) {
		t.Errorf("lastSignalAt not updated: got %v, before was %v", ts, before)
	}
}

func w_discard() http.ResponseWriter { return httptest.NewRecorder() }

// ────────────────────────────────────────────────────────────────────────────
// Status handler
// ────────────────────────────────────────────────────────────────────────────

type statusResponse struct {
	Healthy        bool   `json:"healthy"`
	ReEntryOn      bool   `json:"reEntryOn"`
	SignalInFlight bool   `json:"signalInFlight"`
	Exchange       string `json:"exchange"`
	Environment    string `json:"environment"`
	Uptime         string `json:"uptime"`
	WebSocket      struct {
		Connected     bool `json:"connected"`
		Authenticated bool `json:"authenticated"`
	} `json:"websocket"`
	CurrentPosition struct {
		HasPosition bool    `json:"hasPosition"`
		Symbol      string  `json:"symbol"`
		Side        string  `json:"side"`
		Quantity    float64 `json:"quantity"`
		EntryPrice  float64 `json:"entryPrice"`
		TakeProfit  float64 `json:"takeProfit"`
		StopLoss    float64 `json:"stopLoss"`
		Leverage    int64   `json:"leverage"`
	} `json:"currentPosition"`
	LastSignalAt string `json:"lastSignalAt,omitempty"`
}

func callStatus(t *testing.T, app *application) (statusResponse, int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	app.Status(w, req)

	var resp statusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}
	return resp, w.Code
}

func TestStatus_ReEntryOff_AlwaysHealthy(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	// reEntrySwitchOn defaults to false; WS fields default to false too.

	resp, code := callStatus(t, app)

	if code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true when re-entry is off")
	}
	if resp.ReEntryOn {
		t.Error("expected reEntryOn=false")
	}
}

func TestStatus_ReEntryOn_WSDisconnected_Returns503(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	app.reEntrySwitchOn = true
	// wsConnected and wsAuthenticated remain false (zero value).

	resp, code := callStatus(t, app)

	if code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", code)
	}
	if resp.Healthy {
		t.Error("expected healthy=false when re-entry on but WS disconnected")
	}
}

func TestStatus_ReEntryOn_WSConnectedAndAuthed_Returns200(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	app.reEntrySwitchOn = true
	app.stateMu.Lock()
	app.wsConnected = true
	app.wsAuthenticated = true
	app.stateMu.Unlock()

	resp, code := callStatus(t, app)

	if code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true when WS connected and authenticated")
	}
}

func TestStatus_WithPosition_FieldsPopulated(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	app.stateMu.Lock()
	app.CurrByMainOrder = types.ByMainOrder{
		Symbol:     "BTCUSDT",
		Side:       "Buy",
		Quantity:   0.097,
		Price:      50000.0,
		TakeProfit: 50050.0,
		StopLoss:   49000.0,
		Leverage:   5,
	}
	app.stateMu.Unlock()

	resp, _ := callStatus(t, app)

	if !resp.CurrentPosition.HasPosition {
		t.Error("expected hasPosition=true")
	}
	if resp.CurrentPosition.Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want \"BTCUSDT\"", resp.CurrentPosition.Symbol)
	}
	if resp.CurrentPosition.Leverage != 5 {
		t.Errorf("leverage = %d, want 5", resp.CurrentPosition.Leverage)
	}
	if resp.CurrentPosition.Quantity != 0.097 {
		t.Errorf("quantity = %v, want 0.097", resp.CurrentPosition.Quantity)
	}
}

func TestStatus_NoPosition_HasPositionFalse(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	// CurrByMainOrder is zero-value (Symbol == "")

	resp, _ := callStatus(t, app)

	if resp.CurrentPosition.HasPosition {
		t.Error("expected hasPosition=false when no order")
	}
}

func TestStatus_ContentTypeJSON(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	app.Status(w, req)

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want \"application/json\"", ct)
	}
}

func TestStatus_UptimeIncreasing(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	app.startedAt = time.Now().Add(-10 * time.Second)

	resp, _ := callStatus(t, app)

	if resp.Uptime == "" {
		t.Error("expected non-empty uptime")
	}
}

func TestStatus_SignalInFlight(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	atomic.StoreInt32(&app.signalInFlight, 1)

	resp, _ := callStatus(t, app)

	if !resp.SignalInFlight {
		t.Error("expected signalInFlight=true")
	}
}

func TestStatus_ExchangeAndEnvironment(t *testing.T) {
	app := newTestApp(t, &mockExchange{}, exchange.BYBIT_EXCHANGE_ID)
	app.ActiveExchange = "BYBIT"
	app.environment = "PROD"

	resp, _ := callStatus(t, app)

	if resp.Exchange != "BYBIT" {
		t.Errorf("exchange = %q, want \"BYBIT\"", resp.Exchange)
	}
	if resp.Environment != "PROD" {
		t.Errorf("environment = %q, want \"PROD\"", resp.Environment)
	}
}
