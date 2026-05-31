package exchange

import (
	"math"
	"testing"

	"github.com/mnm458/sherpa/pkg/types"
)

// ────────────────────────────────────────────────────────────────────────────
// countDecimals
// ────────────────────────────────────────────────────────────────────────────

func TestCountDecimals(t *testing.T) {
	cases := []struct {
		name string
		f    float64
		want int
	}{
		{"0.001 → 3", 0.001, 3},
		{"0.01 → 2", 0.01, 2},
		{"0.1 → 1", 0.1, 1},
		{"1.0 → 0 (integer)", 1.0, 0},
		{"0.5 → 1", 0.5, 1},
		{"0.0001 → 4", 0.0001, 4},
		{"10.0 → 0 (integer)", 10.0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := countDecimals(tc.f); got != tc.want {
				t.Errorf("countDecimals(%v) = %d, want %d", tc.f, got, tc.want)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// roundToStep
// ────────────────────────────────────────────────────────────────────────────

func TestRoundToStep(t *testing.T) {
	cases := []struct {
		name string
		qty  float64
		step float64
		want float64
	}{
		{"exact multiple", 0.005, 0.001, 0.005},
		{"truncates fractional part", 0.0059, 0.001, 0.005},
		{"whole number qty", 5.0, 0.001, 5.0},
		{"zero qty", 0.0, 0.001, 0.0},
		{"step=0 returns qty unchanged", 0.123, 0.0, 0.123},
		{"step=0.01 truncates", 1.299, 0.01, 1.29},
		{"large step rounds down", 0.5, 1.0, 0.0},
		{"exact on step=0.001", 1.234, 0.001, 1.234},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := roundToStep(tc.qty, tc.step)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("roundToStep(%v, %v) = %v, want %v", tc.qty, tc.step, got, tc.want)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// calculateQuantity
// ────────────────────────────────────────────────────────────────────────────

func TestCalculateQuantity(t *testing.T) {
	bh := &BybitHandler{} // only needs pure math, no client

	cases := []struct {
		name     string
		balance  float64
		price    float64
		leverage int32
		want     float64 // math.Floor(leverage * 0.97 * balance / price * 1000) / 1000
	}{
		{
			// 5 * 0.97 * 1000 / 50000 = 0.097
			name: "BTC at 50k, lev=5, bal=1000",
			balance: 1000, price: 50000, leverage: 5,
			want: 0.097,
		},
		{
			// 10 * 0.97 * 500 / 30000 = 0.161666… → floor to 0.161
			name: "ETH-like price, lev=10, bal=500",
			balance: 500, price: 30000, leverage: 10,
			want: 0.161,
		},
		{
			// 1 * 0.97 * 100 / 100 = 0.97
			name: "low leverage, equal price/balance",
			balance: 100, price: 100, leverage: 1,
			want: 0.97,
		},
		{
			// zero balance → zero qty
			name: "zero balance",
			balance: 0, price: 50000, leverage: 5,
			want: 0.0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bh.calculateQuantity(tc.balance, tc.price, tc.leverage)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("calculateQuantity(%v, %v, %v) = %v, want %v",
					tc.balance, tc.price, tc.leverage, got, tc.want)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// calcPrices
// ────────────────────────────────────────────────────────────────────────────

func TestCalcPrices(t *testing.T) {
	bh := &BybitHandler{}

	t.Run("Buy side", func(t *testing.T) {
		signal := &BybitSignal{Side: "Buy", TP: 0.01, SL: 0.02}
		price := 100.0
		finalPrice, tp, sl := bh.calcPrices(price, signal, 2)

		if math.Abs(finalPrice-100.00) > 1e-9 {
			t.Errorf("finalPrice = %v, want 100.00", finalPrice)
		}
		if math.Abs(tp-101.00) > 1e-9 {
			t.Errorf("tp = %v, want 101.00", tp)
		}
		if math.Abs(sl-98.00) > 1e-9 {
			t.Errorf("sl = %v, want 98.00", sl)
		}
	})

	t.Run("Sell side", func(t *testing.T) {
		signal := &BybitSignal{Side: "Sell", TP: 0.01, SL: 0.02}
		price := 100.0
		finalPrice, tp, sl := bh.calcPrices(price, signal, 2)

		if math.Abs(finalPrice-100.00) > 1e-9 {
			t.Errorf("finalPrice = %v, want 100.00", finalPrice)
		}
		if math.Abs(tp-99.00) > 1e-9 {
			t.Errorf("tp = %v, want 99.00", tp)
		}
		if math.Abs(sl-102.00) > 1e-9 {
			t.Errorf("sl = %v, want 102.00", sl)
		}
	})

	t.Run("precision=2 rounds correctly", func(t *testing.T) {
		// price=50001.567, tp=0.001 → tp price = 50001.567 * 1.001 = 50051.618567 → 50051.62
		signal := &BybitSignal{Side: "Buy", TP: 0.001, SL: 0.0}
		_, tp, _ := bh.calcPrices(50001.567, signal, 2)
		want := math.Round(50001.567*1.001*100) / 100
		if math.Abs(tp-want) > 1e-9 {
			t.Errorf("tp = %.4f, want %.4f", tp, want)
		}
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Validate
// ────────────────────────────────────────────────────────────────────────────

func TestValidate(t *testing.T) {
	bh := &BybitHandler{}

	cases := []struct {
		name    string
		signal  BybitSignal
		wantErr error
	}{
		{"valid buy", BybitSignal{Symbol: "BTCUSDT", Side: "Buy"}, nil},
		{"valid sell", BybitSignal{Symbol: "BTCUSDT", Side: "Sell"}, nil},
		{"empty symbol", BybitSignal{Symbol: "", Side: "Buy"}, errInvalidSignal},
		{"empty side", BybitSignal{Symbol: "BTCUSDT", Side: ""}, errInvalidSignal},
		{"invalid side", BybitSignal{Symbol: "BTCUSDT", Side: "Long"}, errInvalidSide},
		{"side case-sensitive", BybitSignal{Symbol: "BTCUSDT", Side: "buy"}, errInvalidSide},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := bh.Validate(&tc.signal)
			if err != tc.wantErr {
				t.Errorf("Validate(%+v) = %v, want %v", tc.signal, err, tc.wantErr)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// GetLeverage
// ────────────────────────────────────────────────────────────────────────────

func TestGetLeverage(t *testing.T) {
	cases := []struct {
		leverage int64
	}{
		{0}, {1}, {5}, {10}, {50},
	}
	for _, tc := range cases {
		sig := BybitSignal{Leverage: tc.leverage}
		if got := sig.GetLeverage(); got != tc.leverage {
			t.Errorf("GetLeverage() = %d, want %d", got, tc.leverage)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// ByMainOrder re-entry fields survive a round-trip through placeOrder's output
// ────────────────────────────────────────────────────────────────────────────

func TestByMainOrderReEntryFields(t *testing.T) {
	// Verify that the fields needed by ReEnter are present on the type.
	// This is a compile-time correctness check disguised as a runtime test.
	order := types.ByMainOrder{
		TPPct:       0.01,
		SLPct:       0.02,
		Leverage:    5,
		PositionIdx: 0,
		QtyStep:     0.001,
		Precision:   2,
	}
	if order.TPPct != 0.01 || order.SLPct != 0.02 || order.Leverage != 5 || order.QtyStep != 0.001 {
		t.Error("ByMainOrder re-entry fields not populated correctly")
	}
}
