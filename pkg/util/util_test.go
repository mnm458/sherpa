package util

import (
	"math"
	"testing"
)

func TestRoundToDecimals(t *testing.T) {
	cases := []struct {
		name     string
		num      float64
		decimals int64
		want     float64
	}{
		{"no rounding needed", 1.23, 2, 1.23},
		{"rounds up", 1.235, 2, 1.24},
		{"rounds down", 1.234, 2, 1.23},
		{"zero decimals", 1.7, 0, 2.0},
		{"precision=1", 50001.567, 1, 50001.6},
		{"precision=2 BTC price", 50001.567, 2, 50001.57},
		{"negative number", -1.235, 2, -1.24},
		{"zero", 0.0, 4, 0.0},
		{"large price precision=2", 99999.999, 2, 100000.00},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RoundToDecimals(tc.num, tc.decimals)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("RoundToDecimals(%v, %d) = %v, want %v", tc.num, tc.decimals, got, tc.want)
			}
		})
	}
}
