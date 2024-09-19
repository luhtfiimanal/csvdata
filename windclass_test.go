package csvdata_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/luhtfiimanal/csvdata"
)

func TestClassifyWindDirection(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{0, 0},       // North
		{10, 0},      // North
		{22.5, 45},   // North-East
		{45, 45},     // North-East
		{67.5, 90},   // East
		{90, 90},     // East
		{112.5, 135}, // South-East
		{135, 135},   // South-East
		{157.5, 180}, // South
		{180, 180},   // South
		{202.5, 225}, // South-West
		{225, 225},   // South-West
		{247.5, 270}, // West
		{270, 270},   // West
		{292.5, 315}, // North-West
		{315, 315},   // North-West
		{360, 0},     // Wrap around to North
		{400, 45},    // Above 360 should wrap to North-East
		{-30, 315},   // Negative angle wraps to North-West
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input: %v", tt.input), func(t *testing.T) {
			result := csvdata.ClassifyWindDirection(tt.input)
			if math.Abs(result-tt.expected) > 1e-9 { // Allowing a small error margin for float comparison
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
