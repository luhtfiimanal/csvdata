package csvdata_test

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/luhtfiimanal/csvdata"
)

func TestDataMean(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	timeResultEp := []int64{2, 6, 10, 14, 18, 22}

	reqcolumn := csvdata.SAColumn{
		OutputColumnName: "dewpoint_avg",
		WindowRelative:   [][2]int64{{2, 3}, {4, 6}, {9, 10}, {11, 13}, {14, 16}, {17, 19}},
		TimeResultEp:     &timeResultEp,
		Result:           make([]float64, len(timeResultEp)),
	}

	// Initiate the SmartAggregator
	sa := csvdata.NewSmartAggregator(csvdata.MEAN, &reqcolumn, &wg)

	expected := []float64{2.5, 5.0, 9.5, 12.0, 15.0, 18.0}

	data := []csvdata.Input{
		{Epoch: 0, Value: 0},
		{Epoch: 1, Value: 100},
		{Epoch: 2, Value: 2},
		{Epoch: 3, Value: 3},
		{Epoch: 4, Value: 4},
		{Epoch: 5, Value: 5},
		{Epoch: 6, Value: 6},
		{Epoch: 7, Value: 7},
		{Epoch: 8, Value: 8},
		{Epoch: 9, Value: 9},
		{Epoch: 10, Value: 10},
		{Epoch: 11, Value: 11},
		{Epoch: 12, Value: 12},
		{Epoch: 13, Value: 13},
		{Epoch: 14, Value: 14},
		{Epoch: 15, Value: 15},
		{Epoch: 16, Value: 16},
		{Epoch: 17, Value: 17},
		{Epoch: 18, Value: 18},
		{Epoch: 19, Value: 19},
		{Epoch: 20, Value: 20},
	}

	// Pass the data to the SmartAggregator
	go func() {
		for _, d := range data {
			sa.Data <- d
		}
		// Close the channel
		close(sa.Data)
	}()

	wg.Wait()

	fmt.Print(sa.Column.Result)

	for i, v := range sa.Column.Result {
		if v != expected[i] {
			t.Errorf("got %v, want %v", v, expected[i])
		}
	}
}

func TestDataPick(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	timeResultEp := []int64{2, 6, 10, 14, 18, 22}

	reqcolumn := csvdata.SAColumn{
		OutputColumnName: "dewpoint_pick",
		PickRelative:     []int64{2, 6, 10, 14, 18, 22},
		TimeResultEp:     &timeResultEp,
		Result:           make([]float64, len(timeResultEp)),
	}

	// Initiate the SmartAggregator
	sa := csvdata.NewSmartAggregator(csvdata.PICK, &reqcolumn, &wg)

	expected := []float64{2, 6, 10, 14, 18, 21}

	data := []csvdata.Input{
		{Epoch: 0, Value: 0},
		{Epoch: 1, Value: 100},
		{Epoch: 2, Value: 2},
		{Epoch: 3, Value: 3},
		{Epoch: 4, Value: 4},
		{Epoch: 5, Value: 5},
		{Epoch: 6, Value: 6},
		{Epoch: 7, Value: 7},
		{Epoch: 8, Value: 8},
		{Epoch: 9, Value: 9},
		{Epoch: 10, Value: 10},
		{Epoch: 11, Value: 11},
		{Epoch: 12, Value: 12},
		{Epoch: 13, Value: 13},
		{Epoch: 14, Value: 14},
		{Epoch: 15, Value: 15},
		{Epoch: 16, Value: 16},
		{Epoch: 17, Value: 17},
		{Epoch: 18, Value: 18},
		{Epoch: 19, Value: 19},
		{Epoch: 20, Value: 20},
		{Epoch: 21, Value: 21},
	}

	// Pass the data to the SmartAggregator
	go func() {
		for _, d := range data {
			sa.Data <- d
		}
		// Close the channel
		close(sa.Data)
	}()

	wg.Wait()

	fmt.Print(sa.Column.Result)

	for i, v := range sa.Column.Result {
		if v != expected[i] {
			t.Errorf("got %v, want %v", v, expected[i])
		}
	}
}

// testall
func TestSmartAggregator(t *testing.T) {
	inputs := []csvdata.Input{
		{Epoch: 0, Value: 0},
		{Epoch: 1, Value: 1},
		{Epoch: 2, Value: 2},
		{Epoch: 3, Value: 3},
		{Epoch: 4, Value: 4},
		{Epoch: 5, Value: 5},
		{Epoch: 6, Value: 6},
		{Epoch: 7, Value: 7},
		{Epoch: 8, Value: 8},
		{Epoch: 9, Value: 9},
		{Epoch: 10, Value: 10},
	}
	timeResultEp := []int64{2, 6, 10}
	windowrelative := [][2]int64{{2, 3}, {4, 6}, {8, 10}}
	tests := []struct {
		name           string
		agg            string
		data           []csvdata.Input
		windowRelative [][2]int64
		want           []float64
	}{
		{"SUM", csvdata.SUM, inputs, windowrelative, []float64{5, 15, 27}},
		{"COUNT", csvdata.COUNT, inputs, windowrelative, []float64{2, 3, 3}},
		{"MEAN", csvdata.MEAN, inputs, windowrelative, []float64{2.5, 5.0, 9.0}},
		{"MAX", csvdata.MAX, inputs, windowrelative, []float64{3, 6, 10}},
		{"MIN", csvdata.MIN, inputs, windowrelative, []float64{2, 4, 8}},
		{"FIRST", csvdata.FIRST, inputs, windowrelative, []float64{2, 4, 8}},
		{"LAST", csvdata.LAST, inputs, windowrelative, []float64{3, 6, 10}},
		{"IMAX", csvdata.IMAX, inputs, windowrelative, []float64{3, 6, 10}},
		{"IMIN", csvdata.IMIN, inputs, windowrelative, []float64{2, 4, 8}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1)

			reqcolumn := csvdata.SAColumn{
				OutputColumnName: tt.agg,
				WindowRelative:   tt.windowRelative,
				TimeResultEp:     &timeResultEp,
				Result:           make([]float64, len(timeResultEp)),
			}
			sa := csvdata.NewSmartAggregator(tt.agg, &reqcolumn, &wg)

			// Pass the data to the SmartAggregator
			go func() {
				for _, d := range tt.data {
					sa.Data <- d
				}
				// Close the channel
				close(sa.Data)
			}()

			wg.Wait()
			fmt.Print(sa.Column.Result)

			for i, v := range sa.Column.Result {
				if v != tt.want[i] {
					t.Errorf("got %v, want %v", v, tt.want[i])
				}
			}
		})
	}
}

// TestSmartAggregatorWindDirMax8 tests that the SmartAggregator correctly finds the most frequent wind direction bin.
func TestSmartAggregatorWindDirMax8(t *testing.T) {
	// Prepare the input data (wind directions in degrees)
	inputData := []csvdata.Input{
		{Epoch: 0, Value: 0},     // N
		{Epoch: 1, Value: 50.1},  // NE
		{Epoch: 2, Value: 90},    // E
		{Epoch: 3, Value: 135},   // SE
		{Epoch: 4, Value: 180},   // S
		{Epoch: 5, Value: 225},   // SW
		{Epoch: 6, Value: 270},   // W
		{Epoch: 7, Value: 292.5}, // NW
		{Epoch: 8, Value: 31},    // NE
		{Epoch: 9, Value: 22.7},  // NE
	}

	// We expect NE (45) to be the most frequent bin because it appears 3 times.

	// Set up aggregation with window spanning all epoch data
	col := &csvdata.SAColumn{
		OutputColumnName: "windirmax8",
		TimeResultEp:     &[]int64{0, 10}, // Just one big time window
		WindowRelativeEp: [2]int64{0, 10}, // Relative window
		Result:           make([]float64, 1),
	}

	// Create the smart aggregator for WINDIRMAX8
	var wg sync.WaitGroup
	wg.Add(1)
	agg := csvdata.NewSmartAggregator(csvdata.WINDIRMAX8, col, &wg)

	// Send the input data to the aggregator
	go func() {
		for _, data := range inputData {
			agg.Data <- data
		}
		// Close the channel after sending all the data
		close(agg.Data)
	}()

	// Wait for the aggregator to finish
	wg.Wait()

	// Check if the result is as expected (most frequent bin -> NE -> 45 degrees)
	expected := 45.0
	result := col.Result[0]

	if result != expected {
		t.Errorf("WINDIRMAX8 Agg failed: expected %.1f, got %.1f", expected, result)
	} else {
		t.Logf("WINDIRMAX8 Agg passed: got %.1f", result)
	}
}

// TestSmartLastWithWindowString tests the 'LAST' method of SmartAggregator with an explicit window string
func TestSmartLastWithWindowString(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	// Time epochs and windows for testing
	timeResultEp := []int64{10, 20, 30}
	windowRelativeEp := [][2]int64{
		{6, 10},  // Between 6 to 10
		{16, 20}, // Between 16 to 20
		{26, 30}, // Between 26 to 30
	}

	// Prepare the SAColumn
	reqcolumn := csvdata.SAColumn{
		OutputColumnName: "last_value",
		TimeResultEp:     &timeResultEp,
		WindowRelativeEp: [2]int64{-4, 0},
		WindowRelative:   windowRelativeEp, // Explicit window for testing
		Result:           make([]float64, len(timeResultEp)),
	}

	// Initiate the SmartAggregator with the LAST method
	sa := csvdata.NewSmartAggregator(csvdata.LAST, &reqcolumn, &wg)

	expected := []float64{
		7,          // Latest value within window [-4, 0] for first timeResultEp: epoch between 6 and 10
		math.NaN(), // No data in the second window
		27,         // Latest value within window [-4, 0] for third timeResultEp: epoch between 26 and 30
	}

	// Input data
	data := []csvdata.Input{
		{Epoch: 1, Value: 1},
		{Epoch: 3, Value: 3},
		{Epoch: 7, Value: 7},   // Last value in the first window
		{Epoch: 11, Value: 11}, // Last value in the first window
		{Epoch: 15, Value: 15}, // Outside the second window
		{Epoch: 21, Value: 21}, // Outside the second window
		{Epoch: 27, Value: 27}, // Last value in the third window
	}

	// Pass the data to the aggregator
	go func() {
		for _, d := range data {
			// DEBUG: log each data point being sent
			t.Logf("Sending Epoch: %d, Value: %f", d.Epoch, d.Value)
			sa.Data <- d
		}
		// Close the channel
		close(sa.Data)
	}()

	// Wait for the aggregation to complete
	wg.Wait()

	// DEBUG: Log the window boundaries for sanity check
	for i, win := range sa.Column.WindowRelative {
		t.Logf("Window %d: [%d, %d]", i, win[0], win[1])
	}

	// Check the results
	for i, v := range sa.Column.Result {
		if math.IsNaN(expected[i]) {
			// Test that NaN is produced where we expect it
			if !math.IsNaN(v) {
				t.Errorf("Expected NaN at index %d, got %v", i, v)
			}
		} else if expected[i] != v {
			t.Errorf("Expected %v at index %d, got %v", expected[i], i, v)
		}
	}
}
