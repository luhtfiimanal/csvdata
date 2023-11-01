package csvdata_test

import (
	"fmt"
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
