package csvdata_test

import (
	"testing"

	"github.com/luhtfiimanal/csvdata"
)

func TestAggregator(t *testing.T) {
	inputs := []csvdata.Input{
		{Epoch: 1, Value: 1.0},
		{Epoch: 2, Value: 2.0},
		{Epoch: 3, Value: 3.0},
		{Epoch: 4, Value: 4.0},
		{Epoch: 5, Value: 5.0},
	}
	tests := []struct {
		name string
		data []csvdata.Input
		agg  string
		want float64
	}{
		{"Sum", inputs, csvdata.SUM, 15.0},
		{"Count", inputs, csvdata.COUNT, 5.0},
		{"Mean", inputs, csvdata.MEAN, 3.0},
		{"Max", inputs, csvdata.MAX, 5.0},
		{"Min", inputs, csvdata.MIN, 1.0},
		{"First", inputs, csvdata.FIRST, 1.0},
		{"Last", inputs, csvdata.LAST, 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := csvdata.NewAggregator(tt.agg)
			go func() {
				for _, val := range tt.data {
					agg.Data <- val
				}
				close(agg.Data)
			}()
			result := <-agg.Done
			if result.Value != tt.want {
				t.Errorf("got %v, want %v", result.Value, tt.want)
			}
		})
	}
}

func TestLast(t *testing.T) {
	agg := csvdata.NewAggregator(csvdata.LAST)
	go func() {
		agg.Data <- csvdata.Input{Epoch: 3, Value: 3.0}
		agg.Data <- csvdata.Input{Epoch: 1, Value: 1.0}
		agg.Data <- csvdata.Input{Epoch: 2, Value: 2.0}
		agg.Data <- csvdata.Input{Epoch: 5, Value: 5.0}
		agg.Data <- csvdata.Input{Epoch: 4, Value: 4.0}
		close(agg.Data)
	}()
	result := <-agg.Done
	if result.Value != 5.0 {
		t.Errorf("got %v, want %v", result.Value, 5.0)
	}
}

func TestFirst(t *testing.T) {
	agg := csvdata.NewAggregator(csvdata.FIRST)
	go func() {
		agg.Data <- csvdata.Input{Epoch: 3, Value: 3.0}
		agg.Data <- csvdata.Input{Epoch: 1, Value: 1.0}
		agg.Data <- csvdata.Input{Epoch: 2, Value: 2.0}
		agg.Data <- csvdata.Input{Epoch: 5, Value: 5.0}
		agg.Data <- csvdata.Input{Epoch: 4, Value: 4.0}
		close(agg.Data)
	}()
	result := <-agg.Done
	if result.Value != 1.0 {
		t.Errorf("got %v, want %v", result.Value, 1.0)
	}
}

func BenchmarkAggregator5Number(b *testing.B) {
	inputs := []csvdata.Input{
		{Epoch: 1, Value: 1.0},
		{Epoch: 2, Value: 2.0},
		{Epoch: 3, Value: 3.0},
		{Epoch: 4, Value: 4.0},
		{Epoch: 5, Value: 5.0},
	}
	tests := []struct {
		name string
		data []csvdata.Input
		agg  string
		want float64
	}{
		{"Mean", inputs, csvdata.MEAN, 3.0},
		{"Max", inputs, csvdata.MAX, 5.0},
		{"Min", inputs, csvdata.MIN, 1.0},
		{"First", inputs, csvdata.FIRST, 1.0},
		{"Last", inputs, csvdata.LAST, 5.0},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			agg := csvdata.NewAggregator(tt.agg)
			go func() {
				for _, val := range tt.data {
					agg.Data <- val
				}
				close(agg.Data)
			}()
			result := <-agg.Done
			if result.Value != tt.want {
				b.Errorf("got %v, want %v", result.Value, tt.want)
			}
		})
	}
}

func BenchmarkAggregatorThousand(t *testing.B) {
	tests := []struct {
		name string
		agg  string
	}{
		{"Sum", csvdata.SUM},
		{"Count", csvdata.COUNT},
		{"Mean", csvdata.MEAN},
		{"Max", csvdata.MAX},
		{"Min", csvdata.MIN},
		{"First", csvdata.FIRST},
		{"Last", csvdata.LAST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.B) {
			agg := csvdata.NewAggregator(tt.agg)
			thousands := make([]float64, 10000)
			for i := 0; i < len(thousands); i++ {
				thousands[i] = float64(i)
			}
			go func() {
				for _, val := range thousands {
					agg.Data <- csvdata.Input{Value: val}
				}
				close(agg.Data)
			}()
			<-agg.Done
		})
	}
}
