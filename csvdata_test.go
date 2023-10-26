package csvdata_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func TestCsvAggregatePoint(t *testing.T) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "/home/devawos/dev/csvdata/example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
		},
		EpochOffset:   "-7h",
		StartTime:     time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "1h",
	}

	agg, err := csvdata.CsvAggregatePoint(cfg)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(agg)

	// check if the output is correct
	// the correct output is [dewpoint_avg:23.001486111111134 dewpoint_max:24.63 water_level:50.35723802914642]
	if agg["dewpoint_avg"] != 23.001486111111134 {
		t.Error("dewpoint_avg is not correct")
	}
	if agg["dewpoint_max"] != 24.63 {
		t.Error("dewpoint_max is not correct")
	}
	if agg["water_level"] != 50.35723802914642 {
		t.Error("water_level is not correct")
	}
}

// benchmarking
func BenchmarkCsvAggregatePoint(b *testing.B) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "/home/devawos/dev/csvdata/example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
		},
		EpochOffset:   "7h",
		StartTime:     time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "1h",
	}

	for i := 0; i < b.N; i++ {
		_, err := csvdata.CsvAggregatePoint(cfg)
		if err != nil {
			b.Error(err)
		}
	}
}
