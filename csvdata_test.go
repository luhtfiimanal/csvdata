package csvdata_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func TestCsvAggregatePoint(t *testing.T) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "./example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level_pick", Method: csvdata.PICK, PickTime: time.Date(2023, 1, 10, 3, 0, 0, 0, time.UTC)},
		},
		TimeOffset:    "7h",
		StartTime:     time.Date(2023, 1, 10, 1, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
	}

	agg, err := csvdata.CsvAggregatePoint(cfg)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(agg)

	// check if the output is correct
	// the correct output is [dewpoint_avg:23.001486111111134 dewpoint_max:24.63 water_level:50.35723802914642]
	if agg["dewpoint_avg"] != 23.243294117647054 {
		t.Error("dewpoint_avg is not correct")
	}
	if agg["dewpoint_max"] != 24.63 {
		t.Error("dewpoint_max is not correct")
	}
	if agg["water_level"] != 51.1048775710088 {
		t.Error("water_level is not correct")
	}
	if agg["water_level_pick"] != 53.79 {
		t.Error("water_level_pick is not correct")
	}
}

func TestCsvAggregatePoint_Pick(t *testing.T) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "./example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint", Method: csvdata.PICK, PickTime: time.Date(2023, 1, 10, 3, 0, 0, 0, time.UTC)},
		},
		TimeOffset:    "0h",
		StartTime:     time.Date(2023, 1, 10, 1, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
	}

	agg, err := csvdata.CsvAggregatePoint(cfg)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(agg)

	// check if the output is correct
	// the correct output is [dewpoint_avg:23.001486111111134 dewpoint_max:24.63 water_level:50.35723802914642]
	if agg["dewpoint"] != 24.13 {
		t.Error("dewpoint_avg is not correct")
	}
}

func TestCsvAggregateTable(t *testing.T) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "./example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
		},
		TimeOffset:    "7h",
		StartTime:     time.Date(2023, 1, 10, 13, 24, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 1, 15, 1, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "15m",
	}

	agg, err := csvdata.CsvAggregateTable(cfg)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(agg)
}

// benchmarking
func BenchmarkCsvAggregatePoint(b *testing.B) {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "./example/2006-01-02.csv",
		FileFrequency:    "24h",
		Requests: []csvdata.RequestColumn{
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
			{InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
			{InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level_pick", Method: csvdata.PICK, PickTime: time.Date(2023, 1, 10, 3, 0, 0, 0, time.UTC)},
		},
		TimeOffset:    "7h",
		StartTime:     time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
	}

	for i := 0; i < b.N; i++ {
		_, err := csvdata.CsvAggregatePoint(cfg)
		if err != nil {
			b.Error(err)
		}
	}
}
