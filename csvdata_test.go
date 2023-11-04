package csvdata_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func TestCsvAggregatePoint(t *testing.T) {
	cfg := csvdata.CsvAggregatePointConfigs{
		FileConfig: csvdata.FileConfig{
			FileNamingFormat: "./example/2006-01-02.csv",
			FileFrequency:    "24h",
		},
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
	cfg := csvdata.CsvAggregatePointConfigs{
		FileConfig: csvdata.FileConfig{
			FileNamingFormat: "./example/2006-01-02.csv",
			FileFrequency:    "24h",
		},
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
	cfg := csvdata.CsvAggregateTableConfigs{
		FileConfigs: []csvdata.FileConfig{
			{
				FileNamingFormat: "C:\\Users\\luthf\\Downloads\\Archive/2006-01-02.csv",
				FileFrequency:    "24h",
			},
		},
		Requests: []csvdata.RequestColumnTable{
			{
				InputColumnName:  "dewpoint_avg_60",
				OutputColumnName: "dewpoint_avg",
				Method:           csvdata.MEAN,
				WindowString:     "1h_2h",
			},
			{
				InputColumnName:  "ev_water_temperature_avg_60",
				OutputColumnName: "wtemp_max_yesterday",
				Method:           csvdata.MAX,
				WindowString:     "-23h59m59s_0h",
			},
			{
				InputColumnName:  "ev_water_temperature_avg_60",
				OutputColumnName: "wtemp_min_yesterday",
				Method:           csvdata.MIN,
				WindowString:     "-23h59m59s_0h",
			},
			{
				InputColumnName:  "ev_water_temperature_avg_60",
				OutputColumnName: "wtemp_min_7LT_to_13LT",
				Method:           csvdata.MIN,
				WindowString:     "7h_13h",
			},
		},
		TimeOffset:    "6h30m", //"6h30m",
		StartTime:     time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 1, 4, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "24h", //"24h"
	}

	// known bug. Ketika end time tidak sama dengan aggregate window, maka akan terjadi stuck di goroutine

	result, err := csvdata.CsvAggregateTable(cfg)
	if err != nil {
		t.Error(err)
		return
	}

	result.SaveToCSV("./example/out.csv")
	// test if file is exist
	if _, err := os.Stat("./example/out.csv"); os.IsNotExist(err) {
		t.Error(err)
		return
	}

	// test convert byte to json
	jsonout, _ := result.ToJson5()
	fmt.Println(string(jsonout))
}

// benchmarking
func BenchmarkCsvAggregatePoint(b *testing.B) {
	cfg := csvdata.CsvAggregatePointConfigs{
		FileConfig: csvdata.FileConfig{
			FileNamingFormat: "./example/2006-01-02.csv",
			FileFrequency:    "24h",
		},
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
