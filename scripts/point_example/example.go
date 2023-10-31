package main

import (
	"fmt"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func main() {
	cfg := csvdata.CsvAggregateConfigs{
		FileNamingFormat: "../../example/2006-01-02.csv",
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
		fmt.Println(err)
	}
	fmt.Println(agg)
}
