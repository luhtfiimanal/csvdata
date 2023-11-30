package main

import (
	"fmt"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func main() {

	starttime := time.Now().UTC().UnixMilli()
	cfg := csvdata.CsvAggregateTableConfigs{
		FileConfigs: []csvdata.FileConfig{
			{
				FileNamingFormat: "C:\\Users\\luthf\\git\\luhtfiimanal\\csvdata\\tes\\data\\tanah_2006-01-02.csv",
				FileFrequency:    "24h",
			},
			{
				FileNamingFormat: "C:\\Users\\luthf\\git\\luhtfiimanal\\csvdata\\tes\\data\\angkasa_2006-01-02.csv",
				FileFrequency:    "24h",
			},
		},
		Requests: []csvdata.RequestColumnTable{

			// air pressure
			{InputColumnName: "AP_1200_Avg", OutputColumnName: "AP", Method: csvdata.MEAN},

			// rain
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain", Method: csvdata.SUM},
		},
		TimeOffset:    "0h", //"6h30m",
		StartTime:     time.Date(2023, 11, 3, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "5m", //"24h"
	}

	result, err := csvdata.CsvAggregateTable(cfg)
	if err != nil {
		// Print the error
		fmt.Println("Error: ", err)
		return
	}

	// Print elapsed time
	fmt.Println("Elapsed time: ", time.Now().UTC().UnixMilli()-starttime, "ms")

	// Print the result
	fmt.Println("Result:")
	jsonout, _ := result.ToJson5()
	fmt.Println(string(jsonout))

}
