# README

This is a Golang script to read and aggregate data from csv files.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Constants](#constants)
- API
  - Main Functions
    - [CsvAggregatePoint Function](#csvaggregatepoint-function)
  - Helper Functions
    - [GetNearestPastTimeUnit Function](#getnearestpasttimeunit-function)
- [Benchmarks](#benchmarks)

## Installation

To install the script, run the following command:

```bash
go get github.com/luhtfiimanal/csvdata
```

## Usage

To use the script, import the script in your Golang script:

```go
import "github.com/luhtfiimanal/csvdata"
```

## Constants

The script has the following constants:

### Aggregation Methods

- `SUM`: A `string` constant defining the summation method.
- `COUNT`: A `string` constant defining the count method.
- `MEAN`: A `string` constant defining the mean method.
- `MAX`: A `string` constant defining the maximum method.
- `MIN`: A `string` constant defining the minimum method.
- `FIRST`: A `string` constant defining the first method.
- `LAST`: A `string` constant defining the last method.
- `PICK`: A `string` constant defining the pick method.

### Time Precision

- `SECOND`: A `string` constant defining the second time precision.
- `MICROSECOND`: A `string` constant defining the microsecond time precision.
- `MILLISECOND`: A `string` constant defining the millisecond time precision.

## `CsvAggregatePoint` Function

This is example use of `CsvAggregatePoint` function. The function will aggregate the data from the csv file based on the configuration provided.

### Syntax

The syntax for the function is as follows:

```go
CsvAggregatePoint(cfg CsvAggregateConfigs) (map[string]float64, error)
```

### Parameters

The function takes one parameter:

- `cfg`: A `CsvAggregateConfigs` object which specifies the configuration for the aggregation.

The `CsvAggregateConfigs` object has the following fields:

- `FileNamingFormat`: A `string` defining the file naming format of the csv files. The file naming format must be in Golang time format. The file naming format must contain the year, month and day. The file naming format must be absolute path. Example of file naming format: `/path/to/example/2006/01/2006-01-02.csv`
- `FileFrequency`: A `string` defining the frequency of the csv files. The file frequency must be in Golang time duration string format. Example `24h` for daily csv files.
- `Requests`: A `[]RequestColumn` defining the requests to be made to the csv files. The `RequestColumn` object has the following fields:
  - `InputColumnName`: A `string` defining the input column name of the csv file.
  - `OutputColumnName`: A `string` defining the output column that will be presented in the map output.
  - `Method`: A `string` defining the method to be used for the aggregation. The value accepted are discussed in the [Aggregation Methods](#aggregation-methods) section.
  - `PickTime`: A `time.Time` object defining the time to be picked if the `Method` is "pick". Local time is UTC + `TimeOffset`.
- `TimeOffset`: An `string` defining the epoch offset for the `StartTime`, `EndTime` and output time. `TimeOffset` must be in Golang time duration string format. Example `24m00s` for 24 minutes epoch offset.
- `StartTime`: A `time.Time` object defining the start time of the aggregation, in local time. Local time is UTC + `TimeOffset`.
- `EndTime`: A `time.Time` object defining the end time of the aggregation, in local time. Local time is UTC + `TimeOffset`.
- `TimePrecision`: A `string` defining the time precision of the aggregation. The value accepted are discussed in the [Time Precision](#time-precision) section.
- `AggWindow`: A `string` defining the aggregation window of the aggregation. The aggregation window must be in Golang time duration string format. Example `24h` for daily aggregation window or `1h` for hourly aggregation window.

### Returns

The function will return a `map[string]float64` object representing the aggregated data.

### Example

Here is a usage example of `CsvAggregatePoint` function:

```go
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
```

This will output:

```go
map[dewpoint_avg:23.243294117647054 dewpoint_max:24.63 water_level:51.1048775710088 water_level_pick:53.79]
```

## `GetNearestPastTimeUnit` Function

The `GetNearestPastTimeUnit` function in our Golang script allows you to get the timestamp of the nearest past unit of time, based upon the input parameters you provide.

### Syntax

The syntax for the function is as follows:

```go
GetNearestPastTimeUnit(t time.Time, duration string) time.Time
```

### Parameters

The function takes two parameters:

- `t`: A `time.Time` object which specifies the date and time from where the nearest past unit of time is to be calculated.

- `duration`: A `string` defining the kind of time segment for which you want to find the nearest past point.

  The accepted string values are:
  
  - "1y": 1 year
  - "1M": 1 month
  - "7d": 7 days
  - "2d": 2 days
  - "1d": 1 day
  - "12h": 12 hours
  - "6h": 6 hours
  - "3h": 3 hours
  - "1h": 1 hour
  - "15m": 15 minutes
  - "10m": 10 minutes
  - "5m": 5 minutes
  - "1m": 1 minute

### Returns

The function will return a `time.Time` object representing the timestamp of the nearest past unit of time, based upon the `duration` you provided.

### Example

Here is a usage example of `GetNearestPastTimeUnit` function:

```go
t, _ := time.Parse(time.RFC3339, "2022-01-02T01:44:12Z")
fmt.Println("Nearest Past 1 hour:", GetNearestPastTimeUnit(t, "1h"))
```

This will output:

```
Nearest Past 1 hour: 2022-01-02 01:00:00 +0000 UTC
```

The output is the closest past hour from the time "2022-01-02T01:44:12Z".


## Benchmarks

This is the benchmark result of the script. The benchmark is done on a laptop with i7 - 4 core 16GB RAM machine.

```go
goos: windows
goarch: amd64
pkg: github.com/luhtfiimanal/csvdata
cpu: 12th Gen Intel(R) Core(TM) i7-1260P
BenchmarkAggregator5Number/Mean-16           	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Max-16            	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Min-16            	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/First-16          	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Last-16           	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Sum-16           	1000000000	         0.001557 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Count-16         	1000000000	         0.001550 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Mean-16          	1000000000	         0.002056 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Max-16           	1000000000	         0.001538 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Min-16           	1000000000	         0.002106 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/First-16         	1000000000	         0.001572 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Last-16          	1000000000	       0 B/op	       0 allocs/op
BenchmarkCsvAggregatePoint-16                	     325	   3979880 ns/op	 1650674 B/op	    2938 allocs/op
BenchmarkReadCSVLineByLine-16                	     429	   3445539 ns/op	 1648881 B/op	    2910 allocs/op
BenchmarkReadCSVAllAtOnce-16                 	     292	   3855106 ns/op	 1775838 B/op	    2924 allocs/op
BenchmarkReadCSVSequentiallyAllAtOnce-16     	     175	   7068973 ns/op	 3528036 B/op	    5808 allocs/op
BenchmarkReadCSVConcurrentlyAllAtOnce-16     	     316	   4515772 ns/op	 3527322 B/op	    5810 allocs/op
BenchmarkReadCSVConcurrentlyLineByLine-16    	     460	   4147446 ns/op	 3273157 B/op	    5783 allocs/op
PASS
coverage: 57.2% of statements
ok  	github.com/luhtfiimanal/csvdata	11.305s
```
This is the benchmark result of the script. The benchmark is done on a PC with i7-10700F - 8 core 16GB RAM machine.

```go
goos: windows
goarch: amd64
pkg: github.com/luhtfiimanal/csvdata
cpu: Intel(R) Core(TM) i7-10700F CPU @ 2.90GHz
BenchmarkAggregator5Number/Mean-16           	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Max-16            	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Min-16            	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/First-16          	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Last-16           	1000000000	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Sum-16           	1000000000	         0.001545 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Count-16         	1000000000	         0.001028 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Mean-16          	1000000000	         0.001545 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Max-16           	1000000000	         0.001544 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Min-16           	1000000000	         0.0009926 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/First-16         	1000000000	         0.001000 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Last-16          	1000000000	         0.001000 ns/op	       0 B/op	       0 allocs/op
BenchmarkCsvAggregatePoint-16                	     357	   3334911 ns/op	 1651464 B/op	    2938 allocs/op
BenchmarkReadCSVLineByLine-16                	     567	   2158189 ns/op	 1648887 B/op	    2910 allocs/op
BenchmarkReadCSVAllAtOnce-16                 	     537	   2269073 ns/op	 1775831 B/op	    2923 allocs/op
BenchmarkReadCSVSequentiallyAllAtOnce-16     	     271	   4548520 ns/op	 3528055 B/op	    5808 allocs/op
BenchmarkReadCSVConcurrentlyAllAtOnce-16     	     458	   2627590 ns/op	 3527309 B/op	    5810 allocs/op
BenchmarkReadCSVConcurrentlyLineByLine-16    	     512	   2338535 ns/op	 3273235 B/op	    5784 allocs/op
PASS
coverage: 57.2% of statements
ok  	github.com/luhtfiimanal/csvdata	9.284s
```