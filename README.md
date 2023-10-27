# README

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
  - `Method`: A `string` defining the method to be used for the aggregation. The accepted string values are:
    - "sum": Summation
    - "count": Count
    - "mean": Mean
    - "max": Maximum
    - "min": Minimum
    - "first": First
    - "last": Last
- `TimeOffset`: An `string` defining the epoch offset for the `StartTime`, `EndTime` and output time. `TimeOffset` must be in Golang time duration string format. Example `24m00s` for 24 minutes epoch offset.
- `StartTime`: A `time.Time` object defining the start time of the aggregation, in local time. Local time is UTC + `TimeOffset`.
- `EndTime`: A `time.Time` object defining the end time of the aggregation, in local time. Local time is UTC + `TimeOffset`.
- `TimePrecision`: A `string` defining the time precision of the aggregation. The accepted string values are:
  - "second": Second
  - "microsecond": Microsecond
  - "millisecond": Millisecond
- `AggWindow`: A `string` defining the aggregation window of the aggregation. The aggregation window must be in Golang time duration string format. Example `24h` for daily aggregation window or `1h` for hourly aggregation window.

### Returns

The function will return a `map[string]float64` object representing the aggregated data.

### Example

Here is a usage example of `CsvAggregatePoint` function:

```go
import "github.com/luhtfiimanal/csvdata"

func main() {
  cfg := csvdata.CsvAggregateConfigs{
    FileNamingFormat: "/home/devawos/dev/meteocsv/example/2006-01-02.csv",
    FileFrequency:    "24h",
    Requests: []csvdata.RequestColumn{
      {InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_avg", Method: csvdata.MEAN},
      {InputColumnName: "dewpoint_avg_60", OutputColumnName: "dewpoint_max", Method: csvdata.MAX},
      {InputColumnName: "ev_water_level_avg_60", OutputColumnName: "water_level", Method: csvdata.MEAN},
    },
    EpochOffset:   0,
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
}
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


## Bencmark
This is the benchmark result of the script. The benchmark is done on a 4 core 16GB RAM machine.
```go
goos: linux
goarch: amd64
pkg: github.com/luhtfiimanal/csvdata
cpu: Intel(R) Xeon(R) Bronze 3204 CPU @ 1.90GHz
BenchmarkAggregator5Number/Mean-6          	1000000000	         0.0000260 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Max-6           	1000000000	         0.0000134 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Min-6           	1000000000	         0.0000149 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/First-6         	1000000000	         0.0000194 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregator5Number/Last-6          	1000000000	         0.0000187 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Sum-6          	1000000000	         0.008420 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Count-6        	1000000000	         0.008290 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Mean-6         	1000000000	         0.008646 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Max-6          	1000000000	         0.008687 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Min-6          	1000000000	         0.008664 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/First-6        	1000000000	         0.008466 ns/op	       0 B/op	       0 allocs/op
BenchmarkAggregatorThousand/Last-6         	1000000000	         0.008690 ns/op	       0 B/op	       0 allocs/op
BenchmarkReadCSVLineByLine-6               	     126	   9290296 ns/op	 1648463 B/op	    2910 allocs/op
BenchmarkReadCSVAllAtOnce-6                	     128	   9439474 ns/op	 1775413 B/op	    2923 allocs/op
BenchmarkReadCSVSequentiallyAllAtOnce-6    	      12	  95738186 ns/op	17721998 B/op	   29209 allocs/op
BenchmarkReadCSVConcurrentlyAllAtOnce-6    	      34	  34780653 ns/op	17721476 B/op	   29218 allocs/op
BenchmarkReadCSVConcurrentlyLineByLine-6   	      49	  24273194 ns/op	16452236 B/op	   29091 allocs/op
PASS
coverage: 100.0% of statements
ok  	github.com/luhtfiimanal/csvdata	8.384s
```

