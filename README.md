# README

This is a Golang package to read and aggregate data from CSV files efficiently.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Constants](#constants)
- API
  - Main Functions
    - [CsvAggregatePoint Function](#csvaggregatepoint-function)
    - [CsvAggregateTable Function](#csvaggregatetable-function)
  - Helper Functions
    - [WINDIRMAX8 Aggregation Method](#windirmax8-aggregation-method)
    - [GetNearestPastTimeUnit Function](#getnearestpasttimeunit-function)
- [Benchmarks](#benchmarks)

## Installation

To install the package, run the following command:

```bash
go get github.com/luhtfiimanal/csvdata
```

## Usage

To use the package, import it in your Golang script:

```go
import "github.com/luhtfiimanal/csvdata"
```

## Constants

The package has the following constants:

### Aggregation Methods

The package supports various aggregation methods for CSV data, including the recently added wind direction aggregator. Here's a list of all available methods:

- `SUM`: Summation method
- `COUNT`: Count method
- `MEAN`: Mean method
- `MAX`: Maximum method
- `MIN`: Minimum method
- `FIRST`: First value method
- `LAST`: Last value method
- **`WINDIRMAX8`**: Finds the most frequent wind direction bin based on 8 main wind directions (N, NE, E, SE, S, SW, W, NW)
- `PICK`: Pick specific value method
- `IMAX`: Index of maximum value method
- `IMIN`: Index of minimum value method

### Time Precision

- `SECOND`: Second time precision
- `MICRO`: Microsecond time precision
- `MILLI`: Millisecond time precision

## `CsvAggregatePoint` Function

This function aggregates data from CSV files based on the provided configuration for a single point in time.

### Syntax

```go
CsvAggregatePoint(cfg CsvAggregatePointConfigs) (map[string]float64, error)
```

### Parameters

- `cfg`: A `CsvAggregatePointConfigs` object specifying the configuration for the aggregation.

### Returns

A `map[string]float64` object representing the aggregated data and an error if any.

### Example

Here’s an example usage of the `CsvAggregatePoint` function:

```go
package main

import (
    "fmt"
    "time"
    "github.com/luhtfiimanal/csvdata"
)

func main() {
    // Configure the CSV aggregation
    cfg := csvdata.CsvAggregatePointConfigs{
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
        return
    }
    fmt.Println(agg)
}
```

This will output:

```
map[dewpoint_avg:23.243 dewpoint_max:24.63 water_level:51.105 water_level_pick:53.79]
```

## `CsvAggregateTable` Function

This function aggregates data from CSV files based on the provided configuration for a table of data over time.

### Syntax

```go
CsvAggregateTable(cfg CsvAggregateTableConfigs) (SAResult, error)
```

### Parameters

- `cfg`: A `CsvAggregateTableConfigs` object specifying the configuration for the table aggregation.

### Returns

An `SAResult` object representing the aggregated data table and an error if any.

### Example

Here’s an example usage of the `CsvAggregateTable` function:

```go
package main

import (
    "fmt"
    "time"
    "github.com/luhtfiimanal/csvdata"
)

func main() {
    // Configure the CSV aggregation
    cfg := csvdata.CsvAggregateTableConfigs{
        FileConfigs: []csvdata.FileConfig{
            {
                FileNamingFormat: "../../example/2006-01-02.csv",
                FileFrequency:    "24h",
                FileFrequencyDur: 24 * time.Hour,
            },
        },
        StartTime:     time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
        EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
        TimePrecision: "second",
        Requests: []csvdata.RequestColumnTable{
            {InputColumnName: "temperature", OutputColumnName: "avg_temp", Method: csvdata.MEAN},
            {InputColumnName: "temperature", OutputColumnName: "max_temp", Method: csvdata.MAX},
            {InputColumnName: "temperature", OutputColumnName: "min_temp", Method: csvdata.MIN},
            {InputColumnName: "temperature", OutputColumnName: "max_temp_time", Method: csvdata.IMAX},
            {InputColumnName: "temperature", OutputColumnName: "min_temp_time", Method: csvdata.IMIN},
        },
    }

    result, err := csvdata.CsvAggregateTable(cfg)
    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Println(result)
}
```

This will output the aggregated data over the specified time period.

## `WINDIRMAX8` Aggregation Method

The `WINDIRMAX8` method finds the most frequently occurring wind direction in the dataset by binning the wind directions into **8 bins** (North, North-East, East, South-East, South, South-West, West, and North-West). Each wind direction value is classified into one of these bins (in degrees), and the one with the highest frequency is returned.

### Classification of Wind Directions:

| Direction | Angle (°) | Range (°)              |
|-----------|-----------|------------------------|
| North (N) | 0         | -22.5 to 22.5          |
| North-East (NE) | 45    | 22.5 to 67.5           |
| East (E)  | 90        | 67.5 to 112.5          |
| South-East (SE) | 135  | 112.5 to 157.5         |
| South (S) | 180       | 157.5 to 202.5         |
| South-West (SW) | 225  | 202.5 to 247.5         |
| West (W)  | 270       | 247.5 to 292.5         |
| North-West (NW) | 315  | 292.5 to 337.5         |

### Example Usage of `WINDIRMAX8`:

```go
package main

import (
    "fmt"
    "sync"
    "time"
    "github.com/luhtfiimanal/csvdata"
)

func main() {
    // Configure the CSV aggregation
    cfg := csvdata.CsvAggregateTableConfigs{
        FileConfigs: []csvdata.FileConfig{
            {
                FileNamingFormat: "path_to_your_data/2006-01-02.csv",
                FileFrequency:    "24h",
                FileFrequencyDur: 24 * time.Hour,
            },
        },
        StartTime:     time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
        EndTime:       time.Date(2023, 1, 11, 0, 0, 0, 0, time.UTC),
        TimePrecision: "second",
        Requests: []csvdata.RequestColumnTable{
            {InputColumnName: "wind_direction", OutputColumnName: "common_wind_dir", Method: csvdata.WINDIRMAX8},
        },
    }

    result, err := csvdata.CsvAggregateTable(cfg)
    if err != nil {
        fmt.Println(err)
        return
    }

    // Output the result to CSV
    err = result.SaveToCSV("aggregated_wind_data.csv")
    if err != nil {
        fmt.Println(err)
    }
}
```

In this example, the most frequent wind direction is calculated for each time window and saved to a CSV.

## `GetNearestPastTimeUnit` Function

This function returns the timestamp of the nearest past unit of time based on the input parameters.

### Syntax

```go
GetNearestPastTimeUnit(t time.Time, duration string) time.Time
```

### Parameters

- `t`: A `time.Time` object specifying the reference time.
- `duration`: A `string` defining the time unit (e.g., "1y", "1M", "7d", "24h", "1h", "15m", etc.).

### Returns

A `time.Time` object representing the nearest past time unit.

### Example

Here’s an example usage of the `GetNearestPastTimeUnit` function:

```go
package main

import (
    "fmt"
    "time"
    "github.com/luhtfiimanal/csvdata"
)

func main() {
    t, _ := time.Parse(time.RFC3339, "2022-01-02T01:44:12Z")
    nearestPast := csvdata.GetNearestPastTimeUnit(t, "1h")
    fmt.Println("Nearest Past 1 hour:", nearestPast)
}
```

This will output:

```
Nearest Past 1 hour: 2022-01-02 01:00:00 +0000 UTC
```

## Benchmarks

Benchmark results are not included in the code provided. It's recommended to run benchmarks on your specific system to get accurate performance metrics for your use case.

For running benchmarks, use the following command:

```bash
go test -bench=. -benchmem
```

This will provide information about the performance of various operations in the package, including execution time, memory allocations, and more.