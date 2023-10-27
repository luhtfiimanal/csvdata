package csvdata

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"golang.org/x/exp/constraints"
)

type RequestColumn struct {
	InputColumnName  string
	OutputColumnName string
	Method           string
	PickTime         time.Time
}

const (
	SECOND = "second"
	MICRO  = "microsecond"
	MILLI  = "millisecond"
)

// function to convert epoch to time
func EpochtoTime(epoch int64, precission string) (time.Time, error) {
	switch precission {
	case SECOND:
		dtn := time.Unix(epoch, 0)
		return dtn, nil
	case MICRO:
		dtn := time.UnixMicro(epoch)
		return dtn, nil
	case MILLI:
		dtn := time.UnixMilli(epoch)
		return dtn, nil
	default:
		dtn := time.Unix(epoch, 0)
		return dtn, nil
	}
}

// function to convert time to epoch
func TimetoEpoch(t time.Time, precission string) (int64, error) {
	switch precission {
	case SECOND:
		return t.Unix(), nil
	case MICRO:
		return t.UnixMicro(), nil
	case MILLI:
		return t.UnixMilli(), nil
	default:
		return t.Unix(), nil
	}
}

// function to convert time duration to epoch
func DurationtoEpoch(ds string, precission string) (int64, error) {
	d, err := time.ParseDuration(ds)
	if err != nil {
		return int64(math.NaN()), err
	}
	switch precission {
	case SECOND:
		return int64(d.Seconds()), nil
	case MICRO:
		return int64(d.Microseconds()), nil
	case MILLI:
		return int64(d.Milliseconds()), nil
	default:
		return int64(d.Seconds()), nil
	}
}

func IsBetween[T constraints.Ordered](a, b, c T) bool {
	if a <= b {
		return c >= a && c <= b
	}
	return c >= b && c <= a
}

func findString(slice []string, val string) int {
	for i, item := range slice {
		if item == val {
			return i
		}
	}
	return -1 // return -1 if the string is not found
}

type CsvAggregateConfigs struct {
	FileNamingFormat string
	FileFrequency    string
	FileFrequencyDur time.Duration
	Requests         []RequestColumn
	TimeOffset       string
	TimeOffsetDur    time.Duration
	TimeOffsetEp     int64
	StartTime        time.Time
	EndTime          time.Time
	TimePrecision    string
	AggWindow        string
	AggWindowDur     time.Duration
	AggWindowEp      int64
}

// function to check if string inside []string
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// cheker function to check if the configs are valid
func (cfg *CsvAggregateConfigs) Check(caller string) error {
	var err error

	if !StringInSlice(cfg.FileFrequency, []string{"1y", "1M", "7d", "2d", "1d", "24h", "12h", "6h", "3h", "1h", "15m", "10m", "5m", "1m"}) {
		return fmt.Errorf("FileFrequency must be \"1y\", \"1M\", \"7d\", \"2d\", \"1d\", \"24h\", \"12h\", \"6h\", \"3h\", \"1h\", \"15m\", \"10m\", \"5m\", \"1m\"")
	} else {
		cfg.FileFrequencyDur, err = time.ParseDuration(cfg.FileFrequency)
		if err != nil {
			return fmt.Errorf("FileFrequency %s is not valid", cfg.FileFrequency)
		}
	}

	// check if cfg.StartTime is before cfg.EndTime
	if cfg.StartTime.After(cfg.EndTime) {
		return fmt.Errorf("start time %s is after end time %s", cfg.StartTime, cfg.EndTime)
	}

	// check if cfg.TimePrecision is valid
	switch cfg.TimePrecision {
	case SECOND:
	case MICRO:
	case MILLI:
	default:
		return fmt.Errorf("time precision %s is not valid", cfg.TimePrecision)
	}

	// check offset
	if cfg.TimeOffset == "" {
		cfg.TimeOffsetEp = 0
	} else {
		cfg.TimeOffsetEp, err = DurationtoEpoch(cfg.TimeOffset, cfg.TimePrecision)
		if err != nil {
			return fmt.Errorf("epoch offset %s is not valid", cfg.TimeOffset)
		}
		cfg.TimeOffsetDur, err = time.ParseDuration(cfg.TimeOffset)
		if err != nil {
			return fmt.Errorf("offset %s is not valid", cfg.TimeOffset)
		}
	}

	if caller == "table" {
		// check if cfg.AggWindow is valid
		// try to parse duration
		if !StringInSlice(cfg.AggWindow, []string{"1y", "1M", "7d", "2d", "1d", "12h", "6h", "3h", "1h", "15m", "10m", "5m", "1m"}) {
			return fmt.Errorf("AggWindow must be \"1y\", \"1M\", \"7d\", \"2d\", \"1d\", \"12h\", \"6h\", \"3h\", \"1h\", \"15m\", \"10m\", \"5m\", \"1m\"")
		} else {
			cfg.AggWindowEp, err = DurationtoEpoch(cfg.AggWindow, cfg.TimePrecision)
			if err != nil {
				return fmt.Errorf("AggWindow epoch %s is not valid", cfg.AggWindow)
			}
			cfg.AggWindowDur, err = time.ParseDuration(cfg.AggWindow)
			if err != nil {
				return fmt.Errorf("AggWindow window %s is not valid", cfg.AggWindow)
			}
		}
	}
	return nil
}

// CsvAggregateTable aggregates a table of data
func CsvAggregateTable(cfg CsvAggregateConfigs) (map[string][]float64, error) {

	// check if configs are valid
	err := cfg.Check("table")
	if err != nil {
		return nil, err
	}

	endTimeEpoch, err := TimetoEpoch(cfg.EndTime, cfg.TimePrecision)
	if err != nil {
		return nil, err
	}

	// get the list of epoch, use cfg.AggWindowDur
	startIterDate := GetNearestPastTimeUnit(cfg.StartTime, cfg.AggWindow)
	startIterDateEpoch, _ := TimetoEpoch(startIterDate, cfg.TimePrecision)
	startResultDate := startIterDate.Add(time.Duration(cfg.AggWindowDur))
	startResultDateEpoch, _ := TimetoEpoch(startResultDate, cfg.TimePrecision)
	epochlist := []int64{}
	for i := startResultDateEpoch; i <= endTimeEpoch; i += int64(cfg.AggWindowEp) {
		epochlist = append(epochlist, i)
	}

	// startTimeUTC os the start time in UTC, Starttime minus offset
	startTimeUTC := startIterDate.Add(-cfg.TimeOffsetDur)
	startDateFile := GetNearestPastTimeUnit(startTimeUTC, cfg.FileFrequency)
	endTimeUTC := cfg.EndTime.Add(-cfg.TimeOffsetDur)
	endDateFile := GetNearestPastTimeUnit(endTimeUTC, cfg.FileFrequency).Add(time.Duration(cfg.FileFrequencyDur))

	fmt.Println(startIterDateEpoch)

	// get the list of files dates
	fdates := []time.Time{}
	for d := startDateFile; d.Before(endDateFile); d = d.Add(time.Duration(cfg.FileFrequencyDur)) {
		fdates = append(fdates, d)
	}

	// prepare for aggregation
	retmap := make(map[string][]float64, len(cfg.Requests))
	for _, req := range cfg.Requests {
		retmap[req.OutputColumnName] = make([]float64, len(epochlist))
	}
	// coli := make(map[string]int, len(cfg.Requests))
	aggmap := make(map[string]*Aggregator, len(cfg.Requests))
	for _, req := range cfg.Requests {
		aggmap[req.OutputColumnName] = NewAggregator(req.Method)
	}

	// initial idate
	// idate :=

	return nil, nil

}

func resetaggregators(aggmap *map[string]*Aggregator) {
	for _, agg := range *aggmap {
		agg.Reset()
	}
}

// CsvAggregatePoint aggregates a single point in time
func CsvAggregatePoint(cfg CsvAggregateConfigs) (map[string]float64, error) {

	// check if configs are valid
	err := cfg.Check("point")
	if err != nil {
		return nil, err
	}

	// startTimeUTC os the start time in UTC, Starttime minus offset
	startTimeUTC := cfg.StartTime.Add(-cfg.TimeOffsetDur)
	startDateFile := GetNearestPastTimeUnit(startTimeUTC, cfg.FileFrequency)
	endTimeUTC := cfg.EndTime.Add(-cfg.TimeOffsetDur)
	endDateFile := GetNearestPastTimeUnit(endTimeUTC, cfg.FileFrequency).Add(time.Duration(cfg.FileFrequencyDur))

	startTimeEpoch, err := TimetoEpoch(cfg.StartTime, cfg.TimePrecision)
	if err != nil {
		return nil, err
	}
	endTimeEpoch, err := TimetoEpoch(cfg.EndTime, cfg.TimePrecision)
	if err != nil {
		return nil, err
	}

	// get the list of files dates
	fdates := []time.Time{}
	for d := startDateFile; d.Before(endDateFile); d = d.Add(time.Duration(cfg.FileFrequencyDur)) {
		fdates = append(fdates, d)
	}

	// prepare for aggregation
	retmap := make(map[string]float64, len(cfg.Requests))
	coli := make(map[string]int, len(cfg.Requests))
	aggmap := make(map[string]*Aggregator, len(cfg.Requests))
	for _, req := range cfg.Requests {
		aggmap[req.OutputColumnName] = NewAggregator(req.Method)
		if req.Method == PICK {
			pickTimeEp, err := TimetoEpoch(req.PickTime, cfg.TimePrecision)
			if err != nil {
				return nil, err
			}
			pickTimeEp += cfg.TimeOffsetEp
			aggmap[req.OutputColumnName].PickerDate = &PickerDate{PickEpoch: pickTimeEp}
		}
	}

	// loop through the fdates
	for _, day := range fdates {
		// file name for the day
		filename := day.Format(cfg.FileNamingFormat)

		// read the file
		csvfile, err := os.Open(filename)
		if err != nil {
			csvfile.Close()
			continue
		}

		// read the file
		reader := csv.NewReader(csvfile)

		// get the column name
		csvColNames, err := reader.Read()
		if err != nil {
			csvfile.Close()
			continue
		}
		for _, req := range cfg.Requests {
			coli[req.InputColumnName] = findString(csvColNames, req.InputColumnName)
		}

		// loop through the file
		for {
			// read the line
			line, err := reader.Read()
			if err != nil {
				break
			}

			// convert date
			epochiter, err := strconv.ParseInt(line[0], 10, 64)
			if err != nil {
				continue
			}

			// add offset
			epochiter += cfg.TimeOffsetEp

			// check if the epoch is within
			if !IsBetween(startTimeEpoch, endTimeEpoch, epochiter) {
				continue
			}

			// aggregate
			for _, req := range cfg.Requests {
				inpcolname := req.InputColumnName
				colidx := coli[inpcolname]
				datastr := line[colidx]
				dataiter, err := strconv.ParseFloat(datastr, 64)
				if err != nil {
					continue
				}
				aggmap[req.OutputColumnName].Data <- Input{Epoch: epochiter, Value: dataiter}
			}
		}

		csvfile.Close()
	}

	// work done close all the aggregator
	for _, req := range cfg.Requests {
		close(aggmap[req.OutputColumnName].Data)
	}

	// get the result
	for _, req := range cfg.Requests {
		result := <-aggmap[req.OutputColumnName].Done
		retmap[req.OutputColumnName] = result.Value
	}

	return retmap, nil
}
