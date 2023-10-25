package csvdata

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/exp/constraints"
)

type RequestColumn struct {
	InputColumnName  string
	OutputColumnName string
	Method           string
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
	EpochOffset      time.Duration
	StartTime        time.Time
	EndTime          time.Time
	TimePrecision    string
	AggWindow        string
	AggWindowDur     time.Duration
}

// cheker function to check if the configs are valid
func (cfg *CsvAggregateConfigs) Check(caller string) error {
	var err error
	// Get information about the parent directory
	// fstat, err := os.Stat(cfg.Parentfolder)
	// if os.IsNotExist(err) {
	// 	return fmt.Errorf("parent directory %s does not exist", cfg.Parentfolder)
	// }
	// if !fstat.IsDir() {
	// 	return fmt.Errorf("parent directory %s is not a directory", cfg.Parentfolder)
	// }

	cfg.FileFrequencyDur, err = time.ParseDuration(cfg.FileFrequency)
	if err != nil {
		return fmt.Errorf("file frequency %s is not valid", cfg.FileFrequency)
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

	if caller == "table" {
		// check if cfg.AggWindow is valid
		// try to parse duration
		cfg.AggWindowDur, err = time.ParseDuration(cfg.AggWindow)
		if err != nil {
			return fmt.Errorf("aggregation window %s is not valid", cfg.AggWindow)
		}
	}
	return nil
}

// CsvAggregateTable aggregates a table of data
func CsvAggregateTable(CsvAggregateConfigs) (map[string][]float64, error) {
	return nil, nil
}

// CsvAggregatePoint aggregates a single point in time
func CsvAggregatePoint(cfg CsvAggregateConfigs) (map[string]float64, error) {

	// check if configs are valid
	err := cfg.Check("point")
	if err != nil {
		return nil, err
	}

	startDateFile := GetNearestPastTimeUnit(cfg.StartTime, cfg.FileFrequency)
	endDateFile := GetNearestPastTimeUnit(cfg.EndTime, cfg.FileFrequency).Add(time.Duration(cfg.FileFrequencyDur))

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
		go aggmap[req.OutputColumnName].Do()
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
