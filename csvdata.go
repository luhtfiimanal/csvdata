package csvdata

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/constraints"
)

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

type CsvAggregatePointConfigs struct {
	FileConfig
	Requests      []RequestColumn
	TimeOffset    string
	TimeOffsetDur time.Duration
	TimeOffsetEp  int64
	StartTime     time.Time
	EndTime       time.Time
	TimePrecision string
}

type RequestColumn struct {
	InputColumnName  string
	OutputColumnName string
	Method           string
	PickTime         time.Time
}

type RequestColumnTable struct {
	InputColumnName  string
	OutputColumnName string
	Method           string
	WindowString     string
	WindowEp         [2]int64
	PickRelative     string
	PickEp           int64
	PickTime         time.Time
}

type FileConfig struct {
	FileNamingFormat string
	FileFrequency    string
	FileFrequencyDur time.Duration
}

type CsvAggregateTableConfigs struct {
	FileConfigs   []FileConfig
	TimeOffset    string
	TimeOffsetDur time.Duration
	TimeOffsetEp  int64
	Requests      []RequestColumnTable
	StartTime     time.Time
	EndTime       time.Time
	TimePrecision string
	AggWindow     string
	AggWindowDur  time.Duration
	AggWindowEp   int64
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
func (cfg *CsvAggregatePointConfigs) Check() error {
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

	return nil
}

// cheker function to check if the configs are valid
func (cfg *CsvAggregateTableConfigs) Check() error {
	var err error

	// check for file configs
	for i := range cfg.FileConfigs {
		file := &cfg.FileConfigs[i]
		if !StringInSlice(file.FileFrequency, []string{"1y", "1M", "7d", "2d", "1d", "24h", "12h", "6h", "3h", "1h", "15m", "10m", "5m", "1m"}) {
			return fmt.Errorf("FileFrequency must be \"1y\", \"1M\", \"7d\", \"2d\", \"1d\", \"24h\", \"12h\", \"6h\", \"3h\", \"1h\", \"15m\", \"10m\", \"5m\", \"1m\"")
		} else {
			file.FileFrequencyDur, err = time.ParseDuration(file.FileFrequency)
			if err != nil {
				return fmt.Errorf("FileFrequency %s is not valid", file.FileFrequency)
			}
			// check if the file frequency is zero
			if file.FileFrequencyDur == 0 {
				return fmt.Errorf("FileFrequency %s is zero", file.FileFrequency)
			}
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

	// check if cfg.AggWindow is valid
	// try to parse duration
	cfg.AggWindowEp, err = DurationtoEpoch(cfg.AggWindow, cfg.TimePrecision)
	if err != nil {
		return fmt.Errorf("AggWindow epoch %s is not valid", cfg.AggWindow)
	}
	cfg.AggWindowDur, err = time.ParseDuration(cfg.AggWindow)
	if err != nil {
		return fmt.Errorf("AggWindow window %s is not valid", cfg.AggWindow)
	}

	// check for requests
	for i := range cfg.Requests {
		req := &cfg.Requests[i]
		// check if the input column name is valid
		if req.InputColumnName == "" {
			return fmt.Errorf("input column name is empty")
		}
		// check if the output column name is valid, and if it is empty use the input column name
		if req.OutputColumnName == "" {
			req.OutputColumnName = req.InputColumnName
		}
		// check if the method is valid
		if !StringInSlice(req.Method, []string{SUM, COUNT, MEAN, MAX, MIN, FIRST, LAST, PICK}) {
			return fmt.Errorf("method %s is not valid", req.Method)
		}

		if req.Method == PICK {

			// check if the pick relative is valid
			if req.PickRelative != "" {
				req.PickEp, err = DurationtoEpoch(req.PickRelative, cfg.TimePrecision)
				if err != nil {
					return fmt.Errorf("pick relative %s is not valid", req.PickRelative)
				}
			} else {
				// pickep is zero
				req.PickEp = 0
			}

			// pick must be not greater than aggwindow
			if req.PickEp > cfg.AggWindowEp {
				return fmt.Errorf("pick relative %s is greater than aggwindow %s", req.PickRelative, cfg.AggWindow)
			}

		} else {
			if req.WindowString != "" {
				// Assuming the WindowString is of the form like -23h59m59s_0h, you should parse it like so
				windowParts := strings.Split(req.WindowString, "_")
				if len(windowParts) != 2 {
					return fmt.Errorf("window string should have two parts separated by _")
				}

				start, err := DurationtoEpoch(windowParts[0], cfg.TimePrecision)
				if err != nil {
					return fmt.Errorf("start duration parse error: %v", err)
				}

				end, err := DurationtoEpoch(windowParts[1], cfg.TimePrecision)
				if err != nil {
					return fmt.Errorf("end duration parse error: %v", err)
				}

				// Here you calculate the total duration and check if it's more than aggwindow
				if end-start > cfg.AggWindowEp {
					return fmt.Errorf("the total duration exceeds the limit")
				}

				// store the window
				req.WindowEp = [2]int64{start, end}
			} else {
				// window -cfg.AggWindowEp + 1 to 0
				start := -cfg.AggWindowEp + 1

				// Here you calculate the total duration and check if it's more than aggwindow
				if 0-start > cfg.AggWindowEp {
					return fmt.Errorf("the total duration exceeds the limit")
				}

				req.WindowEp = [2]int64{start, 0}
			}
		}
	}

	return nil
}

// CsvAggregateTable aggregates a table of data
func CsvAggregateTable(cfg CsvAggregateTableConfigs) (SAResult, error) {
	var wg sync.WaitGroup
	var wgfile sync.WaitGroup

	// check if configs are valid
	err := cfg.Check()
	if err != nil {
		zerores := SAResult{}
		return zerores, err
	}

	startTimeEpoch := TimetoEpoch(cfg.StartTime, cfg.TimePrecision)
	endTimeEpoch := TimetoEpoch(cfg.EndTime, cfg.TimePrecision)

	// get the list of epoch, use cfg.AggWindowDur
	// startIterDate := GetNearestPastTimeUnit(cfg.StartTime, cfg.AggWindow)
	// startResultDate := cfg.StartTime.Add(time.Duration(cfg.AggWindowDur))
	// startResultDateEpoch := TimetoEpoch(startResultDate, cfg.TimePrecision)
	epochlist := []int64{}
	for i := startTimeEpoch; i <= endTimeEpoch; i += int64(cfg.AggWindowEp) {
		epochlist = append(epochlist, i)
	}
	// check if the epochlist is empty
	if len(epochlist) == 0 {
		zerores := SAResult{}
		return zerores, fmt.Errorf("no epoch to aggregate")
	}

	// prepare for aggregation
	samap := make(SAMap, len(cfg.Requests))
	for _, req := range cfg.Requests {
		wg.Add(1)
		col := SAColumn{
			OutputColumnName: req.OutputColumnName,
			WindowRelativeEp: req.WindowEp,
			TimeResultEp:     &epochlist,
			Result:           make([]float64, len(epochlist)),
		}
		samap[req.OutputColumnName] = NewSmartAggregator(req.Method, &col, &wg)
	}

	// get the lowest window relative and highest window relative
	var lowestWindowRelative int64 = math.MaxInt64
	var highestWindowRelative int64 = math.MinInt64
	for _, req := range cfg.Requests {
		if req.Method == PICK {
			if req.PickEp < lowestWindowRelative {
				lowestWindowRelative = req.PickEp
			}
			if req.PickEp > highestWindowRelative {
				highestWindowRelative = req.PickEp
			}
		} else {
			if req.WindowEp[0] < lowestWindowRelative {
				lowestWindowRelative = req.WindowEp[0]
			}
			if req.WindowEp[1] > highestWindowRelative {
				highestWindowRelative = req.WindowEp[1]
			}
		}
	}
	// convert lowestWindowRelative and highestWindowRelative to duration
	lowestWindowRelativeDur := EpochToDuration(lowestWindowRelative, cfg.TimePrecision)
	highestWindowRelativeDur := EpochToDuration(highestWindowRelative, cfg.TimePrecision)

	startREADEpoch := startTimeEpoch + lowestWindowRelative
	endREADEpoch := endTimeEpoch + highestWindowRelative

	// loop through the file configs
	for _, filec := range cfg.FileConfigs {
		wgfile.Add(1)
		go func(filec *FileConfig) {
			defer wgfile.Done()
			coli := make(map[string]int, len(cfg.Requests))

			// startTimeUTC os the start time in UTC, Starttime minus offset, and minus lowestWindowRelativeDur
			startTimeREADUTC := cfg.StartTime.Add(-cfg.TimeOffsetDur).Add(lowestWindowRelativeDur)
			startDateFile := GetNearestPastTimeUnit(startTimeREADUTC, filec.FileFrequency)
			endTimeREADUTC := cfg.EndTime.Add(-cfg.TimeOffsetDur).Add(highestWindowRelativeDur)
			endDateFile := GetNearestPastTimeUnit(endTimeREADUTC, filec.FileFrequency).Add(time.Duration(filec.FileFrequencyDur))

			// get the list of files dates
			fdates := []time.Time{}
			for d := startDateFile; d.Before(endDateFile); d = d.Add(time.Duration(filec.FileFrequencyDur)) {
				fdates = append(fdates, d)
			}

			// loop through the fdates
			for _, day := range fdates {
				func() {
					// file name for the day
					filename := day.Format(filec.FileNamingFormat)

					// read the file
					csvfile, err := os.Open(filename)
					if err != nil {
						csvfile.Close()
						return
					}
					defer csvfile.Close()

					// read the file
					reader := csv.NewReader(csvfile)

					// get the column name
					csvColNames, err := reader.Read()
					if err != nil {
						csvfile.Close()
						return
					}
					for _, req := range cfg.Requests {
						coli[req.InputColumnName] = findString(csvColNames, req.InputColumnName)
					}

					// loop through the file
				readloop:
					for {
						// read the line
						line, err := reader.Read()
						if err != nil {
							break readloop
						}

						// convert date
						epochiter, err := strconv.ParseInt(line[0], 10, 64)
						if err != nil {
							continue readloop
						}

						// add offset
						epochiter += cfg.TimeOffsetEp

						// check if the epoch is within
						if !IsBetween(startREADEpoch, endREADEpoch, epochiter) {
							// check if the epoch is after the endREADEpoch
							if epochiter > endREADEpoch {
								break readloop
							}
							continue readloop
						}

						// aggregate
					reqloop:
						for _, req := range cfg.Requests {
							inpcolname := req.InputColumnName
							colidx, ok := coli[inpcolname]
							if !ok {
								continue reqloop
							}
							datastr := line[colidx]
							dataiter, err := strconv.ParseFloat(datastr, 64)
							if err != nil {
								continue reqloop
							}
							samap[req.OutputColumnName].Data <- Input{Epoch: epochiter, Value: dataiter}
						}
					}

				}()
			}
		}(&filec)
	}

	// wait for all the file reader to finish
	wgfile.Wait()
	// work done close all the aggregator
	for _, req := range cfg.Requests {
		close(samap[req.OutputColumnName].Data)
	}
	// wait for all the aggregator to finish
	wg.Wait()

	// generate SAOutput
	sares := samap.SAMapToStruct(cfg.TimePrecision)
	sares.Requests = &cfg.Requests

	return sares, nil
}

// CsvAggregatePoint aggregates a single point in time
func CsvAggregatePoint(cfg CsvAggregatePointConfigs) (map[string]float64, error) {

	// check if configs are valid
	err := cfg.Check()
	if err != nil {
		return nil, err
	}

	// startTimeUTC os the start time in UTC, Starttime minus offset
	startTimeUTC := cfg.StartTime.Add(-cfg.TimeOffsetDur)
	startDateFile := GetNearestPastTimeUnit(startTimeUTC, cfg.FileFrequency)
	endTimeUTC := cfg.EndTime.Add(-cfg.TimeOffsetDur)
	endDateFile := GetNearestPastTimeUnit(endTimeUTC, cfg.FileFrequency).Add(time.Duration(cfg.FileFrequencyDur))

	startTimeEpoch := TimetoEpoch(cfg.StartTime, cfg.TimePrecision)
	endTimeEpoch := TimetoEpoch(cfg.EndTime, cfg.TimePrecision)

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
			pickTimeEp := TimetoEpoch(req.PickTime, cfg.TimePrecision)
			pickTimeEp += cfg.TimeOffsetEp
			aggmap[req.OutputColumnName].PickerDate = &PickerDate{PickEpoch: pickTimeEp}
		}
	}

	// loop through the fdates
	for _, day := range fdates {
		func() {
			// file name for the day
			filename := day.Format(cfg.FileNamingFormat)

			// read the file
			csvfile, err := os.Open(filename)
			if err != nil {
				csvfile.Close()
				return
			}
			defer csvfile.Close()

			// read the file
			reader := csv.NewReader(csvfile)

			// get the column name
			csvColNames, err := reader.Read()
			if err != nil {
				csvfile.Close()
				return
			}
			for _, req := range cfg.Requests {
				coli[req.InputColumnName] = findString(csvColNames, req.InputColumnName)
			}

			// loop through the file
			for {
				// read the line
				line, err := reader.Read()
				if err != nil {
					return
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
		}()
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
