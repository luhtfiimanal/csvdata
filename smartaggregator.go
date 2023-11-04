package csvdata

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

type SmartAggregator struct {
	Agg    string
	Data   chan Input
	Column *SAColumn
}

type SAColumn struct {
	OutputColumnName string
	TimeResultEp     *[]int64
	PickRelativeEp   int64
	PickRelative     []int64
	WindowRelativeEp [2]int64
	WindowRelative   [][2]int64
	Result           []float64
}

func (sac *SAColumn) makeWindow() {
	// check if WindowRelative is not set
	if len(sac.WindowRelative) == 0 {
		// if WindowRelativeEp is set, make WindowRelative based on relative with TimeResultEp
		sac.WindowRelative = make([][2]int64, len(*sac.TimeResultEp))
		for i, v := range *sac.TimeResultEp {
			sac.WindowRelative[i] = [2]int64{v + sac.WindowRelativeEp[0], v + sac.WindowRelativeEp[1]}
		}
	}
}

func (sac *SAColumn) makePickRelative() {
	// check if PickRelative is not set
	if len(sac.PickRelative) == 0 {
		// if PickRelativeEp is set, make PickRelative based on relative with TimeResultEp
		sac.PickRelative = make([]int64, len(*sac.TimeResultEp))
		for i, v := range *sac.TimeResultEp {
			sac.PickRelative[i] = v + sac.PickRelativeEp
		}
	}
}

func (sa *SmartAggregator) drainChannel() {
	for range sa.Data {
	}
}

func NewSmartAggregator(agg string, col *SAColumn, wg *sync.WaitGroup) *SmartAggregator {
	sa := &SmartAggregator{
		Agg:    agg,
		Data:   make(chan Input, 10),
		Column: col,
	}

	go sa.Do(wg)

	return sa
}

func (sa *SmartAggregator) Do(wg *sync.WaitGroup) {
	defer wg.Done()
	switch sa.Agg {
	case SUM:
		sa.Column.makeWindow()
		sa.doSumCountMean(sa.Agg)
	case COUNT:
		sa.Column.makeWindow()
		sa.doSumCountMean(sa.Agg)
	case MEAN:
		sa.Column.makeWindow()
		sa.doSumCountMean(sa.Agg)
	case MAX:
		sa.Column.makeWindow()
		sa.doMinMax(sa.Agg)
	case MIN:
		sa.Column.makeWindow()
		sa.doMinMax(sa.Agg)
	case FIRST:
		sa.Column.makeWindow()
		sa.doFirst()
	case LAST:
		sa.Column.makeWindow()
		sa.doLast()
	case PICK:
		sa.Column.makePickRelative()
		sa.doPick()
	}
}
func (sa *SmartAggregator) doSumCountMean(agg string) {
	// mmake all result nan
	if agg == SUM || agg == MEAN {
		for i := range sa.Column.Result {
			sa.Column.Result[i] = math.NaN()
		}
	}
	var sum float64
	var count float64

	savefunc := func(i int) {
		// do the last calculation
		switch agg {
		case SUM:
			if count != 0 {
				sa.Column.Result[i] = sum
			} else {
				sa.Column.Result[i] = math.NaN()
			}
		case COUNT:
			sa.Column.Result[i] = count
		case MEAN:
			if count != 0 {
				sa.Column.Result[i] = sum / count
			} else {
				sa.Column.Result[i] = math.NaN()
			}
		}
	}

	windowi := 0
	window := sa.Column.WindowRelative[windowi]

channelloop:
	for {
		val, ok := <-sa.Data
		// check if channel is closed
		if !ok {
			savefunc(windowi)
			// break the outer loop
			break channelloop
		}

		// check if the data is in the window
		if val.Epoch >= window[0] && val.Epoch <= window[1] {
			// add the value to the sum
			sum += val.Value
			count++
		} else if val.Epoch > window[1] {
			// remember the value
			savefunc(windowi)
			// loop through the windows until the epoch is less than the window[1]
			for val.Epoch > window[1] {
				windowi++
				if windowi >= len(sa.Column.WindowRelative) {
					break channelloop
				}
				window = sa.Column.WindowRelative[windowi]
			}
			// check if the data is in the window
			if val.Epoch >= window[0] && val.Epoch <= window[1] {
				// add the value to the sum
				sum = val.Value
				count = 1
			} else {
				// reset the sum and count
				sum = 0
				count = 0
			}
		}
	}
	// drain the channel
	sa.drainChannel()
}

func (sa *SmartAggregator) doMinMax(minMax string) {
	// mmake all result nan
	for i := range sa.Column.Result {
		sa.Column.Result[i] = math.NaN()
	}
	var result float64
	if minMax == "min" {
		result = math.MaxFloat64
	} else if minMax == "max" {
		result = -math.MaxFloat64
	}

	savefunc := func(i int) {
		// do the last calculation
		if result == -math.MaxFloat64 || result == math.MaxFloat64 {
			sa.Column.Result[i] = math.NaN()
		} else {
			sa.Column.Result[i] = result
		}
	}

	windowi := 0
	window := sa.Column.WindowRelative[windowi]

channelloop:
	for {
		val, ok := <-sa.Data
		// check if channel is closed
		if !ok {
			savefunc(windowi)
			// break the outer loop
			break channelloop
		}

		// check if the data is in the window
		if val.Epoch >= window[0] && val.Epoch <= window[1] {
			// process the min or max
			if minMax == "min" && val.Value < result {
				result = val.Value
			} else if minMax == "max" && val.Value > result {
				result = val.Value
			}
		} else if val.Epoch > window[1] {
			// remember the value
			savefunc(windowi)

			// loop through the windows until the epoch is less than the window[1]
			for val.Epoch > window[1] {
				windowi++
				if windowi >= len(sa.Column.WindowRelative) {
					break channelloop
				}
				window = sa.Column.WindowRelative[windowi]
			}
			// check if the data is in the window
			if val.Epoch >= window[0] && val.Epoch <= window[1] {
				// add the value to the sum
				result = val.Value
			} else {
				// if not in the next window, reset the max or min
				if minMax == "min" {
					result = math.MaxFloat64
				} else if minMax == "max" {
					result = -math.MaxFloat64
				}
			}
		}

	}
	// drain the channel
	sa.drainChannel()
}

func (sa *SmartAggregator) doFirst() {
	// mmake all result nan
	for i := range sa.Column.Result {
		sa.Column.Result[i] = math.NaN()
	}

	windowi := 0
	window := sa.Column.WindowRelative[windowi]

	// Loop through the window
channelloop:
	for {
		val, ok := <-sa.Data
		// Check if channel is closed
		if !ok {
			break channelloop
		}

		// Check if the data is in the window
		if val.Epoch >= window[0] && val.Epoch <= window[1] {
			// store the first value and move to the next window
			sa.Column.Result[windowi] = val.Value
			windowi++
			if windowi >= len(sa.Column.WindowRelative) {
				break channelloop
			}
			window = sa.Column.WindowRelative[windowi]
		} else if val.Epoch > window[1] {
			// loop through the windows until the epoch is in the window
			for val.Epoch > window[1] {
				windowi++
				if windowi >= len(sa.Column.WindowRelative) {
					break channelloop
				}
				window = sa.Column.WindowRelative[windowi]
			}
			// store the first value and move to the next window
			sa.Column.Result[windowi] = val.Value
			windowi++
			if windowi >= len(sa.Column.WindowRelative) {
				break channelloop
			}
			window = sa.Column.WindowRelative[windowi]
		}
	}
	// drain the channel
	sa.drainChannel()
}

func (sa *SmartAggregator) doLast() {
	// mmake all result nan
	for i := range sa.Column.Result {
		sa.Column.Result[i] = math.NaN()
	}

	windowi := 0
	window := sa.Column.WindowRelative[windowi]

	// Loop through the window
channelloop:
	for {
		val, ok := <-sa.Data
		// Check if channel is closed
		if !ok {
			break channelloop
		}

		// Check if the data is in the window
		if val.Epoch >= window[0] && val.Epoch <= window[1] {
			// store the last value
			sa.Column.Result[windowi] = val.Value
		} else if val.Epoch > window[1] {
			// loop through the windows until the epoch is in the window
			for val.Epoch > window[1] {
				windowi++
				if windowi >= len(sa.Column.WindowRelative) {
					break channelloop
				}
				window = sa.Column.WindowRelative[windowi]
			}
			// store the first value and move to the next window
			sa.Column.Result[windowi] = val.Value
		}
	}
	// drain the channel
	sa.drainChannel()
}

func (sa *SmartAggregator) doPick() {
	// mmake all result nan
	for i := range sa.Column.Result {
		sa.Column.Result[i] = math.NaN()
	}
	var lenlast float64 = math.MaxFloat64
	var leniter float64 = math.MaxFloat64
	// var leniterabs int64 = math.MaxInt64
	// var leniternext int64 = math.MaxInt64
	var pick float64 = math.NaN()
	var pickenow float64
	var pickenext float64
	var pseudoWindow0 float64
	var pseudoWindow1 float64

	pickprocess := func(i int) {
		pickenow = float64(sa.Column.PickRelative[i])
		// get next pick epoch
		if i+1 < len(sa.Column.PickRelative) {
			pickenext = float64(sa.Column.PickRelative[i+1])
			pseudoWindow0 = pseudoWindow1
			pseudoWindow1 = float64(pickenext+pickenow) / 2
		} else {
			pickenext = math.MaxInt64
			pseudoWindow0 = pseudoWindow1
			pseudoWindow1 = math.MaxInt64
		}
		pick = math.NaN()
		lenlast = math.MaxFloat64
	}

	picki := 0
	// get next pick epoch
	pickprocess(picki)

channelloop:
	for {
		val, ok := <-sa.Data
		if !ok {
			// save the last pick
			sa.Column.Result[picki] = pick
			break channelloop
		}

		epochi := float64(val.Epoch)

		// check if the epoch is within the window
		if epochi >= pseudoWindow0 && epochi <= pseudoWindow1 {

			// len check, len is absolute value of the difference between the epoch and the pick epoch
			leniter = math.Abs(epochi - pickenow)

			// if leniter is 0 pick the value and break channel loop
			if leniter == 0 {
				sa.Column.Result[picki] = val.Value
				// move to the next pick
				picki++
				if picki >= len(sa.Column.PickRelative) {
					break channelloop
				}
				pickprocess(picki)
				// reset values
				lenlast = math.MaxFloat64
				leniter = math.MaxFloat64
				pick = math.NaN()
				continue channelloop
			}

			// if leniter is smaller than lenlast, save the pick
			if leniter < lenlast {
				pick = val.Value
				lenlast = leniter
			}

		} else if epochi > pseudoWindow1 {
			// save the last pick
			sa.Column.Result[picki] = pick
			// move until the epoch is in the window
			for epochi > pseudoWindow1 {
				picki++
				if picki >= len(sa.Column.PickRelative) {
					break channelloop
				}
				pickprocess(picki)
			}
		}
	}
	// drain the channel
	sa.drainChannel()
}

// FUNCTIONS
// THIS IS THE MAIN MAP OF WORKING SMARTAGGREGATOR
// Working map of SmartAggregator
type SAMap map[string]*SmartAggregator

func (sm SAMap) SAMapToStruct(timePrecision string) SAResult {
	var timeResultEp *[]int64
	resultMap := make(Columns)
	for _, v := range sm {
		if timeResultEp == nil {
			timeResultEp = v.Column.TimeResultEp
		}
		resultMap[v.Column.OutputColumnName] = &v.Column.Result
	}

	// convert timeResultEp to time.Time
	timeResult := make([]time.Time, len(*timeResultEp))
	for i, v := range *timeResultEp {
		timeResult[i] = EpochtoTime(v, timePrecision)
	}
	return SAResult{
		Columns:   resultMap,
		TimeStamp: &timeResult,
	}
}

// OUTPUT OF SMARTAGGREGATOR
// Output map of SmartAggregator
type Columns map[string]*[]float64

type SAResult struct {
	Columns
	Requests  *[]RequestColumnTable // it is necessary to save the request when we need to convert it to csv, because without it the order of the columns will be random
	TimeStamp *[]time.Time
}

// SaveToCSV saves the SAResult to a csv file
func (result SAResult) SaveToCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	headers := []string{"timeResultEp"}
	for i := range *result.Requests {
		colname := (*result.Requests)[i].OutputColumnName
		headers = append(headers, colname)
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Writing values
	for idx, dte := range *result.TimeStamp {
		line := make([]string, len(result.Columns)+1)
		line[0] = dte.UTC().Format(time.DateTime)

		col := 1
		for col < len(line) {
			// get value from the column using header
			colname := headers[col]
			resValues := result.Columns[colname]
			if idx < len(*resValues) {
				line[col] = strconv.FormatFloat((*resValues)[idx], 'f', -1, 64)
			} else {
				line[col] = ""
			}
			col++
		}
		if err := writer.Write(line); err != nil {
			return err
		}
	}
	return nil
}

// JSON5 output of SAResult
func (result SAResult) ToJson5() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`{"Columns":{`)

	// Write results
	first := true
	for key, arr := range result.Columns {
		if !first {
			buf.WriteString(",")
		}
		first = false

		buf.WriteString(fmt.Sprintf(`"%s":[`, key))
		for i, val := range *arr {
			if i != 0 {
				buf.WriteString(",")
			}

			switch {
			case math.IsNaN(val):
				buf.WriteString(`NaN`)
			case math.IsInf(val, 1):
				buf.WriteString(`Infinity`)
			case math.IsInf(val, -1):
				buf.WriteString(`-Infinity`)
			default:
				buf.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			}
		}
		buf.WriteString("]")
	}

	buf.WriteString(`},"Time":[`)

	// Write time results
	for i, dte := range *result.TimeStamp {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("\"%s\"", dte.UTC().Format("2006-01-02T15:04:05")))
	}

	buf.WriteString("]}")

	return buf.Bytes(), nil
}
