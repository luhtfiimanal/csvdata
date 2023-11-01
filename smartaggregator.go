package csvdata

import (
	"math"
	"sync"
)

type SmartAggregator struct {
	Agg    string
	Data   chan Input
	Column *SAColumn
}

type SAColumn struct {
	OutputColumnName string
	TimeResultEp     *[]int64
	PickRelative     []int64
	WindowRelative   [][2]int64
	Result           []float64
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
		sa.doSumCountMean(sa.Agg)
	case COUNT:
		sa.doSumCountMean(sa.Agg)
	case MEAN:
		sa.doSumCountMean(sa.Agg)
	case MAX:
		sa.doMinMax(sa.Agg)
	case MIN:
		sa.doMinMax(sa.Agg)
	case FIRST:
		sa.doFirst()
	case LAST:
		sa.doLast()
	case PICK:
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
}
