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
	var lennow int64 = math.MaxInt64
	var leniter int64 = math.MaxInt64
	var leniterabs int64 = math.MaxInt64
	var leniternext int64 = math.MaxInt64
	var pick float64 = math.NaN()
	var pickenow int64
	var pickenext int64
	var i int

outerloop:
	for i, pickenow = range sa.Column.PickRelative {
		// get next pick epoch
		if i+1 < len(sa.Column.PickRelative) {
			pickenext = sa.Column.PickRelative[i+1]
		} else {
			pickenext = math.MaxInt64
		}

	channelloop:
		for {
			val, ok := <-sa.Data
			if !ok {
				// save the last pick
				sa.Column.Result[i] = pick
				// break outerloop
				break outerloop
			}

			// len check
			leniter = val.Epoch - pickenow
			leniterabs = int64(math.Abs(float64(leniter)))
			leniternext = int64(math.Abs(float64(val.Epoch - pickenext)))

			// if leniter is 0 pick the value and break channel loop
			if leniter == 0 {
				sa.Column.Result[i] = val.Value
				pick = 0
				lennow = math.MaxInt64
				break channelloop
			}

			// if leniter is smaller than leniternext, and leniter is positive, the point is closer to the next pick, so save the pick, and break the channel loop
			if leniterabs > leniternext && leniter > 0 {
				sa.Column.Result[i] = pick
				pick = val.Value
				lennow = leniterabs
				break channelloop
			}

			// if the leniter is smaller than lennow, save the pick
			if leniterabs < lennow {
				pick = val.Value
				lennow = leniterabs
			}
		}
	}
}
