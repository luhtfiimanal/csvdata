package csvdata

import (
	"math"
)

const (
	SUM   = "sum"
	COUNT = "count"
	MEAN  = "mean"
	MAX   = "max"
	MIN   = "min"
	LAST  = "last"
	FIRST = "first"
	PICK  = "pick"
)

func NewAggregator(agg string) *Aggregator {
	aggret := &Aggregator{
		Agg: agg,
	}

	aggret.Reset()
	return aggret
}

type PickerDate struct {
	PickEpoch int64
}

type Aggregator struct {
	Agg string
	// Column string
	Data chan Input
	Done chan result
	*PickerDate
}

type Input struct {
	Epoch int64
	Value float64
}

type result struct {
	Value float64
}

func (a *Aggregator) Do() {
	switch a.Agg {
	case SUM:
		a.doSum()
	case COUNT:
		a.doCount()
	case MEAN:
		a.doMean()
	case MAX:
		a.doMax()
	case MIN:
		a.doMin()
	case LAST:
		a.doLast()
	case FIRST:
		a.doFirst()
	case PICK:
		a.doPick()
	}
}

func (a *Aggregator) doMean() {
	var sum float64
	var count int
	for val := range a.Data {
		sum += val.Value
		count++
	}
	a.Done <- result{Value: sum / float64(count)}
	close(a.Done)
}

func (a *Aggregator) doMax() {
	max := -math.MaxFloat64
	for val := range a.Data {
		if val.Value > max {
			max = val.Value
		}
	}
	a.Done <- result{Value: max}
	close(a.Done)
}

func (a *Aggregator) doMin() {
	min := math.MaxFloat64
	for val := range a.Data {
		if val.Value < min {
			min = val.Value
		}
	}
	a.Done <- result{Value: min}
	close(a.Done)
}

func (a *Aggregator) doLast() {
	var last float64
	var lastepoch int64
	for val := range a.Data {
		if val.Epoch > lastepoch {
			lastepoch = val.Epoch
			last = val.Value
		}
	}
	a.Done <- result{Value: last}
	close(a.Done)
}

func (a *Aggregator) doPick() {
	// pick nearest value from the picker date
	var pick float64
	var pickepochdist int64
	pickepochdist = math.MaxInt64
	for val := range a.Data {
		epochdist := int64(math.Abs(float64(val.Epoch - a.PickEpoch)))
		if epochdist < pickepochdist {
			pickepochdist = epochdist
			pick = val.Value
		}
	}
	a.Done <- result{Value: pick}
	close(a.Done)
}

func (a *Aggregator) doFirst() {
	var first float64
	var firstepoch int64
	firstepoch = math.MaxInt64
	for val := range a.Data {
		if val.Epoch < firstepoch {
			firstepoch = val.Epoch
			first = val.Value
		}
	}
	a.Done <- result{Value: first}
	close(a.Done)
}

func (a *Aggregator) doSum() {
	var sum float64
	for val := range a.Data {
		sum += val.Value
	}
	a.Done <- result{Value: sum}
	close(a.Done)
}

func (a *Aggregator) doCount() {
	var count int
	for range a.Data {
		count++
	}
	a.Done <- result{Value: float64(count)}
	close(a.Done)
}

func (a *Aggregator) Reset() {
	a.Data = make(chan Input, 3) // reinitialize the Data channel
	a.Done = make(chan result)   // reinitialize the Done channel
	go a.Do()
}
