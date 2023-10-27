package csvdata

import "time"

func GetNearestPastTimeUnit(t time.Time, duration string) time.Time {
	switch duration {
	case "1y":
		return time.Date(t.Year(), time.January, 0, 0, 0, 0, 0, t.Location())
	case "1M":
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location())

	// case day 7d, 2d, 1d, 24h
	case "7d", "2d", "1d", "24h":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	// case hour 12h, 6h, 3h, 1h
	case "12h":
		// create array from 0 to 21 with skip 12
		checkArray := []int{0, 12}
		for _, val := range checkArray {
			hour := t.Hour()
			// check if hour is between val and val+12
			if hour >= val && hour < val+12 {
				return time.Date(t.Year(), t.Month(), t.Day(), val, 0, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	case "6h":
		// create array from 0 to 21 with skip 6
		checkArray := []int{0, 6, 12, 18}
		for _, val := range checkArray {
			hour := t.Hour()
			// check if hour is between val and val+6
			if hour >= val && hour < val+6 {
				return time.Date(t.Year(), t.Month(), t.Day(), val, 0, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	case "3h":
		// create array from 0 to 21 with skip 3
		checkArray := []int{0, 3, 6, 9, 12, 15, 18, 21}
		for _, val := range checkArray {
			hour := t.Hour()
			// check if hour is between val and val+3
			if hour >= val && hour < val+3 {
				return time.Date(t.Year(), t.Month(), t.Day(), val, 0, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	case "1h":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	// case minutes 15m, 10m, 5m, 3m, 1m
	case "15m":
		// create array from 0 to 45 with skip 15
		checkArray := []int{0, 15, 30, 45}
		for _, val := range checkArray {
			minute := t.Minute()
			// check if minute is between val and val+15
			if minute >= val && minute < val+15 {
				return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), val, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	case "10m":
		// create array from 0 to 50 with skip 10
		checkArray := []int{0, 10, 20, 30, 40, 50}
		for _, val := range checkArray {
			minute := t.Minute()
			// check if minute is between val and val+10
			if minute >= val && minute < val+10 {
				return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), val, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	case "5m":
		// create array from 0 to 55 with skip 5
		checkArray := []int{0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}
		for _, val := range checkArray {
			minute := t.Minute()
			// check if minute is between val and val+5
			if minute >= val && minute < val+5 {
				return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), val, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	case "3m":
		// create array from 0 to 57 with skip 3
		checkArray := []int{0, 3, 6, 9, 12, 15, 18, 21, 24, 27,
			30, 33, 36, 39, 42, 45, 48, 51, 54, 57}
		for _, val := range checkArray {
			minute := t.Minute()
			// check if minute is between val and val+3
			if minute >= val && minute < val+3 {
				return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), val, 0, 0, t.Location())
			}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	case "1m":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	default:
		return t
	}
}
