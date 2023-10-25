package csvdata

import "time"

func GetNearestPastTimeUnit(t time.Time, duration string) time.Time {
	switch duration {
	case "1y":
		return time.Date(t.Year(), time.January, 0, 0, 0, 0, 0, t.Location())
	case "1M":
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location())
	case "7d", "2d", "1d":
		days, _ := time.ParseDuration(duration)
		return time.Date(t.Year(), t.Month(), t.Day()-int(days.Hours()/24), 0, 0, 0, 0, t.Location())
	case "12h", "6h", "3h", "1h":
		hours, _ := time.ParseDuration(duration)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()-int(hours.Hours()), 0, 0, 0, t.Location())
	case "15m", "10m", "5m", "1m":
		minutes, _ := time.ParseDuration(duration)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()-int(minutes.Minutes()), 0, 0, t.Location())
	default:
		return t
	}
}
