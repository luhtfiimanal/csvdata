package csvdata

import "math"

// ClassifyWindDirection classifies the given wind direction into one of the 8 wind bins.
//
// This function takes a wind direction in degrees and returns the corresponding primary angle
// representing the bin it falls into. The angles correspond to North, North-East, East,
// South-East, South, South-West, West, and North-West.
//
// Parameters:
// - windDirection: A float64 representing the wind direction in degrees.
//
// Returns:
// - float64: The angle representing the wind direction bin.
func ClassifyWindDirection(windDirection float64) float64 {
	// Normalize the wind direction to the range [0, 360)
	windDirection = math.Mod(windDirection, 360)
	if windDirection < 0 {
		windDirection += 360
	}

	// Classify the wind direction into bins
	switch {
	case windDirection >= -22.5 && windDirection < 22.5:
		return 0 // North
	case windDirection >= 22.5 && windDirection < 67.5:
		return 45 // North-East
	case windDirection >= 67.5 && windDirection < 112.5:
		return 90 // East
	case windDirection >= 112.5 && windDirection < 157.5:
		return 135 // South-East
	case windDirection >= 157.5 && windDirection < 202.5:
		return 180 // South
	case windDirection >= 202.5 && windDirection < 247.5:
		return 225 // South-West
	case windDirection >= 247.5 && windDirection < 292.5:
		return 270 // West
	case windDirection >= 292.5 && windDirection < 337.5:
		return 315 // North-West
	default:
		return 0 // Default case (shouldn't reach here)
	}
}
