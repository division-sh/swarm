package runtime

import (
	"strings"
	"time"
)

// WeekStartUTC returns the most recent weekly reset boundary at 00:00 UTC for the
// configured reset day (e.g. "monday"). Defaults to Monday if invalid.
func WeekStartUTC(now time.Time, resetDay string) time.Time {
	now = now.UTC()
	target := parseWeekday(resetDay)
	// Go: Sunday=0 ... Saturday=6.
	daysBack := (int(now.Weekday()) - int(target) + 7) % 7
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysBack)
	return start
}

// NextWeekResetUTC returns the next weekly reset boundary at 00:00 UTC.
func NextWeekResetUTC(now time.Time, resetDay string) time.Time {
	start := WeekStartUTC(now, resetDay)
	// Always move forward by 7 days to reach the next boundary.
	return start.Add(7 * 24 * time.Hour)
}

func parseWeekday(raw string) time.Weekday {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "sunday":
		return time.Sunday
	case "monday":
		return time.Monday
	case "tuesday":
		return time.Tuesday
	case "wednesday":
		return time.Wednesday
	case "thursday":
		return time.Thursday
	case "friday":
		return time.Friday
	case "saturday":
		return time.Saturday
	default:
		return time.Monday
	}
}
