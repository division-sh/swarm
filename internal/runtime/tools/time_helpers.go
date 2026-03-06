package tools

import (
	"strings"
	"time"
)

func WeekStartUTC(now time.Time, resetDay string) time.Time {
	now = now.UTC()
	target := parseWeekday(resetDay)
	daysBack := (int(now.Weekday()) - int(target) + 7) % 7
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysBack)
	return start
}

func NextWeekResetUTC(now time.Time, resetDay string) time.Time {
	start := WeekStartUTC(now, resetDay)
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
