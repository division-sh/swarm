package runtime

import (
	"testing"
	"time"
)

func TestParseWeekday_AllCases(t *testing.T) {
	cases := map[string]time.Weekday{
		"sunday":    time.Sunday,
		"monday":    time.Monday,
		"tuesday":   time.Tuesday,
		"wednesday": time.Wednesday,
		"thursday":  time.Thursday,
		"friday":    time.Friday,
		"saturday":  time.Saturday,
		"  FrIdAy ": time.Friday,
		"nope":      time.Monday, // default
	}
	for in, want := range cases {
		if got := parseWeekday(in); got != want {
			t.Fatalf("parseWeekday(%q)=%v want %v", in, got, want)
		}
	}
}

