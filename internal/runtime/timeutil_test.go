package runtime

import (
	"testing"
	"time"
)

func TestTimeutil_WeekStartAndNextReset(t *testing.T) {
	now := time.Date(2026, 2, 14, 10, 0, 0, 0, time.UTC) // Saturday
	start := WeekStartUTC(now, "monday")
	if start.Weekday() != time.Monday || start.Hour() != 0 {
		t.Fatalf("unexpected week start: %v", start)
	}
	next := NextWeekResetUTC(now, "monday")
	if !next.After(now) {
		t.Fatalf("expected next reset after now: now=%v next=%v", now, next)
	}

	if parseWeekday("tuesday") != time.Tuesday {
		t.Fatal("expected tuesday")
	}
	if parseWeekday("bad") != time.Monday {
		t.Fatal("expected default monday")
	}
}

