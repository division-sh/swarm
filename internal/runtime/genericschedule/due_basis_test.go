package genericschedule

import (
	"testing"
	"time"
)

func TestEveryDueRequiresRepresentableInterval(t *testing.T) {
	previous := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for _, interval := range []time.Duration{-time.Second, 0, time.Nanosecond, 999 * time.Nanosecond, 1500 * time.Nanosecond} {
		t.Run(interval.String(), func(t *testing.T) {
			basis := EveryDue(interval)
			if err := basis.Validate(); err == nil {
				t.Fatal("unrepresentable interval admitted")
			}
			if _, err := basis.FirstDue(previous); err == nil {
				t.Fatal("unrepresentable first occurrence admitted")
			}
			if _, err := basis.Next(previous); err == nil {
				t.Fatal("unrepresentable restored recurrence admitted")
			}
		})
	}
	for _, interval := range []time.Duration{time.Microsecond, 1500 * time.Microsecond, 5 * time.Minute} {
		basis := EveryDue(interval)
		first, err := basis.FirstDue(previous)
		if err != nil || !first.Equal(previous.Add(interval)) {
			t.Fatalf("first %s = %s, %v", interval, first, err)
		}
		next, err := basis.Next(first)
		if err != nil || !next.Equal(first.Add(interval)) || !next.After(first) {
			t.Fatalf("next %s = %s, %v", interval, next, err)
		}
	}
}

func TestCronDueRequiresFutureOccurrence(t *testing.T) {
	previous := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for _, spec := range []string{"0 0 31 2 *", "@daily", "@every 1m", "CRON_TZ=Europe/Paris 0 9 * * *", "TZ=UTC 0 9 * * *", "0 0 9 * * *", "tomorrow"} {
		t.Run(spec, func(t *testing.T) {
			basis := CronDue(spec)
			if err := basis.Validate(); err == nil {
				t.Fatal("invalid cron admitted")
			}
			if _, err := basis.FirstDue(previous); err == nil {
				t.Fatal("invalid first occurrence admitted")
			}
			if _, err := basis.Next(previous); err == nil {
				t.Fatal("invalid restored recurrence admitted")
			}
		})
	}
	basis := CronDue("0 0 29 2 *")
	first, err := basis.FirstDue(previous)
	want := time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)
	if err != nil || !first.Equal(want) {
		t.Fatalf("leap-day first = %s, %v; want %s", first, err, want)
	}
	next, err := basis.Next(first)
	if err != nil || !next.Equal(time.Date(2032, 2, 29, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("leap-day next = %s, %v", next, err)
	}
	// The library has no executable next leap day within its search horizon.
	if _, err := basis.FirstDue(time.Date(2097, 3, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("zero cron coordinate admitted")
	}
}

func TestCronDueComputesUTCRegardlessOfInputZone(t *testing.T) {
	previous := time.Date(2026, 10, 2, 8, 30, 0, 0, time.UTC)
	basis := CronDue("0 9 * * *")
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("west", -7*60*60), time.FixedZone("east", 9*60*60)} {
		first, err := basis.FirstDue(previous.In(zone))
		if err != nil || !first.Equal(time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)) || first.Location() != time.UTC {
			t.Fatalf("UTC occurrence from %s: %s, %v", zone, first, err)
		}
		next, err := basis.Next(first.In(zone))
		if err != nil || !next.Equal(first.Add(24*time.Hour)) || next.Location() != time.UTC {
			t.Fatalf("UTC recurrence from %s: %s, %v", zone, next, err)
		}
	}
}
