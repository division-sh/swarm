package delivery

import (
	"math"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestCommittedRetryDueUsesCommonUpwardMicrosecondPrecision(t *testing.T) {
	completed := time.Date(2026, 10, 15, 12, 0, 0, 999999000, time.UTC)
	for _, test := range []struct {
		delay time.Duration
		want  time.Duration
	}{
		{time.Nanosecond, time.Microsecond},
		{499 * time.Nanosecond, time.Microsecond},
		{500 * time.Nanosecond, time.Microsecond},
		{999 * time.Nanosecond, time.Microsecond},
		{time.Microsecond, time.Microsecond},
		{1001 * time.Nanosecond, 2 * time.Microsecond},
		{1500 * time.Nanosecond, 2 * time.Microsecond},
		{2 * time.Microsecond, 2 * time.Microsecond},
		{13 * time.Microsecond, 13 * time.Microsecond},
		{23 * time.Millisecond, 23 * time.Millisecond},
	} {
		t.Run(test.delay.String(), func(t *testing.T) {
			got := committedRetryDue(completed, test.delay)
			if !got.Equal(completed.Add(test.want)) || got.Before(completed.Add(test.delay)) || !got.After(completed) {
				t.Fatalf("committed due: completed=%s delay=%s got=%s want=%s", completed, test.delay, got, completed.Add(test.want))
			}
		})
	}
	requested := completed.Add(time.Duration(math.MaxInt64))
	got := committedRetryDue(completed, time.Duration(math.MaxInt64))
	if got.Before(requested) || got.Sub(requested) >= time.Microsecond || got.Nanosecond()%int(time.Microsecond) != 0 {
		t.Fatalf("maximum duration overflowed during rounding: requested=%s got=%s", requested, got)
	}
}

func TestCommittedRetryDueOutcomeDecoding(t *testing.T) {
	settled := time.Date(2026, 10, 15, 12, 0, 0, 123456000, time.UTC)
	due := settled.Add(17 * time.Millisecond)
	for _, test := range []struct {
		name    string
		outcome string
		raw     any
		wantDue bool
		wantErr bool
	}{
		{"retry_time", "retry_scheduled", due, true, false},
		{"retry_encoded_time", "retry_scheduled", due.Format(time.RFC3339Nano), true, false},
		{"retry_null", "retry_scheduled", nil, false, true},
		{"retry_empty", "retry_scheduled", "", false, true},
		{"retry_malformed", "retry_scheduled", "not-a-time", false, true},
		{"retry_equal", "retry_scheduled", settled, false, true},
		{"retry_earlier", "retry_scheduled", settled.Add(-time.Microsecond), false, true},
		{"delivered_null", "delivered", nil, false, false},
		{"dead_letter_null", "dead_letter", nil, false, false},
		{"canceled_null", "canceled", nil, false, false},
		{"terminalized_null", "terminalized", nil, false, false},
		{"delivered_due", "delivered", due, false, true},
		{"dead_letter_due", "dead_letter", due, false, true},
		{"canceled_due", "canceled", due, false, true},
		{"terminalized_due", "terminalized", due, false, true},
		{"terminal_empty_not_null", "delivered", "", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			item := deliverylifecycle.Outcome{ClaimVersion: 3, Outcome: test.outcome, SettledAt: settled}
			actual, err := decodeOutcomeRetryDue(item, test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("invalid committed retry due accepted: %+v raw=%v", item, test.raw)
				}
				return
			}
			if err != nil || (actual != nil) != test.wantDue {
				t.Fatalf("retry due presence: got=%v err=%v wantPresent=%v", actual, err, test.wantDue)
			}
			if actual != nil && !actual.Equal(due) {
				t.Fatalf("committed due changed: got=%v want=%v", actual, due)
			}
		})
	}
}
