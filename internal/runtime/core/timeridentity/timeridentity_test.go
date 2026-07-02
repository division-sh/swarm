package timeridentity

import (
	"testing"
	"time"
)

func TestParseStartTrigger(t *testing.T) {
	trigger, err := ParseStartTrigger("state:active")
	if err != nil {
		t.Fatalf("ParseStartTrigger: %v", err)
	}
	if trigger.Kind != TriggerKindState || trigger.Name != "active" {
		t.Fatalf("trigger = %#v", trigger)
	}
	if !trigger.MatchesStage("active") {
		t.Fatal("expected state trigger to match stage")
	}
}

func TestParseTriggerRejectsUnprefixedValues(t *testing.T) {
	if _, err := ParseStartTrigger("ticket.opened"); err == nil {
		t.Fatal("expected unprefixed trigger to be rejected")
	}
}

func TestParseCancelTriggerRejectsBoot(t *testing.T) {
	if _, err := ParseCancelTrigger("boot"); err == nil {
		t.Fatal("expected cancel_on boot to be rejected")
	}
}

func TestParseDelayDurationSupportsGoDurationAndDayUnit(t *testing.T) {
	tests := []struct {
		raw  string
		want time.Duration
	}{
		{raw: "30m", want: 30 * time.Minute},
		{raw: "1h30m", want: 90 * time.Minute},
		{raw: "7d", want: 7 * 24 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, ok := ParseDelayDuration(tc.raw)
			if !ok {
				t.Fatalf("ParseDelayDuration(%q) did not parse", tc.raw)
			}
			if got != tc.want {
				t.Fatalf("ParseDelayDuration(%q) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseDelayDurationRejectsInvalidOrNonPositiveValues(t *testing.T) {
	for _, raw := range []string{"", "0s", "-1s", "1.5d", "soon"} {
		t.Run(raw, func(t *testing.T) {
			if got, ok := ParseDelayDuration(raw); ok {
				t.Fatalf("ParseDelayDuration(%q) = %s, want rejection", raw, got)
			}
		})
	}
}

func TestTimerHandlePayloadRoundTrip(t *testing.T) {
	handle := WorkflowTimerHandle("check_timer")
	parsed, ok := ParseTimerHandle(handle.PayloadMetadata())
	if !ok {
		t.Fatal("expected workflow timer handle payload to round trip")
	}
	if parsed.Kind != TimerHandleWorkflowTimer || parsed.TimerID != "check_timer" {
		t.Fatalf("parsed = %#v", parsed)
	}
	if parsed.TaskID() != "check_timer" {
		t.Fatalf("TaskID() = %q", parsed.TaskID())
	}
}

func TestAccumulationTimeoutHandleRoundTrip(t *testing.T) {
	bucket := NewAccumulatorBucketRef("collector", "item.arrived")
	handle := AccumulationTimeoutHandle(bucket)
	parsedHandle, ok := ParseTimerHandle(handle.PayloadMetadata())
	if !ok {
		t.Fatal("expected accumulation timeout handle payload to round trip")
	}
	if parsedHandle.Kind != TimerHandleAccumulationTimeout {
		t.Fatalf("parsedHandle = %#v", parsedHandle)
	}
	if parsedHandle.Bucket != bucket {
		t.Fatalf("parsed bucket = %#v, want %#v", parsedHandle.Bucket, bucket)
	}
	if got := parsedHandle.TaskID(); got != "accumulate_timeout:collector:item.arrived" {
		t.Fatalf("TaskID() = %q", got)
	}
}

func TestParseAccumulatorBucketKey(t *testing.T) {
	bucket, ok := ParseAccumulatorBucketKey("collector:item.arrived")
	if !ok {
		t.Fatal("expected accumulator bucket key to parse")
	}
	if bucket.NodeID != "collector" || bucket.EventType != "item.arrived" {
		t.Fatalf("bucket = %#v", bucket)
	}
}
