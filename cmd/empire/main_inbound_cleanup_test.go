package main

import (
	"testing"
	"time"
)

func TestInboundRetentionCutoffUsesSevenDays(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	cutoff := inboundRetentionCutoff(now)
	want := now.Add(-7 * 24 * time.Hour)
	if !cutoff.Equal(want) {
		t.Fatalf("expected cutoff=%s, got %s", want.Format(time.RFC3339), cutoff.Format(time.RFC3339))
	}
}
