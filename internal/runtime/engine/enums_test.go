package engine

import "testing"

func TestIsHandledOutcomeMatrix(t *testing.T) {
	tests := []struct {
		name   string
		status OutcomeStatus
		want   bool
	}{
		{name: "unknown", status: OutcomeUnknown},
		{name: "completed", status: OutcomeCompleted, want: true},
		{name: "blocked", status: OutcomeBlocked, want: true},
		{name: "discarded", status: OutcomeDiscarded},
		{name: "rejected", status: OutcomeRejected},
		{name: "killed", status: OutcomeKilled, want: true},
		{name: "escalated", status: OutcomeEscalated, want: true},
		{name: "waiting", status: OutcomeWaiting, want: true},
		{name: "fanned_out", status: OutcomeFannedOut, want: true},
		{name: "unclassified", status: OutcomeStatus(255)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsHandledOutcome(test.status); got != test.want {
				t.Fatalf("IsHandledOutcome(%d) = %v, want %v", test.status, got, test.want)
			}
		})
	}
}
