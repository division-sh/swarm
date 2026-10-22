package fanoutbarrier

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutRecipientDispositionPreservesTerminalAndPendingPartitions(t *testing.T) {
	for _, test := range []struct {
		name     string
		statuses []deliverylifecycle.Status
		want     fanoutobligation.InheritedTerminalDisposition
		terminal bool
	}{
		{"success", []deliverylifecycle.Status{deliverylifecycle.StatusDelivered}, fanoutobligation.InheritedSucceeded, true},
		{"canceled", []deliverylifecycle.Status{deliverylifecycle.StatusCanceled}, fanoutobligation.InheritedCanceled, true},
		{"failure_before_cancel", []deliverylifecycle.Status{deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusCanceled}, fanoutobligation.InheritedDeadLettered, true},
		{"cancel_before_failure", []deliverylifecycle.Status{deliverylifecycle.StatusCanceled, deliverylifecycle.StatusDeadLetter}, fanoutobligation.InheritedDeadLettered, true},
		{"success_with_cancel", []deliverylifecycle.Status{deliverylifecycle.StatusDelivered, deliverylifecycle.StatusCanceled}, fanoutobligation.InheritedCanceled, true},
		{"success_with_pending", []deliverylifecycle.Status{deliverylifecycle.StatusDelivered, deliverylifecycle.StatusPending}, "", false},
		{"cancel_with_pending", []deliverylifecycle.Status{deliverylifecycle.StatusCanceled, deliverylifecycle.StatusPending}, "", false},
		{"failure_with_pending", []deliverylifecycle.Status{deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusPending}, "", false},
		{"failure_with_executing", []deliverylifecycle.Status{deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusInProgress}, "", false},
		{"failure_with_retry", []deliverylifecycle.Status{deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusFailed}, "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			deliveries := make([]deliverylifecycle.Snapshot, len(test.statuses))
			for i, status := range test.statuses {
				deliveries[i].Status = status
				if status == deliverylifecycle.StatusCanceled {
					deliveries[i].ReasonCode = string(deliverylifecycle.CancellationTerminate)
					deliveries[i].SettledAt = time.Now().UTC()
				}
			}
			disposition, terminal, err := RecipientDisposition(deliveries)
			if err != nil || disposition != test.want || terminal != test.terminal {
				t.Fatalf("disposition=%q terminal=%t err=%v; want %q %t", disposition, terminal, err, test.want, test.terminal)
			}
		})
	}
}

func TestFanOutRecipientDispositionRejectsUnsettledCancellationAndInvalidStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		deliveries []deliverylifecycle.Snapshot
	}{
		{"absent", nil},
		{"unknown_status", []deliverylifecycle.Snapshot{{Status: "unknown"}}},
		{"noncanonical_status", []deliverylifecycle.Snapshot{{Status: " delivered "}}},
		{"cancel_intent_only", []deliverylifecycle.Snapshot{{Status: deliverylifecycle.StatusCanceled, ReasonCode: "terminate"}}},
		{"operational_cancel_reason", []deliverylifecycle.Snapshot{{Status: deliverylifecycle.StatusCanceled, ReasonCode: "run_stopped", SettledAt: time.Now().UTC()}}},
		{"canceled_open_claim", []deliverylifecycle.Snapshot{{Status: deliverylifecycle.StatusCanceled, ReasonCode: "terminate", SettledAt: time.Now().UTC(), ClaimExpiresAt: time.Now().UTC()}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			disposition, terminal, err := RecipientDisposition(test.deliveries)
			if err == nil || disposition != "" || terminal {
				t.Fatalf("invalid evidence gained terminal disposition: %q %t %v", disposition, terminal, err)
			}
		})
	}
}
