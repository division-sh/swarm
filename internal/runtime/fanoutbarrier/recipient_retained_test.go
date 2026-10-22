package fanoutbarrier

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutRetainedRecipientContributionWaitsAndReducesOnce(t *testing.T) {
	statuses := []deliverylifecycle.Status{
		deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress, deliverylifecycle.StatusFailed,
		deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusCanceled,
	}
	for _, retained := range []fanoutobligation.InheritedTerminalDisposition{
		fanoutobligation.InheritedSucceeded, fanoutobligation.InheritedDeadLettered, fanoutobligation.InheritedCanceled,
	} {
		for _, first := range statuses {
			for _, second := range statuses {
				t.Run(string(retained)+"/"+string(first)+"/"+string(second), func(t *testing.T) {
					rows := []deliverylifecycle.Snapshot{retainedRecipientSnapshot(first), retainedRecipientSnapshot(second)}
					before := append([]deliverylifecycle.Snapshot(nil), rows...)
					got, terminal, err := RecipientDispositionWithRetained(rows, retained)
					if !reflect.DeepEqual(rows, before) {
						t.Fatal("reducing retained history changed child delivery evidence")
					}
					wantTerminal := first.Terminal() && second.Terminal()
					var want fanoutobligation.InheritedTerminalDisposition
					if wantTerminal {
						want = fanoutobligation.InheritedSucceeded
						if retained == fanoutobligation.InheritedCanceled || first == deliverylifecycle.StatusCanceled || second == deliverylifecycle.StatusCanceled {
							want = fanoutobligation.InheritedCanceled
						}
						if retained == fanoutobligation.InheritedDeadLettered || first == deliverylifecycle.StatusDeadLetter || second == deliverylifecycle.StatusDeadLetter {
							want = fanoutobligation.InheritedDeadLettered
						}
					}
					if err != nil || got != want || terminal != wantTerminal {
						t.Fatalf("retained=%s child=%s/%s result=%s terminal=%t err=%v; want=%s terminal=%t", retained, first, second, got, terminal, err, want, wantTerminal)
					}
					if retained == fanoutobligation.InheritedSucceeded {
						ordinary, ordinaryTerminal, ordinaryErr := RecipientDisposition(rows)
						if ordinaryErr != nil || ordinary != got || ordinaryTerminal != terminal {
							t.Fatal("successful retained contribution changed ordinary reduction")
						}
					}
				})
			}
		}
	}
}

func retainedRecipientSnapshot(status deliverylifecycle.Status) deliverylifecycle.Snapshot {
	row := deliverylifecycle.Snapshot{Status: status}
	if status == deliverylifecycle.StatusCanceled {
		row.ReasonCode, row.SettledAt = string(deliverylifecycle.CancellationTerminate), time.Unix(1700000000, 0).UTC()
	}
	return row
}

func TestFanOutRetainedRecipientContributionRefusesInventedSettlement(t *testing.T) {
	for _, retained := range []fanoutobligation.InheritedTerminalDisposition{"", "unknown", " canceled ", fanoutobligation.InheritedNoRoute} {
		t.Run("contribution/"+string(retained), func(t *testing.T) {
			got, terminal, err := RecipientDispositionWithRetained([]deliverylifecycle.Snapshot{retainedRecipientSnapshot(deliverylifecycle.StatusDelivered)}, retained)
			if err == nil || got != "" || terminal {
				t.Fatalf("invalid terminal contribution settled a child: %s %t %v", got, terminal, err)
			}
		})
	}
	for _, test := range []struct {
		name string
		rows []deliverylifecycle.Snapshot
	}{
		{"empty", nil},
		{"unknown_status", []deliverylifecycle.Snapshot{{Status: "unknown"}}},
		{"noncanonical_status", []deliverylifecycle.Snapshot{{Status: " delivered "}}},
		{"unsettled_cancellation", []deliverylifecycle.Snapshot{{Status: deliverylifecycle.StatusCanceled, ReasonCode: "terminate"}}},
	} {
		t.Run("child/"+test.name, func(t *testing.T) {
			got, terminal, err := RecipientDispositionWithRetained(test.rows, fanoutobligation.InheritedDeadLettered)
			if err == nil || got != "" || terminal {
				t.Fatalf("retained failure bypassed child evidence: %s %t %v", got, terminal, err)
			}
		})
	}
}
