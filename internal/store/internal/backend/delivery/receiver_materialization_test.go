package delivery

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestReceiverMaterializationCompletedAuthorityIsHistory(t *testing.T) {
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	otherSource, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	selectedID := uuid.NewString()
	normal := func(source correlation.SourceArtifactFact, generation uint64) deliverylifecycle.ExecutionAuthority {
		t.Helper()
		result, err := deliverylifecycle.NewNormalExecutionAuthority(source, uuid.NewString(), generation)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	selected := func(generation uint64) deliverylifecycle.ExecutionAuthority {
		t.Helper()
		result, err := deliverylifecycle.NewSelectedExecutionAuthority(source, selectedID, runID, generation)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	old, next := normal(source, 1), normal(source, 2)
	for _, test := range []struct {
		name        string
		status      deliverylifecycle.Status
		left, right deliverylifecycle.ExecutionAuthority
		foreignRun  bool
		want        bool
	}{
		{"same_pending", deliverylifecycle.StatusPending, old, old, false, true},
		{"same_completed", deliverylifecycle.StatusDelivered, old, old, false, true},
		{"completed_normal_restart", deliverylifecycle.StatusDelivered, old, next, false, true},
		{"pending_other_generation", deliverylifecycle.StatusPending, old, next, false, false},
		{"running_other_generation", deliverylifecycle.StatusInProgress, old, next, false, false},
		{"retry_other_generation", deliverylifecycle.StatusFailed, old, next, false, false},
		{"failed_terminal_other_generation", deliverylifecycle.StatusDeadLetter, old, next, false, false},
		{"completed_foreign_run", deliverylifecycle.StatusDelivered, old, next, true, false},
		{"completed_foreign_source", deliverylifecycle.StatusDelivered, old, normal(otherSource, 2), false, false},
		{"normal_into_selected", deliverylifecycle.StatusDelivered, old, selected(1), false, false},
		{"selected_into_normal", deliverylifecycle.StatusDelivered, selected(1), old, false, false},
		{"selected_same", deliverylifecycle.StatusDelivered, selected(1), selected(1), false, true},
		{"selected_other_generation", deliverylifecycle.StatusDelivered, selected(1), selected(2), false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			materializer := deliverylifecycle.Snapshot{RunID: runID, Status: test.status, Authority: test.left}
			dependent := deliverylifecycle.Snapshot{RunID: runID, Status: deliverylifecycle.StatusPending, Authority: test.right}
			if test.foreignRun {
				dependent.RunID = uuid.NewString()
			}
			if err := validateMaterializerAuthority(materializer, dependent); (err == nil) != test.want {
				t.Fatalf("authority agreement: err=%v wantAccepted=%t", err, test.want)
			}
		})
	}
}
