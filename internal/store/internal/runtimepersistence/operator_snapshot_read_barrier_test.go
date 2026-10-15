package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestOperatorSnapshotReadBoundaryRefusesEscapesAndJoinsFailureBothStores(t *testing.T) {
	if err := InitializeOperatorSnapshotReadBarrierForTest(); err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected, identity := newOperatorSnapshotFaultFixture(t, backend)
			if err := PrepareOperatorSnapshotReadsForTest(ctx, selected); err != nil {
				t.Fatal(err)
			}
			for _, scenario := range []string{"reader_skips_boundary", "reader_failure", "mutation_refused", "cancelled", "empty_cut"} {
				t.Run(scenario, func(t *testing.T) {
					proveOperatorSnapshotReaderFailure(t, ctx, selected, identity, scenario)
				})
			}
		})
	}
}

func proveOperatorSnapshotReaderFailure(t *testing.T, ctx context.Context, selected any, identity agentidentity.Identity, scenario string) {
	t.Helper()
	reader := selected.(interface {
		ListOperatorAgents(context.Context, operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error)
	})

	before := forkReceiverFaultSnapshot(t, ctx, selected)
	cut := OperatorSnapshotTerminalization(identity)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cause := errors.New("reader assertion failed")
	var entered, joined atomic.Bool
	switch scenario {
	case "cancelled":
		cancel()
	case "empty_cut":
		cut = OperatorSnapshotCut{}
	case "mutation_refused":
		cut = OperatorSnapshotTerminalization(mustTestAgentIdentityForRun(identity.RunID, "missing-snapshot-agent", "global"))
	}
	err := ReadOperatorSnapshotAcrossCutForTest(callCtx, selected, cut, func(ctx context.Context) error {
		entered.Store(true)
		defer joined.Store(true)
		switch scenario {
		case "reader_skips_boundary":
			return nil
		case "reader_failure":
			return cause
		default:
			_, err := reader.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{})
			return err
		}
	})
	if err == nil {
		t.Fatal("snapshot fixture accepted a missing boundary or rejected mutation")
	}
	if scenario == "reader_failure" && !errors.Is(err, cause) {
		t.Fatalf("lost original reader assertion failure: %v", err)
	}
	if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
		t.Fatalf("lost caller cancellation: %v", err)
	}
	if entered.Load() != joined.Load() {
		t.Fatal("boundary returned before its reader joined")
	}
	if (scenario == "cancelled" || scenario == "empty_cut") && entered.Load() {
		t.Fatal("invalid cut entered the reader")
	}
	if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("failed cut changed data, siblings or clocks")
	}
	if _, err := reader.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{}); err != nil {
		t.Fatalf("failed boundary left its schema or reader unusable: %v", err)
	}

}
