package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

// This unit fake proves batching and error propagation only. The SQL, recorder,
// and competing-consumer proofs use the real selected stores and runtime logger.
type diagnosticBatchProbe struct {
	Bus
	rows    []diaglog.LifecycleDiagnostic
	lists   []int
	settled []string
	failAt  string
}

func (p *diagnosticBatchProbe) ListPendingAgentLifecycleDiagnostics(_ context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error) {
	p.lists = append(p.lists, limit)
	end := min(limit, len(p.rows))
	return append([]diaglog.LifecycleDiagnostic(nil), p.rows[:end]...), nil
}

func (p *diagnosticBatchProbe) ProjectLifecycleDiagnostic(_ context.Context, item diaglog.LifecycleDiagnostic) error {
	if item.OutboxID == p.failAt {
		return errors.New("selected sink failed")
	}
	if len(p.rows) == 0 || p.rows[0].OutboxID != item.OutboxID {
		return errors.New("unexpected diagnostic identity")
	}
	p.settled = append(p.settled, item.OutboxID)
	p.rows = p.rows[1:]
	return nil
}

func TestLifecycleDiagnosticProjectionDrainsBeyondOneBatchAndFailsClosed(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("sink_failure=%t", fail), func(t *testing.T) {
			probe := &diagnosticBatchProbe{}
			for i := 0; i < 201; i++ {
				probe.rows = append(probe.rows, diaglog.LifecycleDiagnostic{OutboxID: fmt.Sprint(i)})
			}
			if fail {
				probe.failAt = "100"
			}
			manager := &AgentManager{bus: probe, lifecycle: &agentLifecycleCoordinator{}, roles: PersistenceRoles{LifecycleDiagnostics: probe}}
			err := manager.projectLifecycleDiagnostics(context.Background())
			want := 201
			if fail {
				want = 100
			}
			if (err != nil) != fail || len(probe.settled) != want || len(probe.rows) != 201-want {
				t.Fatalf("settled=%d remaining=%d err=%v", len(probe.settled), len(probe.rows), err)
			}
			for _, limit := range probe.lists {
				if limit != 100 {
					t.Fatalf("batch limit=%d want=100", limit)
				}
			}
			if fail {
				manager.bus = nil
				if err := manager.projectLifecycleDiagnostics(context.Background()); err == nil {
					t.Fatal("missing projector accepted retained pending work")
				}
				manager.bus = probe
				probe.failAt = ""
				if err := manager.projectLifecycleDiagnostics(context.Background()); err != nil || len(probe.rows) != 0 {
					t.Fatalf("resume pending batch: remaining=%d err=%v", len(probe.rows), err)
				}
			}
		})
	}
}
