package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
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

// This probe observes the actual replacement/launch path, not SQL atomicity.
// SQL fault, concurrency and historical recovery have both-store proofs.
type reconfigureDiagnosticProbe struct {
	*projectionTestBus
	*lifecyclePersistenceProbe
	pendingMu sync.Mutex
	pending   []diaglog.LifecycleDiagnostic
	projected []string
}

func (p *reconfigureDiagnosticProbe) CommitAgentLifecycleTransition(ctx context.Context, request AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	result, err := p.lifecyclePersistenceProbe.CommitAgentLifecycleTransition(ctx, request)
	if err == nil && !result.Replayed && request.OperationKind == "reconfigure" {
		p.pendingMu.Lock()
		p.pending = append(p.pending, diaglog.LifecycleDiagnostic{OutboxID: result.TransitionID, OperationID: request.OperationID, Identity: request.Identity})
		p.pendingMu.Unlock()
	}
	return result, err
}

func (p *reconfigureDiagnosticProbe) ListPendingAgentLifecycleDiagnostics(_ context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error) {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	return append([]diaglog.LifecycleDiagnostic(nil), p.pending[:min(limit, len(p.pending))]...), nil
}

func (p *reconfigureDiagnosticProbe) ProjectLifecycleDiagnostic(_ context.Context, item diaglog.LifecycleDiagnostic) error {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	for i, pending := range p.pending {
		if pending.OutboxID == item.OutboxID {
			p.pending = append(p.pending[:i], p.pending[i+1:]...)
			p.projected = append(p.projected, item.OperationID)
			return nil
		}
	}
	return nil
}

func TestReconfigureProjectsDiagnosticThroughReplacementLaunch(t *testing.T) {
	probe := &reconfigureDiagnosticProbe{projectionTestBus: newProjectionTestBus(), lifecyclePersistenceProbe: newLifecyclePersistenceProbe()}
	manager := newTestAgentManagerWithOptions(t, probe, func(cfg models.AgentConfig) (Agent, error) {
		return &reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LifecycleStore: probe, PersistenceRoles: PersistenceRoles{LifecycleDiagnostics: probe}})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "diagnostic-reconfigure", Tools: []string{"tool-old"}})
	if err := spawnManagerTestAgent(manager, cfg); err != nil {
		t.Fatal(err)
	}
	if err := manager.Run(managedExecutionTestContext(t, context.Background())); err != nil {
		t.Fatal(err)
	}
	before := lifecycleGenerationForTest(t, manager, cfg.ID)
	if err := reconfigureAgentThroughLifecycleForTest(t, manager, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}}); err != nil {
		t.Fatal(err)
	}
	if generation := lifecycleGenerationForTest(t, manager, cfg.ID); generation != before+1 {
		t.Fatalf("reconfigure generation=%d want=%d", generation, before+1)
	}
	probe.pendingMu.Lock()
	defer probe.pendingMu.Unlock()
	if len(probe.pending) != 0 || len(probe.projected) != 1 {
		t.Fatalf("replacement did not drain its diagnostic: pending=%d projected=%v", len(probe.pending), probe.projected)
	}
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
