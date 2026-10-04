package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type recoveryInspectionReader struct {
	phase  string
	err    error
	reads  []string
	cancel context.CancelFunc
}

func (r *recoveryInspectionReader) read(name string) error {
	r.reads = append(r.reads, name)
	if r.phase == name {
		return r.err
	}
	return nil
}

func (r *recoveryInspectionReader) InspectDynamicFlowRuntimeReadinessForSource(context.Context, correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	return pipeline.DynamicFlowRuntimeReadinessProjection{CurrentPending: make([]pipeline.DynamicFlowRuntimeReadiness, 2)}, r.read("readiness")
}

func (r *recoveryInspectionReader) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return make([]PersistedAgent, 3), r.read("agents")
}
func (r *recoveryInspectionReader) ListFlowInstanceRoutes(context.Context) ([]flowidentity.RunScopedFlowInstance, error) {
	return make([]flowidentity.RunScopedFlowInstance, 4), r.read("routes")
}
func (r *recoveryInspectionReader) ListSelectedContractRouteRecoveryRecords(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
	return make([]SelectedContractRouteRecoveryRecord, 5), r.read("selected")
}
func (r *recoveryInspectionReader) GlobalWorkPresence(context.Context) (pipelineobligation.GlobalWorkPresence, error) {
	if r.cancel != nil {
		r.cancel()
	}
	return pipelineobligation.GlobalWorkPresence{DecisionRouteDue: true}, r.read("presence")
}

func TestRecoverableStateInspectionRequiresCompleteReads(t *testing.T) {
	reader := &recoveryInspectionReader{}
	state, err := InspectRecoverableStateSnapshot(context.Background(), authorActivityTestSourceArtifactFact, reader)
	want := RecoverableStateSnapshot{PendingDynamicFlowRuntimeReadinessCount: 2, PersistedAgentCount: 3, PersistedFlowInstanceRouteCount: 4, PersistedSelectedContractRouteRecoveryCount: 5, ReplayEligibleEventPresent: true}
	if err != nil || state != want || !reflect.DeepEqual(reader.reads, []string{"readiness", "agents", "routes", "selected", "presence"}) {
		t.Fatalf("inspection: %+v, %v, %v", state, err, reader.reads)
	}
	for _, phase := range []string{"readiness", "agents", "routes", "selected", "presence"} {
		t.Run(phase, func(t *testing.T) {
			witness := errors.New("unavailable " + phase)
			reader := &recoveryInspectionReader{phase: phase, err: witness}
			state, err := InspectRecoverableStateSnapshot(context.Background(), authorActivityTestSourceArtifactFact, reader)
			if !errors.Is(err, witness) || state != (RecoverableStateSnapshot{}) {
				t.Fatalf("partial state published: %+v, %v", state, err)
			}
			if reader.reads[len(reader.reads)-1] != phase {
				t.Fatal("inspection continued past failed dependency")
			}
		})
	}
	if _, err := InspectRecoverableStateSnapshot(context.Background(), authorActivityTestSourceArtifactFact, nil); err == nil {
		t.Fatal("missing reader meant empty state")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader = &recoveryInspectionReader{}
	if _, err := InspectRecoverableStateSnapshot(ctx, authorActivityTestSourceArtifactFact, reader); !errors.Is(err, context.Canceled) || len(reader.reads) != 0 {
		t.Fatalf("cancelled inspection: %v, %v", err, reader.reads)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	reader.cancel = cancel
	if state, err := InspectRecoverableStateSnapshot(ctx, authorActivityTestSourceArtifactFact, reader); !errors.Is(err, context.Canceled) || state != (RecoverableStateSnapshot{}) {
		t.Fatalf("late cancellation passed: %+v, %v", state, err)
	}
}
