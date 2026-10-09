package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type recoveryInspectionReader struct {
	phase      string
	err        error
	reads      []string
	cancel     context.CancelFunc
	projection *pipeline.DynamicFlowRuntimeReadinessProjection
}

func (r *recoveryInspectionReader) read(name string) error {
	r.reads = append(r.reads, name)
	if r.phase == name {
		return r.err
	}
	return nil
}

func (r *recoveryInspectionReader) InspectDynamicFlowRuntimeReadinessForSource(context.Context, correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	projection := pipeline.DynamicFlowRuntimeReadinessProjection{CurrentPending: make([]pipeline.DynamicFlowRuntimeReadiness, 2)}
	if r.projection != nil {
		projection = *r.projection
	}
	return projection, r.read("readiness")
}

func (r *recoveryInspectionReader) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return make([]PersistedAgent, 3), r.read("agents")
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
	want := RecoverableStateSnapshot{PendingDynamicFlowRuntimeReadinessCount: 2, PersistedAgentCount: 3, PersistedFlowAttachmentCount: 2, PersistedSelectedContractRouteRecoveryCount: 5, ReplayEligibleEventPresent: true}
	if err != nil || state != want || !reflect.DeepEqual(reader.reads, []string{"readiness", "agents", "selected", "presence"}) {
		t.Fatalf("inspection: %+v, %v, %v", state, err, reader.reads)
	}
	for _, phase := range []string{"readiness", "agents", "selected", "presence"} {
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

func TestRecoverableStateUsesDesiredAttachmentProjection(t *testing.T) {
	pending := pipeline.DynamicFlowRuntimeReadiness{RunStatus: "running", InstanceStatus: "active", Phase: pipeline.FlowAttachmentPlanned}
	completed := pending
	completed.Phase = pipeline.FlowAttachmentReady
	for _, tc := range []struct {
		name       string
		projection pipeline.DynamicFlowRuntimeReadinessProjection
		count      int
		pending    int
	}{
		{"empty", pipeline.DynamicFlowRuntimeReadinessProjection{}, 0, 0},
		{"completed", pipeline.DynamicFlowRuntimeReadinessProjection{CurrentCompleted: []pipeline.DynamicFlowRuntimeReadiness{completed}}, 1, 0},
		{"pending", pipeline.DynamicFlowRuntimeReadinessProjection{CurrentPending: []pipeline.DynamicFlowRuntimeReadiness{pending}}, 1, 1},
		{"completed_transition", pipeline.DynamicFlowRuntimeReadinessProjection{SourceTransitionRequired: []pipeline.DynamicFlowRuntimeReadiness{completed}}, 1, 0},
		{"pending_transition", pipeline.DynamicFlowRuntimeReadinessProjection{SourceTransitionRequired: []pipeline.DynamicFlowRuntimeReadiness{pending}}, 1, 1},
		{"mixed", pipeline.DynamicFlowRuntimeReadinessProjection{
			CurrentPending: []pipeline.DynamicFlowRuntimeReadiness{pending}, CurrentCompleted: []pipeline.DynamicFlowRuntimeReadiness{completed},
			SourceTransitionRequired: []pipeline.DynamicFlowRuntimeReadiness{pending, completed},
		}, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &recoveryInspectionReader{projection: &tc.projection}
			state, err := InspectRecoverableStateSnapshot(context.Background(), authorActivityTestSourceArtifactFact, reader)
			if err != nil || state.PersistedFlowAttachmentCount != tc.count || state.PendingDynamicFlowRuntimeReadinessCount != tc.pending {
				t.Fatalf("desired attachment observation: %+v, %v", state, err)
			}
			onlyAttachment := RecoverableStateSnapshot{PersistedFlowAttachmentCount: state.PersistedFlowAttachmentCount}
			wantClasses := []string{}
			if tc.count > 0 {
				wantClasses = []string{"persisted flow attachments"}
			}
			if onlyAttachment.HasRecoverableWork() != (tc.count > 0) || !reflect.DeepEqual(onlyAttachment.Classes(), wantClasses) {
				t.Fatalf("attachment-only work classification: %+v, %v", onlyAttachment, onlyAttachment.Classes())
			}
			if _, restored := state.Detail()["persisted_flow_instance_route_count"]; restored {
				t.Fatal("recovery restored mirror-backed diagnostics")
			}
		})
	}
}
