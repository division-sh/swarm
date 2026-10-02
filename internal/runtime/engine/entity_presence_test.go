package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type sparseSnapshotRepo struct {
	stubStateRepo
	snapshot StateSnapshot
}

func sparseMutationFrame() (*Executor, *executionFrame) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{
		"work": {Fields: map[string]rc.EntityFieldDecl{"left": {Type: "integer"}, "right": {Type: "integer"}}},
	}})
	frame := &executionFrame{}
	frame.state.State = testStateSnapshot("ready", map[string]any{"left": int64(1), "right": int64(1)}, nil, nil)
	return &Executor{deps: RuntimeDependencies{Source: source}}, frame
}

func TestEntityMutationHandlerReadsOrderedDraft(t *testing.T) {
	exec, frame := sparseMutationFrame()
	err := exec.applyDataAccumulation(frame, rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{
		{TargetField: "left", Value: rc.LiteralExpression(int64(7))},
		{TargetField: "right", Value: rc.RefExpression("entity.left")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if frame.state.State.StateCarrier.Fields["right"] != int64(7) {
		t.Fatalf("stale ordered read: %#v", frame.state.State.StateCarrier.Fields)
	}
}

func TestEntityMutationHandlerMissingSourceRollsBackList(t *testing.T) {
	for _, write := range []rc.WorkflowDataWrite{{Field: "left"}, {SourceField: "missing", TargetField: "right"}} {
		exec, frame := sparseMutationFrame()
		before := cloneStringAnyMap(frame.state.State.StateCarrier.Fields)
		err := exec.applyDataAccumulation(frame, rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{
			{TargetField: "left", Value: rc.LiteralExpression(int64(7))}, write,
		}})
		if err == nil {
			t.Fatal("absent source accepted")
		}
		if !reflect.DeepEqual(frame.state.State.StateCarrier.Fields, before) {
			t.Fatalf("failed list escaped: %#v", frame.state.State.StateCarrier.Fields)
		}
	}
}

func TestEntityMutationHandlerEqualityAtListBoundary(t *testing.T) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{
		"work": {Fields: map[string]rc.EntityFieldDecl{
			"left":  {Type: "text", IsOptional: true, Refinements: rc.SchemaRefinements{EqualTo: "right"}},
			"right": {Type: "text", IsOptional: true},
		}},
	}})
	for _, complete := range []bool{false, true} {
		exec := &Executor{deps: RuntimeDependencies{Source: source}}
		frame := &executionFrame{}
		frame.state.State = testStateSnapshot("ready", nil, nil, nil)
		writes := []rc.WorkflowDataWrite{{TargetField: "left", Value: rc.LiteralExpression("x")}}
		if complete {
			writes = append(writes, rc.WorkflowDataWrite{TargetField: "right", Value: rc.LiteralExpression("x")})
		}
		err := exec.applyDataAccumulation(frame, rc.WorkflowDataAccumulation{Writes: writes})
		if (err == nil) != complete {
			t.Fatalf("complete=%v, error=%v", complete, err)
		}
		if !complete && len(frame.state.State.StateCarrier.Fields) != 0 {
			t.Fatal("invalid half-pair escaped list")
		}
	}
}

func (r sparseSnapshotRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	return r.snapshot, true, nil
}

func TestEntityReloadUsesCompleteStoredCarrier(t *testing.T) {
	for _, fields := range []map[string]any{nil, {}, {"remaining": "value"}} {
		stored := testStateSnapshot("pending", fields, nil, nil)
		exec := &Executor{deps: RuntimeDependencies{StateRepo: sparseSnapshotRepo{snapshot: stored}}}
		request := ExecutionRequest{EntityID: "entity-1", State: testStateSnapshot("pending",
			map[string]any{"removed": "stale"}, map[string]bool{"stale_gate": true}, map[string]map[string]any{"stale_bucket": {"count": 1}})}
		for i := 0; i < 2; i++ {
			got, err := exec.loadState(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.StateCarrier.Fields) != len(fields) {
				t.Fatalf("reloaded fields = %#v, want %#v", got.StateCarrier.Fields, fields)
			}
			for k, v := range fields {
				if !reflect.DeepEqual(got.StateCarrier.Fields[k], v) {
					t.Fatalf("changed field %s", k)
				}
			}
			if len(got.StateCarrier.Gates) != 0 || len(got.StateCarrier.StateBuckets) != 0 {
				t.Fatal("stale gates or buckets resurrected")
			}
			request.State = got
		}
	}
}

func TestEntityReloadRefusesMissingConstruction(t *testing.T) {
	for _, name := range []string{"ordinary", "handler_creation", "materialization_hint", "preview", "missing_identity"} {
		t.Run(name, func(t *testing.T) {
			exec := &Executor{deps: RuntimeDependencies{StateRepo: stubStateRepo{}}}
			request := ExecutionRequest{EntityID: "entity-1", State: testStateSnapshot("ready", map[string]any{"note": "request"}, nil, nil)}
			switch name {
			case "handler_creation":
				request.Handler.CreateEntity = true
			case "materialization_hint":
				if _, survives := reflect.TypeOf(request).FieldByName("EntityMaterializationAdmitted"); survives {
					t.Fatal("ordinary execution still carries materialization permission")
				}
			case "preview":
				request.Preview = true
			case "missing_identity":
				request.EntityID = ""
			}
			_, err := exec.loadState(context.Background(), request)
			failure, typed := failures.EnvelopeFromError(err)
			if !errors.Is(err, ErrUnconstructedWorkflowTarget) || !typed || failure.Class != failures.ClassInternalFailure || failure.Detail.Code != "workflow_target_not_constructed" {
				t.Fatalf("%s admitted request-only state: %v", name, err)
			}
		})
	}
}

func TestEntityReloadDoesNotReapplyConstructorInitials(t *testing.T) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{
		"work": {Fields: map[string]rc.EntityFieldDecl{
			"note":     {Type: "text", IsOptional: true, Initial: "seeded"},
			"provided": {Type: "text", Initial: "initial"},
		}},
	}})
	exec := &Executor{deps: RuntimeDependencies{Source: source, StateRepo: sparseSnapshotRepo{snapshot: testStateSnapshot("ready", nil, nil, nil)}}}
	req := ExecutionRequest{EntityID: "entity-1", State: testStateSnapshot("ready", map[string]any{"provided": "actual"}, nil, nil)}
	state, err := exec.loadState(context.Background(), req)
	if err != nil || len(state.StateCarrier.Fields) != 0 {
		t.Fatalf("reload reapplied construction: fields=%#v err=%v", state.StateCarrier.Fields, err)
	}
}

func TestEntityStepWriteRejectsMetadataAlias(t *testing.T) {
	exec, frame := sparseMutationFrame()
	before := cloneStringAnyMap(frame.state.State.StateCarrier.Fields)
	for _, target := range []string{"metadata.left", "payload.left", "event.left", "policy.left"} {
		if err := exec.writeStepValue(frame, target, int64(7)); err == nil {
			t.Fatalf("raw entity write alias %s accepted", target)
		}
		if !reflect.DeepEqual(before, frame.state.State.StateCarrier.Fields) {
			t.Fatalf("rejected alias %s mutated state", target)
		}
	}
}
