package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func stateListExecutor2293(t *testing.T) (*Executor, *persistentStateRepo, *[]string) {
	t.Helper()
	fields := map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "boolean"}, "message": {Type: "text"}}
	source := mustCompileEngineSource(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{"Review": {Fields: fields}}},
		RootEntities: runtimecontracts.EntityContractsDocument{"subject": {Fields: map[string]runtimecontracts.EntityFieldDecl{
			"reviews": {Type: "[Review]"}, "marker": {Type: "text"},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"review.received": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{"id": {Type: "boolean"}, "message": {Type: "text"}}),
			"review.recorded": {},
		},
	})
	repo := &persistentStateRepo{found: true, snapshot: testStateSnapshot("pending", map[string]any{"marker": "initial"}, nil, nil)}
	commits := &[]string{}
	executor, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithFixtureStages(source, ".", "pending", "pending"), StateRepo: repo,
		MutationOwner: composedMutationOwner{state: repo, order: commits}, Locker: stubLocker{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		"entity.reviews.size() == 1": func(base BaseContext) (bool, error) {
			items, _ := base.Entity.Raw()["reviews"].([]any)
			return len(items) == 1, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return executor, repo, commits
}

func stateListRequest2293(t *testing.T, eventID, payload string) ExecutionRequest {
	t.Helper()
	return ExecutionRequest{
		EntityID: "entity-1", Node: testRootExecutableNode(t, "collector"), HandlerEventKey: "review.received",
		Event: eventtest.RunCreatingRootIngress(eventID, "review.received", "", "", json.RawMessage(payload), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{Target: "reviews", Key: "id"},
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "marker", Value: runtimecontracts.LiteralExpression("handler")},
			}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				Condition: "entity.reviews.size() == 1",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "marker", Value: runtimecontracts.LiteralExpression("rule")},
				}},
				Emit: runtimecontracts.EmitSpec{Event: "review.recorded"},
			}},
		},
	}
}

func Test2293Q1ExecutionCollectsIntoStateAndDiscardsWithoutCommit(t *testing.T) {
	executor, repo, commits := stateListExecutor2293(t)
	first, err := executor.ExecuteSemanticFixture(context.Background(), stateListRequest2293(t, "event-first", `{"id":false,"message":"first"}`))
	if err != nil || len(first.EmitIntents) != 1 || len(*commits) != 1 {
		t.Fatalf("first execution: commits=%d emits=%d error=%v", len(*commits), len(first.EmitIntents), err)
	}
	if repo.snapshot.Fields["marker"] != "rule" {
		t.Fatalf("selection missed the new arrival or rule write lost: %#v", repo.snapshot.Fields)
	}
	items, ok := repo.snapshot.Fields["reviews"].([]any)
	if !ok || len(items) != 1 || items[0].(map[string]any)["id"] != false {
		t.Fatalf("state list lost typed falsy identity: %#v", repo.snapshot.Fields["reviews"])
	}
	if len(repo.snapshot.StateBuckets) != 0 {
		t.Fatalf("collection created a private buffer/receipt bucket: %#v", repo.snapshot.StateBuckets)
	}
	before := stateListSnapshot2293(t, repo.snapshot)
	duplicate, err := executor.ExecuteSemanticFixture(context.Background(), stateListRequest2293(t, "new-publication", `{"message":"first","id":false}`))
	if err != nil || duplicate.Status != OutcomeDiscarded || len(*commits) != 1 || len(duplicate.EmitIntents)+len(duplicate.ActivityIntents) != 0 {
		t.Fatalf("duplicate manufactured a commit/effect: outcome=%s commits=%d error=%v", duplicate.Status, len(*commits), err)
	}
	if after := stateListSnapshot2293(t, repo.snapshot); !bytes.Equal(after, before) {
		t.Fatalf("discarded item changed durable state: before=%s after=%s", before, after)
	}
	_, err = executor.ExecuteSemanticFixture(context.Background(), stateListRequest2293(t, "conflicting-publication", `{"id":false,"message":"different"}`))
	requireAccumulatorFailure(t, err, failures.ClassConflictingDuplicate)
	if len(*commits) != 1 || !bytes.Equal(stateListSnapshot2293(t, repo.snapshot), before) {
		t.Fatal("conflicting item changed durable state")
	}
	_, err = executor.ExecuteSemanticFixture(context.Background(), stateListRequest2293(t, "distinct-key", `{"id":true,"message":"second"}`))
	if err != nil || len(*commits) != 2 {
		t.Fatalf("distinct arrival failed: commits=%d error=%v", len(*commits), err)
	}
	items = repo.snapshot.Fields["reviews"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["id"] != false || items[1].(map[string]any)["id"] != true {
		t.Fatalf("arrival order changed: %#v", items)
	}
}

func Test2293Q1ExecutionPreviewUsesDetachedStateList(t *testing.T) {
	executor, repo, commits := stateListExecutor2293(t)
	request := stateListRequest2293(t, "preview", `{"id":false,"message":"hypothetical"}`)
	request.Preview = true
	before := stateListSnapshot2293(t, repo.snapshot)
	preview, err := executor.ExecuteSemanticFixture(context.Background(), request)
	if err != nil || preview.Committed || len(*commits) != 0 || !bytes.Equal(stateListSnapshot2293(t, repo.snapshot), before) {
		t.Fatalf("preview persisted or changed its input: commits=%d error=%v", len(*commits), err)
	}
	items, ok := preview.StateMutation.Fields["reviews"].([]any)
	if !ok || len(items) != 1 || items[0].(map[string]any)["message"] != "hypothetical" {
		t.Fatalf("preview omitted the collected arrival: %#v", preview.StateMutation.Fields)
	}
	items[0].(map[string]any)["message"] = "caller mutation"
	if !bytes.Equal(stateListSnapshot2293(t, repo.snapshot), before) {
		t.Fatal("mutating the preview changed durable state")
	}
}

func stateListSnapshot2293(t *testing.T, snapshot StateSnapshot) []byte {
	t.Helper()
	raw, err := canonicaljson.MarshalPreservingNumberKinds(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
