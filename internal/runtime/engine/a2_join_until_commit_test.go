package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// This is the executor's owning commit boundary, not a selected-store SQL,
// publication-admission or lifecycle-construction integration fixture.
type a2UntilCommitOwner struct {
	repo      *persistentStateRepo
	err       error
	attempts  []EngineMutation
	committed []EngineMutation
}

func (o *a2UntilCommitOwner) CommitEngineMutation(ctx context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	o.attempts = append(o.attempts, mutation)
	if o.err != nil {
		return CommittedEngineMutation{}, o.err
	}
	if err := o.repo.SaveState(ctx, mutation.Address, mutation.State); err != nil {
		return CommittedEngineMutation{}, err
	}
	o.committed = append(o.committed, mutation)
	return CommittedEngineMutation{Committed: true, EmitIntents: mutation.EmitIntents}, nil
}

func TestA2UntilAndOrdinaryConsumerShareOneEngineCommit(t *testing.T) {
	for _, rejectCommit := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "reject_no_effects"}[rejectCommit], func(t *testing.T) {
			node := testRootExecutableNode(t, "collector")
			spec := rc.JoinSpec{ID: "parts", Stage: "awaiting", Members: rc.JoinMembersSpec{From: "state.expected", By: "payload.member"}, Output: "payload.result",
				OnCompleteFound: true, OnComplete: rc.HandlerRuleEntry{DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{
					{TargetField: "closed_count", Value: rc.RefExpression("join.completed")},
					{TargetField: "reason", Value: rc.RefExpression("join.close_reason")},
				}}}}
			source, _ := a2TypedJoinFixtureSource(t, node, "arrived", spec, rc.CatalogTypeReference{Type: "text"}, map[string]rc.EntityFieldDecl{
				"expected": {Type: "[text]"}, "notice": {Type: "text"}, "closed_count": {Type: "integer"}, "reason": {Type: "text"},
			})
			bundle, _ := semanticview.Bundle(source)
			arrival := bundle.Nodes["collector"].EventHandlers["arrived"]
			arrival.Join.Until = "closed"
			bundle.Nodes["collector"].EventHandlers["arrived"] = arrival
			bundle.Nodes["collector"].EventHandlers["closed"] = rc.SystemNodeEventHandler{
				DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "notice", Value: rc.RefExpression("payload.notice")}}},
				Emit:             rc.EmitSpec{Event: "ordinary.observed", Fields: map[string]rc.ExpressionValue{"notice": rc.RefExpression("payload.notice")}},
			}
			bundle.Events["closed"] = requiredEventPayload(map[string]rc.EventFieldSpec{"notice": {Type: "text"}})
			bundle.Events["ordinary.observed"] = requiredEventPayload(map[string]rc.EventFieldSpec{"notice": {Type: "text"}})
			bundle.FlowTree.Root.Schema = *bundle.RootSchema
			if err := rc.CompileWorkflowSemantics(bundle); err != nil {
				t.Fatal(err)
			}
			source = semanticview.Wrap(bundle)
			plan, found := semanticview.WorkflowJoinPlanForHandler(source, node, "arrived")
			if !found {
				t.Fatal("arrival plan missing")
			}
			resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, "closed")
			if !resolved.Matched || resolved.HandlerEventKey != "closed" || len(resolved.Handler.JoinUntilPlans) != 1 || len(resolved.Handler.DataAccumulation.Writes) != 1 {
				t.Fatal("canonical until projection replaced the ordinary consumer")
			}
			now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
			runID := eventtest.UUID("until-ordinary-run")
			route := flowidentity.StoredRoute(".", runID, runID)
			entry := a2EngineJoinEntry(runID, runID, "awaiting", route)
			arm, err := newEngineTestJoinActivation(node, plan.HandlerEvent, plan.Spec, entry, []string{"a", "b"}, now, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if disposition, err := arm.Add("a", "retained-result"); err != nil || disposition != joinruntime.AddAccepted {
				t.Fatalf("seed first contributor: %s %v", disposition, err)
			}
			buckets := map[string]map[string]any{}
			if err := joinruntime.Store(buckets, arm); err != nil {
				t.Fatal(err)
			}
			state := a2JoinFixtureSnapshot(t, arm, map[string]any{"expected": []any{"a", "b"}, "notice": "before"}, buckets)
			repo := &persistentStateRepo{found: true, snapshot: state}
			before, _, err := repo.LoadState(context.Background(), StateAddress{})
			if err != nil {
				t.Fatal(err)
			}
			owner := &a2UntilCommitOwner{repo: repo}
			commitErr := errors.New("until owning commit refused")
			if rejectCommit {
				owner.err = commitErr
			}
			exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: repo, MutationOwner: owner, Locker: stubLocker{}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			req := ExecutionRequest{EntityID: identity.NormalizeEntityID(runID), Node: node, HandlerEventKey: resolved.HandlerEventKey, Handler: resolved.Handler, Route: route,
				Event: eventtest.RunCreatingRootIngress("close-and-observe", "closed", "", "", json.RawMessage(`{"notice":"transport-only"}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, runID), now.Add(time.Second)), State: state}
			result, err := exec.ExecuteSemanticFixture(a2BoundJoinContext(t, arm), req)
			if len(owner.attempts) != 1 {
				t.Fatalf("closure/ordinary consumer commit attempts=%d error=%v", len(owner.attempts), err)
			}
			mutation := owner.attempts[0]
			closed, found, loadErr := joinruntime.Load(mutation.State.StateCarrier.StateBuckets, node, arm.Key())
			if loadErr != nil || !found || closed.Status != joinruntime.StatusClosed || closed.CloseReason != joinruntime.CloseReasonUntil || !closed.OutcomePending || closed.OutcomeFired {
				t.Fatalf("exact closure missing from owning commit: %#v %v", closed, loadErr)
			}
			if mutation.State.StateCarrier.Fields["notice"] != "transport-only" || mutation.State.StateCarrier.Fields["closed_count"] != nil || mutation.State.NextState != "" || len(mutation.EmitIntents) != 1 {
				t.Fatalf("ordinary action or deferred outcome changed: %#v", mutation)
			}
			if got := eventPayloadMap(t, mutation.EmitIntents[0].Event)["notice"]; got != "transport-only" {
				t.Fatalf("ordinary emit lost its transport payload: %v", got)
			}
			if len(mutation.EmitIntents[0].Context.Joins) != 0 {
				t.Fatal("ordinary business output inherited the until delivery receipt")
			}
			if rejectCommit {
				if !errors.Is(err, commitErr) || result.Committed || len(result.EmitIntents) != 0 || len(owner.committed) != 0 || !reflect.DeepEqual(repo.snapshot, before) {
					t.Fatalf("failed owning commit leaked closure/write/emit: committed=%v error=%v", result.Committed, err)
				}
				return
			}
			if err != nil || !result.Committed || len(result.EmitIntents) != 1 || len(owner.committed) != 1 {
				t.Fatalf("owning commit not acknowledged: committed=%v error=%v", result.Committed, err)
			}
			arrivalHandler, ok := source.ExecutableNodeEventHandler(node, "arrived")
			if !ok {
				t.Fatal("arrival handler missing")
			}
			req.Handler, req.HandlerEventKey, req.JoinDeclaration = arrivalHandler, "arrived", arm.JoinRef().Declaration()
			req.State = repo.snapshot
			completed, err := exec.ExecuteSemanticFixture(context.Background(), a2JoinContinuationRequest(t, req, closed, now.Add(2*time.Second)))
			if err != nil || !completed.Committed || len(owner.committed) != 2 {
				t.Fatalf("deferred until completion: committed=%v error=%v", completed.Committed, err)
			}
			if got := owner.committed[1].State.StateCarrier.Fields; got["closed_count"] != int64(1) || got["reason"] != "until" || got["notice"] != "transport-only" || len(owner.committed[1].EmitIntents) != 0 {
				t.Fatalf("completion did not consume only retained state/join evidence: %#v", got)
			}
		})
	}
}
