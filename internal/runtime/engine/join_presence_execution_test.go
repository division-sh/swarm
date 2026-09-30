package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
)

func TestExecutorJoinOutcomePresenceCompleteAndDeadline(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		for _, safe := range []bool{false, true} {
			name := "complete"
			if timeout {
				name = "timeout"
			}
			if safe {
				name += "/decision"
			} else {
				name += "/unsafe"
			}
			t.Run(name, func(t *testing.T) {
				expression := `join.results.exists(r, r.note == "fallback")`
				if safe {
					expression = `join.results.exists(r, r.?note.orValue("fallback") == "fallback")`
				}
				outcome := rc.HandlerRuleEntry{AdvancesTo: "ready", DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "captured", Value: rc.CELExpression(expression)}}}}
				spec := rc.JoinSpec{ID: "proof", Stage: "awaiting", Output: "payload.result", OutputPath: paths.Parse("payload.result"),
					Members:    rc.JoinMembersSpec{From: "state.expected", By: "payload.member_id"},
					OnComplete: outcome, OnCompleteFound: true, Deadline: &rc.JoinDeadlineSpec{After: "1h", From: rc.JoinDeadlineFromStageEntry}, OnDeadline: outcome, OnDeadlineFound: true}
				types := rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Result": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}
				node := identitytest.RootNode(t, "collector")
				source, handler := a2TypedJoinFixtureSource(t, node, "item.completed", spec, rc.CatalogTypeReference{Type: "Result", Catalog: types}, map[string]rc.EntityFieldDecl{"expected": {Type: "[text]"}, "captured": {Type: "boolean"}})
				spec = *handler.Join
				executor, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
				members := []string{"a"}
				if timeout {
					members = append(members, "b")
				}
				route := flowidentity.StoredRoute(".", semanticExecutionFixtureRunID, semanticExecutionFixtureRunID)
				entry := a2EngineJoinEntry(semanticExecutionFixtureRunID, "work-1", "awaiting", route)
				activation, err := newEngineTestJoinActivation(node, "item.completed", spec, entry, members, now, now.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := joinruntime.Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				req := ExecutionRequest{EntityID: "work-1", Node: node, HandlerEventKey: "item.completed", Handler: handler, Route: route, JoinDeclaration: activation.JoinRef().Declaration(),
					Event: eventtest.RunCreatingRootIngress("arrival", "item.completed", "", "", json.RawMessage(`{"member_id":"a","result":{}}`), 0, semanticExecutionFixtureRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, "work-1"), now),
					State: a2JoinFixtureSnapshot(t, activation, map[string]any{"expected": members, "captured": false}, buckets)}
				arrival, err := executor.ExecuteSemanticFixture(a2BoundJoinContext(t, activation), req)
				if err != nil || arrival.Status != OutcomeWaiting || arrival.StateMutation.NextState != "" || arrival.StateMutation.StateCarrier.Fields["captured"] == true {
					t.Fatalf("arrival evaluated closed outcome: %#v %v", arrival, err)
				}
				req.State.StateCarrier.StateBuckets = arrival.StateMutation.StateCarrier.StateBuckets
				if timeout {
					payload, marshalErr := json.Marshal(activation.TimerHandle().PayloadMetadata())
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					req.Event = eventtest.RuntimeControl("timeout", events.EventType(activation.TimerEventType()), "runtime", activation.TimerTaskID(), payload, 0, semanticExecutionFixtureRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, "work-1"), now.Add(time.Hour))
				} else {
					closed, found, err := joinruntime.Load(req.State.StateCarrier.StateBuckets, node, activation.Key())
					if err != nil || !found {
						t.Fatalf("load closure: %v %v", found, err)
					}
					req = a2JoinContinuationRequest(t, req, closed, now.Add(time.Second))
				}
				result, err := executor.ExecuteSemanticFixture(context.Background(), req)
				if !safe {
					if err == nil || !strings.Contains(err.Error(), "presence decision") {
						t.Fatalf("unsafe outcome error=%v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := result.StateMutation.StateCarrier.Fields["captured"]; got != true {
					t.Fatalf("captured=%v want=true", got)
				}
			})
		}
	}
}
