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
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutorJoinOutcomePresenceCompleteAndTimeout(t *testing.T) {
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
					Members:    rc.JoinMembersSpec{From: "entity.expected", FromPath: paths.Parse("entity.expected"), By: "payload.member_id", ByPath: paths.Parse("payload.member_id")},
					OnComplete: outcome, OnCompleteFound: true, Timeout: rc.JoinTimeoutSpec{After: "1h", Outcome: outcome}, TimeoutFound: true}
				types := rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Result": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}
				node := identitytest.RootNode(t, "collector")
				bundle := &rc.WorkflowContractBundle{
					RootTypes: types, RootEntities: rc.EntityContractsDocument{"work": {Fields: map[string]rc.EntityFieldDecl{"expected": {Type: "list<text>"}, "captured": {Type: "boolean"}}}},
					Semantics: rc.WorkflowSemanticView{Joins: []rc.WorkflowJoinPlan{{Mode: rc.WorkflowJoinModeArrival, Node: node, HandlerEvent: "item.completed", Spec: spec, ResultType: rc.CatalogTypeReference{Type: "Result", Catalog: types}}}},
				}
				executor, err := NewExecutor(RuntimeDependencies{Source: sourceWithFixtureStages(semanticview.Wrap(bundle), ".", "awaiting", "awaiting", "ready"), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
				members := []string{"a"}
				if timeout {
					members = append(members, "b")
				}
				activation, err := newEngineTestJoinActivation(node, "item.completed", spec, "", members, now, now.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := joinruntime.Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				req := ExecutionRequest{EntityID: "work-1", Node: node, HandlerEventKey: "item.completed", Handler: rc.SystemNodeEventHandler{Join: &spec}, JoinDeclaration: activation.JoinRef().Declaration(),
					Event: eventtest.RunCreatingRootIngress("arrival", "item.completed", "", "", json.RawMessage(`{"member_id":"a","result":{}}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "work-1"), now),
					State: testStateSnapshot("awaiting", map[string]any{"expected": members, "captured": false}, nil, buckets)}
				result, err := executor.ExecuteSemanticFixture(context.Background(), req)
				if timeout {
					if err != nil {
						t.Fatal(err)
					}
					if result.Status != OutcomeWaiting {
						t.Fatalf("first arrival=%s", result.Status)
					}
					payload, marshalErr := json.Marshal(activation.TimerHandle().PayloadMetadata())
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					req.Event = eventtest.RunCreatingRootIngress("timeout", events.EventType(activation.TimerEventType()), "runtime", activation.TimerTaskID(), payload, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "work-1"), now.Add(time.Hour))
					req.State.StateCarrier.StateBuckets = result.StateMutation.StateCarrier.StateBuckets
					result, err = executor.ExecuteSemanticFixture(context.Background(), req)
				}
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
