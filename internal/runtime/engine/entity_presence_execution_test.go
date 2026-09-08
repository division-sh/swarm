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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutorCarriesDeclaredEntityPresenceToReaders(t *testing.T) {
	for _, consumer := range []string{"guard", "rule", "write", "emit", "filter"} {
		for _, safe := range []bool{false, true} {
			name := consumer + "/unsafe"
			value := `entity.profile.note`
			if safe {
				name = consumer + "/decision"
				value = `entity.profile.?note.orValue("fallback")`
			}
			t.Run(name, func(t *testing.T) {
				handler := rc.SystemNodeEventHandler{}
				switch consumer {
				case "guard":
					handler.Guard = &rc.GuardSpec{Check: value + ` == "fallback"`}
				case "rule":
					handler.Rules = []rc.HandlerRuleEntry{{ID: "accept", Condition: value + ` == "fallback"`}}
				case "write":
					handler.DataAccumulation = rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "captured", Value: rc.CELExpression(value)}}}
				case "emit":
					handler.Emit = rc.EmitSpec{Event: "work.result", Fields: map[string]rc.ExpressionValue{"note": rc.CELExpression(value)}}
				case "filter":
					handler.Filter = &rc.FilterSpec{ItemsFrom: "payload.items", Condition: value + ` == "fallback"`, StoreAs: "computed.filtered"}
				}
				bundle := &rc.WorkflowContractBundle{
					RootTypes:    rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Profile": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}},
					RootEntities: rc.EntityContractsDocument{"work": {Fields: map[string]rc.EntityFieldDecl{"profile": {Type: "Profile"}, "captured": {Type: "text"}}}},
					Events: map[string]rc.EventCatalogEntry{
						"work.received": {Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"items": {Type: "list<text>"}}, Required: []string{"items"}}},
						"work.result":   {Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"note": {Type: "text"}}, Required: []string{"note"}}},
					},
					Nodes: map[string]rc.SystemNodeContract{"worker": {ID: "worker", EventHandlers: map[string]rc.SystemNodeEventHandler{"work.received": handler}}},
				}
				executor, err := NewExecutor(RuntimeDependencies{Source: semanticview.Wrap(bundle), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{}}, schemaBoundWildcardEvaluator{})
				if err != nil {
					t.Fatal(err)
				}
				_, err = executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
					EntityID: "work-1", Node: identitytest.RootNode(t, "worker"), HandlerEventKey: "work.received",
					Event:   eventtest.ExistingRunRootIngress("entity-presence", events.EventType("work.received"), "", "", json.RawMessage(`{"items":["a"]}`), 0, eventtest.UUID("presence-run"), events.EventEnvelope{}, time.Time{}),
					Handler: handler, State: testStateSnapshot("active", map[string]any{"profile": map[string]any{}, "captured": ""}, nil, map[string]map[string]any{}),
				})
				if safe && err != nil {
					t.Fatal(err)
				}
				if !safe && (err == nil || (!strings.Contains(err.Error(), "presence decision") && !strings.Contains(err.Error(), "entity.profile.note"))) {
					t.Fatalf("unsafe read error=%v", err)
				}
			})
		}
	}
}
