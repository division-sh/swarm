package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutor_GenericAppendReferenceControls(t *testing.T) {
	for _, tc := range []struct {
		name, payload, ref string
		want               any
		wantErr            bool
	}{
		{name: "object", payload: `{"value":{"summary":"a"}}`, ref: "payload.value", want: map[string]any{"summary": "a"}},
		{name: "nested null preserved", payload: `{"value":{"summary":null}}`, ref: "payload.value", want: map[string]any{"summary": nil}},
		{name: "direct null is unresolved", payload: `{"value":null}`, ref: "payload.value", wantErr: true},
		{name: "missing is not null", payload: `{}`, ref: "payload.value", wantErr: true},
		{name: "unsupported root", payload: `{"value":null}`, ref: "secrets.value", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := sourceWithFixtureStages(semanticview.Wrap(&contracts.WorkflowContractBundle{
				RootEntities: contracts.EntityContractsDocument{"work": {Fields: map[string]contracts.EntityFieldDecl{
					"entries": {Type: "[json]"},
				}}},
			}), ".", "active", "active")
			executor, err := NewExecutor(RuntimeDependencies{
				Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{},
				Locker: stubLocker{}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				Node: testRootExecutableNode(t, "writer"), EntityID: "entity-1",
				Event: eventtest.RunCreatingRootIngress("append-event", "finding.received", "", "", json.RawMessage(tc.payload), 0, "", "", events.EventEnvelope{}, time.Time{}),
				State: testStateSnapshot("active", map[string]any{"entries": []any{"before"}}, nil, nil),
				Handler: contracts.SystemNodeEventHandler{DataAccumulation: contracts.WorkflowDataAccumulation{
					Writes: []contracts.WorkflowDataWrite{{
						Operation: contracts.WorkflowDataOperationAppend, TargetRef: "entity.entries",
						Value: contracts.RefExpression(tc.ref),
					}},
				}},
			})
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid reference accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := result.StateMutation.Fields["entries"]; !reflect.DeepEqual(got, []any{"before", tc.want}) {
				t.Fatalf("append = %#v", got)
			}
		})
	}
}
