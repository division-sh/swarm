package tools

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func entityOperationParserContract() entityruntime.Contract {
	return entityruntime.Contract{
		EntityType: "case",
		Entity: runtimecontracts.EntityContract{Fields: map[string]runtimecontracts.EntityFieldDecl{
			"status": {Type: "text"},
			"items":  {Type: "list<text>"},
			"labels": {Type: "map[text]text"},
		}},
	}
}

func TestEntityToolLiteralMutationRetainsCollectionInputs(t *testing.T) {
	contract := entityOperationParserContract()
	for _, tc := range []struct {
		name, field string
		input       map[string]any
		operation   string
	}{
		{"default scalar set", "status", map[string]any{"value": "done"}, ""},
		{"explicit scalar set", "status", map[string]any{"op": "set", "value": "done"}, ""},
		{"default list set", "items", map[string]any{"value": []any{"replacement"}}, ""},
		{"explicit list set", "items", map[string]any{"op": "set", "value": []any{"replacement"}}, ""},
		{"append", "items", map[string]any{"op": "append", "value": "same"}, "append"},
		{"update", "items", map[string]any{"op": "update", "index": float64(1), "value": "new"}, "update"},
		{"map key set", "labels", map[string]any{"op": "set", "key": " literal key ", "value": "new"}, "set"},
		{"whole map set", "labels", map[string]any{"op": "set", "value": map[string]any{"new": "only"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutation, err := entityToolLiteralMutation(contract, tc.field, tc.input)
			if err != nil {
				t.Fatal(err)
			}
			_, hasKey := tc.input["key"]
			_, hasIndex := tc.input["index"]
			if mutation.Operation != tc.operation || mutation.Target != "entity."+tc.field || mutation.HasKey != hasKey || mutation.HasIndex != hasIndex ||
				!reflect.DeepEqual(mutation.Value, tc.input["value"]) || !reflect.DeepEqual(mutation.Key, tc.input["key"]) || !reflect.DeepEqual(mutation.Index, tc.input["index"]) {
				t.Fatalf("literal mutation = %#v, input = %#v", mutation, tc.input)
			}
		})
	}
}

func TestEntityToolLiteralMutationRejectsInvalidOperationsAtomically(t *testing.T) {
	contract := entityOperationParserContract()
	state := map[string]any{"items": []any{"a", "b"}, "labels": map[string]any{"old": "kept"}}
	for _, tc := range []struct {
		name, field string
		input       map[string]any
	}{
		{"missing value", "items", map[string]any{"op": "append"}},
		{"empty op", "items", map[string]any{"op": "", "value": "c"}},
		{"null op", "items", map[string]any{"op": nil, "value": "c"}},
		{"clear", "items", map[string]any{"op": "clear", "value": []any{}}},
		{"merge", "labels", map[string]any{"op": "merge", "key": "a", "value": "c"}},
		{"delete", "labels", map[string]any{"op": "delete", "key": "a", "value": "c"}},
		{"keyed list", "items", map[string]any{"op": "set", "key": "a", "value": "c"}},
		{"append key", "items", map[string]any{"op": "append", "key": "a", "value": "c"}},
		{"append index", "items", map[string]any{"op": "append", "index": 0, "value": "c"}},
		{"set index", "items", map[string]any{"op": "set", "index": 0, "value": "c"}},
		{"default index", "items", map[string]any{"index": 0, "value": "c"}},
		{"default key", "labels", map[string]any{"key": "a", "value": "c"}},
		{"missing index", "items", map[string]any{"op": "update", "value": "c"}},
		{"negative index", "items", map[string]any{"op": "update", "index": -1, "value": "c"}},
		{"fractional index", "items", map[string]any{"op": "update", "index": 0.5, "value": "c"}},
		{"string index", "items", map[string]any{"op": "update", "index": "0", "value": "c"}},
		{"boolean index", "items", map[string]any{"op": "update", "index": true, "value": "c"}},
		{"null index", "items", map[string]any{"op": "update", "index": nil, "value": "c"}},
		{"out of range", "items", map[string]any{"op": "update", "index": 2, "value": "c"}},
		{"wrong list item", "items", map[string]any{"op": "append", "value": []any{"c"}}},
		{"map update", "labels", map[string]any{"op": "update", "index": 0, "value": "c"}},
		{"empty map key", "labels", map[string]any{"op": "set", "key": "", "value": "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := entityruntime.NormalizeState(contract, state)
			if err != nil {
				t.Fatal(err)
			}
			mutation, err := entityToolLiteralMutation(contract, tc.field, tc.input)
			if err == nil {
				_, err = entityruntime.ApplyMutations(contract, state, []entityruntime.Mutation{mutation})
			}
			if err == nil {
				t.Fatalf("invalid operation accepted: %#v", tc.input)
			}
			after, err := entityruntime.NormalizeState(contract, state)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("invalid operation changed input state: %#v, err=%v", state, err)
			}
		})
	}
}

func TestEntityToolLiteralMutationReappliesIndexToFreshPosition(t *testing.T) {
	contract := entityOperationParserContract()
	mutation, err := entityToolLiteralMutation(contract, "items", map[string]any{"op": "update", "index": float64(1), "value": "updated"})
	if err != nil {
		t.Fatal(err)
	}
	initial := map[string]any{"items": []any{"original-0", "original-1"}}
	if _, err := entityruntime.ApplyMutations(contract, initial, []entityruntime.Mutation{mutation}); err != nil {
		t.Fatal(err)
	}
	// A losing draft is discarded; a concurrent whole replacement changes the
	// business identity occupying the original integer position.
	fresh := map[string]any{"items": []any{"replacement-0", "replacement-1", "replacement-2"}}
	got, err := entityruntime.ApplyMutations(contract, fresh, []entityruntime.Mutation{mutation})
	if err != nil || !reflect.DeepEqual(got["items"], []any{"replacement-0", "updated", "replacement-2"}) {
		t.Fatalf("fresh indexed update = %#v, err=%v", got, err)
	}
	if got, err := entityruntime.ApplyMutations(contract, map[string]any{"items": []any{}}, []entityruntime.Mutation{mutation}); err == nil || got != nil {
		t.Fatalf("index removed before reapplication = %#v, %v", got, err)
	}
}

func TestEntityToolMutationOwnerCapturesExactRootAndNestedRoutes(t *testing.T) {
	const runID = "11111111-1111-1111-1111-111111111111"
	source := toolTestSourceWithDeclaredAgent(t, &runtimecontracts.WorkflowContractBundle{}, "writer", ".")
	name, err := agentidentity.DeclaredName("writer", "swarm-test://root/agents/writer")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, flow string
		route      agentidentity.Route
		want       flowidentity.Route
	}{
		{"root", ".", agentidentity.RootRoute(), flowidentity.Route{ScopeKey: ".", InstanceID: runID, InstancePath: runID}},
		{"nested", "parent/child", agentidentity.Route{Presence: agentidentity.RoutePresent, ScopeKey: "parent/child", InstanceID: "child", InstancePath: runID + "/parent/key/child"},
			flowidentity.Route{ScopeKey: "parent/child", InstanceID: "child", InstancePath: runID + "/parent/key/child"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := agentidentity.New(runID, name, tc.route)
			if err != nil {
				t.Fatal(err)
			}
			actor := models.AgentConfig{ID: "writer", Identity: identity, FlowID: tc.flow, FlowPath: identity.FlowInstance()}
			row := map[string]any{"entity_id": runID, "flow_instance": tc.want.InstancePath}
			got, err := entityToolMutationOwner(source, actor, runID, runID, tc.flow, row)
			if err != nil || got != (flowidentity.RunScopedFlowInstance{RunID: runID, Route: tc.want}) {
				t.Fatalf("exact owner = %#v, err=%v", got, err)
			}
			row["flow_instance"] = tc.want.InstancePath + "/sibling"
			if _, err := entityToolMutationOwner(source, actor, runID, runID, tc.flow, row); err == nil {
				t.Fatal("stored sibling route was accepted")
			}
			row["flow_instance"] = tc.want.InstancePath
			if _, err := entityToolMutationOwner(source, actor, "22222222-2222-2222-2222-222222222222", runID, tc.flow, row); err == nil {
				t.Fatal("foreign run was accepted")
			}
			if _, err := entityToolMutationOwner(source, actor, runID, runID, "foreign", row); err == nil {
				t.Fatal("foreign schema flow was accepted")
			}
			row["entity_id"] = "22222222-2222-2222-2222-222222222222"
			if _, err := entityToolMutationOwner(source, actor, runID, runID, tc.flow, row); err == nil {
				t.Fatal("foreign stored entity was accepted")
			}
		})
	}
}
