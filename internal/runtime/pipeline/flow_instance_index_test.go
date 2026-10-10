package pipeline

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func instanceIndexTestSource(t *testing.T, fieldType string) (semanticview.Source, correlation.SourceArtifactFact) {
	t.Helper()
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":          "name: instance-index\n",
		"worker/schema.yaml":   "name: worker\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
		"worker/entities.yaml": "item:\n  item_id: " + fieldType + "\n",
		"worker/events.yaml":   "item.created:\n  item_id: " + fieldType + "\n",
		"detail/schema.yaml":   "name: detail\n",
	})
	bundle, _ := semanticview.Bundle(source)
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	return source, fact
}

func TestFlowInstanceLookupRequiresTypedKeyAndExactParent(t *testing.T) {
	for _, test := range []struct {
		name, fieldType, material string
		value, wrongType          any
	}{
		{"text", "text", "business-key", "business-key", 7},
		{"integer", "integer", "7", int64(7), "7"},
		{"numeric", "double", "7.5", json.Number("7.5"), "7.5"},
		{"boolean", "boolean", "true", true, "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, fact := instanceIndexTestSource(t, test.fieldType)
			runID := uuid.NewString()
			parent, err := flowidentity.StandingForGeneration(source, ".", runID)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := AdmitFlowInstanceKeyMaterial(source, "worker", test.value)
			if err != nil {
				t.Fatal(err)
			}
			request, err := NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", parent, keys)
			if err != nil || !request.Valid() || request.InstanceKey() != test.material || request.ParentInstance() != runID || request.ExactPath() != "" {
				t.Fatalf("lookup lost admitted selection: %+v %v", request, err)
			}
			flow, _ := source.FlowSchemaByID("worker")
			untyped, err := (contracts.TemplateInstanceContract{FlowID: "worker", Field: flow.Instance}).CanonicalKeyMaterial(map[string]any{"item_id": test.wrongType})
			if err != nil {
				t.Fatal(err)
			}
			for _, bad := range [][]contracts.TemplateInstanceKeyValue{
				nil, untyped, {keys[0], keys[0]}, {{Field: keys[0].Field, Value: keys[0].Value}},
			} {
				if _, err := NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", parent, bad); err == nil {
					t.Fatalf("lookup accepted unadmitted keys: %+v", bad)
				}
			}
			keys[0].Value = "changed"
			if _, err := NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", parent, keys); err == nil {
				t.Fatal("lookup accepted text contradicting the captured typed key")
			}
			foreign, _ := flowidentity.StandingForGeneration(source, ".", uuid.NewString())
			if _, err := NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", foreign, untyped); err == nil {
				t.Fatal("lookup accepted a foreign structural parent")
			}
		})
	}
}

func TestFlowInstanceLookupScopeIsBoundedAndIsolated(t *testing.T) {
	source, fact := instanceIndexTestSource(t, "text")
	runID := uuid.NewString()
	root, err := flowidentity.StandingForGeneration(source, ".", runID)
	if err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: root.Route()}
	flows := []string{"worker", "worker"}
	owners := []flowidentity.RunScopedFlowInstance{owner, owner}
	scope, err := NewFlowInstanceLookupScope(source, fact, runID, flows, owners)
	if err != nil || len(scope.FlowIDs()) != 1 || len(scope.Coordinates()) != 1 {
		t.Fatalf("scope: %+v %v", scope, err)
	}
	flows[0], owners[0].RunID = "foreign", uuid.NewString()
	scope.FlowIDs()[0], scope.Coordinates()[0].RunID = "mutated", uuid.NewString()
	if scope.FlowIDs()[0] != "worker" || scope.Coordinates()[0] != owner {
		t.Fatal("lookup scope exposes mutable coordinates")
	}
	empty, err := NewFlowInstanceLookupScope(source, fact, runID, nil, nil)
	if err != nil || !empty.Valid() || len(empty.FlowIDs())+len(empty.Coordinates()) != 0 {
		t.Fatal("empty scope broadened inventory")
	}
	foreign := owner
	foreign.RunID = uuid.NewString()
	if _, err := NewFlowInstanceLookupScope(source, fact, runID, nil, []flowidentity.RunScopedFlowInstance{foreign}); err == nil {
		t.Fatal("scope accepted a foreign run")
	}
	for _, declaration := range []string{"missing", " worker", "worker/"} {
		if _, err := NewFlowInstanceLookupScope(source, fact, runID, []string{declaration}, nil); err == nil {
			t.Fatalf("scope accepted %q", declaration)
		}
	}
}

func TestFlowInstanceLookupRejectsCrossedSourceAndRootCoordinates(t *testing.T) {
	source, fact := instanceIndexTestSource(t, "text")
	runID := uuid.NewString()
	root, _ := flowidentity.StandingForGeneration(source, ".", runID)
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: root.Route()}
	if _, err := NewExactFlowInstanceLookup(source, fact, owner); err != nil {
		t.Fatal(err)
	}
	foreignFact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewExactFlowInstanceLookup(source, foreignFact, owner); err == nil {
		t.Fatal("lookup accepted a foreign source")
	}
	for _, path := range []string{uuid.NewString(), " " + runID, runID + "/"} {
		changed := owner
		changed.Route.InstancePath = path
		if _, err := NewExactFlowInstanceLookup(source, fact, changed); err == nil {
			t.Fatalf("lookup accepted foreign/alternate root %q", path)
		}
	}
	owner.RunID = " " + runID
	if _, err := NewExactFlowInstanceLookup(source, fact, owner); err == nil {
		t.Fatal("lookup accepted ambient normalization")
	}
	request, err := NewDeclaredFlowInstanceLookup(source, fact, runID, ".", flowidentity.Instance{}, nil)
	if err != nil || request.ExactPath() != runID {
		t.Fatalf("root selection: %+v %v", request, err)
	}
	if _, err := NewDeclaredFlowInstanceLookup(source, fact, runID, ".", root, nil); err == nil {
		t.Fatal("selected root accepted a structural parent")
	}
}
