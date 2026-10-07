package bus

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func nestedConnectionConstructionSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	for label, body := range map[string]string{
		"schema.yaml": `name: nested-construction
pins: {inputs: [start]}
connect:
  - {event: start, from: ., to: parent, resolution: select-or-create, key_from: payload.parent_key}
  - {event: start, from: ., to: parent/middle/leaf, resolution: select-or-create, key_from: payload.leaf_key}
`,
		"events.yaml":                      "start:\n  parent_key: text\n  leaf_key: text\n",
		"parent/schema.yaml":               "name: parent\ninstance: id\npins: {inputs: [start]}\n",
		"parent/entities.yaml":             "parent_state:\n  id: text\n",
		"parent/nodes.yaml":                "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.parent_key != ''}\n",
		"parent/middle/schema.yaml":        "name: middle\n",
		"parent/middle/leaf/schema.yaml":   "name: leaf\ninstance: id\npins: {inputs: [start]}\n",
		"parent/middle/leaf/entities.yaml": "leaf_state:\n  id: text\n",
		"parent/middle/leaf/nodes.yaml":    "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.leaf_key != ''}\n",
	} {
		path := filepath.Join(root, label)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func TestA9NestedConnectionSelectionUsesEveryEdgeKeyAndConstructedParent(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatal(issues)
	}
	var parentPlan, leafPlan pinrouting.ConnectRoutePlan
	for _, plan := range graph.Plans() {
		switch plan.ReceiverEndpoint().Readback().FlowID {
		case "parent":
			parentPlan = plan
		case "parent/middle/leaf":
			leafPlan = plan
		}
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	table := &RouteTable{instanceOwners: map[flowidentity.RunScopedFlowInstance]flowidentity.Instance{}}
	add := func(instance flowidentity.Instance) {
		coordinate, err := flowidentity.NewRunScopedFlowInstance(busInternalTestRunID, instance.Route())
		if err != nil {
			t.Fatal(err)
		}
		table.instanceOwners[coordinate] = instance
	}
	add(root)
	var descriptors []pinrouting.Descriptor
	parents := map[string]flowidentity.Instance{}
	leaves := map[string]flowidentity.Instance{}
	for _, parentKey := range []string{"left", "right"} {
		values := map[string]string{"payload.parent_key": parentKey, "payload.leaf_key": "same"}
		parentMaterial, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(parentPlan, pinrouting.AdmitConnectRouteMatchValues(values))
		if !failure.Empty() {
			t.Fatal(failure)
		}
		parent, err := flowidentity.KeyedChild(source, root, "parent", templateInstanceLifecycleInstanceID(parentPlan, parentMaterial.Keys))
		if err != nil {
			t.Fatal(err)
		}
		middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
		if err != nil {
			t.Fatal(err)
		}
		leafMaterial, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(leafPlan, pinrouting.AdmitConnectRouteMatchValues(values))
		if !failure.Empty() {
			t.Fatal(failure)
		}
		leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", templateInstanceLifecycleInstanceID(leafPlan, leafMaterial.Keys))
		if err != nil {
			t.Fatal(err)
		}
		for _, instance := range []flowidentity.Instance{parent, middle, leaf} {
			add(instance)
		}
		parents[parentKey], leaves[parentKey] = parent, leaf
		descriptors = append(descriptors,
			pinrouting.Descriptor{EntityID: parent.EntityID, FlowInstance: parent.InstancePath, AddressFields: map[string]string{"entity.id": parentKey}},
			pinrouting.Descriptor{EntityID: leaf.EntityID, FlowInstance: leaf.InstancePath, AddressFields: map[string]string{"entity.id": "same"}},
		)
	}
	owner := newTemplateInstanceLifecycleOwner(source, table, nil)
	for _, parentKey := range []string{"left", "right"} {
		payload, err := json.Marshal(map[string]any{"parent_key": parentKey, "leaf_key": "same"})
		if err != nil {
			t.Fatal(err)
		}
		event := eventtest.ExistingRunRootIngress(eventtest.UUID(parentKey), "start", "test", "", payload, 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
		ctx := withConnectRoutePlanPreview(context.Background())
		values := map[string]string{"payload.parent_key": parentKey, "payload.leaf_key": "same"}
		_, selection, _, err := owner.Materialize(ctx, event, parentPlan, values, descriptors)
		if err != nil || selection.identity != parents[parentKey] {
			t.Fatalf("parent edge selected another key: %+v err=%v", selection, err)
		}
		if err := selectConnectionConstruction(ctx, selection.identity); err != nil {
			t.Fatal(err)
		}
		materialized, leaf, _, err := owner.Materialize(ctx, event, leafPlan, values, descriptors)
		if err != nil || !materialized.Failure.Empty() || leaf.identity != leaves[parentKey] {
			t.Fatalf("leaf edge lost selected parent: %+v %+v err=%v", materialized, leaf, err)
		}
	}
	if leaves["left"].EntityID == leaves["right"].EntityID {
		t.Fatal("same leaf key in two parents collapsed")
	}
	// Even exactly one existing parent/leaf cannot substitute for a compiled
	// ancestor selection. Keep the complete real root and one constructed branch.
	for coordinate, instance := range table.instanceOwners {
		if instance == parents["right"] || instance == leaves["right"] || instance.ParentEntityID == parents["right"].EntityID {
			delete(table.instanceOwners, coordinate)
		}
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("no-ancestor"), "start", "test", "", []byte(`{"parent_key":"left","leaf_key":"same"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	if _, _, _, err := owner.Materialize(withConnectRoutePlanPreview(context.Background()), event, leafPlan, map[string]string{"payload.parent_key": "left", "payload.leaf_key": "same"}, descriptors); err == nil {
		t.Fatal("sole existing receiver bypassed the missing ancestor edge")
	}
}
