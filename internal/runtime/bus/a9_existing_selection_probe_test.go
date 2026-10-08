package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA9TriageExistingSelectionUsesStoredConstructionAndEdgeKey(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	graph := pinrouting.CompileConnectGraph(source)
	var plan pinrouting.ConnectRoutePlan
	for _, candidate := range graph.Plans() {
		if candidate.ReceiverEndpoint().Readback().FlowID == "parent" {
			plan = candidate
		}
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	stored, err := flowidentity.KeyedChild(source, root, "parent", "stored-one")
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.ValidateConstruction(source, busInternalTestRunID); err != nil {
		t.Fatalf("existing constructor owner rejects setup: %v", err)
	}
	table := &RouteTable{instanceOwners: map[flowidentity.RunScopedFlowInstance]flowidentity.Instance{}}
	for _, owner := range []flowidentity.Instance{root, stored} {
		coordinate, err := flowidentity.NewRunScopedFlowInstance(busInternalTestRunID, owner.Route())
		if err != nil {
			t.Fatal(err)
		}
		table.instanceOwners[coordinate] = owner
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("existing-edge-key"), "start", "test", "", []byte(`{"parent_key":"left","leaf_key":"same"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	owner := newTemplateInstanceLifecycleOwner(source, table, nil, constructionReceiptTestReader{root, stored})
	materialized, selected, _, err := owner.Materialize(withConnectRoutePlanPreview(context.Background()), event, plan,
		map[string]string{"payload.parent_key": "left", "payload.leaf_key": "same"},
		[]pinrouting.Descriptor{{EntityID: stored.EntityID, FlowInstance: stored.InstancePath, AddressFields: map[string]string{"entity.id": "left"}}})
	if err != nil || !materialized.Failure.Empty() || selected.identity != stored {
		t.Fatalf("per-edge key failed to reuse admitted stored construction: failure=%v selection=%+v want=%+v err=%v", materialized.Failure, selected, stored, err)
	}
}

func TestA9StoredReceiverSelectionPreservesIdentityAndChecksKeys(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    contracts.FlowInputResolutionMode
		keys    []string
		failure pinrouting.ConnectRoutePlanFailure
		derived bool
	}{
		{"select", contracts.FlowInputResolutionModeSelect, []string{"acct-1"}, 0, false},
		{"reuse", contracts.FlowInputResolutionModeSelectOrCreate, []string{"acct-1"}, 0, false},
		{"select wrong key", contracts.FlowInputResolutionModeSelect, []string{"acct-other"}, pinrouting.ConnectFailureTargetUnresolved, false},
		{"select missing key", contracts.FlowInputResolutionModeSelect, []string{""}, pinrouting.ConnectFailureTargetUnresolved, false},
		{"select ambiguous", contracts.FlowInputResolutionModeSelect, []string{"acct-1", "acct-1"}, pinrouting.ConnectFailureTargetAmbiguous, false},
		{"reuse ambiguous", contracts.FlowInputResolutionModeSelectOrCreate, []string{"acct-1", "acct-1"}, pinrouting.ConnectFailureTargetAmbiguous, false},
		{"routable derived path with wrong key", contracts.FlowInputResolutionModeSelect, []string{"acct-other"}, pinrouting.ConnectFailureTargetUnresolved, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := connectRoutePlanCarriedKeyResolutionSource(t, test.mode)
			var plan pinrouting.ConnectRoutePlan
			for _, candidate := range pinrouting.CompileConnectGraph(source).Plans() {
				if candidate.ReceiverEndpoint().Readback().FlowID == "account" && candidate.Readback().Source.FlowID == "producer" && candidate.ReceiverLocalEvent() == "account.ready" {
					plan = candidate
				}
			}
			if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != test.mode {
				t.Fatalf("test selected a different compiled edge: %+v", plan.Readback())
			}
			root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
			table, err := DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := table.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(root.Route()), Instance: root}); err != nil {
				t.Fatal(err)
			}
			var descriptors []pinrouting.Descriptor
			var stored []flowidentity.Instance
			for index, key := range test.keys {
				instanceID := []string{"stored-one", "stored-two"}[index]
				if test.derived {
					field, err := contracts.ParseTemplateInstanceField("account_id")
					if err != nil {
						t.Fatal(err)
					}
					instanceID = templateInstanceLifecycleInstanceID(plan, []contracts.TemplateInstanceKeyValue{{Field: field, Value: "acct-1"}})
				}
				instance, err := flowidentity.KeyedChild(source, root, "account", instanceID)
				if err != nil {
					t.Fatal(err)
				}
				instance.EntityID = eventtest.UUID(instanceID + "-native-header")
				if err := table.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(instance.Route()), Instance: instance}); err != nil {
					t.Fatal(err)
				}
				stored = append(stored, instance)
				descriptors = append(descriptors, pinrouting.Descriptor{EntityID: instance.EntityID, FlowInstance: instance.InstancePath, AddressFields: map[string]string{"entity.account_id": key}})
			}
			if test.derived && len(table.evaluateConnectPlan(busInternalTestRunID, plan, []events.RouteIdentity{plan.ReceiverRoute(stored[0].InstancePath, stored[0].EntityID)}).Recipients()) != 1 {
				t.Fatal("counterexample requires an installed routable derived address")
			}
			event := eventtest.ExistingRunRootIngress(eventtest.UUID(test.name), "producer/account.ready", "test", "", []byte(`{"account_id":"acct-1"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
			owner := newTemplateInstanceLifecycleOwner(source, table, nil, append(constructionReceiptTestReader{root}, stored...))
			materialized, selected, handled, err := owner.Materialize(withConnectRoutePlanPreview(context.Background()), event, plan,
				map[string]string{"payload.account_id": "acct-1"}, descriptors)
			if err != nil || !handled || materialized.Failure != test.failure {
				t.Fatalf("stored selection: %+v selected=%+v handled=%t err=%v", materialized, selected, handled, err)
			}
			if test.failure.Empty() && (selected.identity != stored[0] || selected.Activation != nil || materialized.Target.EntityID != stored[0].EntityID) {
				t.Fatalf("selection recomputed or reactivated stored identity: %+v want=%+v", selected, stored[0])
			}
			for _, instance := range stored {
				if actual := table.instanceOwners[testRunScopedFlowRoute(instance.Route())]; actual != instance {
					t.Fatalf("selection mutated a stored construction: %+v -> %+v", instance, actual)
				}
			}
		})
	}
}

func TestA9StoredLeafSelectionExcludesOtherStructuralParents(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	var plan pinrouting.ConnectRoutePlan
	for _, candidate := range pinrouting.CompileConnectGraph(source).Plans() {
		if candidate.ReceiverEndpoint().Readback().FlowID == "parent/middle/leaf" {
			plan = candidate
		}
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	add := func(instance flowidentity.Instance) {
		t.Helper()
		if err := table.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(instance.Route()), Instance: instance}); err != nil {
			t.Fatal(err)
		}
	}
	add(root)
	receipts := constructionReceiptTestReader{root}
	ctx := withConnectRoutePlanPreview(context.Background())
	var leaves []flowidentity.Instance
	var descriptors []pinrouting.Descriptor
	for _, discriminator := range []string{"left-stored", "right-stored"} {
		parent, err := flowidentity.KeyedChild(source, root, "parent", discriminator)
		if err != nil {
			t.Fatal(err)
		}
		add(parent)
		receipts = append(receipts, parent)
		if discriminator == "left-stored" {
			if err := selectConnectionConstruction(ctx, parent); err != nil {
				t.Fatal(err)
			}
		}
		middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
		if err != nil {
			t.Fatal(err)
		}
		add(middle)
		receipts = append(receipts, middle)
		leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", "same-stored")
		if err != nil {
			t.Fatal(err)
		}
		add(leaf)
		receipts = append(receipts, leaf)
		leaves = append(leaves, leaf)
		descriptors = append(descriptors, pinrouting.Descriptor{EntityID: leaf.EntityID, FlowInstance: leaf.InstancePath, AddressFields: map[string]string{"entity.id": "same-business-key"}})
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("parent-exclusion"), "start", "test", "", []byte(`{"parent_key":"left","leaf_key":"same-business-key"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	owner := newTemplateInstanceLifecycleOwner(source, table, nil, receipts)
	materialized, selected, _, err := owner.Materialize(ctx, event, plan, map[string]string{"payload.leaf_key": "same-business-key"}, descriptors)
	if err != nil || !materialized.Failure.Empty() || selected.identity != leaves[0] || materialized.Target.FlowInstance == leaves[1].InstancePath {
		t.Fatalf("same key under another parent changed selection: %+v selected=%+v err=%v", materialized, selected, err)
	}
}

func TestA9TriageSchemalessAncestryConsumesAdmittedFlowNodes(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyNestedProducerTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{
		Mode: canonicalrouting.SelectResolutionSelect,
	}))
	scope, found := source.FlowScopeByID("left")
	if !found || scope.Mode != "static" {
		t.Fatalf("admitted descendant did not establish its keyless ancestor: %+v found=%t", scope, found)
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, flowidentity.EntityID(busInternalTestRunID), "")
	if _, err := flowidentity.KeylessChild(source, root, "left"); err != nil {
		t.Fatalf("constructor rejected admitted schema-omitted FlowNode: %v", err)
	}
}
