package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type constructionReceiptTestReader []flowidentity.Instance

func (r constructionReceiptTestReader) LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	for _, instance := range r {
		if owner.RunID == busInternalTestRunID && instance.Route() == owner.Route && instance.EntityID == entityID {
			return pipeline.FlowConstructionPublicationEvidence{Identity: instance}, nil
		}
	}
	return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("absent test construction receipt")
}

type constructionReceiptFailureTestReader struct{ err error }

func (r constructionReceiptFailureTestReader) LoadFlowConstructionPublication(context.Context, flowidentity.RunScopedFlowInstance, string) (pipeline.FlowConstructionPublicationEvidence, error) {
	return pipeline.FlowConstructionPublicationEvidence{}, r.err
}

func TestA9ConstructionSelectionRequiresExactReceiptWithoutPlannerOrCacheFallback(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	parent.EntityID = eventtest.UUID("non-derived-parent")
	contradictoryParent := parent
	contradictoryParent.ParentRoute.FlowInstance = eventtest.UUID("foreign-parent-run")
	table := &RouteTable{instanceOwners: map[flowidentity.RunScopedFlowInstance]flowidentity.Instance{
		testRunScopedFlowRoute(root.Route()): root, testRunScopedFlowRoute(parent.Route()): parent,
	}}
	independent := errors.New("independent receipt failure")
	for _, test := range []struct {
		name    string
		reader  pipeline.FlowConstructionPublicationReader
		wantErr error
		valid   bool
	}{
		{name: "genuine receipt without planner", reader: constructionReceiptTestReader{root, parent}, valid: true},
		{name: "missing reader with populated cache"},
		{name: "absent receipt", reader: constructionReceiptTestReader{root}},
		{name: "foreign run", reader: constructionReceiptTestReader{root, parent}},
		{name: "wrong same-key entity", reader: constructionReceiptTestReader{root, parent}},
		{name: "wrong route", reader: constructionReceiptTestReader{root, parent}},
		{name: "declaration as instance", reader: constructionReceiptTestReader{root, parent}},
		{name: "contradictory parent receipt", reader: constructionReceiptTestReader{root, contradictoryParent}},
		{name: "independent failure", reader: constructionReceiptFailureTestReader{independent}, wantErr: independent},
		{name: "cancellation", reader: constructionReceiptFailureTestReader{context.Canceled}, wantErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newTemplateInstanceLifecycleOwner(source, table, nil, test.reader)
			instances, err := owner.constructionOwners(context.Background(), busInternalTestRunID)
			if err != nil || len(instances) != 0 {
				t.Fatalf("process cache became committed construction evidence: %+v %v", instances, err)
			}
			run, entity := busInternalTestRunID, parent.EntityID
			if test.name == "foreign run" {
				run = eventtest.UUID("foreign-run")
			}
			if test.name == "wrong same-key entity" {
				entity = eventtest.UUID("wrong-entity")
			}
			path := parent.InstancePath
			if test.name == "wrong route" {
				path = "other/stored-parent"
			}
			if test.name == "declaration as instance" {
				path = parent.TemplateID
			}
			actual, err := owner.constructionInstance(context.Background(), run, parent.TemplateID, path, entity, instances)
			if test.valid {
				if err != nil || actual != parent {
					t.Fatalf("reader-only selection lost stored identity: %+v %v", actual, err)
				}
			} else if err == nil || actual != (flowidentity.Instance{}) || test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("receipt refusal lost its boundary/error: %+v %v", actual, err)
			}
		})
	}
}

func TestA9MissingCreationPlannerCannotBorrowReceiptAuthority(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelectOrCreate)
	plan := mustInstanceKeyConnectRoutePlan(t, source)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	producer, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	owner := newTemplateInstanceLifecycleOwner(source, nil, nil, constructionReceiptTestReader{root, producer})
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("no-creation-planner"), "producer/account.ready", "test", "", []byte(`{"account_id":"acct-1"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	materialized, decision, handled, err := owner.Materialize(withConnectRoutePlanPreview(context.Background()), event, plan, map[string]string{"payload.account_id": "acct-1"}, nil)
	if err != nil || !handled || materialized.Failure != pinrouting.ConnectFailureLifecycleUnavailable || decision.Activation != nil || decision.identity != (flowidentity.Instance{}) {
		t.Fatalf("receipt authority authorized new construction: %+v %+v handled=%t err=%v", materialized, decision, handled, err)
	}
}

func nestedConnectionConstructionSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyNestedKeyedConnectionSelection(t))
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
	receipts := constructionReceiptTestReader{root}
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
			receipts = append(receipts, instance)
		}
		parents[parentKey], leaves[parentKey] = parent, leaf
		descriptors = append(descriptors,
			pinrouting.Descriptor{EntityID: parent.EntityID, FlowInstance: parent.InstancePath, AddressFields: map[string]string{"entity.id": parentKey}},
			pinrouting.Descriptor{EntityID: leaf.EntityID, FlowInstance: leaf.InstancePath, AddressFields: map[string]string{"entity.id": "same"}},
		)
	}
	owner := newTemplateInstanceLifecycleOwner(source, table, nil, receipts)
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

func TestA9ConnectionAncestorCandidatesAreResolvedOnlyByDependentPaths(t *testing.T) {
	for _, test := range []struct {
		name           string
		dependentEvent string
		competing      bool
		targeted       bool
		ambiguous      bool
		wantRoutes     int
	}{
		{"independent leaves beside dependent edge", "start", false, false, false, 4},
		{"nonmatching descendant with distinct parents", "other", true, false, false, 4},
		{"target filtered descendant with distinct parents", "start", true, true, false, 1},
		{"ambiguous required ancestor refuses before commit", "start", true, false, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyConnectionAncestorCandidates(t, test.dependentEvent, test.competing))
			store := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}}
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstancePlanner: newTestFlowInstanceActivationOwner(store.Activate)})
			if err != nil {
				t.Fatal(err)
			}
			root := installConnectionSourceConstructionForRun(t, eb, source, ".", busInternalTestRunID)
			parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
			if err != nil {
				t.Fatal(err)
			}
			middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", "stored-leaf")
			if err != nil {
				t.Fatal(err)
			}
			add := func(instance flowidentity.Instance, key string) {
				t.Helper()
				coordinate := testRunScopedFlowRoute(instance.Route())
				if err := eb.RouteTable().AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: coordinate, Instance: instance}); err != nil {
					t.Fatal(err)
				}
				store.installConstructionReceipt(coordinate, pipeline.FlowConstructionPublicationEvidence{Identity: instance})
				store.flowInstances = append(store.flowInstances, ActiveFlowInstanceDescriptor{
					Identity: instance, RunID: busInternalTestRunID, InstanceID: instance.InstanceID,
					EntityID: instance.EntityID, FlowInstance: instance.InstancePath, FlowTemplate: instance.TemplateID,
					AddressFields: map[string]string{"entity.id": key},
				})
			}
			add(parent, "parent-key")
			add(middle, "")
			add(leaf, "leaf-key")
			var workers []flowidentity.Instance
			for _, key := range []string{"parent-key", "leaf-key"} {
				worker, err := flowidentity.KeyedChild(source, root, "worker", key)
				if err != nil {
					t.Fatal(err)
				}
				add(worker, key)
				workers = append(workers, worker)
			}
			if test.competing {
				other, err := flowidentity.KeyedChild(source, root, "parent", "other-parent")
				if err != nil {
					t.Fatal(err)
				}
				add(other, "leaf-key")
			}
			before := make(map[flowidentity.RunScopedFlowInstance]flowidentity.Instance)
			for coordinate, instance := range eb.RouteTable().instanceOwners {
				before[coordinate] = instance
			}
			envelope := events.EventEnvelope{}
			if test.targeted {
				envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "worker", FlowInstance: workers[0].InstancePath, EntityID: workers[0].EntityID})
			}
			event := eventtest.ExistingRunRootIngress(eventtest.UUID(test.name), "start", "test", "", []byte(`{"parent_key":"parent-key","leaf_key":"leaf-key"}`), 0, busInternalTestRunID, envelope, time.Now().UTC())
			check, err := eb.CheckPublishRecipientPlan(context.Background(), event)
			if test.ambiguous {
				if err == nil || !strings.Contains(err.Error(), "selected competing ancestors of parent") {
					t.Fatalf("required ancestor ambiguity was not refused: %+v %v", check, err)
				}
				if err := eb.Publish(context.Background(), event); err == nil || !strings.Contains(err.Error(), "selected competing ancestors of parent") {
					t.Fatalf("ambiguous publication was not refused: %v", err)
				}
			} else if err != nil || check.TargetFailure != "" || len(check.DeliveryRoutes) != test.wantRoutes {
				t.Fatalf("independent/dependent selections: %+v err=%v, want %d routes", check, err, test.wantRoutes)
			}
			if len(store.events) != 0 || len(store.routes) != 0 || len(store.activations) != 0 || !reflect.DeepEqual(before, eb.RouteTable().instanceOwners) {
				t.Fatal("planning or rejected ambiguity mutated publication/construction state")
			}
		})
	}
}

func TestA9ConnectionAncestorCandidatesDeduplicateOnlyExactOwners(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	ctx := withConnectRoutePlanPreview(context.Background())
	for range 2 {
		if err := selectConnectionConstruction(ctx, parent); err != nil {
			t.Fatal(err)
		}
	}
	preview := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if len(preview.selected["parent"]) != 1 {
		t.Fatal("same exact owner was registered twice")
	}
	otherRun := root
	otherRun.InstancePath, otherRun.EntityID = eventtest.UUID("other-run"), eventtest.UUID("other-run")
	other, err := flowidentity.KeyedChild(source, otherRun, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	if err := selectConnectionConstruction(ctx, other); err != nil {
		t.Fatal(err)
	}
	owner := newTemplateInstanceLifecycleOwner(source, nil, nil, nil)
	actual, err := owner.selectedConstructionChild(ctx, busInternalTestRunID, root, "parent", nil, preview.selected)
	if err != nil || actual != parent {
		t.Fatalf("foreign structural parent changed exact selection: %+v %v", actual, err)
	}
	for _, test := range []struct {
		name       string
		candidates []flowidentity.Instance
		want       flowidentity.Instance
		refused    bool
	}{
		{"no constructed provider receiver", nil, flowidentity.Instance{}, false},
		{"exact provider receiver", []flowidentity.Instance{parent}, parent, false},
		{"ambiguous provider execution dependency", []flowidentity.Instance{parent, other}, flowidentity.Instance{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := providerConstructionCandidate("parent", test.candidates)
			if (err != nil) != test.refused || actual != test.want {
				t.Fatalf("provider execution dependency lost uniqueness: %+v %v", actual, err)
			}
		})
	}
}
