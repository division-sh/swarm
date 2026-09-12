package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestForkedSourceCanonicalTargetOwnersExcludeAndPreserveReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			childRunID := uuid.NewString()
			requireRunningRunForTest(t, ctx, selected, childRunID, time.Now().UTC())
			instance := "freeze/instance"
			entityID := runtimepipeline.FlowInstanceEntityID(instance)
			seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, instance, "active", "source")
			before, err := selected.ListSelectedRunTargetOwners(ctx, runID)
			if err != nil || len(before) != 1 || before[0].EntityID != entityID || before[0].FlowInstance != instance {
				t.Fatalf("canonical owners before freeze = %#v, %v", before, err)
			}
			snapshot, disposition, err := selected.ForkRunSource(ctx, runtimerunlifecycle.ForkSourceRequest{
				RunID: runID, ContinuedAsRunID: childRunID, EndedAt: time.Now().UTC(),
			})
			if err != nil || disposition != runtimerunlifecycle.MutationApplied || snapshot.State != runtimerunlifecycle.StateForked {
				t.Fatalf("freeze = %#v, %v, %v", snapshot, disposition, err)
			}
			after, err := selected.ListSelectedRunTargetOwners(ctx, runID)
			if err != nil || len(after) != 0 {
				t.Fatalf("canonical owners after freeze = %#v, %v", after, err)
			}
			exact, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.Route{ScopeKey: "freeze", InstanceID: "instance", InstancePath: instance})
			if err != nil {
				t.Fatal(err)
			}
			record, found, err := selected.LoadWorkflowEntityState(ctx, exact, runtimeidentity.NormalizeEntityID(entityID))
			if err != nil || !found || record.EntityID != entityID || record.CurrentState != "active" {
				t.Fatalf("historical state must remain readable = %#v, found=%t, %v", record, found, err)
			}
		})
	}
}

func TestEventBusCompositionOwnerIncludesStateWithoutLifecycleOnBothStores(t *testing.T) {
	scopes := []struct {
		name, flow, instance, other string
		source                      func(*testing.T) semanticview.Source
	}{
		{"singleton", "owner", "owner", "owner/child/instance", func(t *testing.T) semanticview.Source { return stateOnlyAcquisitionSource(t, "owner") }},
		{"static", "owner", "owner", "owner/other", func(t *testing.T) semanticview.Source {
			return stateOnlyAcquisitionSourceWithMode(t, "owner", runtimecontracts.FlowModeStatic)
		}},
		{"template", "owner", "owner/instance", "owner/other", func(t *testing.T) semanticview.Source {
			return stateOnlyAcquisitionSourceWithMode(t, "owner", runtimecontracts.FlowModeTemplate)
		}},
		{"parent-singleton-child-template", "parent", "parent", "parent/child/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeSingleton, "child", runtimecontracts.FlowModeTemplate)
		}},
		{"parent-template-child-singleton", "parent", "parent/instance", "parent/child", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeTemplate, "child", runtimecontracts.FlowModeSingleton)
		}},
		{"nested-template", "child", "parent/child/instance", "parent/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeTemplate, "child", runtimecontracts.FlowModeTemplate)
		}},
		{"sibling-prefix", "owner", "owner/instance", "owner-other/instance", func(t *testing.T) semanticview.Source {
			return stateOnlySiblingAcquisitionSource(t, "owner", runtimecontracts.FlowModeTemplate, "owner-other", runtimecontracts.FlowModeTemplate)
		}},
		{"deep-template", "grandchild", "parent/child/grandchild/instance", "parent/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyDeepAcquisitionSource(t, "parent", "child", "grandchild")
		}},
		{"root", ".", "", "child", func(t *testing.T) semanticview.Source {
			return stateOnlyRootAcquisitionSource(t, "different-authored-name")
		}},
	}
	cases := []struct {
		name, node, state, lifecycle, failure              string
		initialize, duplicateOwner, wrongOwner, appearance bool
	}{
		{name: "existing", node: "selector", state: "active"},
		{name: "missing-with-business-key-sibling", node: "selector", failure: "owner is missing"},
		{name: "initialize-with-business-key-sibling", node: "upserter", initialize: true},
		{name: "initializer-reuses-state", node: "upserter", state: "active"},
		{name: "exact-state-appears-before-commit", node: "upserter", initialize: true, appearance: true},
		{name: "wrong-canonical-owner", node: "upserter", state: "active", wrongOwner: true, failure: "disagrees with canonical handler identity"},
		{name: "duplicate-exact-owners", node: "selector", state: "active", duplicateOwner: true, failure: "ambiguous"},
		{name: "terminal-state", node: "selector", state: "done", failure: "owner is unavailable"},
		{name: "draining-companion", node: "selector", state: "active", lifecycle: "draining", failure: "owner is unavailable"},
		{name: "terminated-companion", node: "selector", state: "active", lifecycle: "terminated", failure: "owner is unavailable"},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scope := range scopes {
			for _, tc := range cases {
				t.Run(backend+"/"+scope.name+"/"+tc.name, func(t *testing.T) {
					selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
					source := scope.source(t)
					instance := scope.instance
					entityID := runtimepipeline.FlowInstanceEntityID(instance)
					if scope.flow == "." {
						instance = runID
						entityID = runID
					}
					otherEntity := uuid.NewString()
					seedStateOnlyAcquisitionEntity(t, backend, db, runID, otherEntity, scope.other, "active", "same-business-key")
					if tc.state != "" {
						id := entityID
						if tc.wrongOwner {
							id = uuid.NewString()
						}
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, id, instance, tc.state, "different-business-key")
					}
					if tc.duplicateOwner {
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), instance, "active", "same-business-key")
					}
					if tc.lifecycle != "" {
						seedStateOnlyAcquisitionLifecycle(t, backend, db, runID, instance, tc.lifecycle)
					}
					node, err := runtimeidentity.AdmitExecutableNodeDeclaration(scope.flow, tc.node)
					if err != nil {
						t.Fatal(err)
					}
					handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
					if err != nil {
						t.Fatal(err)
					}
					newBus := func() *runtimebus.EventBus {
						bus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
							ContractBundle: source,
							RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
								return []runtimebus.DeliveryRouteBlueprint{{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.RouteIdentity{FlowID: scope.flow, FlowInstance: instance}, Handler: handler.ForEvent("test.node_emitted")}}, nil
							},
						})
						if err != nil {
							t.Fatal(err)
						}
						return bus
					}
					bus := newBus()
					evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.node_emitted", "", "", []byte(`{"account_id":"same-business-key"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if tc.failure != "" {
						err := bus.Publish(ctx, evt)
						if err == nil || !strings.Contains(err.Error(), tc.failure) {
							t.Fatalf("Publish=%v, want %q", err, tc.failure)
						}
						assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
						return
					}
					plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
					if err != nil || len(plan.DeliveryRoutes) != 1 {
						t.Fatalf("plan=%#v err=%v", plan, err)
					}
					wantRoute := events.RouteIdentity{FlowID: scope.flow, FlowInstance: instance, EntityID: entityID}.Normalized()
					if plan.DeliveryRoutes[0].Target.Route() != wantRoute || plan.DeliveryRoutes[0].Target.MaterializingEntity() != tc.initialize {
						t.Fatalf("wrong owner: %#v want %#v initialize=%t", plan.DeliveryRoutes, wantRoute, tc.initialize)
					}
					if tc.appearance {
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, instance, "active", "different-business-key")
					}
					if err := bus.Publish(ctx, evt); err != nil {
						t.Fatal(err)
					}
					prepared, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
					if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
						t.Fatalf("load=%t %v %#v", found, err, prepared)
					}
					target := prepared.DeliveryRoutes[0].Target
					if target.Route() != wantRoute || target.MaterializingEntity() != (tc.initialize && !tc.appearance) {
						t.Fatalf("persisted target=%#v", target)
					}
					// Late rows and a reconstructed publisher cannot re-elect an accepted receiver.
					seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), scope.other+"/later", "active", "same-business-key")
					if err := newBus().Publish(ctx, evt); err != nil {
						t.Fatalf("duplicate after reconstruction: %v", err)
					}
					again, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
					if err != nil || !found || len(again.DeliveryRoutes) != 1 || again.DeliveryRoutes[0].Target != target {
						t.Fatalf("duplicate rewrote receiver: %#v %t %v", again, found, err)
					}
					assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
					assertStateOnlyAcquisitionLifecycleCount(t, backend, db, runID, instance, 0)
					var siblingFields string
					if err := db.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, otherEntity).Scan(&siblingFields); err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(siblingFields, "same-business-key") {
						t.Fatalf("sibling mutated: %s", siblingFields)
					}
				})
			}
		}
	}
}

func TestWorkflowEntityStateSelectionOwnerUsesExactAuthoredScope(t *testing.T) {
	parentID := "parent"
	childID := "child"
	tests := []struct {
		name       string
		parentMode string
		path       string
		want       bool
	}{
		{name: "singleton exact", parentMode: runtimecontracts.FlowModeSingleton, path: "parent", want: true},
		{name: "singleton rejects concrete suffix", parentMode: runtimecontracts.FlowModeSingleton, path: "parent/instance"},
		{name: "singleton rejects nested template", parentMode: runtimecontracts.FlowModeSingleton, path: "parent/child/instance"},
		{name: "template direct instance", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/instance", want: true},
		{name: "template rejects authored child singleton", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/child"},
		{name: "template rejects authored child instance", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/child/instance"},
		{name: "template rejects unrelated", parentMode: runtimecontracts.FlowModeTemplate, path: "sibling/instance"},
		{name: "template rejects nested arbitrary path", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/arbitrary/instance"},
		{name: "template rejects trailing slash alias", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/instance/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := stateOnlyNestedAcquisitionSource(t, parentID, test.parentMode, childID, runtimecontracts.FlowModeSingleton)
			owner, err := runtimepipeline.AdmitWorkflowEntityStateSelectionOwner(source, parentID, "")
			if err != nil {
				t.Fatal(err)
			}
			if got := owner.Owns(test.path); got != test.want {
				t.Fatalf("owner.Owns(%q) = %t, want %t", test.path, got, test.want)
			}
		})
	}
}

type stateOnlyAcquisitionStore interface {
	storeTestDurableEventBusStore
	runtimepipeline.WorkflowEntityStatePersistenceReader
}

func openStateOnlyAcquisitionStore(t *testing.T, backend string) (stateOnlyAcquisitionStore, *sql.DB, context.Context, string) {
	t.Helper()
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(storeTestWorkContext(t, testAuthorActivityContext()), runID)
	if backend == "sqlite" {
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(selected.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
		return selected, selected.backend.ConstructionHandle(), ctx, runID
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := newTestPostgresStore(t, db)
	requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
	return selected, db, ctx, runID
}

func stateOnlyAcquisitionSource(t *testing.T, flowID string) semanticview.Source {
	return stateOnlyAcquisitionSourceWithMode(t, flowID, runtimecontracts.FlowModeSingleton)
}

func stateOnlyAcquisitionSourceWithMode(t *testing.T, flowID, mode string) semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Path: flowID, Paths: runtimecontracts.FlowContractPaths{FlowPath: flowID},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: flowID, Mode: mode, InitialState: "active",
			States: []string{"active", "done"}, TerminalStates: []string{"done"},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"selector": {
				SubscribesTo:  []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}},
			},
			"upserter": {
				SubscribesTo:  []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {CreateEntity: true}},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "state-only-acquisition", Version: "1",
			FlowInitial:  map[string]string{flowID: "active"},
			FlowStates:   map[string][]string{flowID: {"active", "done"}},
			FlowTerminal: map[string][]string{flowID: {"done"}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{flowID: flow.Schema},
	}
	return semanticview.Wrap(admitStateOnlyAcquisitionEntityContracts(t, bundle, []string{flowID}))
}

func stateOnlyNestedAcquisitionSource(t *testing.T, parentID, parentMode, childID, childMode string) semanticview.Source {
	flow := func(id, path, mode string) runtimecontracts.FlowContractView {
		return runtimecontracts.FlowContractView{
			Path: path, Paths: runtimecontracts.FlowContractPaths{FlowPath: id},
			Schema: runtimecontracts.FlowSchemaDocument{
				Name: id, Mode: mode, InitialState: "active",
				States: []string{"active", "done"}, TerminalStates: []string{"done"},
			},
			Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				"selector": {
					SubscribesTo:  []string{"test.node_emitted"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}},
				},
				"upserter": {
					SubscribesTo:  []string{"test.node_emitted"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {CreateEntity: true}},
				},
			},
		}
	}
	parent := flow(parentID, parentID, parentMode)
	child := flow(childID, parentID+"/"+childID, childMode)
	parent.Children = []runtimecontracts.FlowContractView{child}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{parent}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "state-only-nested-acquisition", Version: "1",
			FlowInitial:  map[string]string{parentID: "active", childID: "active"},
			FlowStates:   map[string][]string{parentID: {"active", "done"}, childID: {"active", "done"}},
			FlowTerminal: map[string][]string{parentID: {"done"}, childID: {"done"}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				parentID: &root.Children[0],
				childID:  &root.Children[0].Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			parentID: root.Children[0].Schema,
			childID:  root.Children[0].Children[0].Schema,
		},
	}
	return semanticview.Wrap(admitStateOnlyAcquisitionEntityContracts(t, bundle, []string{parentID, childID}))
}

func stateOnlyAcquisitionFlow(flowID, path, mode string) runtimecontracts.FlowContractView {
	return runtimecontracts.FlowContractView{
		Path: path, Paths: runtimecontracts.FlowContractPaths{FlowPath: flowID},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: flowID, Mode: mode, InitialState: "active",
			States: []string{"active", "done"}, TerminalStates: []string{"done"},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"selector": {
				SubscribesTo:  []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}},
			},
			"upserter": {
				SubscribesTo:  []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {CreateEntity: true}},
			},
		},
	}
}

func stateOnlySiblingAcquisitionSource(t *testing.T, firstID, firstMode, secondID, secondMode string) semanticview.Source {
	first := stateOnlyAcquisitionFlow(firstID, firstID, firstMode)
	second := stateOnlyAcquisitionFlow(secondID, secondID, secondMode)
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{first, second}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "state-only-sibling-acquisition", Version: "1",
			FlowInitial:  map[string]string{firstID: "active", secondID: "active"},
			FlowStates:   map[string][]string{firstID: {"active", "done"}, secondID: {"active", "done"}},
			FlowTerminal: map[string][]string{firstID: {"done"}, secondID: {"done"}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				firstID:  &root.Children[0],
				secondID: &root.Children[1],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			firstID: root.Children[0].Schema, secondID: root.Children[1].Schema,
		},
	}
	return semanticview.Wrap(admitStateOnlyAcquisitionEntityContracts(t, bundle, []string{firstID, secondID}))
}

func stateOnlyDeepAcquisitionSource(t *testing.T, parentID, childID, grandchildID string) semanticview.Source {
	parent := stateOnlyAcquisitionFlow(parentID, parentID, runtimecontracts.FlowModeSingleton)
	child := stateOnlyAcquisitionFlow(childID, parentID+"/"+childID, runtimecontracts.FlowModeSingleton)
	grandchild := stateOnlyAcquisitionFlow(grandchildID, parentID+"/"+childID+"/"+grandchildID, runtimecontracts.FlowModeTemplate)
	child.Children = []runtimecontracts.FlowContractView{grandchild}
	parent.Children = []runtimecontracts.FlowContractView{child}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{parent}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "state-only-deep-acquisition", Version: "1",
			FlowInitial:  map[string]string{parentID: "active", childID: "active", grandchildID: "active"},
			FlowStates:   map[string][]string{parentID: {"active", "done"}, childID: {"active", "done"}, grandchildID: {"active", "done"}},
			FlowTerminal: map[string][]string{parentID: {"done"}, childID: {"done"}, grandchildID: {"done"}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				parentID:     &root.Children[0],
				childID:      &root.Children[0].Children[0],
				grandchildID: &root.Children[0].Children[0].Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			parentID:     root.Children[0].Schema,
			childID:      root.Children[0].Children[0].Schema,
			grandchildID: root.Children[0].Children[0].Children[0].Schema,
		},
	}
	return semanticview.Wrap(admitStateOnlyAcquisitionEntityContracts(t, bundle, []string{parentID, childID, grandchildID}))
}

func stateOnlyRootAcquisitionSource(t *testing.T, workflowName string) semanticview.Source {
	const rootFlowID = "."
	root := stateOnlyAcquisitionFlow(rootFlowID, rootFlowID, runtimecontracts.FlowModeStatic)
	root.Schema.Name = workflowName
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: workflowName, Version: "1",
			FlowInitial:  map[string]string{rootFlowID: "active"},
			FlowStates:   map[string][]string{rootFlowID: {"active", "done"}},
			FlowTerminal: map[string][]string{rootFlowID: {"done"}},
		},
		FlowTree:    flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{rootFlowID: &root}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{rootFlowID: root.Schema},
	}
	return semanticview.Wrap(admitStateOnlyAcquisitionEntityContracts(t, bundle, nil))
}

func admitStateOnlyAcquisitionEntityContracts(t *testing.T, base *runtimecontracts.WorkflowContractBundle, flowIDs []string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	flowIDs = append([]string(nil), flowIDs...)
	sort.Strings(flowIDs)
	var packageFlows strings.Builder
	for _, flowID := range flowIDs {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			t.Fatal("state-only acquisition contract requires nonblank flow ID")
		}
		fmt.Fprintf(&packageFlows, "  - id: %s\n    flow: %s\n    mode: template\n", flowID, flowID)
		writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, flowID, "schema.yaml"), fmt.Sprintf("name: %s\nmode: template\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n", flowID))
		writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, flowID, "entities.yaml"), "review_item: {}\n")
	}

	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: state-only-acquisition\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "entities.yaml"), "review_item: {}\n")
	admitted, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(runtimepipeline.WorkflowRepoRoot(), root, runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("load state-only acquisition entity contracts: %v", err)
	}
	admitted.Semantics = base.Semantics
	admitted.FlowTree = base.FlowTree
	admitted.FlowSchemas = base.FlowSchemas
	admitted.URIRegistry = base.URIRegistry
	admitted.Nodes = base.Nodes
	admitted.Events = base.Events
	return admitted
}

func writeStateOnlyAcquisitionFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func seedStateOnlyAcquisitionEntity(t *testing.T, backend string, db *sql.DB, runID, entityID, instancePath, state, accountID string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	fields, err := json.Marshal(map[string]any{"account_id": accountID})
	if err != nil {
		t.Fatal(err)
	}
	query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'review_item', ?, '{}', ?, '{}', '{}', 1, ?, ?, ?)`
	args := []any{runID, entityID, instancePath, state, string(fields), now, now, now}
	if backend == "postgres" {
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'review_item', $4, '{}'::jsonb, $5::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $6, $6, $6)`
		args = []any{runID, entityID, instancePath, state, string(fields), now}
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed state-only acquisition entity: %v", err)
	}
}

func seedStateOnlyAcquisitionLifecycle(t *testing.T, backend string, db *sql.DB, runID, instancePath, status string) {
	t.Helper()
	query := `INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, terminated_at, created_at) VALUES (?, ?, 'review', 'static', '{}', ?, ?, ?)`
	now := time.Now().UTC().Truncate(time.Microsecond)
	var terminatedAt any
	if status == "terminated" {
		terminatedAt = now
	}
	args := []any{runID, instancePath, status, terminatedAt, now}
	if backend == "postgres" {
		query = `INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, terminated_at, created_at) VALUES ($1::uuid, $2, 'review', 'static', '{}'::jsonb, $3, $4, $5)`
		args = []any{runID, instancePath, status, terminatedAt, now}
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed state-only acquisition lifecycle: %v", err)
	}
}

func assertStateOnlyAcquisitionMutationCounts(t *testing.T, backend string, db *sql.DB, eventID string, wantEvents, wantDeliveries int) {
	t.Helper()
	queries := []struct {
		name string
		want int
		sql  string
	}{
		{name: "events", want: wantEvents, sql: "SELECT COUNT(*) FROM events WHERE event_id = ?"},
		{name: "deliveries", want: wantDeliveries, sql: "SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?"},
	}
	for _, check := range queries {
		query := check.sql
		if backend == "postgres" {
			query = strings.Replace(query, "?", "$1::uuid", 1)
		}
		var count int
		if err := db.QueryRowContext(context.Background(), query, eventID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", check.name, err)
		}
		if count != check.want {
			t.Fatalf("%s rows = %d, want %d", check.name, count, check.want)
		}
	}
}

func assertStateOnlyAcquisitionLifecycleCount(t *testing.T, backend string, db *sql.DB, runID, instancePath string, want int) {
	t.Helper()
	query := "SELECT COUNT(*) FROM flow_instances WHERE run_id = ? AND instance_path = ?"
	if backend == "postgres" {
		query = "SELECT COUNT(*) FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2"
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, runID, instancePath).Scan(&count); err != nil {
		t.Fatalf("count lifecycle rows for %s: %v", instancePath, err)
	}
	if count != want {
		t.Fatalf("lifecycle rows for %s = %d, want %d", instancePath, count, want)
	}
}
