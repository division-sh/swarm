package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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
			seedWorkflowHeaderProjectionFixture(t, ctx, db, runID, entityID, instance, "freeze", "review_item", "active", "{}", time.Now().UTC())
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

func TestEventBusCompositionOwnerExactConnectedReceiverBothStores(t *testing.T) {
	scopes := []struct {
		name, flow, instance, other string
		source                      func(*testing.T) semanticview.Source
		template                    bool
	}{
		{"singleton", "owner", "owner", "owner/child/instance", func(t *testing.T) semanticview.Source { return stateOnlyAcquisitionSource(t, "owner") }, false},
		{"static", "owner", "owner", "owner/other", func(t *testing.T) semanticview.Source {
			return stateOnlyAcquisitionSourceWithMode(t, "owner", runtimecontracts.FlowModeStatic)
		}, false},
		{"template", "owner", "owner/instance", "owner/other", func(t *testing.T) semanticview.Source {
			return stateOnlyAcquisitionSourceWithMode(t, "owner", runtimecontracts.FlowModeTemplate)
		}, true},
		{"parent-singleton-child-template", "parent", "parent", "parent/child/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeStatic, "child", runtimecontracts.FlowModeTemplate, "parent")
		}, false},
		{"parent-template-child-singleton", "parent", "parent/instance", "parent/child", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeTemplate, "child", runtimecontracts.FlowModeStatic, "parent")
		}, true},
		{"nested-template", "parent/child", "parent/child/instance", "parent/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyNestedAcquisitionSource(t, "parent", runtimecontracts.FlowModeTemplate, "child", runtimecontracts.FlowModeTemplate, "parent/child")
		}, true},
		{"sibling-prefix", "owner", "owner/instance", "owner-other/instance", func(t *testing.T) semanticview.Source {
			return stateOnlySiblingAcquisitionSource(t, "owner", runtimecontracts.FlowModeTemplate, "owner-other", runtimecontracts.FlowModeTemplate)
		}, true},
		{"deep-template", "parent/child/grandchild", "parent/child/grandchild/instance", "parent/instance", func(t *testing.T) semanticview.Source {
			return stateOnlyDeepAcquisitionSource(t, "parent", "child", "grandchild")
		}, true},
		{"root", ".", "", "child", func(t *testing.T) semanticview.Source {
			return stateOnlyRootAcquisitionSource(t, "different-authored-name")
		}, false},
	}
	cases := []struct {
		name, node, state, lifecycle, failure  string
		duplicateOwner, wrongOwner, appearance bool
	}{
		{name: "existing", node: "selector", state: "active"},
		{name: "missing-with-business-key-sibling", node: "selector", failure: "owner is missing"},
		{name: "initialize-with-business-key-sibling", node: "upserter", failure: "owner is missing"},
		{name: "initializer-reuses-state", node: "upserter", state: "active"},
		{name: "exact-state-appears-before-commit", node: "upserter", appearance: true, failure: "owner is missing"},
		{name: "wrong-canonical-owner", node: "upserter", state: "active", wrongOwner: true, failure: "disagrees with receiver entity"},
		{name: "duplicate-exact-owners", node: "selector", state: "active", duplicateOwner: true},
		{name: "terminal-state", node: "selector", state: "done", failure: "owner is unavailable"},
		{name: "draining-companion", node: "selector", state: "active", lifecycle: "draining", failure: "owner is unavailable"},
		{name: "terminated-companion", node: "selector", state: "active", lifecycle: "terminated", failure: "owner is unavailable"},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scope := range scopes {
			for _, tc := range cases {
				// Template ingress requires an active, source-bound instance. Owner
				// edge cases without one belong to the separate persistence and
				// ClassifyDeliveryTargetOwnership matrices, not this routed path.
				if scope.template && tc.name != "existing" && tc.name != "initializer-reuses-state" && tc.name != "terminal-state" {
					continue
				}
				t.Run(backend+"/"+scope.name+"/"+tc.name, func(t *testing.T) {
					source := scope.source(t)
					bundle, ok := semanticview.Bundle(source)
					if !ok || bundle.SourceArtifact == nil {
						t.Fatal("state-only acquisition fixture has no admitted artifact")
					}
					selected, db, ctx, runID := openStateOnlyAcquisitionStoreWithSource(t, backend, source)
					scopeFact, ok := runtimeauthoractivity.ScopeFromContext(ctx)
					if !ok {
						t.Fatal("state-only acquisition fixture has no bundle scope")
					}
					lease, err := selected.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scopeFact, []runtimeauthoractivity.EventDescriptor{
						{EventType: "test.node_emitted.selector", Disposition: runtimeauthoractivity.StoryAuthored},
						{EventType: "test.node_emitted.upserter", Disposition: runtimeauthoractivity.StoryAuthored},
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(lease.Release)
					instance := scope.instance
					entityID := runtimepipeline.FlowInstanceEntityID(instance)
					if scope.flow == "." {
						instance = runID
						entityID = runID
					}
					rootConstruction := sqliteFlowActivationRequest(bundle, ".", runID, "", runID)
					rootConstruction.Instance = runtimeflowidentity.Stored(source, ".", runID, runID, runID, "")
					rootConstruction.OccurredAt = time.Now().UTC()
					rootPlan := constructHistoricalSourceFixture(t, ctx, selected.(agentFixtureFlowStore), rootConstruction)
					var constructed runtimeflowidentity.Instance
					if scope.template {
						construction := sqliteFlowActivationRequest(bundle, scope.flow, "instance", "", instance)
						view, found := bundle.FlowViewByID(scope.flow)
						if !found || view.Parent == nil {
							t.Fatal("template fixture requires its compiled structural parent")
						}
						var parents []runtimeflowidentity.Instance
						plans := []runtimepipeline.FlowInstanceActivationPlan{rootPlan}
						for i := 0; i < len(plans); i++ {
							plan := plans[i]
							plans = append(plans, plan.Children...)
							if plan.Identity.TemplateID == view.Parent.Paths.FlowPath {
								parents = append(parents, plan.Identity)
							}
						}
						if len(parents) == 0 && view.Parent.Parent != nil && view.Parent.Parent.Paths.FlowPath == rootPlan.Identity.TemplateID {
							parent, err := runtimeflowidentity.KeyedChild(source, rootPlan.Identity, view.Parent.Paths.FlowPath, "parent-instance")
							if err != nil {
								t.Fatal(err)
							}
							parentPlan := constructHistoricalSourceFixture(t, ctx, selected.(agentFixtureFlowStore), runtimepipeline.FlowInstanceActivationRequest{
								ContractBundle: source, Instance: parent, OccurredAt: time.Now().UTC(),
								ConstructorInput: "test.fixture_parent.construct", ResolvedKey: "parent-instance",
								TriggerEvent: eventtest.ExistingRunRootIngress(uuid.NewString(), "test.fixture_parent.construct", "fixture", "", []byte(`{"account_id":"parent-business-key","instance_key":"parent-instance"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC()),
							})
							parents = append(parents, parentPlan.Identity)
						}
						if len(parents) != 1 {
							t.Fatal("template fixture lacks one exact committed structural parent")
						}
						child, err := runtimeflowidentity.KeyedChild(source, parents[0], scope.flow, "instance")
						if err != nil {
							t.Fatal(err)
						}
						construction.Instance = child
						constructed = child
						instance, entityID = child.InstancePath, child.EntityID
						construction.ConstructorInput = "test.node_emitted.selector"
						construction.ResolvedKey = "instance"
						creatingPayload := []byte(`{"account_id":"different-business-key","instance_key":"instance"}`)
						if scope.name == "nested-template" {
							creatingPayload = []byte(`{"account_id":"different-business-key","instance_key":"instance","parent_key":"parent-instance"}`)
						}
						construction.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "test.node_emitted.selector", "", "", creatingPayload, 0, runID, events.EventEnvelope{}, time.Now().UTC())
						constructHistoricalSourceFixture(t, ctx, selected.(agentFixtureFlowStore), construction)
					}
					otherEntity := uuid.NewString()
					seedStateOnlyAcquisitionEntity(t, backend, db, runID, otherEntity, scope.other, "active", "same-business-key")
					if tc.state == "" {
						for _, table := range []string{"flow_instances", "entity_state"} {
							column := "instance_path"
							if table == "entity_state" {
								column = "flow_instance"
							}
							if _, err := db.ExecContext(ctx, "DELETE FROM "+table+" WHERE run_id=$1 AND "+column+"=$2", runID, instance); err != nil {
								t.Fatal(err)
							}
						}
					} else {
						for _, table := range []string{"flow_instances", "entity_state"} {
							if _, err := db.ExecContext(ctx, "UPDATE "+table+" SET current_state=$1 WHERE run_id=$2 AND entity_id=$3", tc.state, runID, entityID); err != nil {
								t.Fatal(err)
							}
						}
					}
					if tc.wrongOwner {
						wrongID := uuid.NewString()
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, wrongID, instance, "active", "different-business-key")
						if _, err := db.ExecContext(ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID); err != nil {
							t.Fatal(err)
						}
						seedWorkflowHeaderProjectionFixture(t, ctx, db, runID, wrongID, instance, scope.flow, "review_item", "active", "{}", time.Now().UTC())
					}
					if tc.duplicateOwner {
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), instance, "active", "same-business-key")
					}
					if tc.lifecycle != "" {
						if _, err := db.ExecContext(ctx, `UPDATE flow_instances SET status=$1 WHERE run_id=$2 AND instance_path=$3`, tc.lifecycle, runID, instance); err != nil {
							t.Fatal(err)
						}
					}
					node, err := runtimeidentity.AdmitExecutableNodeDeclaration(scope.flow, tc.node)
					if err != nil {
						t.Fatal(err)
					}
					handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
					if err != nil {
						t.Fatal(err)
					}
					eventType := events.EventType("test.node_emitted." + tc.node)
					newBus := func() *runtimebus.EventBus {
						bus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
							ContractBundle: source, SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
							RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
								target := events.RouteIdentity{FlowID: scope.flow, FlowInstance: instance}
								if tc.wrongOwner {
									target.EntityID = entityID
								}
								return []runtimebus.DeliveryRouteBlueprint{{Recipient: events.MustNodeDeliveryRecipient(node), Target: target, Handler: handler.ForEvent(eventType)}}, nil
							},
						})
						if err != nil {
							t.Fatal(err)
						}
						if scope.template {
							identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.StoredRoute(scope.flow, "instance", instance))
							if err != nil {
								t.Fatal(err)
							}
							if err := flowroutefixture.StageAndPublish(ctx, bus, runtimebus.FlowInstanceRouteMaterializationRequest{
								Identity: identity, Instance: constructed, ActivationVariables: map[string]string{"entity.instance_key": "instance"},
							}); err != nil {
								t.Fatal(err)
							}
						}
						return bus
					}
					bus := newBus()
					payload := []byte(`{"account_id":"same-business-key","instance_key":"instance"}`)
					envelope := events.EventEnvelope{}
					if scope.name == "nested-template" {
						payload = []byte(`{"account_id":"same-business-key","instance_key":"instance","parent_key":"parent-instance"}`)
					}
					evt := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(eventType), "", "", payload, 0, runID, envelope, time.Now().UTC())
					if tc.wrongOwner && scope.flow != "." {
						evt = eventtest.TargetRouted(evt, events.RouteIdentity{FlowID: scope.flow, FlowInstance: instance, EntityID: entityID})
					}
					if tc.failure != "" {
						failure := tc.failure
						if scope.flow == "." && tc.state == "" {
							failure = "canonical activation planner"
						}
						if scope.flow == "." && tc.wrongOwner {
							failure = "root construction identity contradicts"
						}
						err := bus.Publish(ctx, evt)
						if err == nil || !strings.Contains(err.Error(), failure) {
							t.Fatalf("Publish=%v, want %q", err, failure)
						}
						if tc.appearance {
							seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, instance, "active", "different-business-key")
							if err := bus.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), failure) {
								t.Fatalf("field-row appearance became construction authority: %v", err)
							}
						}
						assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
						return
					}
					plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
					wantRoute := events.RouteIdentity{FlowID: scope.flow, FlowInstance: instance, EntityID: entityID}.Normalized()
					wantRoutes := map[events.RouteIdentity]bool{wantRoute: false}
					if scope.name == "nested-template" {
						parent := constructed.ParentRoute
						wantRoutes[events.RouteIdentity{FlowID: parent.FlowID, FlowInstance: parent.FlowInstance, EntityID: parent.EntityID}.Normalized()] = false
					}
					if err != nil || len(plan.DeliveryRoutes) != len(wantRoutes) {
						t.Fatalf("plan=%#v err=%v", plan, err)
					}
					for _, route := range plan.DeliveryRoutes {
						target := route.Target.Route().Normalized()
						seen, known := wantRoutes[target]
						if !known || seen || route.Target.MaterializingEntity() {
							t.Fatalf("wrong or repeated constructed owner: %#v want %#v", plan.DeliveryRoutes, wantRoutes)
						}
						wantRoutes[target] = true
					}
					if err := bus.Publish(ctx, evt); err != nil {
						t.Fatal(err)
					}
					prepared, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
					if err != nil || !found || len(prepared.DeliveryRoutes) != len(wantRoutes) {
						t.Fatalf("load=%t %v %#v", found, err, prepared)
					}
					for _, route := range prepared.DeliveryRoutes {
						target := route.Target.Route().Normalized()
						seen, known := wantRoutes[target]
						if !known || !seen || route.Target.MaterializingEntity() {
							t.Fatalf("wrong or repeated persisted target=%#v", route.Target)
						}
						wantRoutes[target] = false
					}
					// Late rows and a reconstructed publisher cannot re-elect an accepted receiver.
					seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), scope.other+"/later", "active", "same-business-key")
					if err := newBus().Publish(ctx, evt); err != nil {
						t.Fatalf("duplicate after reconstruction: %v", err)
					}
					again, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
					if err != nil || !found || !reflect.DeepEqual(again.DeliveryRoutes, prepared.DeliveryRoutes) {
						t.Fatalf("duplicate rewrote receiver: %#v %t %v", again, found, err)
					}
					assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, len(wantRoutes))
					assertStateOnlyAcquisitionLifecycleCount(t, backend, db, runID, instance, 1)
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
		{name: "singleton exact", parentMode: runtimecontracts.FlowModeStatic, path: "parent", want: true},
		{name: "singleton rejects concrete suffix", parentMode: runtimecontracts.FlowModeStatic, path: "parent/instance"},
		{name: "singleton rejects nested template", parentMode: runtimecontracts.FlowModeStatic, path: "parent/child/instance"},
		{name: "template direct instance", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/instance", want: true},
		{name: "template rejects authored child singleton", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/child"},
		{name: "template rejects authored child instance", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/child/instance"},
		{name: "template rejects unrelated", parentMode: runtimecontracts.FlowModeTemplate, path: "sibling/instance"},
		{name: "template rejects nested arbitrary path", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/arbitrary/instance"},
		{name: "template rejects trailing slash alias", parentMode: runtimecontracts.FlowModeTemplate, path: "parent/instance/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := stateOnlyNestedAcquisitionSource(t, parentID, test.parentMode, childID, runtimecontracts.FlowModeStatic, parentID)
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

func openStateOnlyAcquisitionStoreWithSource(t *testing.T, backend string, source semanticview.Source) (stateOnlyAcquisitionStore, *sql.DB, context.Context, string) {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("state-only acquisition source requires admitted declarations")
	}
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash())), runID)
	fixture := semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, Artifact: bundle.SourceArtifact}
	if backend == "sqlite" {
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		db := selected.backend.ConstructionHandle()
		requireRunFixtureForTest(t, ctx, selected, fixture)
		return selected, db, ctx, runID
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := newTestPostgresStore(t, db)
	requireRunFixtureForTest(t, ctx, selected, fixture)
	return selected, db, ctx, runID
}

func stateOnlyAcquisitionSource(t *testing.T, flowID string) semanticview.Source {
	return stateOnlyAcquisitionSourceWithMode(t, flowID, runtimecontracts.FlowModeStatic)
}

func stateOnlyAcquisitionSourceWithMode(t *testing.T, flowID, mode string) semanticview.Source {
	return loadStateOnlyAcquisitionSource(t, "state-only-acquisition", map[string]string{flowID: mode}, flowID)
}

func stateOnlyNestedAcquisitionSource(t *testing.T, parentID, parentMode, childID, childMode, targetFlow string) semanticview.Source {
	return loadStateOnlyAcquisitionSource(t, "state-only-nested-acquisition", map[string]string{
		parentID: parentMode, parentID + "/" + childID: childMode,
	}, targetFlow)
}

func stateOnlySiblingAcquisitionSource(t *testing.T, firstID, firstMode, secondID, secondMode string) semanticview.Source {
	return loadStateOnlyAcquisitionSource(t, "state-only-sibling-acquisition", map[string]string{
		firstID: firstMode, secondID: secondMode,
	}, firstID)
}

func stateOnlyDeepAcquisitionSource(t *testing.T, parentID, childID, grandchildID string) semanticview.Source {
	return loadStateOnlyAcquisitionSource(t, "state-only-deep-acquisition", map[string]string{
		parentID:                 runtimecontracts.FlowModeStatic,
		parentID + "/" + childID: runtimecontracts.FlowModeStatic,
		parentID + "/" + childID + "/" + grandchildID: runtimecontracts.FlowModeTemplate,
	}, parentID+"/"+childID+"/"+grandchildID)
}

func stateOnlyRootAcquisitionSource(t *testing.T, workflowName string) semanticview.Source {
	return loadStateOnlyAcquisitionSource(t, workflowName, map[string]string{".": runtimecontracts.FlowModeStatic}, ".")
}

func loadStateOnlyAcquisitionSource(t *testing.T, workflowName string, modes map[string]string, targetFlow string) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyStateOnlyAcquisition(t, workflowName, modes, targetFlow)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(runtimepipeline.WorkflowRepoRoot(), root, runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("load state-only acquisition contracts: %v", err)
	}
	return semanticview.Wrap(bundle)
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
	fields, err := json.Marshal(map[string]any{"account_id": accountID, "instance_key": "instance"})
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
