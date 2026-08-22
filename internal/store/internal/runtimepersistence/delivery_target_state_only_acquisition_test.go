package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEventBusDeclaredKeyAcquisitionIncludesStateWithoutLifecycleOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			newBusForSource := func(t *testing.T, source semanticview.Source, flowID, nodeID string) *runtimebus.EventBus {
				t.Helper()
				node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, flowID, nodeID)
				if err != nil {
					t.Fatal(err)
				}
				handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
				if err != nil {
					t.Fatal(err)
				}
				bus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
					ContractBundle: source,
					RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
						return []runtimebus.DeliveryRouteBlueprint{{
							Recipient: events.MustNodeDeliveryRecipient(node),
							Target:    events.RouteIdentity{FlowID: flowID, FlowInstance: flowID + "/hostile-preselection"},
							Handler:   handler.ForEvent("test.node_emitted"),
						}}, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return bus
			}
			newBus := func(t *testing.T, flowID, nodeID string) *runtimebus.EventBus {
				t.Helper()
				return newBusForSource(t, stateOnlyAcquisitionSource(flowID), flowID, nodeID)
			}
			newEvent := func(accountID string) events.Event {
				payload, err := json.Marshal(map[string]any{"account_id": accountID})
				if err != nil {
					t.Fatal(err)
				}
				return eventtest.ExistingRunRootIngress(
					uuid.NewString(), "test.node_emitted", "", "", payload,
					0, runID, events.EventEnvelope{}, time.Now().UTC(),
				)
			}

			t.Run("targeted declared-key handlers preserve exact owner", func(t *testing.T) {
				flowID := "targeted-" + uuid.NewString()
				exact := events.RouteIdentity{FlowID: flowID, FlowInstance: flowID, EntityID: uuid.NewString()}.Normalized()
				competing := events.RouteIdentity{FlowID: flowID, FlowInstance: flowID + "/competing", EntityID: uuid.NewString()}.Normalized()
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, exact.EntityID, exact.FlowInstance, "active", "exact-owner-key")
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, competing.EntityID, competing.FlowInstance, "active", "payload-key")

				source := stateOnlyAcquisitionSource(flowID)
				node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, flowID, "selector")
				if err != nil {
					t.Fatal(err)
				}
				handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
				if err != nil {
					t.Fatal(err)
				}
				bus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
					ContractBundle: source,
					RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
						return []runtimebus.DeliveryRouteBlueprint{{
							Recipient: events.MustNodeDeliveryRecipient(node), Target: exact,
							Handler: handler.ForEvent("test.node_emitted"),
						}}, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				payload, err := json.Marshal(map[string]any{"account_id": "payload-key"})
				if err != nil {
					t.Fatal(err)
				}
				evt := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "test.node_emitted", "", "", payload, 0, runID,
					events.EnvelopeForTargetRoute(events.EventEnvelope{}, exact), time.Now().UTC(),
				)
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish targeted declared-key handlers: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found {
					t.Fatalf("load targeted event: found=%t err=%v", found, err)
				}
				want := events.MustExistingEntityTarget(exact)
				seenRecipients := map[string]bool{}
				for _, route := range persisted.DeliveryRoutes {
					if route.Target != want {
						t.Fatalf("targeted route = %#v, want exact owner %#v and never competing owner %#v", route, want, competing)
					}
					seenRecipients[route.Recipient.LocalID()] = true
				}
				for _, nodeID := range []string{"selector", "upserter"} {
					if !seenRecipients[nodeID] {
						t.Fatalf("targeted routes = %#v, missing %s declared-key consumer", persisted.DeliveryRoutes, nodeID)
					}
				}

				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), flowID+"/later", "active", "payload-key")
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("duplicate targeted publish replanned: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 2)
			})

			t.Run("select chooses state-only owner", func(t *testing.T) {
				flowID := "select-" + uuid.NewString()
				accountID := "state-only-select-" + uuid.NewString()
				entityID := uuid.NewString()
				instancePath := flowID
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), "unrelated-"+uuid.NewString(), "active", accountID)
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, instancePath, "active", accountID)
				evt := newEvent(accountID)
				bus := newBus(t, flowID, "selector")
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil {
					t.Fatalf("plan state-only select: %v", err)
				}
				want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: entityID})
				if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want {
					t.Fatalf("state-only select routes = %#v, want %#v", plan.DeliveryRoutes, want)
				}
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish state-only select: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
			})

			t.Run("select excludes nested child template owner", func(t *testing.T) {
				parentID := "parent-singleton-" + uuid.NewString()
				childID := "child-template-" + uuid.NewString()
				accountID := "nested-template-key-" + uuid.NewString()
				childPath := parentID + "/" + childID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), childPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeSingleton, childID, runtimecontracts.FlowModeTemplate)
				if err := newBusForSource(t, source, parentID, "selector").Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
					t.Fatalf("nested child template selection error = %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
				assertStateOnlyAcquisitionLifecycleCount(t, backend, db, childPath, 0)
			})

			t.Run("select or create ignores nested child and materializes parent", func(t *testing.T) {
				parentID := "parent-upsert-" + uuid.NewString()
				childID := "child-upsert-" + uuid.NewString()
				accountID := "nested-upsert-key-" + uuid.NewString()
				childPath := parentID + "/" + childID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), childPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeSingleton, childID, runtimecontracts.FlowModeTemplate)
				bus := newBusForSource(t, source, parentID, "upserter")
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil {
					t.Fatalf("plan parent select-or-create: %v", err)
				}
				if len(plan.DeliveryRoutes) != 1 || !plan.DeliveryRoutes[0].Target.MaterializingEntity() || plan.DeliveryRoutes[0].Target.Route().FlowInstance != parentID {
					t.Fatalf("parent select-or-create routes = %#v, want materializing parent %q and never child %q", plan.DeliveryRoutes, parentID, childPath)
				}
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish parent select-or-create: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
				assertStateOnlyAcquisitionLifecycleCount(t, backend, db, childPath, 0)
			})

			t.Run("select excludes nested child singleton from parent template", func(t *testing.T) {
				parentID := "parent-template-" + uuid.NewString()
				childID := "child-singleton-" + uuid.NewString()
				accountID := "nested-singleton-key-" + uuid.NewString()
				childPath := parentID + "/" + childID
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), childPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeTemplate, childID, runtimecontracts.FlowModeSingleton)
				if err := newBusForSource(t, source, parentID, "selector").Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
					t.Fatalf("nested child singleton selection error = %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
				assertStateOnlyAcquisitionLifecycleCount(t, backend, db, childPath, 0)
			})

			t.Run("select preserves direct parent template owner beside nested child", func(t *testing.T) {
				parentID := "parent-template-valid-" + uuid.NewString()
				childID := "child-template-competing-" + uuid.NewString()
				accountID := "parent-template-key-" + uuid.NewString()
				parentEntityID := uuid.NewString()
				parentPath := parentID + "/instance"
				childPath := parentID + "/" + childID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), childPath, "active", accountID)
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, parentEntityID, parentPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeTemplate, childID, runtimecontracts.FlowModeTemplate)
				bus := newBusForSource(t, source, parentID, "selector")
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil {
					t.Fatalf("plan parent template owner: %v", err)
				}
				want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: parentID, FlowInstance: parentPath, EntityID: parentEntityID})
				if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want {
					t.Fatalf("parent template routes = %#v, want %#v and never child %q", plan.DeliveryRoutes, want, childPath)
				}
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish parent template owner: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
				assertStateOnlyAcquisitionLifecycleCount(t, backend, db, childPath, 0)
			})

			t.Run("actual nested child template selects its own state", func(t *testing.T) {
				parentID := "actual-child-parent-" + uuid.NewString()
				childID := "actual-child-" + uuid.NewString()
				accountID := "actual-child-key-" + uuid.NewString()
				entityID := uuid.NewString()
				childPath := parentID + "/" + childID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, childPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeSingleton, childID, runtimecontracts.FlowModeTemplate)
				bus := newBusForSource(t, source, childID, "selector")
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish actual nested child owner: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found {
					t.Fatalf("load actual child event: found=%t err=%v", found, err)
				}
				want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: childID, FlowInstance: childPath, EntityID: entityID})
				if len(persisted.DeliveryRoutes) != 1 || persisted.DeliveryRoutes[0].Target != want {
					t.Fatalf("actual child routes = %#v, want %#v", persisted.DeliveryRoutes, want)
				}
			})

			t.Run("stamped parent target preserves contradiction for application rejection", func(t *testing.T) {
				parentID := "stamped-parent-" + uuid.NewString()
				childID := "stamped-child-" + uuid.NewString()
				accountID := "stamped-child-key-" + uuid.NewString()
				entityID := uuid.NewString()
				childPath := parentID + "/" + childID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, childPath, "active", accountID)
				source := stateOnlyNestedAcquisitionSource(parentID, runtimecontracts.FlowModeSingleton, childID, runtimecontracts.FlowModeTemplate)
				node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, parentID, "selector")
				if err != nil {
					t.Fatal(err)
				}
				handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
				if err != nil {
					t.Fatal(err)
				}
				hostile := events.RouteIdentity{FlowID: parentID, FlowInstance: childPath, EntityID: entityID}.Normalized()
				bus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
					ContractBundle: source,
					RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
						return []runtimebus.DeliveryRouteBlueprint{{
							Recipient: events.MustNodeDeliveryRecipient(node), Target: hostile,
							Handler: handler.ForEvent("test.node_emitted"),
						}}, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				payload, err := json.Marshal(map[string]any{"account_id": accountID})
				if err != nil {
					t.Fatal(err)
				}
				evt := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "test.node_emitted", "", "", payload, 0, runID,
					events.EnvelopeForTargetRoute(events.EventEnvelope{}, hostile), time.Now().UTC(),
				)
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("persist immutable stamped parent/child contradiction: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found {
					t.Fatalf("load stamped parent/child contradiction: found=%t err=%v", found, err)
				}
				want := events.MustExistingEntityTarget(hostile)
				if len(persisted.DeliveryRoutes) != 1 || persisted.DeliveryRoutes[0].Target != want {
					t.Fatalf("stamped contradiction target = %#v, want immutable %#v", persisted.DeliveryRoutes, want)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
				assertStateOnlyAcquisitionLifecycleCount(t, backend, db, childPath, 0)
			})

			t.Run("sibling prefix flow never enters parent cardinality", func(t *testing.T) {
				parentID := "sibling-prefix-" + uuid.NewString()
				siblingID := parentID + "-other"
				accountID := "sibling-prefix-key-" + uuid.NewString()
				siblingPath := siblingID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), siblingPath, "active", accountID)
				evt := newEvent(accountID)
				source := stateOnlySiblingAcquisitionSource(parentID, runtimecontracts.FlowModeTemplate, siblingID, runtimecontracts.FlowModeTemplate)
				if err := newBusForSource(t, source, parentID, "selector").Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
					t.Fatalf("sibling-prefix selection error = %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
			})

			t.Run("arbitrary-depth child selects its own state", func(t *testing.T) {
				parentID := "deep-parent-" + uuid.NewString()
				childID := "deep-child-" + uuid.NewString()
				grandchildID := "deep-grandchild-" + uuid.NewString()
				accountID := "deep-key-" + uuid.NewString()
				entityID := uuid.NewString()
				grandchildPath := parentID + "/" + childID + "/" + grandchildID + "/instance"
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, grandchildPath, "active", accountID)
				source := stateOnlyDeepAcquisitionSource(parentID, childID, grandchildID)

				parentEvent := newEvent(accountID)
				if err := newBusForSource(t, source, parentID, "selector").Publish(ctx, parentEvent); err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
					t.Fatalf("deep descendant selected by parent: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, parentEvent.ID(), 0, 0)

				childEvent := newEvent(accountID)
				if err := newBusForSource(t, source, grandchildID, "selector").Publish(ctx, childEvent); err != nil {
					t.Fatalf("publish actual arbitrary-depth child owner: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, childEvent.ID())
				if err != nil || !found {
					t.Fatalf("load arbitrary-depth child event: found=%t err=%v", found, err)
				}
				want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: grandchildID, FlowInstance: grandchildPath, EntityID: entityID})
				if len(persisted.DeliveryRoutes) != 1 || persisted.DeliveryRoutes[0].Target != want {
					t.Fatalf("arbitrary-depth child routes = %#v, want %#v", persisted.DeliveryRoutes, want)
				}
			})

			t.Run("static flow preserves exact state owner", func(t *testing.T) {
				for _, ownerCase := range []struct {
					name   string
					source func(string) semanticview.Source
				}{
					{name: "static", source: func(flowID string) semanticview.Source {
						return stateOnlyAcquisitionSourceWithMode(flowID, runtimecontracts.FlowModeStatic)
					}},
				} {
					t.Run(ownerCase.name, func(t *testing.T) {
						flowID := ownerCase.name + "-owner-" + uuid.NewString()
						accountID := ownerCase.name + "-key-" + uuid.NewString()
						entityID := uuid.NewString()
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, flowID, "active", accountID)
						evt := newEvent(accountID)
						if err := newBusForSource(t, ownerCase.source(flowID), flowID, "selector").Publish(ctx, evt); err != nil {
							t.Fatalf("publish %s exact owner: %v", ownerCase.name, err)
						}
						persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
						if err != nil || !found {
							t.Fatalf("load %s exact owner: found=%t err=%v", ownerCase.name, found, err)
						}
						want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: flowID, EntityID: entityID})
						if len(persisted.DeliveryRoutes) != 1 || persisted.DeliveryRoutes[0].Target != want {
							t.Fatalf("%s routes = %#v, want %#v", ownerCase.name, persisted.DeliveryRoutes, want)
						}
					})
				}
			})

			t.Run("root flow admits only explicit run identity", func(t *testing.T) {
				flowID := "root-owner-" + uuid.NewString()
				owner, err := runtimepipeline.AdmitWorkflowEntityStateSelectionOwner(stateOnlyRootAcquisitionSource(flowID), flowID)
				if err != nil {
					t.Fatal(err)
				}
				if !owner.Owns(runID) || owner.Owns(flowID+"/child") {
					t.Fatalf("root owner route classification: run=%t child=%t", owner.Owns(runID), owner.Owns(flowID+"/child"))
				}
			})

			t.Run("select or create chooses established state-only owner", func(t *testing.T) {
				flowID := "upsert-existing-" + uuid.NewString()
				accountID := "state-only-existing-" + uuid.NewString()
				entityID := uuid.NewString()
				instancePath := flowID
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, entityID, instancePath, "active", accountID)
				evt := newEvent(accountID)
				bus := newBus(t, flowID, "upserter")
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish select-or-create state-only match: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found {
					t.Fatalf("load state-only selected event: found=%t err=%v", found, err)
				}
				want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: entityID})
				if len(persisted.DeliveryRoutes) != 1 || persisted.DeliveryRoutes[0].Target != want {
					t.Fatalf("persisted target = %#v, want established state-only owner %#v", persisted.DeliveryRoutes, want)
				}
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("duplicate state-only selected publish: %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 1, 1)
			})

			t.Run("select or create converges on exact state-only appearance", func(t *testing.T) {
				flowID := "upsert-race-" + uuid.NewString()
				accountID := "state-only-race-" + uuid.NewString()
				evt := newEvent(accountID)
				bus := newBus(t, flowID, "upserter")
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil || len(plan.DeliveryRoutes) != 1 || !plan.DeliveryRoutes[0].Target.MaterializingEntity() {
					t.Fatalf("initial select-or-create plan = %#v err=%v, want materializing target", plan.DeliveryRoutes, err)
				}
				target := plan.DeliveryRoutes[0].Target.Route()
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, target.EntityID, target.FlowInstance, "active", accountID)
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish after exact state-only appearance: %v", err)
				}
				persisted, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil {
					t.Fatalf("load prepared event: %v", err)
				}
				if !found {
					t.Fatal("published event was not found")
				}
				if len(persisted.DeliveryRoutes) != 1 || !persisted.DeliveryRoutes[0].Target.ExistingEntity() || persisted.DeliveryRoutes[0].Target.Route() != target {
					t.Fatalf("persisted target = %#v, want exact existing state-only target %#v", persisted.DeliveryRoutes, target)
				}
			})

			t.Run("conflicting exact state-only appearance fails before persistence", func(t *testing.T) {
				flowID := "upsert-conflict-" + uuid.NewString()
				accountID := "state-only-conflict-" + uuid.NewString()
				evt := newEvent(accountID)
				bus := newBus(t, flowID, "upserter")
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil || len(plan.DeliveryRoutes) != 1 {
					t.Fatalf("plan deterministic target: routes=%#v err=%v", plan.DeliveryRoutes, err)
				}
				target := plan.DeliveryRoutes[0].Target.Route()
				seedStateOnlyAcquisitionEntity(t, backend, db, runID, target.EntityID, target.FlowInstance, "active", "wrong-key")
				if err := bus.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_or_create_entity_conflict") {
					t.Fatalf("conflicting state-only appearance error = %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
			})

			t.Run("ambiguous state-only matches fail before persistence", func(t *testing.T) {
				flowID := "ambiguous-" + uuid.NewString()
				accountID := "state-only-ambiguous-" + uuid.NewString()
				for index := 0; index < 2; index++ {
					seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), flowID, "active", accountID)
				}
				evt := newEvent(accountID)
				if err := newBus(t, flowID, "selector").Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_entity_ambiguous") {
					t.Fatalf("ambiguous state-only select error = %v", err)
				}
				assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
			})

			t.Run("terminal state and terminated lifecycle are excluded", func(t *testing.T) {
				for _, excluded := range []struct {
					name           string
					state          string
					lifecycleState string
				}{
					{name: "terminal-state", state: "done"},
					{name: "terminated-lifecycle", state: "active", lifecycleState: "terminated"},
				} {
					t.Run(excluded.name, func(t *testing.T) {
						flowID := excluded.name + "-" + uuid.NewString()
						accountID := excluded.name + "-" + uuid.NewString()
						instancePath := flowID
						seedStateOnlyAcquisitionEntity(t, backend, db, runID, uuid.NewString(), instancePath, excluded.state, accountID)
						if excluded.lifecycleState != "" {
							seedStateOnlyAcquisitionLifecycle(t, backend, db, instancePath, excluded.lifecycleState)
						}
						evt := newEvent(accountID)
						if err := newBus(t, flowID, "selector").Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
							t.Fatalf("excluded state-only owner error = %v", err)
						}
						assertStateOnlyAcquisitionMutationCounts(t, backend, db, evt.ID(), 0, 0)
					})
				}
			})
		})
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
			source := stateOnlyNestedAcquisitionSource(parentID, test.parentMode, childID, runtimecontracts.FlowModeSingleton)
			owner, err := runtimepipeline.AdmitWorkflowEntityStateSelectionOwner(source, parentID)
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

func stateOnlyAcquisitionSource(flowID string) semanticview.Source {
	return stateOnlyAcquisitionSourceWithMode(flowID, runtimecontracts.FlowModeSingleton)
}

func stateOnlyAcquisitionSourceWithMode(flowID, mode string) semanticview.Source {
	binding := []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: runtimepaths.Parse("payload.account_id")}}
	flow := runtimecontracts.FlowContractView{
		Path: flowID, Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID, Mode: mode},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: flowID, Mode: mode, InitialState: "active",
			States: []string{"active", "done"}, TerminalStates: []string{"done"},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"selector": {
				ID: "selector", SubscribesTo: []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: binding}}},
			},
			"upserter": {
				ID: "upserter", SubscribesTo: []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: binding}}},
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
	return semanticview.Wrap(bundle)
}

func stateOnlyNestedAcquisitionSource(parentID, parentMode, childID, childMode string) semanticview.Source {
	binding := []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: runtimepaths.Parse("payload.account_id")}}
	flow := func(id, path, mode string) runtimecontracts.FlowContractView {
		return runtimecontracts.FlowContractView{
			Path: path, Paths: runtimecontracts.FlowContractPaths{ID: id, Flow: id, Mode: mode},
			Schema: runtimecontracts.FlowSchemaDocument{
				Name: id, Mode: mode, InitialState: "active",
				States: []string{"active", "done"}, TerminalStates: []string{"done"},
			},
			Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				"selector": {
					ID: "selector", SubscribesTo: []string{"test.node_emitted"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: binding}}},
				},
				"upserter": {
					ID: "upserter", SubscribesTo: []string{"test.node_emitted"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: binding}}},
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
	return semanticview.Wrap(bundle)
}

func stateOnlyAcquisitionFlow(flowID, path, mode string) runtimecontracts.FlowContractView {
	binding := []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: runtimepaths.Parse("payload.account_id")}}
	return runtimecontracts.FlowContractView{
		Path: path, Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID, Mode: mode},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: flowID, Mode: mode, InitialState: "active",
			States: []string{"active", "done"}, TerminalStates: []string{"done"},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"selector": {
				ID: "selector", SubscribesTo: []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: binding}}},
			},
			"upserter": {
				ID: "upserter", SubscribesTo: []string{"test.node_emitted"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"test.node_emitted": {SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: binding}}},
			},
		},
	}
}

func stateOnlySiblingAcquisitionSource(firstID, firstMode, secondID, secondMode string) semanticview.Source {
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
	return semanticview.Wrap(bundle)
}

func stateOnlyDeepAcquisitionSource(parentID, childID, grandchildID string) semanticview.Source {
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
	return semanticview.Wrap(bundle)
}

func stateOnlyRootAcquisitionSource(flowID string) semanticview.Source {
	root := stateOnlyAcquisitionFlow(flowID, flowID, runtimecontracts.FlowModeStatic)
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: flowID, Version: "1",
			FlowInitial:  map[string]string{flowID: "active"},
			FlowStates:   map[string][]string{flowID: {"active", "done"}},
			FlowTerminal: map[string][]string{flowID: {"done"}},
		},
		FlowTree:    flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{flowID: root.Schema},
	}
	return semanticview.Wrap(bundle)
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

func seedStateOnlyAcquisitionLifecycle(t *testing.T, backend string, db *sql.DB, instancePath, status string) {
	t.Helper()
	query := `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at) VALUES (?, 'review', 'static', '{}', ?, ?, ?)`
	now := time.Now().UTC().Truncate(time.Microsecond)
	args := []any{instancePath, status, now, now}
	if backend == "postgres" {
		query = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at) VALUES ($1, 'review', 'static', '{}'::jsonb, $2, $3, $3)`
		args = []any{instancePath, status, now}
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

func assertStateOnlyAcquisitionLifecycleCount(t *testing.T, backend string, db *sql.DB, instancePath string, want int) {
	t.Helper()
	query := "SELECT COUNT(*) FROM flow_instances WHERE instance_id = ?"
	if backend == "postgres" {
		query = "SELECT COUNT(*) FROM flow_instances WHERE instance_id = $1"
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, instancePath).Scan(&count); err != nil {
		t.Fatalf("count lifecycle rows for %s: %v", instancePath, err)
	}
	if count != want {
		t.Fatalf("lifecycle rows for %s = %d, want %d", instancePath, count, want)
	}
}
