package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverPublicationExplicitTargetsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, canonicalrouting.ExplicitAgentSelectorDocuments(), nil)
			source := semanticview.Wrap(f.bundle)
			runID := correlation.RunIDFromContext(f.ctx)
			creating := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "work.requested", "explicit-target-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			request.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
			request.TriggerEvent, request.OccurredAt = creating, creating.CreatedAt()
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil || len(plan.Children) != 2 {
				t.Fatalf("prepare both real connected receivers: %+v %v", plan, err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
				t.Fatalf("construct original receiver tree: %+v %v", result, err)
			}
			targets := make(map[string]events.RouteIdentity)
			for _, child := range plan.Children {
				targets[child.Identity.TemplateID] = events.RouteIdentity{FlowID: child.Identity.TemplateID, FlowInstance: child.Identity.InstancePath, EntityID: child.Identity.EntityID}
			}
			targets["."] = events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
			foreignRun := uuid.NewString()
			foreignContext := correlation.WithRunID(f.ctx, foreignRun)
			requireRunFixtureForTest(t, foreignContext, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: foreignRun, Artifact: f.bundle.SourceArtifact, BundleHash: f.bundle.SourceArtifact.BundleHash()})
			foreignCreating := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "work.requested", "foreign-selector-proof", "", []byte(`{}`), 0, foreignRun, events.EventEnvelope{}, eventtest.RootRoutingSource(foreignRun), time.Now().UTC())
			foreignRequest := sqliteFlowActivationRequest(f.bundle, ".", foreignRun, "", foreignRun)
			foreignRequest.Instance = flowidentity.Stored(source, ".", foreignRun, foreignRun, foreignRun, "")
			foreignRequest.TriggerEvent, foreignRequest.OccurredAt = foreignCreating, foreignCreating.CreatedAt()
			foreignPlan, err := f.manager.PrepareFlowInstanceActivation(foreignContext, foreignRequest)
			if err != nil || len(foreignPlan.Children) != 2 {
				t.Fatalf("prepare actual foreign-run evidence: %+v %v", foreignPlan, err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(foreignContext, foreignPlan); err != nil || !result.Acknowledged {
				t.Fatalf("construct actual foreign run: %+v %v", result, err)
			}
			foreignTarget := events.RouteIdentity{FlowID: foreignPlan.Identity.TemplateID, FlowInstance: foreignPlan.Identity.InstancePath, EntityID: foreignPlan.Identity.EntityID}
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := authoractivity.ScopeFromContext(f.ctx)
			catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			publisher := f.newRuntimeEventBus(t, bus.EventBusOptions{TemplateInstancePlanner: f.manager})
			store := f.store.(interface {
				CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
				LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
				DeliverySnapshotsForEvent(context.Context, string) ([]deliverylifecycle.Snapshot, error)
				deliverylifecycle.Store
			})
			snapshot := func() map[string]SelectedForkStorageTableSnapshot {
				t.Helper()
				observed, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				return observed
			}
			otherFlow := targets["review"]
			otherFlow.FlowID = "unknown"
			otherInstance := targets["review"]
			otherInstance.FlowInstance = targets["other"].FlowInstance
			otherEntity := targets["review"]
			otherEntity.EntityID = targets["other"].EntityID
			for _, cell := range []struct {
				name   string
				target events.RouteIdentity
				valid  bool
				runID  string
			}{
				{"selected_review", targets["review"], true, runID},
				{"selected_other", targets["other"], true, runID},
				{"selected_root", targets["."], true, runID},
				{"selected_foreign_root_control", foreignTarget, true, foreignRun},
				{"missing_header_entity", events.RouteIdentity{FlowID: "review", FlowInstance: "review"}, false, runID},
				{"foreign_run", foreignTarget, false, runID},
				{"foreign_flow", otherFlow, false, runID},
				{"foreign_instance", otherInstance, false, runID},
				{"foreign_header_entity", otherEntity, false, runID},
			} {
				t.Run(cell.name, func(t *testing.T) {
					ctx := correlation.WithRunID(f.ctx, cell.runID)
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "work.requested", "explicit-target-proof", "", []byte(`{}`), 0, cell.runID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, cell.target), eventtest.RootRoutingSource(cell.runID), time.Now().UTC())
					before := snapshot()
					prepared, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
					if err != nil {
						if cell.valid {
							t.Fatalf("valid exact target refused: %v", err)
						}
					} else {
						if len(prepared) != 1 {
							t.Fatalf("publication count: %d", len(prepared))
						}
						defer func() {
							if err := publisher.ReleaseEnginePublications(context.WithoutCancel(f.ctx), prepared); err != nil {
								t.Error(err)
							}
						}()
						command := prepared[0].(bus.EnginePublicationPlan).PublicationCommand()
						if len(command.Activations) != 0 {
							t.Fatalf("target selection activated a receiver: %+v", command.Activations)
						}
						if !cell.valid {
							if len(command.Commit.DeliveryRoutes) != 0 {
								t.Fatalf("hostile target acquired consumers: %+v", command.Commit.DeliveryRoutes)
							}
						} else {
							wantRoutes := 1
							if cell.target.FlowID == "review" {
								wantRoutes = 2
							}
							if len(command.Commit.DeliveryRoutes) != wantRoutes {
								t.Fatalf("exact target did not exclude sibling: %+v", command.Commit.DeliveryRoutes)
							}
							var route events.DeliveryRoute
							for _, candidate := range command.Commit.DeliveryRoutes {
								if candidate.Recipient.IsAgent() {
									route = candidate
								} else if !candidate.Target.ExistingEntity() || candidate.Target.Route() != cell.target {
									t.Fatalf("mixed consumer changed node target: %+v", candidate)
								}
							}
							actorTarget := cell.target
							actorTarget.EntityID = ""
							expectedCreatingID := creating.ID()
							if cell.runID == foreignRun {
								expectedCreatingID = foreignCreating.ID()
							}
							if route.Target.Route() != actorTarget || !route.Target.EntitylessReceiver() || route.Initialization.ConstructionTarget().Route() != cell.target || route.Initialization.CreatingEventID() != expectedCreatingID || route.AgentIdentity.RunID != cell.runID {
								t.Fatalf("target/dependency lost exact selected owner: %+v", route)
							}
							if !reflect.DeepEqual(before, snapshot()) {
								t.Fatal("target preparation mutated durable state")
							}
							result, err := store.CommitPublication(ctx, command)
							if err != nil || !result.Acknowledged || result.AppendOutcome != bus.EventAppendInserted || len(result.DeliveryHandoffs) != wantRoutes {
								t.Fatalf("commit selected exact actor: %+v %v", result, err)
							}
							stored, found, err := store.LoadPreparedPublishEvent(ctx, event.ID())
							if err != nil || !found {
								t.Fatalf("read selected publication: found=%v err=%v", found, err)
							}
							projected, _, projectionErr := bus.ResolvePreparedPublishEventTargetProjection(event, command.Commit.DeliveryRoutes)
							if projectionErr != nil {
								t.Fatalf("project selected publication target: %v", projectionErr)
							}
							if validationErr := stored.Validate(); validationErr != nil {
								t.Fatalf("validate stored selected publication: %v", validationErr)
							}
							snapshots, err := store.DeliverySnapshotsForEvent(ctx, event.ID())
							if err != nil || len(snapshots) != wantRoutes {
								t.Fatalf("read selected delivery snapshots: count=%d err=%v", len(snapshots), err)
							}
							for position, snapshot := range snapshots {
								t.Logf("selected publication snapshot: position=%d delivery=%s created_at=%s identity=%#v route=%#v", position, snapshot.DeliveryID, snapshot.CreatedAt.Format(time.RFC3339Nano), snapshot.RouteIdentity, snapshot.Route)
							}
							t.Logf("selected publication routes (including actor, construction dependency and connect claim): expected=%#v actual=%#v", command.Commit.DeliveryRoutes, stored.DeliveryRoutes)
							t.Logf("selected publication normalized envelope: expected=%#v actual=%#v", projected.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope())
							if !receiverPublicationRoutesEqual(command.Commit.DeliveryRoutes, stored.DeliveryRoutes) {
								t.Errorf("readback changed selected delivery routes/dependency/claim: expected=%#v actual=%#v", command.Commit.DeliveryRoutes, stored.DeliveryRoutes)
							}
							if !reflect.DeepEqual(stored.Event.Event().NormalizedEnvelope(), projected.NormalizedEnvelope()) {
								t.Errorf("readback changed selected normalized envelope: expected=%#v actual=%#v", projected.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope())
							}
							if t.Failed() {
								t.FailNow()
							}
							if err := publisher.ReleaseEnginePublications(ctx, prepared); err != nil {
								t.Fatal(err)
							}
							exact := snapshot()
							fresh := f.newRuntimeEventBus(t, bus.EventBusOptions{TemplateInstancePlanner: f.manager})
							replayed, err := fresh.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
							if err != nil || len(replayed) != 1 {
								t.Fatalf("original public selector cannot replay stored actor: %v", err)
							}
							replay := replayed[0].(bus.EnginePublicationPlan).PublicationCommand()
							if len(replay.Activations) != 0 || !receiverPublicationRoutesEqual(command.Commit.DeliveryRoutes, replay.Commit.DeliveryRoutes) {
								t.Fatal("replay reselected a sibling or replaced original dependency/claim")
							}
							duplicate, err := store.CommitPublication(ctx, replay)
							if err != nil || !duplicate.Acknowledged || duplicate.AppendOutcome != bus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 {
								t.Fatalf("exact selector replay created fresh work: %+v %v", duplicate, err)
							}
							if err := fresh.ReleaseEnginePublications(ctx, replayed); err != nil {
								t.Fatal(err)
							}
							changedSelector := targets["review"]
							if changedSelector == cell.target {
								changedSelector = targets["other"]
							}
							changed := eventtest.TargetRouted(event, changedSelector)
							if plans, err := fresh.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: changed}}); err == nil {
								_ = fresh.ReleaseEnginePublications(ctx, plans)
								t.Fatal("changed public selector reused another receiver's durable receipt")
							}
							id := mustReceiverDeliveryID(t, event.ID(), route)
							original, err := store.Snapshot(ctx, id)
							if err != nil {
								t.Fatal(err)
							}
							for _, omission := range []bool{true, false} {
								hostile := route
								if omission {
									hostile.Initialization = events.ReceiverInitialization{}
								} else {
									hostile.Initialization, err = events.AdmitStoredFlowReceiverInitialization(cell.runID, uuid.NewString(), route.Initialization.ConstructionTarget(), route.Initialization.ReceiverRoute())
									if err != nil {
										t.Fatal(err)
									}
								}
								refused, err := store.ClaimDelivery(ctx, original.Authority, stored.Event.Event(), hostile)
								if err != nil || refused.Disposition != deliverylifecycle.ClaimAbsent {
									t.Fatalf("hostile construction receipt acquired execution: omission=%t claim=%+v err=%v", omission, refused, err)
								}
							}
							if !reflect.DeepEqual(exact, snapshot()) {
								t.Fatal("exact selector replay mutated original evidence")
							}
							return
						}
					}
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("target admission mutated construction or execution evidence")
					}
				})
			}
		})
	}
}
