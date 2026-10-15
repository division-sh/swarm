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
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverPublicationOrderingContrastBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, canonicalrouting.ExplicitAgentSelectorDocuments(), nil)
			source := semanticview.Wrap(f.bundle)
			runID := correlation.RunIDFromContext(f.ctx)
			creating := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("2677/"+backend+"/creating"), "work.requested", "2677-ordering", "", []byte(`{}`), 0, runID,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			request.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
			request.TriggerEvent, request.OccurredAt = creating, creating.CreatedAt()
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil || len(plan.Children) != 2 {
				t.Fatalf("prepare real connected receivers: %+v err=%v", plan, err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
				t.Fatalf("construct original receiver tree: %+v err=%v", result, err)
			}
			targets := make(map[string]events.RouteIdentity)
			for _, child := range plan.Children {
				targets[child.Identity.TemplateID] = events.RouteIdentity{FlowID: child.Identity.TemplateID, FlowInstance: child.Identity.InstancePath, EntityID: child.Identity.EntityID}
			}
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
			event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("2677/"+backend+"/publication"), "work.requested", "2677-ordering", "", []byte(`{}`), 0, runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, targets["review"]), eventtest.RootRoutingSource(runID), time.Now().UTC())
			before := snapshot()
			prepared, err := publisher.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: event}})
			if err != nil || len(prepared) != 1 {
				t.Fatalf("prepare mixed publication: count=%d err=%v", len(prepared), err)
			}
			t.Cleanup(func() {
				if err := publisher.ReleaseEnginePublications(context.WithoutCancel(f.ctx), prepared); err != nil {
					t.Error(err)
				}
			})
			command := prepared[0].(bus.EnginePublicationPlan).PublicationCommand()
			if len(command.Activations) != 0 || len(command.Commit.DeliveryRoutes) != 2 || !reflect.DeepEqual(before, snapshot()) {
				t.Fatal("mixed target preparation changed construction or durable state")
			}
			var actor, node events.DeliveryRoute
			for _, route := range command.Commit.DeliveryRoutes {
				if route.Recipient.IsAgent() {
					actor = route
				} else if !route.Recipient.IsNode() || !route.Target.ExistingEntity() || route.Target.Route() != targets["review"] || route.ConnectClaim.Empty() {
					t.Fatalf("unexpected mixed node target: %#v", route)
				} else {
					node = route
				}
			}
			actorTarget := targets["review"]
			actorTarget.EntityID = ""
			if !actor.Target.EntitylessReceiver() || actor.Target.Route() != actorTarget || actor.Initialization.ConstructionTarget().Route() != targets["review"] || actor.Initialization.CreatingEventID() != creating.ID() || actor.AgentIdentity.RunID != runID || actor.ConnectClaim.Empty() {
				t.Fatalf("mixed actor lost its exact construction/claim: %#v", actor)
			}
			// Oppose the native delivery-ID tie-break without changing any route fact.
			command.Commit.DeliveryRoutes = append([]events.DeliveryRoute(nil), command.Commit.DeliveryRoutes...)
			if mustReceiverDeliveryID(t, event.ID(), command.Commit.DeliveryRoutes[0]) < mustReceiverDeliveryID(t, event.ID(), command.Commit.DeliveryRoutes[1]) {
				command.Commit.DeliveryRoutes[0], command.Commit.DeliveryRoutes[1] = command.Commit.DeliveryRoutes[1], command.Commit.DeliveryRoutes[0]
			}
			committed, err := store.CommitPublication(f.ctx, command)
			if err != nil || !committed.Acknowledged || committed.AppendOutcome != bus.EventAppendInserted || len(committed.DeliveryHandoffs) != 2 {
				t.Fatalf("initial mixed publication: %+v err=%v", committed, err)
			}
			if err := publisher.ReleaseEnginePublications(f.ctx, prepared); err != nil {
				t.Fatal(err)
			}
			initialSnapshots, err := store.DeliverySnapshotsForEvent(f.ctx, event.ID())
			if err != nil || len(initialSnapshots) != 2 {
				t.Fatalf("initial native membership: %+v err=%v", initialSnapshots, err)
			}
			firstID, secondID := initialSnapshots[0].DeliveryID, initialSnapshots[1].DeliveryID
			beforeTie := snapshot()
			for _, wrong := range [][3]string{
				{eventtest.UUID("2677-missing-event"), firstID, secondID},
				{event.ID(), firstID, eventtest.UUID("2677-missing-delivery")},
				{event.ID(), firstID, firstID},
			} {
				if err := TieExactPendingPublicationCreatedAtFaultForTest(f.ctx, f.store, wrong[0], wrong[1], wrong[2]); err == nil || !reflect.DeepEqual(beforeTie, snapshot()) {
					t.Fatalf("wrong membership accepted or mutated state: %v err=%v", wrong, err)
				}
			}
			if err := TieExactPendingPublicationCreatedAtFaultForTest(f.ctx, f.store, event.ID(), firstID, secondID); err != nil {
				t.Fatal(err)
			}
			afterTie := snapshot()
			if initialSnapshots[0].CreatedAt.Equal(initialSnapshots[1].CreatedAt) {
				if !reflect.DeepEqual(beforeTie, afterTie) {
					t.Fatal("pre-existing timestamp tie was not a no-op")
				}
			} else {
				assertForkReceiverFaultOnlyColumns(t, beforeTie, afterTie, []forkReceiverFaultEdit{{table: "event_deliveries", key: "delivery_id", value: secondID, columns: []string{"created_at"}}})
			}
			if err := TieExactPendingPublicationCreatedAtFaultForTest(f.ctx, f.store, event.ID(), secondID, firstID); err != nil || !reflect.DeepEqual(afterTie, snapshot()) {
				t.Fatalf("verified existing tie was not a no-op: %v", err)
			}
			stored, found, err := store.LoadPreparedPublishEvent(f.ctx, event.ID())
			if err != nil || !found {
				t.Fatalf("read mixed publication: found=%v err=%v", found, err)
			}
			if err := stored.Validate(); err != nil {
				t.Fatal(err)
			}
			projected, _, err := bus.ResolvePreparedPublishEventTargetProjection(event, command.Commit.DeliveryRoutes)
			if err != nil || !reflect.DeepEqual(projected.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope()) {
				t.Fatalf("initial readback changed exact envelope: err=%v expected=%#v actual=%#v", err, projected.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope())
			}
			if !receiverPublicationRoutesEqual(command.Commit.DeliveryRoutes, stored.DeliveryRoutes) {
				t.Fatalf("initial publication lost route membership/full values: expected=%#v actual=%#v", command.Commit.DeliveryRoutes, stored.DeliveryRoutes)
			}
			snapshots, err := store.DeliverySnapshotsForEvent(f.ctx, event.ID())
			if err != nil || len(snapshots) != 2 {
				t.Fatalf("read exact native snapshots: count=%d err=%v", len(snapshots), err)
			}
			for position, observed := range snapshots {
				id, err := deliverylifecycle.DeliveryID(event.ID(), observed.Route)
				if err != nil || id != observed.DeliveryID || observed.ClaimVersion != 0 || observed.Status != deliverylifecycle.StatusPending {
					t.Fatalf("unexpected native delivery facts: %+v err=%v", observed, err)
				}
				t.Logf("native snapshot position=%d delivery=%s created_at=%s identity=%#v route=%#v", position, observed.DeliveryID, observed.CreatedAt.Format(time.RFC3339Nano), observed.RouteIdentity, observed.Route)
			}
			if !snapshots[0].CreatedAt.Equal(initialSnapshots[0].CreatedAt) || !snapshots[1].CreatedAt.Equal(initialSnapshots[0].CreatedAt) || snapshots[0].DeliveryID >= snapshots[1].DeliveryID {
				t.Fatal("fresh native snapshots did not prove the injected earlier-timestamp tie and ID ordering")
			}
			t.Logf("injected native timestamp tie=true; original ordered-slice oracle=%t; proposed=%#v stored=%#v", reflect.DeepEqual(command.Commit.DeliveryRoutes, stored.DeliveryRoutes), command.Commit.DeliveryRoutes, stored.DeliveryRoutes)
			exact := snapshot()
			for index, routes := range [][]events.DeliveryRoute{
				{stored.DeliveryRoutes[0], stored.DeliveryRoutes[1]},
				{stored.DeliveryRoutes[1], stored.DeliveryRoutes[0]},
			} {
				projected, _, err := bus.ResolvePreparedPublishEventTargetProjection(event, routes)
				if err != nil {
					t.Fatal(err)
				}
				validation := stored
				validation.DeliveryRoutes = routes
				if err := validation.Validate(); err != nil || !reflect.DeepEqual(projected.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope()) {
					t.Fatalf("permutation changed validation/envelope: err=%v expected=%#v actual=%#v", err, stored.Event.Event().NormalizedEnvelope(), projected.NormalizedEnvelope())
				}
				permuted := command
				permuted.Commit.DeliveryRoutes = routes
				duplicate, err := store.CommitPublication(f.ctx, permuted)
				if err != nil || !duplicate.Acknowledged || duplicate.AppendOutcome != bus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 {
					t.Fatalf("permuted exact duplicate: permutation=%d result=%+v err=%v", index, duplicate, err)
				}
				if !reflect.DeepEqual(exact, snapshot()) {
					t.Fatalf("permutation%d changed durable state", index)
				}
				t.Logf("deterministic contrast permutation=%d ordered-slice-oracle=%t full-membership=true validation=PASS duplicate=exact envelope=%#v routes=%#v", index, reflect.DeepEqual(routes, stored.DeliveryRoutes), projected.NormalizedEnvelope(), routes)
			}
			fresh := f.newRuntimeEventBus(t, bus.EventBusOptions{TemplateInstancePlanner: f.manager})
			replayed, err := fresh.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: event}})
			if err != nil || len(replayed) != 1 {
				t.Fatalf("fresh-reader replay: count=%d err=%v", len(replayed), err)
			}
			t.Cleanup(func() {
				if err := fresh.ReleaseEnginePublications(context.WithoutCancel(f.ctx), replayed); err != nil {
					t.Error(err)
				}
			})
			replay := replayed[0].(bus.EnginePublicationPlan).PublicationCommand()
			if len(replay.Activations) != 0 || !reflect.DeepEqual(replay.Commit.DeliveryRoutes, stored.DeliveryRoutes) {
				t.Fatal("fresh-reader replay changed exact persisted route order/values")
			}
			duplicate, err := store.CommitPublication(f.ctx, replay)
			if err != nil || !duplicate.Acknowledged || duplicate.AppendOutcome != bus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 {
				t.Fatalf("fresh-reader duplicate: %+v err=%v", duplicate, err)
			}
			if err := fresh.ReleaseEnginePublications(f.ctx, replayed); err != nil {
				t.Fatal(err)
			}
			if plans, err := fresh.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: eventtest.TargetRouted(event, targets["other"])}}); err == nil {
				_ = fresh.ReleaseEnginePublications(f.ctx, plans)
				t.Fatal("changed selector reused another receiver's stored receipt")
			}
			id := mustReceiverDeliveryID(t, event.ID(), actor)
			original, err := store.Snapshot(f.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			for _, omit := range []bool{true, false} {
				hostile := actor
				if omit {
					hostile.Initialization = events.ReceiverInitialization{}
				} else {
					hostile.Initialization, err = events.AdmitStoredFlowReceiverInitialization(runID, eventtest.UUID("2677-foreign-creating"), actor.Initialization.ConstructionTarget(), actor.Initialization.ReceiverRoute())
					if err != nil {
						t.Fatal(err)
					}
				}
				claim, err := store.ClaimDelivery(f.ctx, original.Authority, stored.Event.Event(), hostile)
				if err != nil || claim.Disposition != deliverylifecycle.ClaimAbsent {
					t.Fatalf("hostile construction acquired execution: omit=%t claim=%+v err=%v", omit, claim, err)
				}
			}
			assertReceiverPublicationOrderingHostility(t, command.Commit.DeliveryRoutes)
			if !reflect.DeepEqual(exact, snapshot()) {
				t.Fatal("readback/replay/changed-selector refusal changed durable evidence")
			}
			t.Run("ordered_slice_false_negative", func(t *testing.T) {
				if reflect.DeepEqual(stored.DeliveryRoutes, command.Commit.DeliveryRoutes) {
					t.Fatal("injected timestamp tie must oppose preparation order")
				}
				if !receiverPublicationRoutesEqual(command.Commit.DeliveryRoutes, stored.DeliveryRoutes) {
					t.Fatal("order-only counterexample changed complete route membership")
				}
			})
			claimed, err := store.ClaimDelivery(f.ctx, original.Authority, stored.Event.Event(), node)
			if err != nil || claimed.Disposition != deliverylifecycle.ClaimAcquired {
				t.Fatalf("real claim precondition: %+v err=%v", claimed, err)
			}
			afterClaim := snapshot()
			if err := TieExactPendingPublicationCreatedAtFaultForTest(f.ctx, f.store, event.ID(), firstID, secondID); err == nil || !reflect.DeepEqual(afterClaim, snapshot()) {
				t.Fatalf("already-claimed membership accepted or mutated state: %v", err)
			}
			t.Log("full native membership/envelope/claim/replay/conservation and fixture refusal controls PASS; timestamp tie explicitly injected")
		})
	}
}

// Preparation order is incidental; every unique route must retain all its values.
func receiverPublicationRoutesEqual(expected, actual []events.DeliveryRoute) bool {
	if len(expected) != len(actual) {
		return false
	}
	members := make(map[events.DeliveryRouteIdentity]events.DeliveryRoute, len(expected))
	for _, route := range expected {
		id, err := route.Identity()
		if err != nil {
			return false
		}
		if _, duplicate := members[id]; duplicate {
			return false
		}
		members[id] = route
	}
	for _, route := range actual {
		id, err := route.Identity()
		if err != nil {
			return false
		}
		want, found := members[id]
		if !found || !reflect.DeepEqual(want, route) {
			return false
		}
		delete(members, id)
	}
	return len(members) == 0
}

func assertReceiverPublicationOrderingHostility(t *testing.T, routes []events.DeliveryRoute) {
	t.Helper()
	for _, hostile := range [][]events.DeliveryRoute{
		{routes[0]}, {routes[0], routes[0]}, {routes[0], routes[1], routes[1]},
	} {
		if receiverPublicationRoutesEqual(routes, hostile) || receiverPublicationRoutesEqual(hostile, routes) || receiverPublicationRoutesEqual(hostile, hostile) && len(hostile) == 2 {
			t.Fatal("diagnostic membership comparison lost cardinality/duplicate detection")
		}
	}
	for _, mutate := range []func(*events.DeliveryRoute){
		func(route *events.DeliveryRoute) { route.ConnectClaim = events.ConnectExecutionClaim{} },
		func(route *events.DeliveryRoute) { route.Initialization = events.ReceiverInitialization{} },
		func(route *events.DeliveryRoute) { route.AgentIdentity.RunID = eventtest.UUID("2677-foreign-run") },
	} {
		hostile := append([]events.DeliveryRoute(nil), routes...)
		for index := range hostile {
			if hostile[index].Recipient.IsAgent() {
				mutate(&hostile[index])
			}
		}
		if receiverPublicationRoutesEqual(routes, hostile) || receiverPublicationRoutesEqual(hostile, routes) {
			t.Fatal("diagnostic membership comparison ignored actor/dependency/connect claim")
		}
	}
}

func TestReceiverPublicationRouteMembershipRequiresFullValues(t *testing.T) {
	target := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: eventtest.UUID("2677-membership-header")})
	routes := make([]events.DeliveryRoute, 0, 2)
	for _, name := range []string{"first", "second"} {
		node, err := identity.AdmitExecutableNodeDeclaration("review", name)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: target})
	}
	changed := append([]events.DeliveryRoute(nil), routes...)
	// Identity normalization omits empty joins. Full-value equality must not.
	changed[0].Context.Joins = []events.JoinAdmissionReceipt{}
	wantID, err := routes[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	changedID, err := changed[0].Identity()
	if err != nil || changedID != wantID || reflect.DeepEqual(routes[0], changed[0]) {
		t.Fatalf("same-identity changed-value precondition: err=%v", err)
	}
	if err := events.ValidateDeliveryRoutes(changed); err != nil {
		t.Fatalf("same-identity control must retain valid route values: %v", err)
	}
	for _, cell := range []struct {
		name             string
		expected, actual []events.DeliveryRoute
		equal            bool
	}{
		{"identical", routes, routes, true},
		{"permutation", routes, []events.DeliveryRoute{routes[1], routes[0]}, true},
		{"missing_actual", routes, routes[:1], false},
		{"missing_expected", routes[:1], routes, false},
		{"extra_actual", routes, []events.DeliveryRoute{routes[0], routes[1], routes[1]}, false},
		{"extra_expected", []events.DeliveryRoute{routes[0], routes[1], routes[1]}, routes, false},
		{"duplicate_actual", routes, []events.DeliveryRoute{routes[0], routes[0]}, false},
		{"duplicate_expected", []events.DeliveryRoute{routes[0], routes[0]}, routes, false},
		{"duplicate_both", []events.DeliveryRoute{routes[0], routes[0]}, []events.DeliveryRoute{routes[0], routes[0]}, false},
		{"invalid_actual", routes, []events.DeliveryRoute{routes[0], {}}, false},
		{"invalid_expected", []events.DeliveryRoute{routes[0], {}}, routes, false},
		{"invalid_both", []events.DeliveryRoute{{}}, []events.DeliveryRoute{{}}, false},
		{"same_identity_changed_actual", routes, changed, false},
		{"same_identity_changed_expected", changed, routes, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if got := receiverPublicationRoutesEqual(cell.expected, cell.actual); got != cell.equal {
				t.Fatalf("complete unique route membership: got=%t want=%t", got, cell.equal)
			}
		})
	}
}
