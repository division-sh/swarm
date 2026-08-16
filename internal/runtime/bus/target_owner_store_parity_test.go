package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type targetOwnerParityStore interface {
	runtimebus.EventStore
	runtimebus.PreparedPublishEventReader
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
}

func TestCrossFlowMaterializingTargetOwnershipRoundTripOnBothBackends(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ParentConnect)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			selected := newTargetOwnerParityStore(t, backend, ctx)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
				repo,
				canonicalrouting.CopyRootOutputSingletonConnect(t),
				runtimecontracts.DefaultPlatformSpecFile(repo),
			)
			if err != nil {
				t.Fatalf("load root-to-singleton target-owner fixture: %v", err)
			}
			source := semanticview.Wrap(bundle)
			runID := uuid.NewString()
			sourceEntityID := uuid.NewString()
			rootOwner := events.RouteIdentity{
				FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: sourceEntityID,
			}.Normalized()
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "root-producer")),
				Target:    events.MustExistingEntityTarget(rootOwner),
			})
			routingSource, err := events.NewRootRoutingSource(sourceEntityID)
			if err != nil {
				t.Fatalf("construct root routing source: %v", err)
			}
			eventID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngressWithRoutingSource(
				eventID, events.EventType("root.ready"), "root-producer", "",
				json.RawMessage(`{"entity_id":"consumer-one"}`), 0, runID, "",
				events.EventEnvelope{}, routingSource, time.Now().UTC(),
			)
			wantRoute := events.RouteIdentity{
				FlowID: "consumer", FlowInstance: "consumer", EntityID: runtimeflowidentity.EntityID("consumer"),
			}.Normalized()
			wantOwner := events.MustMaterializingEntityTarget(wantRoute)
			if rootOwner.EntityID == wantRoute.EntityID {
				t.Fatal("source and receiver owner fixtures must remain distinguishable")
			}

			first, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("create first EventBus: %v", err)
			}
			plan, err := first.CheckPublishRecipientPlan(ctx, evt)
			if err != nil {
				t.Fatalf("preflight first delivery: %v", err)
			}
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 {
				t.Fatalf("preflight failure/routes = %q/%#v, want one materializing receiver", plan.TargetFailure, plan.DeliveryRoutes)
			}
			assertDurableTargetOwnerRoute(t, plan.DeliveryRoutes[0], wantOwner)
			if err := first.Publish(ctx, evt); err != nil {
				t.Fatalf("publish first delivery: %v", err)
			}
			persisted := requireDurableTargetOwnerRoutes(t, ctx, selected, eventID, wantOwner)
			beforeDuplicate := requireDurableTargetOwnerPublicProjection(t, ctx, selected, eventID, wantOwner)

			restarted, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("create restarted EventBus: %v", err)
			}
			deliveries := subscribeInternalDeliveriesForTest(t, restarted, persisted[0].Recipient.ID())
			duplicateCtx := runtimedelivery.WithRoute(
				runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID),
				events.DeliveryRoute{Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: "foreign", FlowInstance: "foreign/changed-topology", EntityID: uuid.NewString(),
				})},
			)
			if err := restarted.Publish(duplicateCtx, evt); err != nil {
				t.Fatalf("publish exact duplicate after restart: %v", err)
			}
			requireNoTargetOwnerDelivery(t, deliveries, "durable duplicate")
			afterDuplicateRoutes := requireDurableTargetOwnerRoutes(t, ctx, selected, eventID, wantOwner)
			if !afterDuplicateRoutes[0].ConnectClaim.Equal(persisted[0].ConnectClaim) {
				t.Fatal("persisted connect claim changed across restart and contradictory current topology")
			}
			afterDuplicate := requireDurableTargetOwnerPublicProjection(t, ctx, selected, eventID, wantOwner)
			if beforeDuplicate != afterDuplicate {
				t.Fatalf("public target-owner projection changed across duplicate:\nbefore=%s\nafter=%s", beforeDuplicate, afterDuplicate)
			}
			conflicting := eventtest.RunCreatingRootIngressWithRoutingSource(
				eventID, events.EventType("root.ready"), "root-producer", "",
				json.RawMessage(`{"entity_id":"different"}`), 0, runID, "",
				events.EventEnvelope{}, routingSource, evt.CreatedAt(),
			)
			if err := restarted.Publish(duplicateCtx, conflicting); !errors.Is(err, events.ErrEventIdentityConflict) {
				t.Fatalf("conflicting duplicate error = %v, want event identity conflict", err)
			}
			requireDurableTargetOwnerRoutes(t, ctx, selected, eventID, wantOwner)

			if _, err := restarted.RecoverPersistedPipeline(duplicateCtx, runtimepipelineobligation.ClaimedWork{
				Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
			}, nil); err != nil {
				t.Fatalf("recover persisted target-owner route: %v", err)
			}
			select {
			case delivery := <-deliveries:
				assertDurableTargetOwnerRoute(t, delivery.HandoffRoute(), wantOwner)
				if got := delivery.Event().TargetRoute().Normalized(); got != wantRoute {
					t.Fatalf("replayed event target = %#v, want %#v", got, wantRoute)
				}
				if err := delivery.Complete(); err != nil {
					t.Fatalf("complete replayed delivery: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for persisted receiver-owned replay")
			}
			if len(persisted) != 1 || persisted[0].Target != wantOwner {
				t.Fatalf("original persisted target changed after replay: %#v", persisted)
			}
			requireDurableTargetOwnerPublicProjection(t, ctx, selected, eventID, wantOwner)
		})
	}
}

func TestPreparedPublishAggregateCorruptionRejectsBothStoreReadbackAndExactDuplicate(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			selected := newTargetOwnerParityStore(t, backend, ctx)
			db := storetest.Database(selected)
			runID := uuid.NewString()
			target := events.RouteIdentity{
				FlowID: "review", FlowInstance: "review/one", EntityID: uuid.NewString(),
			}.Normalized()
			evt := eventtest.RunCreatingRootIngress(
				uuid.NewString(), events.EventType("prepared.aggregate.corruption"), "fixture", "", json.RawMessage(`{}`), 0,
				runID, "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC(),
			)
			routes := []events.DeliveryRoute{
				{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "validator-one")), Target: events.MustExistingEntityTarget(target)},
				{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "validator-two")), Target: events.MustExistingEntityTarget(target)},
			}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, evt, routes, runtimepipelineobligation.ScopeSubscribed)

			originalIdentity, err := routes[1].Identity()
			if err != nil {
				t.Fatalf("derive original route identity: %v", err)
			}
			corruptRoute := events.DeliveryRoute{
				Recipient: routes[0].Recipient,
				Target:    events.MustMaterializingEntityTarget(target),
			}
			corruptIdentity, err := corruptRoute.Identity()
			if err != nil {
				t.Fatalf("derive corrupt route identity: %v", err)
			}
			corruptTarget, err := json.Marshal(corruptRoute.Target)
			if err != nil {
				t.Fatalf("encode corrupt route target: %v", err)
			}
			corruptPreparedDeliveryRow(
				t, ctx, db, backend, evt.ID(),
				events.EncodeDeliveryRouteIdentity(originalIdentity),
				corruptRoute.Recipient.ID(),
				events.EncodeDeliveryRouteIdentity(corruptIdentity),
				string(corruptTarget),
			)
			before := loadPreparedDeliveryCorruption(t, ctx, db, backend, evt.ID(), events.EncodeDeliveryRouteIdentity(corruptIdentity))

			if _, found, err := selected.LoadPreparedPublishEvent(ctx, evt.ID()); found || err == nil || !strings.Contains(err.Error(), "conflicting target ownership kinds") {
				t.Fatalf("corrupt prepared readback = found:%v err:%v, want aggregate contradiction", found, err)
			}
			restarted, err := newScopedTestEventBus(selected)
			if err != nil {
				t.Fatalf("create duplicate EventBus: %v", err)
			}
			duplicateCtx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if err := restarted.Publish(duplicateCtx, evt); err == nil || !strings.Contains(err.Error(), "conflicting target ownership kinds") {
				t.Fatalf("corrupt exact duplicate error = %v, want aggregate contradiction", err)
			}
			after := loadPreparedDeliveryCorruption(t, ctx, db, backend, evt.ID(), events.EncodeDeliveryRouteIdentity(corruptIdentity))
			if before != after {
				t.Fatalf("rejected corrupt duplicate mutated durable row: before=%#v after=%#v", before, after)
			}
		})
	}
}

type preparedDeliveryCorruption struct {
	count          int
	subscriberID   string
	routeIdentity  string
	targetEncoding string
}

func corruptPreparedDeliveryRow(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	backend string,
	eventID string,
	originalIdentity string,
	subscriberID string,
	corruptIdentity string,
	corruptTarget string,
) {
	t.Helper()
	query := `UPDATE event_deliveries SET subscriber_id=?, route_identity=?, delivery_target_route=? WHERE event_id=? AND route_identity=?`
	if backend == "postgres" {
		query = `UPDATE event_deliveries SET subscriber_id=$1, route_identity=$2, delivery_target_route=$3::jsonb WHERE event_id=$4::uuid AND route_identity=$5`
	}
	result, err := db.ExecContext(ctx, query, subscriberID, corruptIdentity, corruptTarget, eventID, originalIdentity)
	if err != nil {
		t.Fatalf("corrupt prepared delivery row: %v", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		t.Fatalf("corrupt prepared delivery rows = %d err=%v, want one", changed, err)
	}
}

func loadPreparedDeliveryCorruption(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	backend string,
	eventID string,
	routeIdentity string,
) preparedDeliveryCorruption {
	t.Helper()
	query := `SELECT COUNT(*), MIN(subscriber_id), MIN(route_identity), MIN(delivery_target_route) FROM event_deliveries WHERE event_id=? AND route_identity=?`
	if backend == "postgres" {
		query = `SELECT COUNT(*), MIN(subscriber_id), MIN(route_identity), MIN(delivery_target_route::text) FROM event_deliveries WHERE event_id=$1::uuid AND route_identity=$2`
	}
	var got preparedDeliveryCorruption
	if err := db.QueryRowContext(ctx, query, eventID, routeIdentity).Scan(
		&got.count, &got.subscriberID, &got.routeIdentity, &got.targetEncoding,
	); err != nil {
		t.Fatalf("load corrupt prepared delivery row: %v", err)
	}
	return got
}

func newTargetOwnerParityStore(t *testing.T, backend string, ctx context.Context) targetOwnerParityStore {
	t.Helper()
	switch backend {
	case "sqlite":
		return storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return storetest.AdmitPostgresRuntimeStore(t, db)
	default:
		t.Fatalf("unsupported backend %q", backend)
		return nil
	}
}

func assertDurableTargetOwnerRoute(t *testing.T, route events.DeliveryRoute, want events.DeliveryTargetOwnership) {
	t.Helper()
	if route.Recipient.LocalID() != "consumer-node" || route.Target != want || route.ConnectClaim.Empty() {
		t.Fatalf("delivery route = %#v, want consumer-node at %s %#v with exact connect claim", route, want.Code(), want.Route())
	}
}

func requireDurableTargetOwnerRoutes(
	t *testing.T,
	ctx context.Context,
	selected targetOwnerParityStore,
	eventID string,
	want events.DeliveryTargetOwnership,
) []events.DeliveryRoute {
	t.Helper()
	routes, err := selected.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		t.Fatalf("list persisted target-owner routes: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("persisted target-owner routes = %#v, want one", routes)
	}
	assertDurableTargetOwnerRoute(t, routes[0], want)
	return routes
}

func requireDurableTargetOwnerPublicProjection(
	t *testing.T,
	ctx context.Context,
	selected targetOwnerParityStore,
	eventID string,
	want events.DeliveryTargetOwnership,
) string {
	t.Helper()
	view, err := selected.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("load public target-owner projection: %v", err)
	}
	if view.NoDelivery != nil || len(view.Deliveries) != 1 {
		t.Fatalf("public target-owner settlement = deliveries:%#v no_delivery:%#v", view.Deliveries, view.NoDelivery)
	}
	if len(view.DeadLetters) != 0 {
		t.Fatalf("public target-owner dead letters = %#v, want none", view.DeadLetters)
	}
	delivery := view.Deliveries[0]
	route := want.Route().Normalized()
	if delivery.SubscriberID != testFlowNode(t, "consumer", "consumer-node").Key() || delivery.Target.Kind != want.Code() ||
		delivery.Target.FlowID != route.FlowID || delivery.Target.FlowInstance != route.FlowInstance || delivery.Target.EntityID != route.EntityID {
		t.Fatalf("public target-owner delivery = %#v, want consumer-node at %s %#v", delivery, want.Code(), route)
	}
	snapshot, err := view.EventSnapshot()
	if err != nil {
		t.Fatalf("load public event snapshot: %v", err)
	}
	if got := snapshot.TargetRoute().Normalized(); got != route {
		t.Fatalf("public event target = %#v, want %#v", got, route)
	}
	projection, err := json.Marshal(struct {
		EventID string                              `json:"event_id"`
		Target  operatorread.OperatorDeliveryTarget `json:"target"`
	}{EventID: view.EventID, Target: delivery.Target})
	if err != nil {
		t.Fatalf("marshal public target-owner projection: %v", err)
	}
	return string(projection)
}

func requireNoTargetOwnerDelivery(t *testing.T, deliveries <-chan *worklifetime.EventDelivery, label string) {
	t.Helper()
	select {
	case delivery := <-deliveries:
		_ = delivery.Complete()
		t.Fatalf("%s unexpectedly redelivered event %s", label, delivery.Event().ID())
	case <-time.After(100 * time.Millisecond):
	}
}
