package storetest

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestBusRoutingStorageObservationsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			reads := []func(context.Context, any) (bool, error){
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusRoutingEventNames(ctx, owner)
					return rows == nil, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusAncestorDeliveryDiagnostics(ctx, owner)
					return rows == nil, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					count, err := CountBusValidationDeadLetterNode(ctx, owner, "fixture-node")
					return count == 0, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					count, err := CountUndeclaredBusChildEvents(ctx, owner)
					return count == 0, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					count, err := CountUndeclaredBusChildEntities(ctx, owner)
					return count == 0, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusRoutingEventDiagnostics(ctx, owner)
					return rows == nil, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusRoutingDeliveryDiagnostics(ctx, owner)
					return rows == nil, err
				},
			}
			for _, read := range reads {
				if zero, err := read(context.Background(), selected); !zero || err != nil {
					t.Fatalf("empty physical observation changed: zero=%t error=%v", zero, err)
				}
				for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
					if zero, err := read(context.Background(), owner); !zero || err == nil {
						t.Fatalf("foreign %T supplied provisional/successful evidence: zero=%t error=%v", owner, zero, err)
					}
				}
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				if zero, err := read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled observation supplied evidence: zero=%t error=%v", zero, err)
				}
			}
			wantPayload := `{"value":1}`
			if backend.name == "postgres" {
				wantPayload = `{"value": 1}`
			}
			assertBusRoutingPhysicalScope(t, selected, wantPayload)
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range reads {
				if zero, err := read(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed observation supplied evidence: zero=%t error=%v", zero, err)
				}
			}
		})
	}
}

func assertBusRoutingPhysicalScope(t *testing.T, selected RunFixtureStore, wantPayload string) {
	t.Helper()
	ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
	at := time.Unix(1700000800, 0).UTC()
	run, otherRun := uuid.NewString(), uuid.NewString()
	RequireRun(t, ctx, selected, RunFixture{RunID: run, Origin: ScenarioSetupOrigin(), StartedAt: at})
	RequireRun(t, ctx, selected, RunFixture{RunID: otherRun, Origin: ScenarioSetupOrigin(), StartedAt: at})
	put := func(run, name string, offset time.Duration, routes []events.DeliveryRoute) {
		t.Helper()
		event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "fixture", "", []byte(`{"value":1}`), 0, run,
			events.EventEnvelope{}, eventtest.RootRoutingSource(run), at.Add(offset))
		if routes == nil {
			CommitSemanticEvent(t, ctx, selected, event)
		} else {
			CommitSemanticEventWithRoutes(t, ctx, selected, event, routes, pipelineobligation.ScopeSubscribed)
		}
	}
	put(run, "fixture.bare", 0, nil)
	put(otherRun, "child/validate.start", time.Second, nil)
	put(run, "child/validation.done", 2*time.Second, nil)
	node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
	if err != nil {
		t.Fatal(err)
	}
	put(run, "validate.requested", 3*time.Second, []events.DeliveryRoute{{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
	}})
	wantNames := []string{"fixture.bare", "child/validate.start", "child/validation.done", "validate.requested"}
	assertBusRoutingChronology(t, ctx, selected, node, wantNames)
	assertBusRoutingMutationScope(t, ctx, selected, run, otherRun, node, at, wantNames, wantPayload)
}

func assertBusRoutingChronology(t *testing.T, ctx context.Context, selected RunFixtureStore, node runtimeidentity.ExecutableNode, wantNames []string) {
	t.Helper()
	if names, err := ReadBusRoutingEventNames(ctx, selected); err != nil || !slices.Equal(names, wantNames) {
		t.Fatalf("global chronology lost foreign run/order: %v/%v", names, err)
	}
	if count, err := CountUndeclaredBusChildEvents(ctx, selected); err != nil || count != 2 {
		t.Fatalf("child event count lost global physical scope: %d/%v", count, err)
	}
	rows, err := ReadBusRoutingDeliveryDiagnostics(ctx, selected)
	if err != nil || len(rows) != 4 {
		t.Fatalf("left-join diagnostic lost bare events or valid delivery: %+v/%v", rows, err)
	}
	for i, row := range rows {
		if row.Name != wantNames[i] || row.HasSubscriber != (i == 3) {
			t.Fatalf("left-join physical NULL/order changed: %+v", row)
		}
	}
	if rows[3].SubscriberID != node.Key() || rows[3].Status != "pending" {
		t.Fatalf("real delivery facts changed: %+v", rows[3])
	}
}

func assertBusRoutingMutationScope(t *testing.T, ctx context.Context, selected RunFixtureStore, run, otherRun string, node runtimeidentity.ExecutableNode, at time.Time, wantNames []string, wantPayload string) {
	t.Helper()
	owner := selected.(interface {
		SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
		TerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error)
	})
	// Native scenario imports are read-only projections, not execution headers.
	setup, err := owner.SetupScenarioEntities(ctx, pipeline.ScenarioSetupRequest{RunID: otherRun, CreatedAt: at,
		Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "child", EntityID: uuid.NewString(), FlowInstance: "child/one", EntityType: "fixture", CurrentState: "pending"}}})
	if err != nil || !setup.Acknowledged || len(setup.Entities) != 1 {
		t.Fatalf("native read-only scenario setup: %+v/%v", setup, err)
	}
	if count, err := CountUndeclaredBusChildEntities(ctx, selected); err != nil || count != 1 {
		t.Fatalf("child entity count was synthetic or gained run/header filtering: %d/%v", count, err)
	}
	if _, err := owner.TerminalizeRun(ctx, run, "fixture_stop"); err != nil {
		t.Fatal(err)
	}
	if count, err := CountBusValidationDeadLetterNode(ctx, selected, node.Key()); err != nil || count != 1 {
		t.Fatalf("dead-letter count lost exact real event/node/status: %d/%v", count, err)
	}
	if count, err := CountBusValidationDeadLetterNode(ctx, selected, "foreign-node"); err != nil || count != 0 {
		t.Fatalf("dead-letter count borrowed another node: %d/%v", count, err)
	}
	if events, err := ReadBusRoutingEventDiagnostics(ctx, selected); err != nil || len(events) < 4 || events[1].Name != wantNames[1] || events[1].Payload != wantPayload {
		t.Fatalf("global event diagnostic lost physical bytes/order: %+v/%v", events, err)
	}
}
