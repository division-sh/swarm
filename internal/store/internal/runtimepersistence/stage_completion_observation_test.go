package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	pipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestStageCompletionPhysicalStateKeepsExactCaseAndScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			requireDefaultSourceArtifactForTest(t, ctx, fixture.store)
			setup := fixture.store.(interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
			})
			runID, entityID, otherRun, otherEntity := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, row := range []struct{ runID, entityID, flow, state string }{{runID, entityID, "discovery", "Ready"}, {otherRun, otherEntity, "other", "ready"}} {
				if _, err := setup.SetupScenarioEntities(ctx, pipeline.ScenarioSetupRequest{RunID: row.runID, CreatedAt: time.Now().UTC(), Entities: []pipeline.ScenarioSetupEntityRequest{{
					Alias: "subject", EntityID: row.entityID, FlowInstance: row.flow, EntityType: "product", CurrentState: row.state,
				}}}); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if state, err := ReadStageCompletionDiscoveryStateForTest(ctx, fixture.store, runID, entityID); err != nil || state != "Ready" {
				t.Fatalf("exact case-sensitive discovery state=%q/%v", state, err)
			}
			if got, err := ReadStageCompletionFieldlessStorageForTest(ctx, fixture.store, runID, "Ready"); err != nil || got != (StageCompletionFieldlessStorage{Fields: 1}) {
				t.Fatalf("exact header/field cardinality=%+v/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical observations escaped original read owner: %+v", counts)
			}
			for _, pair := range [][2]string{{runID, otherEntity}, {otherRun, entityID}, {otherRun, otherEntity}, {uuid.NewString(), entityID}} {
				if got, err := ReadStageCompletionDiscoveryStateForTest(ctx, fixture.store, pair[0], pair[1]); !errors.Is(err, sql.ErrNoRows) || got != "" {
					t.Fatalf("wrong run/entity/flow supplied state=%q/%v", got, err)
				}
			}
			if got, err := ReadStageCompletionFieldlessStorageForTest(ctx, fixture.store, uuid.NewString(), "Ready"); err != nil || got != (StageCompletionFieldlessStorage{}) {
				t.Fatalf("foreign run supplied cardinality=%+v/%v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertStageCompletionReadRefusal(t, cancelled, fixture.store, runID, entityID)
			assertStageCompletionReadRefusal(t, ctx, fixture.store, "invalid", "invalid")
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertStageCompletionReadRefusal(t, ctx, fixture.store, runID, entityID)
		})
	}
}

func TestStageCompletionDeliveryCountKeepsExactNodeStatusAndEventScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(storeTestDurableEventBusStore)
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "flow.finish", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			routes := []events.DeliveryRoute{
				testEntitylessNodeDeliveryRoute(mustPersistenceRootNode("node_1").Key()),
				testEntitylessNodeDeliveryRoute(mustPersistenceRootNode("node_2").Key()),
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatal(err)
			}
			if count, err := CountStageCompletionNodeDeliveriesForTest(ctx, selected, event.RunID(), "flow.finish", routes[0].Recipient.ID(), routes[1].Recipient.ID()); err != nil || count != 0 {
				t.Fatalf("pending nodes supplied delivered evidence=%d/%v", count, err)
			}
			settleFanOutBarrierRouteSuccess(t, ctx, selected, event, routes[0])
			for _, row := range []struct {
				run, name, node, root string
				want                  int
			}{
				{event.RunID(), "flow.finish", routes[0].Recipient.ID(), "", 1},
				{event.RunID(), "flow.finish", routes[0].Recipient.ID(), routes[0].Recipient.ID(), 1},
				{event.RunID(), "flow.finish", routes[0].Recipient.ID(), routes[1].Recipient.ID(), 1},
				{event.RunID(), "flow.finish", routes[1].Recipient.ID(), "", 0},
				{uuid.NewString(), "flow.finish", routes[0].Recipient.ID(), "", 0},
				{event.RunID(), "FLOW.FINISH", routes[0].Recipient.ID(), "", 0},
			} {
				if count, err := CountStageCompletionNodeDeliveriesForTest(ctx, selected, row.run, row.name, row.node, row.root); err != nil || count != row.want {
					t.Fatalf("exact delivered predicate %+v: %d/%v", row, count, err)
				}
			}
			settleFanOutBarrierRouteSuccess(t, ctx, selected, event, routes[1])
			if count, err := CountStageCompletionNodeDeliveriesForTest(ctx, selected, event.RunID(), "flow.finish", routes[0].Recipient.ID(), routes[1].Recipient.ID()); err != nil || count != 2 {
				t.Fatalf("two distinct delivered nodes=%d/%v", count, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertStageCompletionReadRefusal(t, cancelled, selected, event.RunID(), uuid.NewString())
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertStageCompletionReadRefusal(t, ctx, selected, event.RunID(), uuid.NewString())
		})
	}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertStageCompletionReadRefusal(t, context.Background(), selected, uuid.NewString(), uuid.NewString())
	}
}

func assertStageCompletionReadRefusal(t *testing.T, ctx context.Context, selected any, runID, entityID string) {
	t.Helper()
	if count, err := CountStageCompletionNodeDeliveriesForTest(ctx, selected, runID, "flow.finish", "node", ""); err == nil || count != 0 {
		t.Fatalf("refused delivery count supplied evidence=%d/%v", count, err)
	}
	if got, err := ReadStageCompletionDiscoveryStateForTest(ctx, selected, runID, entityID); err == nil || got != "" {
		t.Fatalf("refused state read supplied evidence=%q/%v", got, err)
	}
	if got, err := ReadStageCompletionFieldlessStorageForTest(ctx, selected, runID, "Ready"); err == nil || got != (StageCompletionFieldlessStorage{}) {
		t.Fatalf("refused cardinality read supplied evidence=%+v/%v", got, err)
	}
}
