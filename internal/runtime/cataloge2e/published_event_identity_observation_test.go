package cataloge2e

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogPublishedEntityRefreshUsesCanonicalHeaderBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			h := &runtimeHarness{t: t, backend: backend, ctx: ctx, eventEntityIDs: map[string]string{}}
			var setup storetest.RunFixtureStore
			var selected any
			if backend == catalogBackendPostgres {
				h.pg = storetest.StartPostgresRuntimeStore(t)
				setup, selected = h.pg, h.pg
				registerTestAuthorActivityCatalog(t, h.pg, "fixture.identity")
			} else {
				h.sqlite = storetest.StartSQLiteRuntimeStore(t)
				setup, selected = h.sqlite, h.sqlite
				registerTestAuthorActivityCatalog(t, h.sqlite, "fixture.identity")
			}
			storetest.RequireRun(t, ctx, setup, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID})
			entityID := uuid.NewString()
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.identity", "", "",
				[]byte(`{"entity_id":"payload-is-not-header-authority"}`), 0, catalogRuntimeRunID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			storetest.CommitSemanticEvent(t, ctx, selected, event)
			h.refreshPublishedEventEntityID(event.ID())
			if h.eventEntityIDs[event.ID()] != entityID {
				t.Fatalf("publication cache did not consume the canonical header: %+v", h.eventEntityIDs)
			}
			missing := uuid.NewString()
			h.eventEntityIDs[missing] = "retained-prior-evidence"
			h.refreshPublishedEventEntityID(missing)
			h.refreshPublishedEventEntityID("")
			if len(h.eventEntityIDs) != 2 || h.eventEntityIDs[missing] != "retained-prior-evidence" {
				t.Fatalf("missing observation rewrote prior cache evidence: %+v", h.eventEntityIDs)
			}
		})
	}
}
