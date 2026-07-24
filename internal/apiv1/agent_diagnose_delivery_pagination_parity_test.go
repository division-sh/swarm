package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type agentDiagnosePaginationStore interface {
	AgentConversationReadStore
	UpsertAgent(context.Context, runtimemanager.PersistedAgent) error
}

func TestAgentDiagnoseExactDeliveryPaginationParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T, context.Context) (agentDiagnosePaginationStore, *sql.DB, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T, ctx context.Context) (agentDiagnosePaginationStore, *sql.DB, bool) {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				return selected, selected.DB, true
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, _ context.Context) (agentDiagnosePaginationStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return selected, db, false
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			selected, db, sqlite := backend.open(t, ctx)
			registrar, ok := selected.(authorActivityTestCatalogRegistrar)
			if !ok {
				t.Fatalf("%T does not register author activity catalogs", selected)
			}
			registerScopedAPITestCatalog(t, registrar, nil)

			now := time.Now().UTC().Add(-time.Minute)
			runID := uuid.NewString()
			runQuery := `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
			if sqlite {
				runQuery = `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
			}
			if _, err := db.ExecContext(ctx, runQuery, runID, now.Add(-time.Minute)); err != nil {
				t.Fatalf("seed diagnosis run: %v", err)
			}
			agentID := "diagnose-agent"
			if err := selected.UpsertAgent(ctx, runtimemanager.PersistedAgent{
				Config: runtimeactors.AgentConfig{
					ID: agentID, Role: "worker", Type: "managed", Model: "regular", ExecutionMode: "live",
					Memory: agentmemory.PlatformDefault(), Config: json.RawMessage(`{"system_prompt":"diagnose"}`),
				},
				Status: "active", StartedAt: now,
			}); err != nil {
				t.Fatalf("upsert diagnosis agent: %v", err)
			}

			event := eventtest.PersistedProjection(
				uuid.NewString(), "trace.visible", "gateway", "", json.RawMessage(`{"diagnose":true}`), 0,
				runID, "", events.EventEnvelope{}, now,
			)
			routes := []events.DeliveryRoute{
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent),
					SubscriberID:   agentID,
					Target:         events.RouteIdentity{FlowID: "diagnose", FlowInstance: "diagnose/one", EntityID: uuid.NewString()},
				},
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent),
					SubscriberID:   agentID,
					Target:         events.RouteIdentity{FlowID: "diagnose", FlowInstance: "diagnose/two", EntityID: uuid.NewString()},
				},
			}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, routes, runtimepipelineobligation.ScopeSubscribed)
			wantDeliveryIDs := make([]string, 0, len(routes))
			for _, route := range routes {
				obligation, err := runtimedelivery.NewObligation(event.ID(), runID, route)
				if err != nil {
					t.Fatalf("derive diagnosis delivery identity: %v", err)
				}
				wantDeliveryIDs = append(wantDeliveryIDs, obligation.DeliveryID())
			}
			sort.Strings(wantDeliveryIDs)

			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now:                func() time.Time { return now.Add(time.Minute) },
					AgentConversations: selected,
				}),
			})
			first := rpcCall(t, handler, fmt.Sprintf(
				`{"jsonrpc":"2.0","id":"first","method":"agent.diagnose","params":{"agent_id":%q,"queue_limit":1}}`,
				agentID,
			))
			firstDeliveryID, cursor := requireAgentDiagnoseDeliveryPage(t, first, 2)
			if firstDeliveryID != wantDeliveryIDs[0] || cursor == "" {
				t.Fatalf("first diagnosis page = delivery %q cursor %q, want %q plus cursor", firstDeliveryID, cursor, wantDeliveryIDs[0])
			}
			second := rpcCall(t, handler, fmt.Sprintf(
				`{"jsonrpc":"2.0","id":"second","method":"agent.diagnose","params":{"agent_id":%q,"queue_limit":1,"queue_cursor":%q}}`,
				agentID, cursor,
			))
			secondDeliveryID, next := requireAgentDiagnoseDeliveryPage(t, second, 2)
			if secondDeliveryID != wantDeliveryIDs[1] || next != "" {
				t.Fatalf("second diagnosis page = delivery %q cursor %q, want %q and end", secondDeliveryID, next, wantDeliveryIDs[1])
			}
		})
	}
}

func requireAgentDiagnoseDeliveryPage(t *testing.T, response rpcResponse, wantCount int) (string, string) {
	t.Helper()
	if response.Error != nil {
		t.Fatalf("agent.diagnose error = %#v", response.Error)
	}
	queue := asMap(t, asMap(t, response.Result)["queue"])
	if queue["pending_count"] != float64(wantCount) {
		t.Fatalf("agent.diagnose pending_count = %#v, want %d", queue["pending_count"], wantCount)
	}
	deliveries, ok := queue["pending_deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("agent.diagnose pending deliveries = %#v, want one", queue["pending_deliveries"])
	}
	deliveryID, _ := asMap(t, deliveries[0])["delivery_id"].(string)
	cursor, _ := queue["next_cursor"].(string)
	return deliveryID, cursor
}
