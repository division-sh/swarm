package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type notifyAllChildrenStore interface {
	runtimebus.EventStore
	ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error)
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error)
}

type notifyAllChildrenRuntime struct {
	bus     *runtimebus.EventBus
	manager *runtimemanager.AgentManager
}

func TestNotifyAllChildrenRuntimeConformance_MixedValidAndStaleRoutesPersistAndReplayOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return &store.PostgresStore{DB: db}, db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, backend.DB
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			fixedEngineNow := time.Date(2026, time.July, 12, 12, 0, 0, 1, time.UTC)
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
			runtime := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow })

			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			assertNotifyAllChildrenRunPersisted(t, ctx, backend, db, runID)
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.account.register.requested", map[string]any{
					"portfolio_id": "portfolio-main",
					"account_id":   accountID,
				})
			}

			descriptors := notifyAllChildrenAccountDescriptors(t, ctx, backend)
			if len(descriptors) != 3 {
				dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
				t.Fatalf("active account descriptors = %#v, want A/B/stale", descriptors)
			}
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				if _, ok := descriptors[accountID]; !ok {
					t.Fatalf("active account descriptor %q missing from %#v", accountID, descriptors)
				}
			}

			orderedMembership := []string{"acct-b", "acct-a", "acct-b"}
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  orderedMembership,
			})
			orderedNotifyID := publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "ordered-duplicate",
			})
			orderedItems := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, orderedNotifyID)
			assertNotifyAllChildrenItemSequence(t, orderedItems, orderedMembership)
			assertNotifyAllChildrenDistinctItemTimestamps(t, ctx, backend, db, runID, orderedNotifyID, len(orderedMembership))
			for index, item := range orderedItems {
				routes, err := backend.ListEventDeliveryRoutes(ctx, item.ID)
				if err != nil {
					t.Fatalf("ordered item %d ListEventDeliveryRoutes(%s): %v", index, item.AccountID, err)
				}
				want := descriptors[item.AccountID]
				if len(routes) != 1 || routes[0].Target.FlowInstance != want.FlowInstance || routes[0].Target.EntityID != want.EntityID {
					t.Fatalf("ordered item %d persisted routes = %#v, want only %s/%s", index, routes, want.FlowInstance, want.EntityID)
				}
			}

			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a", "acct-b", "acct-stale"},
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, "portfolio/portfolio", "account_ids", []any{"acct-a", "acct-b", "acct-stale"})

			stale := descriptors["acct-stale"]
			if err := runtime.manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
				ContractBundle: source,
				Instance: runtimeflowidentity.Stored(
					source,
					notifyallchildren.ChildFlowID,
					stale.FlowInstance,
					stale.InstanceID,
					stale.EntityID,
					"",
				),
				FinalState: "active",
			}); err != nil {
				t.Fatalf("deactivate stale account: %v", err)
			}

			notifyID := publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "refresh",
			})
			itemEvents := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, notifyID)
			if len(itemEvents) != 3 {
				t.Fatalf("fan-out item events = %#v, want exactly A/B/stale", itemEvents)
			}
			items := notifyAllChildrenItemIDsByAccount(t, itemEvents)

			for _, accountID := range []string{"acct-a", "acct-b"} {
				itemID := items[accountID]
				routes, err := backend.ListEventDeliveryRoutes(ctx, itemID)
				if err != nil {
					t.Fatalf("ListEventDeliveryRoutes(%s): %v", accountID, err)
				}
				want := descriptors[accountID]
				if len(routes) != 1 || routes[0].Target.FlowInstance != want.FlowInstance || routes[0].Target.EntityID != want.EntityID {
					t.Fatalf("persisted %s routes = %#v, want only %s/%s", accountID, routes, want.FlowInstance, want.EntityID)
				}
				assertNotifyAllChildrenMetadata(t, ctx, backend, db, want.FlowInstance, "last_command", "refresh")
			}

			staleID := items["acct-stale"]
			if routes, err := backend.ListEventDeliveryRoutes(ctx, staleID); err != nil || len(routes) != 0 {
				t.Fatalf("stale routes = %#v err=%v, want none", routes, err)
			}
			failure := loadNotifyAllChildrenFailure(t, ctx, backend, db, staleID)
			if failure.Class != runtimefailures.ClassTargetUnreachable || !strings.Contains(failure.Detail.Code, "target") {
				t.Fatalf("stale failure = %#v, want platform.target_unreachable with route detail", failure)
			}
			assertNotifyAllChildrenFlowInstanceCount(t, ctx, backend, db, 3)

			// A later supported write changes current membership and state. Replaying
			// the original A item must still use its persisted route and payload.
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a"},
			})
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "newer",
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "newer")
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-b"},
			})

			originalA := items["acct-a"]
			deleteNotifyAllChildrenReceipts(t, ctx, backend, db, originalA)
			eventCountBefore := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID)
			restarted := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow })
			missing, err := backend.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-24*time.Hour), 20)
			if err != nil {
				t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
			}
			var replay events.Event
			for _, record := range missing {
				if record.Event.ID() == originalA {
					replay = record.Event
					break
				}
			}
			if replay.ID() == "" {
				t.Fatalf("original A item %s missing from persisted replay rows %#v", originalA, missing)
			}
			if err := restarted.bus.ReleasePendingPersistedDeliveriesForEvent(ctx, replay); err != nil {
				t.Fatalf("ReleasePendingPersistedDeliveriesForEvent: %v", err)
			}
			waitNotifyAllChildrenBus(t, restarted.bus)
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "refresh")
			if got := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID); got != eventCountBefore {
				t.Fatalf("item event count after replay = %d, want %d; replay must not re-expand current membership", got, eventCountBefore)
			}
			routes, err := backend.ListEventDeliveryRoutes(ctx, originalA)
			if err != nil || len(routes) != 1 || routes[0].Target.FlowInstance != descriptors["acct-a"].FlowInstance {
				t.Fatalf("replayed persisted A route = %#v err=%v", routes, err)
			}
		})
	}
}

func newNotifyAllChildrenRuntime(t *testing.T, backend notifyAllChildrenStore, db *sql.DB, source semanticview.Source, engineNow func() time.Time) notifyAllChildrenRuntime {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	var manager *runtimemanager.AgentManager
	eventBus, err := runtimebus.NewEventBusWithOptions(backend, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("agent manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if routeStore, ok := backend.(runtimebus.FlowInstanceRoutePersistence); ok {
		routes, err := routeStore.ListFlowInstanceRoutes(context.Background())
		if err != nil {
			t.Fatalf("ListFlowInstanceRoutes: %v", err)
		}
		for _, route := range routes {
			if err := eventBus.AddFlowInstanceRouteContext(context.Background(), runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
				t.Fatalf("restore flow-instance route %s: %v", route.InstancePath, err)
			}
		}
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if sqliteStore, ok := backend.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	manager = runtimemanager.NewAgentManagerWithOptions(eventBus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	module := conformanceLoadedWorkflowModule{
		source:   source,
		workflow: workflow,
		nodes:    nodes,
		guards:   runtimepipeline.NewContractGuardRegistry(source),
		actions:  runtimepipeline.NewContractActionRegistry(source),
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(eventBus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
		TestEngineEmitNow: engineNow,
	})
	return notifyAllChildrenRuntime{bus: eventBus, manager: manager}
}

func publishNotifyAllChildrenEvent(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, runID, localEvent string, payload map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", localEvent, err)
	}
	id := uuid.NewString()
	evt := eventtest.RootIngress(
		id,
		events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, localEvent)),
		"",
		"",
		raw,
		0,
		runID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	if err := eventBus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged(%s): %v", localEvent, err)
	}
	waitNotifyAllChildrenBus(t, eventBus)
	return id
}

func waitNotifyAllChildrenBus(t *testing.T, eventBus *runtimebus.EventBus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
}

func assertNotifyAllChildrenRunPersisted(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID string) {
	t.Helper()
	query := `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM runs WHERE run_id = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
		t.Fatalf("query notify-all-children run: %v", err)
	}
	if count != 1 {
		t.Fatalf("persisted notify-all-children run count = %d, want 1 from supported event admission", count)
	}
}

func notifyAllChildrenAccountDescriptors(t *testing.T, ctx context.Context, backend notifyAllChildrenStore) map[string]runtimebus.ActiveFlowInstanceDescriptor {
	t.Helper()
	descriptors, err := backend.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	out := map[string]runtimebus.ActiveFlowInstanceDescriptor{}
	for _, descriptor := range descriptors {
		if descriptor.FlowTemplate != notifyallchildren.ChildFlowID {
			continue
		}
		if accountID := descriptor.AddressFields["entity.account_id"]; accountID != "" {
			out[accountID] = descriptor
		}
	}
	return out
}

type notifyAllChildrenItemEvent struct {
	ID        string
	AccountID string
}

func loadNotifyAllChildrenItemEvents(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, sourceEventID string) []notifyAllChildrenItemEvent {
	t.Helper()
	query := `SELECT event_id::text, payload FROM events WHERE run_id = $1::uuid AND event_name = $2 AND source_event_id = $3::uuid ORDER BY created_at, event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id, payload FROM events WHERE run_id = ? AND event_name = ? AND source_event_id = ? ORDER BY created_at, event_id`
	}
	rows, err := db.QueryContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID)
	if err != nil {
		t.Fatalf("query fan-out item events: %v", err)
	}
	defer rows.Close()
	out := []notifyAllChildrenItemEvent{}
	for rows.Next() {
		var id string
		var raw any
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatalf("scan fan-out item event: %v", err)
		}
		payload := map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &payload); err != nil {
			t.Fatalf("decode fan-out item payload: %v", err)
		}
		accountID, _ := payload["account_id"].(string)
		out = append(out, notifyAllChildrenItemEvent{ID: id, AccountID: accountID})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read fan-out item events: %v", err)
	}
	return out
}

func assertNotifyAllChildrenItemSequence(t *testing.T, items []notifyAllChildrenItemEvent, want []string) {
	t.Helper()
	got := make([]string, 0, len(items))
	for _, item := range items {
		got = append(got, item.AccountID)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("persisted fan-out item sequence = %#v, want %#v with order and duplicates preserved", got, want)
	}
}

func assertNotifyAllChildrenDistinctItemTimestamps(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, sourceEventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(DISTINCT created_at) FROM events WHERE run_id = $1::uuid AND event_name = $2 AND source_event_id = $3::uuid`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(DISTINCT created_at) FROM events WHERE run_id = ? AND event_name = ? AND source_event_id = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID).Scan(&count); err != nil {
		t.Fatalf("count distinct persisted fan-out timestamps: %v", err)
	}
	if count != want {
		t.Fatalf("distinct persisted fan-out timestamps = %d, want %d from equal engine clock ticks", count, want)
	}
}

func notifyAllChildrenItemIDsByAccount(t *testing.T, items []notifyAllChildrenItemEvent) map[string]string {
	t.Helper()
	out := make(map[string]string, len(items))
	for _, item := range items {
		if _, exists := out[item.AccountID]; exists {
			t.Fatalf("fan-out item events contain duplicate account %q where unique membership was required: %#v", item.AccountID, items)
		}
		out[item.AccountID] = item.ID
	}
	return out
}

func countNotifyAllChildrenItemEvents(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = $2`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID, "portfolio/account.notify.requested").Scan(&count); err != nil {
		t.Fatalf("count fan-out item events: %v", err)
	}
	return count
}

func assertNotifyAllChildrenMetadata(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, flowInstance, field string, want any) {
	t.Helper()
	query := `SELECT fields FROM entity_state WHERE flow_instance = $1 ORDER BY updated_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT fields FROM entity_state WHERE flow_instance = ? ORDER BY updated_at DESC LIMIT 1`
	}
	wantJSON, _ := json.Marshal(want)
	deadline := time.Now().Add(5 * time.Second)
	var (
		fields  map[string]any
		gotJSON []byte
		lastErr error
	)
	for time.Now().Before(deadline) {
		var raw any
		if err := db.QueryRowContext(ctx, query, flowInstance).Scan(&raw); err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		fields = map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &fields); err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		gotJSON, _ = json.Marshal(fields[field])
		if string(gotJSON) == string(wantJSON) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
	t.Fatalf("%s.%s = %s, want %s (all fields %#v, last error %v)", flowInstance, field, gotJSON, wantJSON, fields, lastErr)
}

func loadNotifyAllChildrenFailure(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, eventID string) runtimefailures.Envelope {
	t.Helper()
	query := `SELECT failure::text FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT failure FROM dead_letters WHERE original_event_id = ? ORDER BY created_at DESC LIMIT 1`
	}
	var raw any
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&raw); err != nil {
		t.Fatalf("load stale target failure: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(notifyAllChildrenJSONBytes(raw))
	if err != nil {
		t.Fatalf("decode stale target failure: %v", err)
	}
	return failure
}

func assertNotifyAllChildrenFlowInstanceCount(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM flow_instances WHERE flow_template = $1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM flow_instances WHERE flow_template = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, notifyallchildren.ChildFlowID).Scan(&count); err != nil {
		t.Fatalf("count account flow instances: %v", err)
	}
	if count != want {
		t.Fatalf("account flow instances = %d, want %d", count, want)
	}
}

func deleteNotifyAllChildrenReceipts(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, eventID string) {
	t.Helper()
	query := `DELETE FROM event_receipts WHERE event_id = $1::uuid AND ((subscriber_type = 'platform' AND subscriber_id = 'pipeline') OR subscriber_type = 'node')`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `DELETE FROM event_receipts WHERE event_id = ? AND ((subscriber_type = 'platform' AND subscriber_id = 'pipeline') OR subscriber_type = 'node')`
	}
	if _, err := db.ExecContext(ctx, query, eventID); err != nil {
		t.Fatalf("delete replay receipts: %v", err)
	}
	deliveryQuery := `UPDATE event_deliveries SET status = 'pending', reason_code = 'matched_node_subscription', delivered_at = NULL WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id <> '__runtime_replay_scope__'`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		deliveryQuery = `UPDATE event_deliveries SET status = 'pending', reason_code = 'matched_node_subscription', delivered_at = NULL WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id <> '__runtime_replay_scope__'`
	}
	if _, err := db.ExecContext(ctx, deliveryQuery, eventID); err != nil {
		t.Fatalf("reset replay delivery: %v", err)
	}
}

func notifyAllChildrenJSONBytes(raw any) []byte {
	switch typed := raw.(type) {
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(raw))
	}
}

func dumpNotifyAllChildrenRuntimeState(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB) {
	t.Helper()
	queries := []string{
		`SELECT event_name, event_id, payload FROM events ORDER BY created_at, event_id`,
		`SELECT event_id, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(failure, '') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure, ''), COALESCE(delivery_target_route, '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT flow_instance, current_state, fields FROM entity_state ORDER BY flow_instance`,
		`SELECT instance_id, flow_template, status, config FROM flow_instances ORDER BY instance_id`,
		`SELECT original_event_id, failure FROM dead_letters ORDER BY created_at`,
	}
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Logf("notify-all-children diagnostic query failed: %v", err)
			continue
		}
		columns, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for i := range values {
				destinations[i] = &values[i]
			}
			if err := rows.Scan(destinations...); err != nil {
				t.Logf("notify-all-children diagnostic scan failed: %v", err)
				break
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			t.Logf("notify-all-children %v: %v", columns, values)
		}
		_ = rows.Close()
	}
	_ = backend
}
