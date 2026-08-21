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
			newBus := func(t *testing.T, flowID, nodeID string) *runtimebus.EventBus {
				t.Helper()
				source := stateOnlyAcquisitionSource(flowID)
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
	binding := []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: runtimepaths.Parse("payload.account_id")}}
	flow := runtimecontracts.FlowContractView{
		Path: flowID, Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID, Mode: runtimecontracts.FlowModeSingleton},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: flowID, Mode: runtimecontracts.FlowModeSingleton, InitialState: "active",
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
