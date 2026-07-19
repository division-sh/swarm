package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type fanInBarrierConformanceStore interface {
	runtimebus.EventStore
	runtimebus.FlowInstanceRoutePersistence
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
}

type fanInBarrierRuntime struct {
	bus           *runtimebus.EventBus
	diagnostics   *fanInBarrierDiagnosticBus
	workflowStore *runtimepipeline.WorkflowInstanceStore
}

type fanInBarrierDiagnosticBus struct {
	*runtimebus.EventBus
	mu      sync.Mutex
	entries []runtimepipeline.RuntimeLogEntry
}

func (b *fanInBarrierDiagnosticBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.mu.Lock()
	b.entries = append(b.entries, entry)
	b.mu.Unlock()
	return b.EventBus.LogRuntime(ctx, entry)
}

func (b *fanInBarrierDiagnosticBus) snapshot() []runtimepipeline.RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]runtimepipeline.RuntimeLogEntry(nil), b.entries...)
}

func TestFanInBarrierCanonicalRuntimeCompletesAfterRestartOnBothBackends(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInBarrier)
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)

	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (fanInBarrierConformanceStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, backend.DB
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			seedFanInBarrierRun(t, ctx, backend, db, runID)
			runtime := newFanInBarrierRuntime(t, backend, db, source)
			seedFanInBarrierPortfolioShell(t, ctx, runtime.workflowStore, bundle)
			const periodID = "2026-Q3"
			memberA := uuid.NewString()
			memberB := uuid.NewString()

			publishFanInBarrierEvent(t, ctx, runtime.bus, source, uuid.NewString(), "portfolio", "portfolio.setup", map[string]any{
				"portfolio_id":           "portfolio",
				"expected_operating_ids": []string{memberA, memberB},
				"period_id":              periodID,
			})
			portfolio := loadFanInBarrierPortfolio(t, ctx, runtime.workflowStore)
			if portfolio.CurrentState != "awaiting" {
				dumpFanInBarrierEvents(t, ctx, backend, db)
				t.Logf("fan-in runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				if instances, listErr := runtime.workflowStore.List(ctx); listErr != nil {
					t.Logf("list fan-in workflow instances: %v", listErr)
				} else {
					t.Logf("fan-in workflow instances: %#v", instances)
				}
				t.Fatalf("portfolio state after setup = %q, want awaiting", portfolio.CurrentState)
			}
			activation := loadFanInBarrierActivation(t, portfolio, periodID)
			if activation.Status != joinruntime.StatusOpen || activation.Completed() != 0 || activation.Expected() != 2 {
				t.Fatalf("activation after setup = %#v, want open 0/2", activation)
			}

			publishFanInBarrierEvent(t, ctx, runtime.bus, source, memberB, "ingress", "operating.report.requested", map[string]any{
				"period_id": periodID,
				"revenue":   22,
			})
			portfolio = loadFanInBarrierPortfolio(t, ctx, runtime.workflowStore)
			activation = loadFanInBarrierActivation(t, portfolio, periodID)
			if portfolio.CurrentState != "awaiting" || activation.Status != joinruntime.StatusOpen || activation.Completed() != 1 {
				dumpFanInBarrierEvents(t, ctx, backend, db)
				t.Logf("fan-in runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				t.Fatalf("partial barrier = state:%s activation:%#v, want awaiting open 1/2", portfolio.CurrentState, activation)
			}

			// Reconstruct both EventBus and PipelineCoordinator. The second arrival
			// must consume the persisted activation rather than in-memory state.
			runtime = newFanInBarrierRuntime(t, backend, db, source)
			publishFanInBarrierEvent(t, ctx, runtime.bus, source, memberA, "ingress", "operating.report.requested", map[string]any{
				"period_id": periodID,
				"revenue":   11,
			})
			portfolio = loadFanInBarrierPortfolio(t, ctx, runtime.workflowStore)
			activation = loadFanInBarrierActivation(t, portfolio, periodID)
			if portfolio.CurrentState != "complete" || activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("completed barrier = state:%s activation:%#v", portfolio.CurrentState, activation)
			}
			results := activation.Results()
			if len(results) != 2 || results[0] != float64(11) || results[1] != float64(22) {
				t.Fatalf("barrier results = %#v, want declared membership order [11 22]", results)
			}
		})
	}
}

func newFanInBarrierRuntime(t *testing.T, backend fanInBarrierConformanceStore, db *sql.DB, source semanticview.Source) fanInBarrierRuntime {
	t.Helper()
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if sqliteStore, ok := backend.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	var (
		coordinator *runtimepipeline.PipelineCoordinator
		manager     *runtimemanager.AgentManager
	)
	eventBus, err := newScopedTestEventBus(t, backend, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("fan-in barrier instance manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, route := range mustFanInBarrierRoutes(t, backend) {
		if err := eventBus.RestorePersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route.Identity}); err != nil {
			t.Fatalf("restore fan-in route %s: %v", route.Identity.InstancePath, err)
		}
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
		source: source, workflow: workflow, nodes: nodes,
		guards:  runtimepipeline.NewContractGuardRegistry(source),
		actions: runtimepipeline.NewContractActionRegistry(source),
	}
	diagnosticBus := &fanInBarrierDiagnosticBus{EventBus: eventBus}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(diagnosticBus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
	})
	return fanInBarrierRuntime{bus: eventBus, diagnostics: diagnosticBus, workflowStore: workflowStore}
}

func seedFanInBarrierRun(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, db *sql.DB, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `INSERT INTO runs (run_id, status) VALUES (?, 'running')`
	}
	if _, err := db.ExecContext(ctx, query, runID); err != nil {
		t.Fatalf("seed fan-in barrier run: %v", err)
	}
}

func seedFanInBarrierPortfolioShell(t *testing.T, ctx context.Context, workflowStore *runtimepipeline.WorkflowInstanceStore, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	entityID := runtimepipeline.FlowInstanceEntityID("portfolio")
	if err := workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      "portfolio",
		StorageRef:      "portfolio",
		WorkflowName:    "portfolio",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "collecting",
		Metadata: map[string]any{
			"entity_id":     entityID,
			"portfolio_id":  "portfolio",
			"flow_path":     "portfolio",
			"instance_id":   "portfolio",
			"instance_kind": "singleton",
		},
	}); err != nil {
		t.Fatalf("seed portfolio singleton identity shell: %v", err)
	}
}

func mustFanInBarrierRoutes(t *testing.T, backend fanInBarrierConformanceStore) []runtimebus.FlowInstanceRouteRecord {
	t.Helper()
	routes, err := backend.ListFlowInstanceRoutes(testAuthorActivityContext(context.Background()))
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	out := make([]runtimebus.FlowInstanceRouteRecord, 0, len(routes))
	for _, route := range routes {
		out = append(out, runtimebus.FlowInstanceRouteRecord{Identity: route})
	}
	return out
}

func publishFanInBarrierIngress(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, eventID, localEvent string, payload map[string]any) {
	t.Helper()
	publishFanInBarrierEvent(t, ctx, eventBus, source, eventID, "ingress", localEvent, payload)
}

func publishFanInBarrierEvent(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, eventID, flowID, localEvent string, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", localEvent, err)
	}
	evt := eventtest.RootIngress(
		eventID,
		events.EventType(source.ResolveFlowEventReference(flowID, localEvent)),
		flowID,
		"",
		raw,
		0,
		runtimecorrelation.RunIDFromContext(ctx),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	if err := eventBus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged(%s): %v", localEvent, err)
	}
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 10*time.Second)
	defer cancel()
	if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence(%s): %v", localEvent, err)
	}
}

func loadFanInBarrierPortfolio(t *testing.T, ctx context.Context, workflowStore *runtimepipeline.WorkflowInstanceStore) runtimepipeline.WorkflowInstance {
	t.Helper()
	instance, ok, err := workflowStore.Load(ctx, "portfolio")
	if err != nil || !ok {
		t.Fatalf("load portfolio = found:%v err:%v", ok, err)
	}
	return instance
}

func loadFanInBarrierActivation(t *testing.T, instance runtimepipeline.WorkflowInstance, periodID string) joinruntime.Activation {
	t.Helper()
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		t.Fatalf("load portfolio state carrier: %v", err)
	}
	key := joinruntime.ActivationKey("awaiting", "awaiting", periodID)
	activation, ok, err := joinruntime.Load(carrier.StateBuckets, "portfolio-collector", key)
	if err != nil || !ok {
		t.Fatalf("load portfolio barrier activation %q = found:%v err:%v", key, ok, err)
	}
	return activation
}

func dumpFanInBarrierEvents(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, db *sql.DB) {
	t.Helper()
	query := `SELECT event_id::text, event_name, payload::text FROM events ORDER BY created_at, event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id, event_name, payload FROM events ORDER BY created_at, event_id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Logf("query fan-in events: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, eventName, payload string
		if err := rows.Scan(&eventID, &eventName, &payload); err != nil {
			t.Logf("scan fan-in event: %v", err)
			return
		}
		routes, routeErr := backend.ListEventDeliveryRoutes(ctx, eventID)
		t.Logf("fan-in event %s %s payload=%s routes=%#v err=%v", eventID, eventName, payload, routes, routeErr)
	}
	deliveryQuery := `SELECT event_id::text, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure::text, '') FROM event_deliveries ORDER BY created_at, delivery_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		deliveryQuery = `SELECT event_id, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure, '') FROM event_deliveries ORDER BY created_at, delivery_id`
	}
	deliveryRows, err := db.QueryContext(ctx, deliveryQuery)
	if err != nil {
		t.Logf("query fan-in deliveries: %v", err)
		return
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var eventID, subscriberID, status, reason, failure string
		if err := deliveryRows.Scan(&eventID, &subscriberID, &status, &reason, &failure); err != nil {
			t.Logf("scan fan-in delivery: %v", err)
			return
		}
		t.Logf("fan-in delivery event=%s subscriber=%s status=%s reason=%s failure=%s", eventID, subscriberID, status, reason, failure)
	}
}
