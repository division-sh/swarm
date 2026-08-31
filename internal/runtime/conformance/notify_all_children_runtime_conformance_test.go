package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type notifyAllChildrenStore interface {
	conformanceDurableEventBusStore
	runtimepipeline.WorkflowPersistenceOwner
	runtimemanager.ManagerPersistence
	storetest.AgentFixtureStore
	runtimemanager.AgentLifecycleStateReader
	ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error)
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error)
}

type failingNotifyAllChildrenPostgresStore struct {
	*store.PostgresStore
	failNextRouteReplacement atomic.Bool
	transientRouteFailures   atomic.Int32
}

func (s *failingNotifyAllChildrenPostgresStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient postgres exact route replacement failure")
	}
	return s.PostgresStore.ReplaceFlowInstanceRouteRecords(ctx, identity, routes)
}

func (s *failingNotifyAllChildrenPostgresStore) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient postgres exact route replacement failure")
	}
	return s.PostgresStore.ReplaceFlowInstanceRouteTopology(ctx, sets)
}

type failingNotifyAllChildrenSQLiteStore struct {
	*store.SQLiteRuntimeStore
	failNextRouteReplacement atomic.Bool
	transientRouteFailures   atomic.Int32
}

func (s *failingNotifyAllChildrenSQLiteStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient sqlite exact route replacement failure")
	}
	return s.SQLiteRuntimeStore.ReplaceFlowInstanceRouteRecords(ctx, identity, routes)
}

func (s *failingNotifyAllChildrenSQLiteStore) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient sqlite exact route replacement failure")
	}
	return s.SQLiteRuntimeStore.ReplaceFlowInstanceRouteTopology(ctx, sets)
}

type notifyAllChildrenRuntime struct {
	bus              *runtimebus.EventBus
	diagnostics      *fanInBarrierDiagnosticBus
	manager          *runtimemanager.AgentManager
	pipeline         *runtimepipeline.PipelineCoordinator
	workOwner        *worklifetime.RuntimeOccurrence
	selected         notifyAllChildrenStore
	bundleSourceFact runtimecorrelation.BundleSourceFact
	genericSchedules *runtimegenericschedule.Lifecycle
}

type notifyAllChildrenRuntimeOptions struct {
	realMockAgents         bool
	agentGate              *notifyAllChildrenAgentGate
	processTopology        *notifyAllChildrenProcessTopology
	bundleSourceFact       runtimecorrelation.BundleSourceFact
	enableGenericSchedules bool
	maintenanceInterval    time.Duration
}

type notifyAllChildrenGenericScheduleLogger struct {
	t testing.TB
}

func (l notifyAllChildrenGenericScheduleLogger) GenericScheduleFailure(_ context.Context, action, activationID string, err error) {
	l.t.Logf("generic schedule %s for %s failed: %v", action, activationID, err)
}

func (l notifyAllChildrenGenericScheduleLogger) GenericScheduleCatchupWarning(_ context.Context, activationID string, depth int) {
	l.t.Logf("generic schedule %s catchup depth %d", activationID, depth)
}

type notifyAllChildrenProcessTopology struct {
	capability        runtimestartupownership.ProcessCapability
	runtimeInstanceID string
	nextGeneration    uint64
}

type notifyAllChildrenLifecycleOwner struct {
	mu    sync.RWMutex
	grant runtimestartupownership.GenerationGrant
}

func (o *notifyAllChildrenLifecycleOwner) bind(grant runtimestartupownership.GenerationGrant) error {
	if grant == nil {
		return fmt.Errorf("notify-all-children lifecycle generation grant is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.grant != nil {
		return fmt.Errorf("notify-all-children lifecycle generation grant is already bound")
	}
	o.grant = grant
	return nil
}

func (o *notifyAllChildrenLifecycleOwner) current() (runtimestartupownership.GenerationGrant, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.grant == nil {
		return nil, fmt.Errorf("notify-all-children lifecycle generation grant is not bound")
	}
	return o.grant, nil
}

func (o *notifyAllChildrenLifecycleOwner) ProcessExecutionBinding() (runtimemanager.ProcessExecutionBinding, error) {
	grant, err := o.current()
	if err != nil {
		return runtimemanager.ProcessExecutionBinding{}, err
	}
	return grant.ProcessExecutionBinding()
}

func (o *notifyAllChildrenLifecycleOwner) CommitAgentLifecycleTransition(
	ctx context.Context,
	req runtimemanager.AgentLifecycleTransition,
) (runtimemanager.AgentLifecycleTransitionResult, error) {
	grant, err := o.current()
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	return grant.CommitAgentLifecycleTransition(ctx, req)
}

func newNotifyAllChildrenProcessTopology(t testing.TB, ctx context.Context, selected notifyAllChildrenStore) *notifyAllChildrenProcessTopology {
	t.Helper()
	runtimeInstanceID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "notify-all-children-conformance", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire notify-all-children process topology capability: %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-capability.Done():
			return
		default:
		}
		if err := capability.Release(context.Background()); err != nil {
			t.Errorf("release notify-all-children process topology capability: %v", err)
		}
	})
	return &notifyAllChildrenProcessTopology{capability: capability, runtimeInstanceID: runtimeInstanceID}
}

func (p *notifyAllChildrenProcessTopology) install(
	t testing.TB,
	ctx context.Context,
	manager *runtimemanager.AgentManager,
	source semanticview.Source,
	bundleSourceFact runtimecorrelation.BundleSourceFact,
	lifecycle *notifyAllChildrenLifecycleOwner,
) {
	t.Helper()
	if p == nil || p.capability == nil {
		t.Fatal("notify-all-children process topology capability is required")
	}
	bundleHash, bundleSource := bundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := manager.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil {
		t.Fatalf("compile notify-all-children static topology: %v", err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct notify-all-children source set: %v", err)
	}
	current, exists, err := p.capability.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatalf("load notify-all-children source set: %v", err)
	}
	if !exists || current.Revision != plan.Revision {
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = p.capability.RestoreSourceSet(ctx, commit)
		} else {
			_, err = p.capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			t.Fatalf("commit notify-all-children source set: %v", err)
		}
	}
	p.nextGeneration++
	grant, err := p.capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource, RuntimeInstanceID: p.runtimeInstanceID,
		RuntimeGeneration: p.nextGeneration, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue notify-all-children generation grant: %v", err)
	}
	if err := lifecycle.bind(grant); err != nil {
		t.Fatalf("bind notify-all-children lifecycle generation grant: %v", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(plan.Revision, bundleHash, bundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatalf("construct notify-all-children static admission: %v", err)
	}
	if err := manager.InstallStartupTopology(grant, admission, plan); err != nil {
		t.Fatalf("install notify-all-children startup topology: %v", err)
	}
	if err := manager.RebindLifecycleExecutionForStartup(ctx); err != nil {
		t.Fatalf("rebind notify-all-children lifecycle execution: %v", err)
	}
	if err := manager.ReconcileStaticTopologyForStartup(ctx, source); err != nil {
		t.Fatalf("reconcile notify-all-children static topology: %v", err)
	}
}

type notifyAllChildrenAgentGate struct {
	mu      sync.Mutex
	entries map[string]*notifyAllChildrenAgentGateEntry
}

type notifyAllChildrenAgentGateEntry struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type gatedNotifyAllChildrenAgent struct {
	runtimemanager.Agent
	flowInstance string
	gate         *notifyAllChildrenAgentGate
}

func newNotifyAllChildrenAgentGate() *notifyAllChildrenAgentGate {
	return &notifyAllChildrenAgentGate{entries: map[string]*notifyAllChildrenAgentGateEntry{}}
}

func (g *notifyAllChildrenAgentGate) block(flowInstance string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entries[strings.Trim(strings.TrimSpace(flowInstance), "/")] = &notifyAllChildrenAgentGateEntry{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *notifyAllChildrenAgentGate) wrapFactory(delegate runtimemanager.AgentFactory) runtimemanager.AgentFactory {
	return func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		agent, err := delegate(cfg)
		if err != nil {
			return nil, err
		}
		return &gatedNotifyAllChildrenAgent{
			Agent:        agent,
			flowInstance: cfg.Identity.FlowInstance(),
			gate:         g,
		}, nil
	}
}

func (a *gatedNotifyAllChildrenAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.gate.mu.Lock()
	entry := a.gate.entries[a.flowInstance]
	a.gate.mu.Unlock()
	if entry != nil {
		entry.once.Do(func() { close(entry.started) })
		select {
		case <-entry.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return a.Agent.OnEvent(ctx, evt)
}

func (g *notifyAllChildrenAgentGate) waitStarted(t testing.TB, flowInstance string) {
	t.Helper()
	g.mu.Lock()
	entry := g.entries[strings.Trim(strings.TrimSpace(flowInstance), "/")]
	g.mu.Unlock()
	if entry == nil {
		t.Fatalf("no agent gate for %s", flowInstance)
	}
	select {
	case <-entry.started:
	case <-time.After(10 * time.Second):
		t.Fatalf("agent at %s did not reach the execution gate", flowInstance)
	}
}

func (g *notifyAllChildrenAgentGate) release(flowInstance string) {
	g.mu.Lock()
	entry := g.entries[strings.Trim(strings.TrimSpace(flowInstance), "/")]
	g.mu.Unlock()
	if entry != nil {
		select {
		case <-entry.release:
		default:
			close(entry.release)
		}
	}
}

func TestFanOutDeliveryBarrierCompletesThroughRealEventBusAndPublicReadbackOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.DatabaseForTest(selected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{FanOutDeliveryBarrier: true})
			runtime := newNotifyAllChildrenRuntime(
				t,
				selected,
				db,
				source,
				time.Now,
				notifyAllChildrenRuntimeOptions{enableGenericSchedules: true},
			)
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "fan-out-delivery-barrier", runtime.bundleSourceFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}

			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			accountIDs := []string{"acct-c", "acct-a", "acct-b"}
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  accountIDs,
			})
			notifyID := publishNotifyAllChildrenEventAsync(t, ctx, runtime, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "barrier-proof",
			})
			waitNotifyAllChildrenRuntime(t, runtime, runID)

			items := loadNotifyAllChildrenItemEvents(t, ctx, selected, db, runID, notifyID)
			if len(items) != len(accountIDs) {
				t.Fatalf("fan-out item events = %#v, want %d", items, len(accountIDs))
			}
			operatorStore, ok := selected.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			if !ok {
				t.Fatalf("notify-all-children store %T lacks operator event readback", selected)
			}
			for _, item := range items {
				view, err := operatorStore.LoadOperatorEvent(ctx, item.ID)
				if err != nil {
					t.Fatalf("load fan-out item %s: %v", item.ID, err)
				}
				allDelivered := len(view.Deliveries) == 2
				for _, delivery := range view.Deliveries {
					allDelivered = allDelivered && delivery.Status == string(runtimedelivery.StatusDelivered) && delivery.Terminal
				}
				if view.NoDelivery != nil || !allDelivered {
					t.Fatalf("fan-out item %s public settlement = deliveries:%#v no_delivery:%#v", item.ID, view.Deliveries, view.NoDelivery)
				}
			}

			completionID := loadNotifyAllChildrenSingleEventID(t, ctx, selected, db, runID, source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, "portfolio.notify.completed"))
			completion, err := operatorStore.LoadOperatorEvent(ctx, completionID)
			if err != nil {
				t.Fatalf("load fan-out barrier output: %v", err)
			}
			wantPayload := map[string]int{
				"total": len(accountIDs), "succeeded": len(accountIDs), "dead_lettered": 0,
				"no_route": 0, "semantic_rejected": 0, "canceled": 0,
			}
			for field, want := range wantPayload {
				if got, ok := completion.Payload[field].(float64); !ok || int(got) != want {
					t.Fatalf("barrier output %s = %#v, want %d; payload=%#v", field, completion.Payload[field], want, completion.Payload)
				}
			}
			if completion.NoDelivery == nil || len(completion.Deliveries) != 0 {
				t.Fatalf("barrier business output settlement = deliveries:%#v no_delivery:%#v", completion.Deliveries, completion.NoDelivery)
			}
			var status string
			if err := db.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1`, runID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "fired" {
				t.Fatalf("fan-out barrier status = %s, want fired", status)
			}
			summary, err := selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.BarrierArmed != 0 || summary.BarrierPending != 0 || summary.BarrierTerminal != 1 || summary.BlocksCompletion() {
				t.Fatalf("public fan-out barrier summary = %#v err=%v", summary, err)
			}
		})
	}
}

func TestNumericFanOutReporterShapeCompletesAndPreservesSemanticRejectionsOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.DatabaseForTest(selected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.setup(t)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericReporterSink: true})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{
				maintenanceInterval: 10 * time.Millisecond,
			})

			validRunID := uuid.NewString()
			validCtx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), validRunID)
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, validCtx, "numeric-fan-out-reporter", runtime.bundleSourceFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, validCtx, runtime, source, validRunID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-numeric-valid",
			})
			assertNotifyAllChildrenMetadata(t, validCtx, selected, db, "portfolio", "portfolio_id", "portfolio-numeric-valid")
			for batch := 0; batch < 20; batch++ {
				rows := make([]map[string]any, 0, 25)
				for row := 0; row < 25; row++ {
					ordinal := batch*25 + row
					rows = append(rows, map[string]any{
						"account_id": fmt.Sprintf("numeric-%03d", ordinal),
						"eng_roles":  ordinal,
						"gem_score":  float64(ordinal) + 0.25,
					})
				}
				publishNotifyAllChildrenEventAsync(t, validCtx, runtime, source, validRunID, "portfolio.accounts.register.requested", map[string]any{
					"portfolio_id": "portfolio-numeric-valid",
					"account_ids":  rows,
				})
			}
			waitNotifyAllChildrenFanOutCursor(t, runtime, db, validRunID, 500)
			waitNotifyAllChildrenRuntimeWithin(t, runtime, validRunID, 5*time.Minute)

			validSummary, err := selected.FanOutRunSummary(validCtx, validRunID, time.Now().UTC())
			if err != nil {
				t.Fatalf("valid FanOutRunSummary: %v", err)
			}
			if validSummary.Intents != 20 || validSummary.Cardinality != 500 || validSummary.Cursor != 500 || validSummary.Committed != 500 || validSummary.SemanticRejected != 0 || validSummary.SemanticRejectionSample != nil || validSummary.Settled != 500 || validSummary.Unsettled != 0 || validSummary.Owed != 0 {
				t.Fatalf("valid numeric reporter summary = %#v", validSummary)
			}
			registrations := loadNotifyAllChildrenNumericRegistrations(t, validCtx, selected, db, validRunID)
			if len(registrations) != 500 {
				t.Fatalf("valid numeric registration events = %d, want 500", len(registrations))
			}
			first := registrations["numeric-000"]
			if first.ID == "" || first.EngRoles != 0 || first.GemScore != 0.25 {
				t.Fatalf("first numeric registration = %#v", first)
			}
			last := registrations["numeric-499"]
			if last.ID == "" || last.EngRoles != 499 || last.GemScore != 499.25 {
				t.Fatalf("last numeric registration = %#v", last)
			}
			operatorStore, ok := selected.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			if !ok {
				t.Fatalf("numeric fan-out store %T lacks operator event readback", selected)
			}
			view, err := operatorStore.LoadOperatorEvent(validCtx, last.ID)
			if err != nil {
				t.Fatalf("load numeric registration operator event: %v", err)
			}
			if view.Payload["eng_roles"] != float64(499) || view.Payload["gem_score"] != 499.25 || len(view.Deliveries) != 0 || view.NoDelivery == nil {
				t.Fatalf("numeric registration operator readback = payload:%#v deliveries:%#v", view.Payload, view.Deliveries)
			}

			mixedRunID := validRunID
			mixedCtx := validCtx
			publishNotifyAllChildrenEvent(t, mixedCtx, runtime, source, mixedRunID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "portfolio-numeric-valid",
				"account_ids": []map[string]any{
					{"account_id": "mixed-before", "eng_roles": 1, "gem_score": 1.25},
					{"account_id": "mixed-rejected", "eng_roles": 2, "gem_score": "not-a-number"},
					{"account_id": "mixed-after", "eng_roles": 3, "gem_score": 3.25},
				},
			})
			waitNotifyAllChildrenRuntimeWithin(t, runtime, mixedRunID, 5*time.Minute)
			mixedSummary, err := selected.FanOutRunSummary(mixedCtx, mixedRunID, time.Now().UTC())
			if err != nil {
				t.Fatalf("mixed FanOutRunSummary: %v", err)
			}
			if mixedSummary.Intents != 21 || mixedSummary.Cardinality != 503 || mixedSummary.Cursor != 503 || mixedSummary.Committed != 502 || mixedSummary.SemanticRejected != 1 || mixedSummary.Settled != 502 || mixedSummary.Unsettled != 0 || mixedSummary.Owed != 0 {
				t.Fatalf("mixed numeric reporter summary = %#v", mixedSummary)
			}
			sample := mixedSummary.SemanticRejectionSample
			if sample == nil || sample.Ordinal != 1 || sample.Failure.Detail.Code != "emit_payload_contract_violation" {
				t.Fatalf("mixed semantic rejection sample = %#v", sample)
			}
			attrs := sample.Failure.Detail.Attributes
			if attrs["event"] != "portfolio/account.registered" || attrs["kind"] != "schema_mismatch" || attrs["path"] != "$.gem_score" || attrs["constraint"] != "type" || attrs["expected"] != "number" || attrs["actual"] != "string" {
				t.Fatalf("mixed semantic rejection evidence = %#v", attrs)
			}
			mixedRegistrations := loadNotifyAllChildrenNumericRegistrations(t, mixedCtx, selected, db, mixedRunID)
			if len(mixedRegistrations) != 502 || mixedRegistrations["mixed-before"].ID == "" || mixedRegistrations["mixed-after"].ID == "" || mixedRegistrations["mixed-rejected"].ID != "" {
				t.Fatalf("mixed numeric registrations = %#v", mixedRegistrations)
			}
			for _, accountID := range []string{"mixed-before", "mixed-after"} {
				accepted, err := operatorStore.LoadOperatorEvent(mixedCtx, mixedRegistrations[accountID].ID)
				if err != nil || len(accepted.Deliveries) != 0 || accepted.NoDelivery == nil {
					t.Fatalf("mixed accepted registration %s readback = %#v err=%v", accountID, accepted, err)
				}
			}
			assertNotifyAllChildrenFanOutRunStatus(t, mixedCtx, selected, mixedRunID)
		})
	}
}

func TestNumericFanOutInternalDeliverySettlementCompletesOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.DatabaseForTest(selected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.setup(t)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{
				NumericRegistrationRows:   true,
				NumericInternalSettlement: true,
			})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{
				maintenanceInterval: 10 * time.Millisecond,
			})
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "numeric-fan-out-internal-settlement", runtime.bundleSourceFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-numeric-internal",
			})
			rows := make([]map[string]any, 0, 25)
			for ordinal := 0; ordinal < 25; ordinal++ {
				rows = append(rows, map[string]any{
					"account_id": fmt.Sprintf("internal-%02d", ordinal),
					"eng_roles":  ordinal,
					"gem_score":  float64(ordinal) + 0.25,
				})
			}
			publishNotifyAllChildrenEventAsync(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "portfolio-numeric-internal",
				"account_ids":  rows,
			})
			waitNotifyAllChildrenFanOutCursor(t, runtime, db, runID, len(rows))
			waitNotifyAllChildrenRuntimeWithin(t, runtime, runID, time.Minute)

			summary, err := selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Cardinality != len(rows) || summary.Cursor != len(rows) || summary.Committed != len(rows) || summary.Settled != len(rows) || summary.Unsettled != 0 || summary.Owed != 0 {
				t.Fatalf("internal-delivery fan-out summary = %#v err=%v", summary, err)
			}
			registrations := loadNotifyAllChildrenNumericRegistrations(t, ctx, selected, db, runID)
			if len(registrations) != len(rows) {
				t.Fatalf("internal-delivery registrations = %d, want %d", len(registrations), len(rows))
			}
			operatorStore, ok := selected.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			if !ok {
				t.Fatalf("numeric fan-out store %T lacks operator event readback", selected)
			}
			last, err := operatorStore.LoadOperatorEvent(ctx, registrations["internal-24"].ID)
			if err != nil || len(last.Deliveries) != 1 || last.NoDelivery != nil {
				t.Fatalf("internal-delivery operator readback = %#v err=%v", last, err)
			}
		})
	}
}

func TestNumericFanOutTemplateSelectOrCreateMaterializesOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.DatabaseForTest(selected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.setup(t)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{maintenanceInterval: 10 * time.Millisecond})
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "numeric-fan-out-template-materialization", runtime.bundleSourceFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-numeric-template"})
			rows := []map[string]any{
				{"account_id": "template-zero", "eng_roles": 0, "gem_score": 0.25},
				{"account_id": "template-negative", "eng_roles": -7, "gem_score": -7.5},
				{"account_id": "template-positive", "eng_roles": 19, "gem_score": 19.75},
			}
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "portfolio-numeric-template",
				"account_ids":  rows,
			})
			summary, err := selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Cardinality != len(rows) || summary.Committed != len(rows) || summary.Settled != len(rows) || summary.SemanticRejected != 0 {
				t.Fatalf("template numeric fan-out summary = %#v err=%v", summary, err)
			}
			descriptors := notifyAllChildrenAccountDescriptors(t, ctx, selected)
			if len(descriptors) != len(rows) {
				t.Fatalf("template numeric downstream entities = %#v, want %d", descriptors, len(rows))
			}
			for _, row := range rows {
				accountID := row["account_id"].(string)
				descriptor, ok := descriptors[accountID]
				if !ok {
					t.Fatalf("template numeric downstream entity %s was not materialized", accountID)
				}
				assertNotifyAllChildrenMetadata(t, ctx, selected, db, descriptor.FlowInstance, "account_id", accountID)
				assertNotifyAllChildrenMetadata(t, ctx, selected, db, descriptor.FlowInstance, "eng_roles", row["eng_roles"])
				assertNotifyAllChildrenMetadata(t, ctx, selected, db, descriptor.FlowInstance, "gem_score", row["gem_score"])
			}
		})
	}
}

func assertNotifyAllChildrenFanOutRunStatus(t *testing.T, ctx context.Context, selected notifyAllChildrenStore, runID string) {
	t.Helper()
	runs, ok := selected.(apiv1.RunReadStore)
	if !ok {
		t.Fatalf("numeric fan-out store %T lacks the canonical run-read API owner", selected)
	}
	const token = "numeric-fan-out-status-token"
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"),
		AuthTokens:       []string{token},
		Handlers:         apiv1.OperatorRunReadHandlers(apiv1.RunReadHandlerOptions{Runs: runs}),
	})
	if err != nil {
		t.Fatalf("construct run.diagnose API: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write API token: %v", err)
	}
	configFile := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(configFile, []byte("runtime:\n  execution_posture: live\n"), 0o600); err != nil {
		t.Fatalf("write CLI runtime config: %v", err)
	}

	var jsonOut, jsonErr bytes.Buffer
	if code := cliapp.Execute(ctx, []string{
		"run", "status", runID, "--json", "--config", configFile, "--api-server", server.URL, "--api-token-file", tokenFile,
	}, &jsonOut, &jsonErr, nil); code != 0 {
		t.Fatalf("numeric fan-out JSON run status code=%d stderr=%s stdout=%s", code, jsonErr.String(), jsonOut.String())
	}
	result := map[string]any{}
	if err := json.Unmarshal(jsonOut.Bytes(), &result); err != nil {
		t.Fatalf("decode numeric fan-out JSON run status: %v; output=%s", err, jsonOut.String())
	}
	fanOut, _ := result["fan_out"].(map[string]any)
	sample, _ := fanOut["semantic_rejection_sample"].(map[string]any)
	failure, _ := sample["failure"].(map[string]any)
	detail, _ := failure["detail"].(map[string]any)
	attributes, _ := detail["attributes"].(map[string]any)
	if fanOut["committed"] != float64(502) || fanOut["semantic_rejected"] != float64(1) || fanOut["rejected"] != nil || attributes["path"] != "$.gem_score" {
		t.Fatalf("numeric fan-out API/CLI JSON projection = %#v", fanOut)
	}

	var humanOut, humanErr bytes.Buffer
	if code := cliapp.Execute(ctx, []string{
		"run", "status", runID, "--config", configFile, "--api-server", server.URL, "--api-token-file", tokenFile,
	}, &humanOut, &humanErr, nil); code != 0 {
		t.Fatalf("numeric fan-out human run status code=%d stderr=%s stdout=%s", code, humanErr.String(), humanOut.String())
	}
	for _, want := range []string{"502 committed items", "1 semantic rejection", "$.gem_score", "emit_payload_contract_violation"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("numeric fan-out human run status missing %q:\n%s", want, humanOut.String())
		}
	}
}

type notifyAllChildrenNumericRegistration struct {
	ID       string
	EngRoles int
	GemScore float64
}

func loadNotifyAllChildrenNumericRegistrations(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID string) map[string]notifyAllChildrenNumericRegistration {
	t.Helper()
	query := `SELECT CAST(event_id AS TEXT),payload FROM events WHERE run_id=$1::uuid AND event_name=$2 ORDER BY created_at,event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id,payload FROM events WHERE run_id=? AND event_name=? ORDER BY created_at,event_id`
	}
	rows, err := db.QueryContext(ctx, query, runID, "portfolio/account.registered")
	if err != nil {
		t.Fatalf("query numeric registration events: %v", err)
	}
	defer rows.Close()
	out := map[string]notifyAllChildrenNumericRegistration{}
	for rows.Next() {
		var (
			id  string
			raw any
		)
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatalf("scan numeric registration event: %v", err)
		}
		payload := map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &payload); err != nil {
			t.Fatalf("decode numeric registration event: %v", err)
		}
		accountID, _ := payload["account_id"].(string)
		engRoles, _ := payload["eng_roles"].(float64)
		gemScore, _ := payload["gem_score"].(float64)
		if accountID == "" {
			t.Fatalf("numeric registration event %s lacks account identity: %#v", id, payload)
		}
		out[accountID] = notifyAllChildrenNumericRegistration{ID: id, EngRoles: int(engRoles), GemScore: gemScore}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read numeric registration events: %v", err)
	}
	return out
}

func loadNotifyAllChildrenSingleEventID(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, eventName string) string {
	t.Helper()
	query := `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1::uuid AND event_name=$2 ORDER BY created_at,event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id FROM events WHERE run_id=? AND event_name=? ORDER BY created_at,event_id`
	}
	rows, err := db.QueryContext(ctx, query, runID, eventName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := make([]string, 0, 2)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("events %s for run %s = %#v, want exactly one", eventName, runID, ids)
	}
	return ids[0]
}

func TestDynamicFlowSourceRevisionConvergesExactAgentSetAndFencesPredecessorsOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := &failingNotifyAllChildrenPostgresStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db),
				}
				return selected, db,
					func() { selected.failNextRouteReplacement.Store(true) },
					selected.transientRouteFailures.Load
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32) {
				base := storetest.StartSQLiteRuntimeStore(t)
				selected := &failingNotifyAllChildrenSQLiteStore{SQLiteRuntimeStore: base}
				return selected, storetest.DatabaseForTest(base),
					func() { selected.failNextRouteReplacement.Store(true) },
					selected.transientRouteFailures.Load
			},
		},
	} {
		for _, mode := range []struct {
			name     string
			autoEmit bool
		}{
			{name: "no_auto_emit"},
			{name: "emitted_creation", autoEmit: true},
		} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				selected, db, failNextRouteReplacement, transientRouteFailures := tc.setup(t)
				proveDynamicFlowSourceRevisionConvergence(
					t,
					selected,
					db,
					failNextRouteReplacement,
					transientRouteFailures,
					mode.autoEmit,
				)
			})
		}
	}
}

func proveDynamicFlowSourceRevisionConvergence(
	t *testing.T,
	selected notifyAllChildrenStore,
	db *sql.DB,
	failNextRouteReplacement func(),
	transientRouteFailures func() int32,
	autoEmit bool,
) {
	t.Helper()
	runID := uuid.NewString()
	sourceV1 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 1,
		AutoEmitOnCreate:      autoEmit,
	})
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	processTopology := newNotifyAllChildrenProcessTopology(t, ctx, selected)
	scopeV1, ok := semanticview.FlowScopeByID(sourceV1, notifyallchildren.ChildFlowID)
	if !ok || len(scopeV1.Agents) != 2 {
		t.Fatalf("v1 account agent contract = %#v found=%t, want reader/retired", scopeV1.Agents, ok)
	}
	runtimeV1 := newNotifyAllChildrenRuntime(t, selected, db, sourceV1, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: processTopology, bundleSourceFact: authorActivityTestBundleSourceFact,
	})
	if err := runtimeV1.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "dynamic-flow-source-v1", runtimeV1.bundleSourceFact)); err != nil {
		t.Fatalf("run v1 manager: %v", err)
	}

	publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtimeV1, sourceV1, runID, "portfolio.opened", map[string]any{
		"portfolio_id": "portfolio-main",
	})
	publishNotifyAllChildrenEvent(t, ctx, runtimeV1, sourceV1, runID, "portfolio.account.register.requested", map[string]any{
		"portfolio_id": "portfolio-main",
		"account_id":   "acct-revision",
	})
	descriptor, ok := notifyAllChildrenAccountDescriptors(t, ctx, selected)["acct-revision"]
	if !ok {
		t.Fatal("created account descriptor is missing")
	}
	if _, err := runtimeV1.manager.HydrateForStartup(ctx); err != nil {
		t.Fatalf("finalize v1 readiness through startup owner: %v", err)
	}
	initialReadiness := waitNotifyAllChildrenRuntimeReadiness(t, ctx, runtimeV1.pipeline, runID, descriptor.FlowInstance)
	if got := !initialReadiness.CreationEventEmittedAt.IsZero(); got != autoEmit {
		t.Fatalf("initial creation completion = %t, want %t", got, autoEmit)
	}
	readerID := "account-reader"
	retiredID := "account-retired"
	writerID := "account-writer"
	v1Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	if v1Agents[readerID].Config.Role != "reader-v1" || v1Agents[retiredID].Config.Role != "retired" {
		t.Fatalf("v1 dynamic agents = %#v", v1Agents)
	}
	readerGenerationV1 := v1Agents[readerID].LifecycleGeneration

	sourceV2 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 2,
		AutoEmitOnCreate:      autoEmit,
	})
	scopeV2, ok := semanticview.FlowScopeByID(sourceV2, notifyallchildren.ChildFlowID)
	if !ok || len(scopeV2.Agents) != 2 {
		t.Fatalf("v2 account agent contract = %#v found=%t, want reader/writer", scopeV2.Agents, ok)
	}
	if _, found := scopeV2.Agents["retired"]; found {
		t.Fatalf("v2 account contract retained removed agent: %#v", scopeV2.Agents)
	}
	runtimeV2 := newNotifyAllChildrenRuntime(t, selected, db, sourceV2, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: processTopology, bundleSourceFact: authorActivityTestBundleSourceFact,
	})
	ctxV2 := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(context.Background(), runtimeV2.bundleSourceFact), runID)
	if err := runtimeV2.manager.Run(managedConformanceExecutionContextForBundle(t, ctxV2, "dynamic-flow-source-v2", runtimeV2.bundleSourceFact)); err != nil {
		t.Fatalf("run v2 manager: %v", err)
	}
	reconcileCtx := worklifetime.WithOccurrence(ctxV2, runtimeV2.workOwner)
	failNextRouteReplacement()
	sourceRevisionErr := make(chan error, 1)
	start := make(chan struct{})
	var mutations sync.WaitGroup
	mutations.Add(1)
	go func() {
		defer mutations.Done()
		<-start
		sourceRevisionErr <- runtimeV2.pipeline.CommitDynamicFlowRuntimeReadinessReconciliation(
			reconcileCtx, time.Now().UTC(), runtimeV2.manager,
		)
	}()
	close(start)
	mutations.Wait()
	close(sourceRevisionErr)
	if err := <-sourceRevisionErr; err != nil && !strings.Contains(err.Error(), "injected transient") {
		t.Fatalf("reconcile revised source: %v", err)
	}
	revisedReadiness := waitNotifyAllChildrenRuntimeReadiness(t, ctxV2, runtimeV2.pipeline, runID, descriptor.FlowInstance)
	if failures := transientRouteFailures(); failures != 1 {
		t.Fatalf("transient revised-route failures = %d, want exactly one automatic-retry trigger", failures)
	}

	v2Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	if _, found := v2Agents[retiredID]; found {
		t.Fatalf("removed agent %s remains active: %#v", retiredID, v2Agents[retiredID])
	}
	reader := v2Agents[readerID]
	if reader.Config.Role != "reader-v2" || reader.LifecycleGeneration <= readerGenerationV1 {
		t.Fatalf("changed reader = %#v, want reader-v2 generation after %d", reader, readerGenerationV1)
	}
	if writer := v2Agents[writerID]; writer.Config.Role != "writer" || writer.LifecycleGeneration == 0 {
		t.Fatalf("added writer = %#v", writer)
	}
	if _, err := runtimeV2.manager.ResolveAgentConfig(retiredID, descriptor.FlowInstance); err == nil {
		t.Fatalf("removed agent %s remains process-visible", retiredID)
	}
	if cfg, err := runtimeV2.manager.ResolveAgentConfig(readerID, descriptor.FlowInstance); err != nil || cfg.Role != "reader-v2" {
		t.Fatalf("changed reader process config = %#v err=%v", cfg, err)
	}
	if cfg, err := runtimeV2.manager.ResolveAgentConfig(writerID, descriptor.FlowInstance); err != nil || cfg.Role != "writer" {
		t.Fatalf("added writer process config = %#v err=%v", cfg, err)
	}
	if _, exists := reflect.TypeOf(runtimeV2.manager).MethodByName("SpawnAgent"); exists {
		t.Fatal("retired generic agent hire writer remains exported")
	}
	if cfg, err := runtimeV1.manager.ResolveAgentConfig(readerID, descriptor.FlowInstance); err != nil || cfg.Role != "reader-v1" {
		t.Fatalf("stale predecessor process projection changed after replay rejection: %#v err=%v", cfg, err)
	}
	if revisedReadiness.Plan.WorkflowVersion != sourceV2.WorkflowVersion() {
		t.Fatalf("revised runtime readiness = %#v", revisedReadiness)
	}
	if revisedReadiness.CreationEventEmittedAt != initialReadiness.CreationEventEmittedAt {
		t.Fatalf("creation completion changed across source revision: before=%s after=%s", initialReadiness.CreationEventEmittedAt, revisedReadiness.CreationEventEmittedAt)
	}
	terminated, found, err := selected.LoadAgentLifecycleState(ctx, v1Agents[retiredID].Config.Identity)
	if err != nil || !found || terminated.Phase != runtimemanager.AgentLifecycleTerminated {
		t.Fatalf("removed lifecycle state = %#v found=%t err=%v", terminated, found, err)
	}

	runtimeV3 := newNotifyAllChildrenRuntime(t, selected, db, sourceV2, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: processTopology, bundleSourceFact: authorActivityTestBundleSourceFact,
	})
	startupV3, err := runtimeV3.manager.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctxV2, runtimeV3.bundleSourceFact, true)
	if err != nil {
		t.Fatalf("canonicalize v2 restart topology: %v", err)
	}
	if err := runtimeV3.manager.Run(managedConformanceExecutionContextForBundle(t, ctxV2, "dynamic-flow-source-v2-restart", runtimeV3.bundleSourceFact)); err != nil {
		t.Fatalf("run v2 restart manager: %v", err)
	}
	if err := runtimeV3.manager.CompleteDynamicFlowRuntimeStartupTopology(ctxV2, startupV3); err != nil {
		t.Fatalf("reconstruct v2 restart topology: %v", err)
	}
	if _, err := runtimeV3.manager.HydrateForStartup(ctxV2); err != nil {
		t.Fatalf("restart hydration: %v", err)
	}
	for _, agentID := range []string{readerID, writerID} {
		if _, err := runtimeV3.manager.ResolveAgentConfig(agentID, descriptor.FlowInstance); err != nil {
			t.Fatalf("restart omitted exact active agent %s", agentID)
		}
	}
	if _, err := runtimeV3.manager.ResolveAgentConfig(retiredID, descriptor.FlowInstance); err == nil {
		t.Fatalf("restart resurrected removed agent %s", retiredID)
	}

	sourceV3 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 3,
		AutoEmitOnCreate:      autoEmit,
	})
	runtimeV4 := newNotifyAllChildrenRuntime(t, selected, db, sourceV3, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: processTopology, bundleSourceFact: authorActivityTestBundleSourceFact,
	})
	ctxV3 := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(context.Background(), runtimeV4.bundleSourceFact), runID)
	if err := runtimeV4.manager.Run(managedConformanceExecutionContextForBundle(t, ctxV3, "dynamic-flow-source-v3", runtimeV4.bundleSourceFact)); err != nil {
		t.Fatalf("run v3 manager: %v", err)
	}
	failNextRouteReplacement()
	if err := runtimeV4.pipeline.CommitDynamicFlowRuntimeReadinessReconciliation(
		worklifetime.WithOccurrence(ctxV3, runtimeV4.workOwner),
		time.Now().UTC(),
		runtimeV4.manager,
	); err != nil && !strings.Contains(err.Error(), "injected transient") {
		t.Fatalf("reconcile reintroduced source: %v", err)
	}
	waitNotifyAllChildrenRuntimeReadiness(t, ctxV3, runtimeV4.pipeline, runID, descriptor.FlowInstance)
	if failures := transientRouteFailures(); failures != 2 {
		t.Fatalf("transient revised-route failures = %d, want one per revised source", failures)
	}
	v3Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	reintroduced := v3Agents[retiredID]
	if reintroduced.Config.Role != "returned" || reintroduced.LifecycleGeneration <= terminated.Generation {
		t.Fatalf("reintroduced lifecycle = %#v, want successor after %#v", reintroduced, terminated)
	}
	if transitions := countNotifyAllChildrenLifecycleTransitions(
		t,
		ctx,
		selected,
		db,
		retiredID,
		runtimemanager.AgentLifecycleTerminated,
		runtimemanager.AgentLifecycleRegistered,
	); transitions != 1 {
		t.Fatalf("terminated-to-registered transitions = %d, want exactly one", transitions)
	}
	if reader := v3Agents[readerID]; reader.Config.Role != "reader-v3" {
		t.Fatalf("v3 reader = %#v", reader)
	}
	if cfg, err := runtimeV4.manager.ResolveAgentConfig(retiredID, descriptor.FlowInstance); err != nil || cfg.Role != "returned" {
		t.Fatalf("reintroduced process config = %#v err=%v", cfg, err)
	}

	runtimeV5 := newNotifyAllChildrenRuntime(t, selected, db, sourceV3, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: processTopology, bundleSourceFact: authorActivityTestBundleSourceFact,
	})
	startupV5, err := runtimeV5.manager.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctxV3, runtimeV5.bundleSourceFact, true)
	if err != nil {
		t.Fatalf("canonicalize v3 restart topology: %v", err)
	}
	if err := runtimeV5.manager.Run(managedConformanceExecutionContextForBundle(t, ctxV3, "dynamic-flow-source-v3-restart", runtimeV5.bundleSourceFact)); err != nil {
		t.Fatalf("run v3 restart manager: %v", err)
	}
	if err := runtimeV5.manager.CompleteDynamicFlowRuntimeStartupTopology(ctxV3, startupV5); err != nil {
		t.Fatalf("reconstruct v3 restart topology: %v", err)
	}
	if _, err := runtimeV5.manager.HydrateForStartup(ctxV3); err != nil {
		t.Fatalf("reintroduced restart hydration: %v", err)
	}
	for _, agentID := range []string{readerID, writerID, retiredID} {
		if _, err := runtimeV5.manager.ResolveAgentConfig(agentID, descriptor.FlowInstance); err != nil {
			t.Fatalf("reintroduced restart omitted active agent %s", agentID)
		}
	}
}

func countNotifyAllChildrenLifecycleTransitions(
	t *testing.T,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	agentID string,
	previous runtimemanager.AgentLifecyclePhase,
	next runtimemanager.AgentLifecyclePhase,
) int {
	t.Helper()
	query := `
		SELECT COUNT(*)
		FROM agent_lifecycle_transition_facts
		WHERE agent_id = $1 AND previous_phase = $2 AND next_phase = $3
	`
	if _, ok := backend.(*failingNotifyAllChildrenSQLiteStore); ok {
		query = `
			SELECT COUNT(*)
			FROM agent_lifecycle_transition_facts
			WHERE agent_id = ? AND previous_phase = ? AND next_phase = ?
		`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, agentID, string(previous), string(next)).Scan(&count); err != nil {
		t.Fatalf("count lifecycle transitions for %s: %v", agentID, err)
	}
	return count
}

func TestDynamicFlowTerminalizationAndRouteReplacementRollbackTogetherOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB, func())
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func()) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := &failingNotifyAllChildrenPostgresStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db),
				}
				return selected, db, func() {
					if _, err := db.Exec(`
						CREATE FUNCTION fail_notify_route_retirement() RETURNS trigger AS $$
						BEGIN
							RAISE EXCEPTION 'injected postgres exact route replacement failure';
						END;
						$$ LANGUAGE plpgsql;
						CREATE TRIGGER fail_notify_route_retirement
						BEFORE UPDATE OF status ON routing_rules
						FOR EACH ROW WHEN (NEW.status = 'inactive')
						EXECUTE FUNCTION fail_notify_route_retirement();
					`); err != nil {
						t.Fatalf("install postgres route-retirement failure: %v", err)
					}
				}
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func()) {
				base := storetest.StartSQLiteRuntimeStore(t)
				selected := &failingNotifyAllChildrenSQLiteStore{SQLiteRuntimeStore: base}
				db := storetest.DatabaseForTest(base)
				return selected, db, func() {
					if _, err := db.Exec(`
						CREATE TRIGGER fail_notify_route_retirement
						BEFORE UPDATE OF status ON routing_rules
						FOR EACH ROW WHEN NEW.status = 'inactive'
						BEGIN
							SELECT RAISE(ABORT, 'injected sqlite exact route replacement failure');
						END
					`); err != nil {
						t.Fatalf("install sqlite route-retirement failure: %v", err)
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, failReplacement := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now)

			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.account.register.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_id":   "acct-rollback",
			})
			descriptor, ok := notifyAllChildrenAccountDescriptors(t, ctx, selected)["acct-rollback"]
			if !ok {
				t.Fatal("created account descriptor is missing")
			}
			route := runtimeflowidentity.StoredRoute(
				notifyallchildren.ChildFlowID,
				descriptor.InstanceID,
				descriptor.FlowInstance,
			)
			before, err := selected.ListFlowInstanceRouteRecords(ctx, route)
			if err != nil || len(before) == 0 {
				t.Fatalf("load prior exact route set: routes=%#v err=%v", before, err)
			}
			if !runtime.bus.HasFlowInstanceRoute(route) {
				t.Fatal("created process route is not active before terminalization")
			}

			failReplacement()
			err = runtime.manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
				ContractBundle: source,
				Instance: runtimeflowidentity.Stored(
					source,
					notifyallchildren.ChildFlowID,
					descriptor.FlowInstance,
					descriptor.InstanceID,
					descriptor.EntityID,
					"",
				),
				FinalState: "active",
			})
			if err == nil || !strings.Contains(err.Error(), "exact route replacement failure") {
				t.Fatalf("DeactivateFlowInstanceModel error = %v, want injected route replacement failure", err)
			}

			if got := loadNotifyAllChildrenFlowInstanceStatus(t, ctx, selected, db, descriptor.FlowInstance); got != "active" {
				t.Fatalf("flow instance status after replacement rollback = %q, want active", got)
			}
			after, err := selected.ListFlowInstanceRouteRecords(ctx, route)
			if err != nil || !slices.EqualFunc(after, before, func(a, b runtimebus.FlowInstanceRouteRecord) bool {
				return a == b
			}) {
				t.Fatalf("exact route set after replacement rollback: before=%#v after=%#v err=%v", before, after, err)
			}
			if !runtime.bus.HasFlowInstanceRoute(route) {
				t.Fatal("process route retired despite selected mutation rollback")
			}
		})
	}
}

func TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity(t *testing.T) {
	for _, nameCase := range []struct {
		name    string
		agentID string
		opts    notifyallchildren.Options
	}{
		{name: "omitted_id", agentID: "account-worker"},
		{name: "literal_override", agentID: "account-handler", opts: notifyallchildren.Options{ExplicitAgentName: true}},
	} {
		for _, tc := range []struct {
			name  string
			setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
		}{
			{
				name: "postgres",
				setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
					_, db, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					return storetest.AdmitPostgresRuntimeStore(t, db), db
				},
			},
			{
				name: "sqlite",
				setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
					backend := storetest.StartSQLiteRuntimeStore(t)
					return backend, storetest.DatabaseForTest(backend)
				},
			},
		} {
			for _, cardinality := range []int{1, 2, 3} {
				t.Run(fmt.Sprintf("%s/%s/n=%d", nameCase.name, tc.name, cardinality), func(t *testing.T) {
					ctx := testAuthorActivityContext(context.Background())
					backend, db := tc.setup(t)
					runID := uuid.NewString()
					ctx = runtimecorrelation.WithRunID(ctx, runID)
					source := notifyallchildren.LoadSource(t, nameCase.opts)
					gate := newNotifyAllChildrenAgentGate()
					runtime := newNotifyAllChildrenRuntime(
						t,
						backend,
						db,
						source,
						time.Now,
						notifyAllChildrenRuntimeOptions{realMockAgents: true, agentGate: gate},
					)
					t.Cleanup(func() {
						if t.Failed() {
							t.Logf("notify-all-children runtime diagnostics: %#v", runtime.diagnostics.snapshot())
						}
					})
					if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "notify-all-children-fixed-slug", runtime.bundleSourceFact)); err != nil {
						t.Fatalf("run manager: %v", err)
					}

					publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
						"portfolio_id": "portfolio-main",
					})
					accountIDs := make([]string, 0, cardinality)
					for i := 0; i < cardinality; i++ {
						accountIDs = append(accountIDs, fmt.Sprintf("acct-%d", i+1))
					}
					publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
						"portfolio_id": "portfolio-main",
						"account_ids":  accountIDs,
					})

					descriptors := notifyAllChildrenAccountDescriptors(t, ctx, backend)
					if len(descriptors) != cardinality {
						t.Fatalf("active account descriptors = %#v, want %d", descriptors, cardinality)
					}
					assertNotifyAllChildrenConcreteAgentSet(t, ctx, backend, descriptors, nameCase.agentID)

					blocked := descriptors[accountIDs[len(accountIDs)-1]].FlowInstance
					gate.block(blocked)
					publishNotifyAllChildrenEventAsync(t, ctx, runtime, source, runID, "portfolio.notify.requested", map[string]any{
						"portfolio_id": "portfolio-main",
						"command":      "notify-every-account",
					})
					gate.waitStarted(t, blocked)

					for _, accountID := range accountIDs[:len(accountIDs)-1] {
						waitNotifyAllChildrenEntityState(t, ctx, backend, db, descriptors[accountID].FlowInstance, "completed")
					}
					if got := loadNotifyAllChildrenEntityState(t, ctx, backend, db, blocked); got != "active" {
						t.Fatalf("blocked sibling status = %q, want active while another instance is terminal", got)
					}
					if _, err := runtime.manager.ResolveAgentConfig(nameCase.agentID, blocked); err != nil {
						t.Fatalf("blocked sibling agent disappeared after another instance terminated: %v", err)
					}
					waitNotifyAllChildrenAgentDeliveryStatus(t, ctx, backend, db, runID, nameCase.agentID, blocked, "in_progress")

					gate.release(blocked)
					waitNotifyAllChildrenRuntime(t, runtime, runID)
					for _, accountID := range accountIDs {
						instancePath := descriptors[accountID].FlowInstance
						waitNotifyAllChildrenEntityState(t, ctx, backend, db, instancePath, "completed")
						waitNotifyAllChildrenAgentDeliveryStatus(t, ctx, backend, db, runID, nameCase.agentID, instancePath, "delivered")
						assertNotifyAllChildrenAgentEmissionSettledToSameInstanceNode(t, ctx, db, tc.name, runID, nameCase.agentID, instancePath)
						waitNotifyAllChildrenAgentAbsent(t, runtime.manager, nameCase.agentID, instancePath)
					}
					assertNotifyAllChildrenCompletedTurns(t, ctx, backend, db, runID, nameCase.agentID, cardinality)
					if active, err := backend.LoadAgents(ctx); err != nil {
						t.Fatalf("LoadAgents after independent terminalization: %v", err)
					} else if len(active) != 0 {
						t.Fatalf("active agents after independent terminalization = %#v, want none", active)
					}
				})
			}
		}
	}
}

func assertNotifyAllChildrenAgentEmissionSettledToSameInstanceNode(t testing.TB, ctx context.Context, db *sql.DB, backend, runID, agentID, flowInstance string) {
	t.Helper()
	query := `
		SELECT COUNT(*)
		FROM events e
		JOIN event_deliveries d ON d.event_id = e.event_id
		WHERE e.run_id = ?
		  AND e.produced_by_type = 'agent'
		  AND e.produced_by = ?
		  AND e.flow_instance = ?
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = ?
		  AND d.status = 'delivered'
		  AND e.route_settlement IS NOT NULL`
	args := []any{runID, agentID, flowInstance, conformanceNode(t, "account", "account-node").Key()}
	if backend == "postgres" {
		query = strings.Replace(query, "e.run_id = ?", "e.run_id = $1::uuid", 1)
		query = strings.Replace(query, "e.produced_by = ?", "e.produced_by = $2", 1)
		query = strings.Replace(query, "e.flow_instance = ?", "e.flow_instance = $3", 1)
		query = strings.Replace(query, "d.subscriber_id = ?", "d.subscriber_id = $4", 1)
	}
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("query template-agent same-instance delivery settlement: %v", err)
	}
	if count != 1 {
		t.Fatalf("template-agent settled node deliveries for %s = %d, want exactly one", flowInstance, count)
	}
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
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, storetest.DatabaseForTest(backend)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			fixedEngineNow := time.Date(2026, time.July, 12, 12, 0, 0, 1, time.UTC)
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
			processTopology := newNotifyAllChildrenProcessTopology(
				t,
				testAuthorActivityContextForBundle(context.Background(), conformanceBundleSourceFact(t, source)),
				backend,
			)
			runtime := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow }, notifyAllChildrenRuntimeOptions{processTopology: processTopology})
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "notify-all-children-stale-route", runtime.bundleSourceFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}

			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			assertNotifyAllChildrenRunPersisted(t, ctx, backend, db, runID)
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.account.register.requested", map[string]any{
					"portfolio_id": "portfolio-main",
					"account_id":   accountID,
				})
			}

			descriptors := notifyAllChildrenAccountDescriptors(t, ctx, backend)
			if len(descriptors) != 3 {
				dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
				t.Logf("notify-all-children runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				t.Fatalf("active account descriptors = %#v, want A/B/stale", descriptors)
			}
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				if _, ok := descriptors[accountID]; !ok {
					t.Fatalf("active account descriptor %q missing from %#v", accountID, descriptors)
				}
			}

			orderedMembership := []string{"acct-b", "acct-a", "acct-b"}
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  orderedMembership,
			})
			orderedNotifyID := publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "ordered-duplicate",
			})
			orderedItems := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, orderedNotifyID)
			if len(orderedItems) != len(orderedMembership) {
				dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
				t.Logf("notify-all-children runtime diagnostics: %#v", runtime.diagnostics.snapshot())
			}
			assertNotifyAllChildrenItemSequence(t, orderedItems, orderedMembership)
			assertNotifyAllChildrenContiguousItemOrdinals(t, orderedItems)
			for index, item := range orderedItems {
				routes, err := backend.ListEventDeliveryRoutes(ctx, item.ID)
				if err != nil {
					t.Fatalf("ordered item %d ListEventDeliveryRoutes(%s): %v", index, item.AccountID, err)
				}
				want := descriptors[item.AccountID]
				assertNotifyAllChildrenExactRoutes(t, routes, want)
			}

			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a", "acct-b", "acct-stale"},
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, "portfolio", "account_ids", []any{"acct-a", "acct-b", "acct-stale"})

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

			notifyID := publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "refresh",
			})
			itemEvents := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, notifyID)
			if len(itemEvents) != 3 {
				dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
				t.Logf("notify-all-children runtime diagnostics: %#v", runtime.diagnostics.snapshot())
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
				assertNotifyAllChildrenExactRoutes(t, routes, want)
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
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a"},
			})
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "newer",
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "newer")
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-b"},
			})

			originalA := items["acct-a"]
			deleteNotifyAllChildrenPipelineReceipt(t, ctx, backend, db, originalA)
			claimed, err := backend.PipelineObligations().ClaimEvent(ctx, originalA, runtimepipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatalf("claim original pipeline obligation: %v", err)
			}
			replay := claimed.Event
			if err := backend.PipelineObligations().Release(ctx, claimed.Claim); err != nil {
				t.Fatalf("release original pipeline obligation: %v", err)
			}
			routes, err := backend.ListEventDeliveryRoutes(ctx, originalA)
			if err != nil {
				t.Fatalf("original A persisted routes: %v", err)
			}
			assertNotifyAllChildrenExactRoutes(t, routes, descriptors["acct-a"])
			recoveryEvent := eventtest.RuntimeControl(
				uuid.NewString(), replay.Type(), "workflow-runtime", "", replay.Payload(), replay.ChainDepth()+1,
				runID, replay.ID(), replay.Envelope(), fixedEngineNow.Add(time.Second),
			)
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, backend, recoveryEvent, routes,
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)
			eventCountBefore := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID)
			restarted := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow }, notifyAllChildrenRuntimeOptions{processTopology: processTopology})
			startNotifyAllChildrenDeliveryContinuations(t, ctx, backend, restarted, recoveryEvent.ID(), routes[0])
			waitNotifyAllChildrenRuntime(t, restarted, runID)
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "refresh")
			if got := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID); got != eventCountBefore {
				t.Fatalf("item event count after replay = %d, want %d; replay must not re-expand current membership", got, eventCountBefore)
			}
			routes, err = backend.ListEventDeliveryRoutes(ctx, recoveryEvent.ID())
			if err != nil {
				t.Fatalf("recovered persisted A routes: %v", err)
			}
			assertNotifyAllChildrenExactRoutes(t, routes, descriptors["acct-a"])
		})
	}
}

func assertNotifyAllChildrenExactRoutes(
	t testing.TB,
	routes []events.DeliveryRoute,
	want runtimebus.ActiveFlowInstanceDescriptor,
) {
	t.Helper()
	if len(routes) != 2 {
		t.Fatalf("persisted routes = %#v, want exact node and agent routes", routes)
	}
	var nodeFound, agentFound bool
	for _, route := range routes {
		target := route.Target.Route()
		if target.FlowInstance != want.FlowInstance || target.EntityID != want.EntityID {
			t.Fatalf("persisted route = %#v, want target %s/%s", route, want.FlowInstance, want.EntityID)
		}
		switch {
		case route.Recipient.IsNode():
			nodeFound = route.Recipient.ID() == conformanceNode(t, "account", "account-node").Key()
		case route.Recipient.IsAgent():
			identity := route.AgentIdentity.Normalize()
			if err := identity.Validate(); err != nil {
				t.Fatalf("persisted account-worker identity = %#v: %v", route.AgentIdentity, err)
			}
			agentFound = route.Recipient.ID() == "account-worker" &&
				identity.AgentID() == "account-worker" &&
				identity.FlowInstance() == want.FlowInstance
		}
	}
	if !nodeFound || !agentFound {
		t.Fatalf("persisted routes = %#v, want account-node and exact account-worker", routes)
	}
}

func startNotifyAllChildrenDeliveryContinuations(
	t *testing.T,
	ctx context.Context,
	backend notifyAllChildrenStore,
	runtime notifyAllChildrenRuntime,
	eventID string,
	route events.DeliveryRoute,
) {
	t.Helper()
	deliveryID, err := runtimedelivery.DeliveryID(eventID, route)
	if err != nil {
		t.Fatalf("derive recovery delivery identity: %v", err)
	}
	snapshot, err := backend.Snapshot(ctx, deliveryID)
	if err != nil {
		t.Fatalf("load recovery delivery authority: %v", err)
	}
	if err := backend.ActivateDeliveryAuthority(ctx, snapshot.Authority); err != nil {
		t.Fatalf("activate recovery delivery authority: %v", err)
	}
	if err := runtime.bus.SetDeliveryAuthority(snapshot.Authority); err != nil {
		t.Fatalf("configure recovery delivery authority: %v", err)
	}
	coordinator, err := runtimedeliverycontinuation.New(
		backend,
		backend,
		snapshot.Authority,
		runtime.workOwner,
		runtime.bus,
		func(_ context.Context, reportErr error) {
			t.Errorf("delivery continuation failed: %v", reportErr)
		},
	)
	if err != nil {
		t.Fatalf("construct delivery continuation coordinator: %v", err)
	}
	if err := runtime.bus.SetDeliveryContinuationOwner(coordinator); err != nil {
		t.Fatalf("configure delivery continuation owner: %v", err)
	}
	if err := coordinator.Start(testAuthorActivityContextForBundle(ctx, runtime.bundleSourceFact)); err != nil {
		t.Fatalf("start delivery continuation coordinator: %v", err)
	}
	t.Cleanup(func() {
		retireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Retire(retireCtx); err != nil {
			t.Errorf("retire delivery continuation coordinator: %v", err)
		}
	})
}

func newNotifyAllChildrenRuntime(
	t *testing.T,
	backend notifyAllChildrenStore,
	db *sql.DB,
	source semanticview.Source,
	engineNow func() time.Time,
	options ...notifyAllChildrenRuntimeOptions,
) notifyAllChildrenRuntime {
	t.Helper()
	var opts notifyAllChildrenRuntimeOptions
	if len(options) > 0 {
		opts = options[0]
	}
	bundleSourceFact := opts.bundleSourceFact
	if bundleSourceFact.Validate() != nil {
		bundleSourceFact = conformanceBundleSourceFact(t, source)
	}
	if opts.processTopology == nil {
		opts.processTopology = newNotifyAllChildrenProcessTopology(
			t,
			testAuthorActivityContextForBundle(context.Background(), bundleSourceFact),
			backend,
		)
	}
	var coordinator *runtimepipeline.PipelineCoordinator
	var manager *runtimemanager.AgentManager
	workOwner := conformanceTestRuntimeOccurrence(t, bundleSourceFact.BundleHash())
	var runtimeControlEvents []string
	if opts.enableGenericSchedules {
		runtimeControlEvents = append(runtimeControlEvents, "platform.join_complete")
	}
	eventBus, err := newScopedTestEventBus(t, backend, durableConformanceEventBusOptions(backend, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: bundleSourceFact,
		WorkOwner:        workOwner,
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
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("agent manager is not initialized")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return fmt.Errorf("agent manager is not initialized")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	}), runtimeControlEvents...)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	var (
		genericSchedules *runtimegenericschedule.Lifecycle
		scheduler        *runtimepipeline.Scheduler
	)
	if opts.enableGenericSchedules {
		genericScheduleStore, ok := backend.(runtimegenericschedule.Store)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement generic schedule persistence", backend)
		}
		scheduler = runtimepipeline.NewSchedulerWithWorkOwner(workOwner)
		genericSchedules, err = runtimegenericschedule.NewLifecycle(
			genericScheduleStore,
			scheduler,
			eventBus,
			eventBus.EngineDispatcher(),
			notifyAllChildrenGenericScheduleLogger{t: t},
			executionposture.Live,
		)
		if err != nil {
			t.Fatalf("construct notify-all-children generic schedule lifecycle: %v", err)
		}
	}
	if routeStore, ok := backend.(runtimebus.FlowInstanceRoutePersistence); ok {
		routes, err := routeStore.ListFlowInstanceRoutes(testAuthorActivityContextForBundle(context.Background(), bundleSourceFact))
		if err != nil {
			t.Fatalf("ListFlowInstanceRoutes: %v", err)
		}
		for _, route := range routes {
			if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
				t.Fatalf("restore flow-instance route %s: %v", route.InstancePath, err)
			}
		}
	}
	var (
		agentFactory runtimemanager.AgentFactory
		sessionStore runtimesessions.Registry
		llmBackend   string
	)
	if opts.realMockAgents {
		if bundle, ok := semanticview.Bundle(source); !ok || bundle == nil {
			t.Fatal("notify-all-children mock runtime requires a bundle-backed source")
		}
		effectStore, ok := backend.(runtimeeffects.Store)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement completion effect persistence", backend)
		}
		completionStore, ok := backend.(runtimeeffects.CompletionStore)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement completion settlement persistence", backend)
		}
		heartbeatStore, ok := backend.(runtimeeffects.CompletionHeartbeatStore)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement completion heartbeat persistence", backend)
		}
		conversations, ok := backend.(runtimellm.ConversationPersistence)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement conversation persistence", backend)
		}
		cfg := &config.Config{}
		cfg.LLM.Backend = llmselection.BackendMock
		cfg.LLM.Session.LockTTL = time.Minute
		sessionStore = runtimesessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL)
		modelRuntime := runtimellm.NewMockRuntime(
			cfg,
			sessionStore,
			"notify-all-children-conformance",
			conversations,
			eventBus,
			liveTestCompletionController(effectStore, completionStore, heartbeatStore, discardCompletionSpendProjection{}),
		)
		profile, err := llmselection.ResolveActiveBackend(llmselection.BackendMock)
		if err != nil {
			t.Fatalf("resolve mock profile: %v", err)
		}
		modelRuntimes, err := runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{}, modelRuntime)
		if err != nil {
			t.Fatalf("build mock agent runtime set: %v", err)
		}
		authority := runtimeauthority.NewSourceProvider(source)
		emitRegistry := runtimetools.NewEmitRegistry(source, authority)
		toolExecutor := runtimetools.NewExecutorWithOptions(eventBus, runtimetools.ExecutorOptions{
			Config:            cfg,
			WorkflowSource:    source,
			ModelRuntimes:     modelRuntimes,
			AuthorityProvider: authority,
			EmitRegistry:      emitRegistry,
		})
		agentFactory = runtimeagents.NewLLMAgentFactory(
			modelRuntimes,
			toolExecutor,
			runtimeagents.LLMAgentOptions{},
		)
		if opts.agentGate != nil {
			agentFactory = opts.agentGate.wrapFactory(agentFactory)
		}
		llmBackend = llmselection.BackendMock
	}
	workflowPersistence := runtimepipeline.NewWorkflowPersistence(backend)
	switch sqliteStore := backend.(type) {
	case *store.SQLiteRuntimeStore:
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
	case *failingNotifyAllChildrenSQLiteStore:
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
	}
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
	diagnosticBus := &fanInBarrierDiagnosticBus{EventBus: eventBus}
	coordinatorOptions := runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live,
		Module:           module,
		BundleSourceFact: bundleSourceFact,
		InstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("agent manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
		InstanceDeactivator: func(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
			if manager == nil {
				return fmt.Errorf("agent manager is not initialized")
			}
			return manager.DeactivateFlowInstanceModel(ctx, req)
		},
		Persistence:             workflowPersistence,
		RunLifecycle:            backend,
		PipelineObligations:     backend.PipelineObligations(),
		DeliveryStore:           backend,
		DeadLetters:             backend,
		DecisionCards:           backend,
		ProposedEffects:         backend,
		HumanTasks:              backend,
		DecisionCardDraftExpiry: backend,
		HumanTaskExpiry:         backend,
		DeliveryRuntime:         eventBus,
		FlowRoutes:              eventBus,
		TimerScheduler:          scheduler,
		GenericSchedules:        genericSchedules,
		TestEngineEmitNow:       engineNow,
		WorkOwner:               workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(diagnosticBus, coordinatorOptions)

	generationLifecycle := &notifyAllChildrenLifecycleOwner{}
	var lifecycleStore runtimemanager.AgentLifecyclePersistence = generationLifecycle
	manager = ownConformanceTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(eventBus, agentFactory, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		BaseContext:       testAuthorActivityContextForBundle(context.Background(), bundleSourceFact),
		BundleSourceFact:  bundleSourceFact,
		WorkflowInstances: coordinator,
		WorkOwner:         workOwner,
		DeliveryStore:     backend,
		LifecycleStore:    lifecycleStore,
		SemanticSource:    source,
		Sessions:          sessionStore,
		LLMBackend:        llmBackend,
		PersistenceRoles:  conformanceManagerPersistenceRoles(backend, eventBus, coordinator), ReceiverExecution: eventreceiver.NormalExecution(),
	}, backend))
	opts.processTopology.install(t, testAuthorActivityContextForBundle(context.Background(), bundleSourceFact), manager, source, bundleSourceFact, generationLifecycle)
	if opts.enableGenericSchedules {
		candidateOwner, ok := backend.(runtimerunlifecycle.CandidateOwner)
		if !ok {
			t.Fatalf("notify-all-children store %T does not implement run lifecycle candidates", backend)
		}
		executor, err := runtimerunlifecycle.NewExecutor(
			candidateOwner,
			runtimerunlifecycle.CandidateScope{BundleHash: bundleSourceFact.BundleHash()},
			notifyAllChildrenTerminalCatalog(source),
			workOwner,
			runtimerunlifecycle.ExecutorOptions{GenericSchedules: genericSchedules},
		)
		if err != nil {
			t.Fatalf("construct notify-all-children completion executor: %v", err)
		}
		runtimeCtx := worklifetime.WithRuntimeOccurrence(testAuthorActivityContextForBundle(context.Background(), bundleSourceFact), workOwner)
		registration, err := candidateOwner.RegisterCompletionCandidateSink(
			runtimeCtx,
			runtimerunlifecycle.CandidateScope{BundleHash: bundleSourceFact.BundleHash()},
			executor,
		)
		if err != nil {
			t.Fatalf("register notify-all-children completion executor: %v", err)
		}
		if err := executor.Start(runtimeCtx); err != nil {
			registration.Release()
			t.Fatalf("start notify-all-children completion executor: %v", err)
		}
		t.Cleanup(func() {
			if err := executor.Retire(context.Background()); err != nil {
				t.Errorf("retire notify-all-children completion executor: %v", err)
			}
			registration.Release()
			if err := genericSchedules.Stop(context.Background()); err != nil {
				t.Errorf("stop notify-all-children generic schedules: %v", err)
			}
		})
	}
	if opts.maintenanceInterval > 0 {
		coordinator.SetTestMaintenanceInterval(opts.maintenanceInterval)
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(testAuthorActivityContextForBundle(context.Background(), bundleSourceFact))
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		coordinator.RunMaintenance(maintenanceCtx)
	}()
	t.Cleanup(func() {
		stopMaintenance()
		<-maintenanceDone
	})
	return notifyAllChildrenRuntime{
		bus: eventBus, diagnostics: diagnosticBus, manager: manager, pipeline: coordinator,
		workOwner: workOwner, selected: backend, bundleSourceFact: bundleSourceFact, genericSchedules: genericSchedules,
	}
}

func notifyAllChildrenTerminalCatalog(source semanticview.Source) runtimerunlifecycle.TerminalCatalog {
	workflow := source.FlowTerminalStages("")
	flows := make(map[string][]string)
	for flowID := range source.FlowSchemaEntries() {
		states := source.FlowTerminalStages(flowID)
		if len(states) == 0 {
			continue
		}
		flows[flowID] = states
		if path := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"); path != "" {
			flows[path] = states
		}
	}
	return runtimerunlifecycle.NewTerminalCatalog(workflow, flows)
}

func loadNotifyAllChildrenAgentsByID(
	t testing.TB,
	ctx context.Context,
	selected notifyAllChildrenStore,
) map[string]runtimemanager.PersistedAgent {
	t.Helper()
	agents, err := selected.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	out := make(map[string]runtimemanager.PersistedAgent, len(agents))
	for _, agent := range agents {
		out[strings.TrimSpace(agent.Config.ID)] = agent
	}
	return out
}

func assertNotifyAllChildrenConcreteAgentSet(
	t testing.TB,
	ctx context.Context,
	selected notifyAllChildrenStore,
	descriptors map[string]runtimebus.ActiveFlowInstanceDescriptor,
	agentID string,
) {
	t.Helper()
	agents, err := selected.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	remaining := make(map[string]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		remaining[descriptor.FlowInstance] = struct{}{}
	}
	var matched int
	for _, agent := range agents {
		if agent.Config.ID != agentID {
			continue
		}
		identity, err := agent.Config.ConcreteIdentity()
		if err != nil {
			t.Fatalf("%s concrete identity: %v", agentID, err)
		}
		if _, ok := remaining[identity.FlowInstance()]; !ok {
			t.Fatalf("%s has unexpected concrete route %#v", agentID, identity)
		}
		delete(remaining, identity.FlowInstance())
		matched++
	}
	if matched != len(descriptors) || len(remaining) != 0 {
		t.Fatalf("fixed-slug concrete agents matched=%d remaining=%v all=%#v", matched, remaining, agents)
	}
}

func waitNotifyAllChildrenRuntimeReadiness(
	t testing.TB,
	ctx context.Context,
	workflow *runtimepipeline.PipelineCoordinator,
	runID string,
	instancePath string,
) runtimepipeline.DynamicFlowRuntimeReadiness {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var (
		last  runtimepipeline.DynamicFlowRuntimeReadiness
		found bool
		err   error
	)
	for time.Now().Before(deadline) {
		last, found, err = workflow.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(instancePath))
		if err == nil && found && !last.TopologyReadyAt.IsZero() {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("runtime readiness did not converge: found=%v readiness=%#v err=%v", found, last, err)
	return runtimepipeline.DynamicFlowRuntimeReadiness{}
}

func waitNotifyAllChildrenEntityState(
	t testing.TB,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	instancePath string,
	want string,
) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if got := loadNotifyAllChildrenEntityState(t, ctx, backend, db, instancePath); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if concrete, ok := t.(*testing.T); ok {
		dumpNotifyAllChildrenRuntimeState(concrete, ctx, backend, db)
	}
	t.Fatalf(
		"flow instance %s status = %q, want %q",
		instancePath,
		loadNotifyAllChildrenEntityState(t, ctx, backend, db, instancePath),
		want,
	)
}

func loadNotifyAllChildrenEntityState(
	t testing.TB,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	instancePath string,
) string {
	t.Helper()
	query := `SELECT current_state FROM entity_state WHERE flow_instance = $1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT current_state FROM entity_state WHERE flow_instance = ?`
	}
	var state string
	if err := db.QueryRowContext(ctx, query, instancePath).Scan(&state); err != nil {
		t.Fatalf("load entity state %s: %v", instancePath, err)
	}
	return strings.TrimSpace(state)
}

func waitNotifyAllChildrenAgentDeliveryStatus(
	t testing.TB,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	runID string,
	agentID string,
	flowInstance string,
	want string,
) {
	t.Helper()
	query := `
		SELECT status
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
		  AND agent_flow_instance_path = $3
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1
	`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `
			SELECT status
			FROM event_deliveries
			WHERE run_id = ?
			  AND subscriber_type = 'agent'
			  AND subscriber_id = ?
			  AND agent_flow_instance_path = ?
			ORDER BY created_at DESC, delivery_id DESC
			LIMIT 1
		`
	}
	deadline := time.Now().Add(15 * time.Second)
	var (
		got     string
		lastErr error
	)
	for time.Now().Before(deadline) {
		lastErr = db.QueryRowContext(ctx, query, runID, agentID, flowInstance).Scan(&got)
		if lastErr == nil && strings.TrimSpace(got) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"%s delivery at %s = %q err=%v, want %q",
		agentID,
		flowInstance,
		strings.TrimSpace(got),
		lastErr,
		want,
	)
}

func assertNotifyAllChildrenCompletedTurns(
	t testing.TB,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	runID string,
	agentID string,
	wantInstances int,
) {
	t.Helper()
	query := `
		SELECT COUNT(*), COUNT(DISTINCT flow_instance)
		FROM agent_turns
		WHERE run_id = $1::uuid AND agent_id = $2 AND failure IS NULL
	`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `
			SELECT COUNT(*), COUNT(DISTINCT flow_instance)
			FROM agent_turns
			WHERE run_id = ? AND agent_id = ? AND failure IS NULL
		`
	}
	var turns, instances int
	if err := db.QueryRowContext(ctx, query, runID, agentID).Scan(&turns, &instances); err != nil {
		t.Fatalf("count completed %s turns: %v", agentID, err)
	}
	if turns < wantInstances || instances != wantInstances {
		t.Fatalf(
			"completed %s turns=%d distinct instances=%d, want at least %d turns across %d instances",
			agentID,
			turns,
			instances,
			wantInstances,
			wantInstances,
		)
	}
}

func waitNotifyAllChildrenAgentAbsent(
	t testing.TB,
	manager *runtimemanager.AgentManager,
	agentID string,
	flowInstance string,
) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = manager.ResolveAgentConfig(agentID, flowInstance)
		if lastErr != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("terminated concrete agent %s remains process-visible: %v", flowInstance, lastErr)
}

func loadNotifyAllChildrenFlowInstanceStatus(
	t testing.TB,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	instancePath string,
) string {
	t.Helper()
	query := `SELECT status FROM flow_instances WHERE instance_id = $1`
	if _, ok := backend.(*failingNotifyAllChildrenSQLiteStore); ok {
		query = `SELECT status FROM flow_instances WHERE instance_id = ?`
	}
	var status string
	if err := db.QueryRowContext(ctx, query, instancePath).Scan(&status); err != nil {
		t.Fatalf("load flow instance status %s: %v", instancePath, err)
	}
	return strings.TrimSpace(status)
}

func publishNotifyAllChildrenEvent(t *testing.T, ctx context.Context, runtime notifyAllChildrenRuntime, source semanticview.Source, runID, localEvent string, payload map[string]any) string {
	t.Helper()
	id := publishNotifyAllChildrenEventClass(t, ctx, runtime, source, runID, localEvent, payload, false)
	waitNotifyAllChildrenRuntime(t, runtime, runID)
	return id
}

func publishNotifyAllChildrenRunCreatingEvent(t *testing.T, ctx context.Context, runtime notifyAllChildrenRuntime, source semanticview.Source, runID, localEvent string, payload map[string]any) string {
	t.Helper()
	id := publishNotifyAllChildrenEventClass(t, ctx, runtime, source, runID, localEvent, payload, true)
	waitNotifyAllChildrenRuntime(t, runtime, runID)
	return id
}

func publishNotifyAllChildrenEventClass(t *testing.T, ctx context.Context, runtime notifyAllChildrenRuntime, source semanticview.Source, runID, localEvent string, payload map[string]any, runCreating bool) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", localEvent, err)
	}
	id := uuid.NewString()
	eventType := events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, localEvent))
	createdAt := time.Now().UTC()
	evt := eventtest.ExistingRunRootIngress(
		id, eventType, notifyallchildren.OwnerFlowID, "", raw, 0, runID, events.EventEnvelope{}, createdAt,
	)
	if runCreating {
		evt = eventtest.RunCreatingRootIngress(
			id, eventType, notifyallchildren.OwnerFlowID, "", raw, 0, runID, "", events.EventEnvelope{}, createdAt,
		)
	}
	publishCtx := testAuthorActivityContextForBundle(ctx, runtime.bundleSourceFact)
	if err := runtime.bus.PublishAcknowledged(publishCtx, evt); err != nil {
		t.Fatalf("PublishAcknowledged(%s): %v", localEvent, err)
	}
	return id
}

func publishNotifyAllChildrenEventAsync(t *testing.T, ctx context.Context, runtime notifyAllChildrenRuntime, source semanticview.Source, runID, localEvent string, payload map[string]any) string {
	t.Helper()
	return publishNotifyAllChildrenEventClass(t, ctx, runtime, source, runID, localEvent, payload, false)
}

func waitNotifyAllChildrenRuntime(t *testing.T, runtime notifyAllChildrenRuntime, runID string) {
	t.Helper()
	waitNotifyAllChildrenRuntimeWithin(t, runtime, runID, 30*time.Second)
}

func waitNotifyAllChildrenRuntimeWithin(t *testing.T, runtime notifyAllChildrenRuntime, runID string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(context.Background(), runtime.bundleSourceFact), timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := runtime.bus.WaitForQuiescence(ctx); err != nil {
			t.Fatalf("WaitForQuiescence: %v", err)
		}
		summary, err := runtime.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
		if err != nil {
			t.Fatalf("FanOutRunSummary(%s): %v", runID, err)
		}
		if summary.Owed == 0 && summary.Open == 0 && summary.Blocked == 0 && summary.Unsettled == 0 && summary.BarrierArmed == 0 && summary.BarrierPending == 0 {
			if err := runtime.bus.WaitForQuiescence(ctx); err != nil {
				t.Fatalf("WaitForQuiescence after fan-out settlement: %v", err)
			}
			confirmed, err := runtime.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil {
				t.Fatalf("confirm FanOutRunSummary(%s): %v", runID, err)
			}
			if confirmed.Owed == 0 && confirmed.Open == 0 && confirmed.Blocked == 0 && confirmed.Unsettled == 0 && confirmed.BarrierArmed == 0 && confirmed.BarrierPending == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for durable fan-out settlement: %v; summary=%#v", ctx.Err(), summary)
		case <-ticker.C:
		}
	}
}

func waitNotifyAllChildrenFanOutCursor(t *testing.T, runtime notifyAllChildrenRuntime, db *sql.DB, runID string, cardinality int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(context.Background(), runtime.bundleSourceFact), 5*time.Minute)
	defer cancel()
	query := `SELECT COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1::uuid`
	if _, ok := runtime.selected.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0) FROM fan_out_intents WHERE run_id=?`
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var total, cursor, owed int
		if err := db.QueryRowContext(ctx, query, runID).Scan(&total, &cursor, &owed); err != nil {
			t.Fatalf("load fan-out cursor: %v", err)
		}
		if total == cardinality && cursor == cardinality && owed == 0 {
			if err := runtime.bus.WaitForQuiescence(ctx); err != nil {
				t.Fatalf("wait fan-out EventBus settlement: %v", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for fan-out cursor: %v; total=%d cursor=%d owed=%d", ctx.Err(), total, cursor, owed)
		case <-ticker.C:
		}
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
	CreatedAt string
	Ordinal   int
}

func loadNotifyAllChildrenItemEvents(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, sourceEventID string) []notifyAllChildrenItemEvent {
	t.Helper()
	query := `SELECT e.event_id::text,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=$1::uuid AND e.event_name=$2 AND e.source_event_id=$3::uuid AND o.outcome_kind='committed' ORDER BY o.ordinal`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT e.event_id,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=? AND e.event_name=? AND e.source_event_id=? AND o.outcome_kind='committed' ORDER BY o.ordinal`
	}
	rows, err := db.QueryContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID)
	if err != nil {
		t.Fatalf("query fan-out item events: %v", err)
	}
	defer rows.Close()
	out := []notifyAllChildrenItemEvent{}
	for rows.Next() {
		var id string
		var ordinal int
		var raw, createdAt any
		if err := rows.Scan(&id, &raw, &createdAt, &ordinal); err != nil {
			t.Fatalf("scan fan-out item event: %v", err)
		}
		payload := map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &payload); err != nil {
			t.Fatalf("decode fan-out item payload: %v", err)
		}
		accountID, _ := payload["account_id"].(string)
		out = append(out, notifyAllChildrenItemEvent{ID: id, AccountID: accountID, CreatedAt: fmt.Sprint(createdAt), Ordinal: ordinal})
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
		t.Fatalf("persisted fan-out item sequence = %#v (%#v), want %#v with order and duplicates preserved", got, items, want)
	}
}

func assertNotifyAllChildrenContiguousItemOrdinals(t *testing.T, items []notifyAllChildrenItemEvent) {
	t.Helper()
	for want, item := range items {
		if item.Ordinal != want {
			t.Fatalf("persisted fan-out item ordinal[%d] = %d (%#v), want contiguous durable order", want, item.Ordinal, items)
		}
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

func loadNotifyAllChildrenMetadata(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, flowInstance string) map[string]any {
	t.Helper()
	query := `SELECT fields FROM entity_state WHERE flow_instance = $1 ORDER BY updated_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT fields FROM entity_state WHERE flow_instance = ? ORDER BY updated_at DESC LIMIT 1`
	}
	var raw any
	if err := db.QueryRowContext(ctx, query, flowInstance).Scan(&raw); err != nil {
		t.Fatalf("load %s metadata: %v", flowInstance, err)
	}
	fields := map[string]any{}
	if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &fields); err != nil {
		t.Fatalf("decode %s metadata: %v", flowInstance, err)
	}
	return fields
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

func deleteNotifyAllChildrenPipelineReceipt(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, eventID string) {
	t.Helper()
	query := `DELETE FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `DELETE FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	if _, err := db.ExecContext(ctx, query, eventID); err != nil {
		t.Fatalf("delete replay receipts: %v", err)
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
		`SELECT event_id, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(failure::text, '') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure::text, ''), COALESCE(delivery_target_route::text, '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`,
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
