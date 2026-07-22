package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const eventBusTestRunID = "99999999-9999-9999-9999-999999999999"

type testCommitPublishTransaction struct {
	active   []string
	begin    func(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
	finalize func(context.Context, runtimebus.CommitPublishRequest) error
}

func (t *testCommitPublishTransaction) BeginPreparedPublish(ctx context.Context, prepared runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	outcome := runtimebus.EventAppendInserted
	var err error
	if t.begin != nil {
		outcome, err = t.begin(ctx, prepared.AdmittedEvent())
	}
	if err == nil && outcome == runtimebus.EventAppendInserted {
		t.active = append(t.active, prepared.AdmittedEvent().ID())
	}
	return outcome, err
}

func (t *testCommitPublishTransaction) FinalizePreparedPublish(ctx context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	req := finalization.Request()
	if len(t.active) == 0 || t.active[len(t.active)-1] != req.Event.ID() {
		return errors.New("prepared event finalization does not match the active event")
	}
	if t.finalize != nil {
		if err := t.finalize(ctx, req); err != nil {
			return err
		}
	}
	t.active = t.active[:len(t.active)-1]
	return nil
}

func prepareTestCommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan, transaction *testCommitPublishTransaction) (runtimebus.PreparedPublish, error) {
	postCommit := make([]runtimepipeline.OwnerAction, 0, 4)
	rollback := make([]runtimepipeline.OwnerAction, 0, 4)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
	prepared, err := plan.PrepareCommitPublish(runtimebus.WithCommitPublishTransaction(ctx, transaction))
	if err != nil {
		runtimepipeline.FlushPipelineRollbackActions(rollback)
		return runtimebus.PreparedPublish{}, err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return prepared, nil
}

type retainedConnectionCommitStore struct {
	runtimebus.InMemoryEventStore
	conn *sql.Conn
}

func (s *retainedConnectionCommitStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	ctx = runtimepipeline.WithPipelineSQLConnContext(ctx, s.conn)
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{})
}

type dispatchContextObserver struct {
	t      *testing.T
	called bool
}

func (o *dispatchContextObserver) NotifyLifecycle(ctx context.Context, signal runtimelifecycleprobe.Signal) {
	if signal.Kind != runtimelifecycleprobe.PostCommitDispatchStarted {
		return
	}
	o.called = true
	if _, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		o.t.Error("post-commit dispatch retained the transaction-owned SQL connection")
	}
	if _, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok {
		o.t.Error("post-commit dispatch retained the completed SQL transaction")
	}
}

func TestCommittedPublishDispatchDropsTransactionConnectionCapability(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	observer := &dispatchContextObserver{t: t}
	bus, err := newScopedTestEventBus(&retainedConnectionCommitStore{conn: conn}, runtimebus.EventBusOptions{TestLifecycleProbe: observer})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "provider", "", []byte(`{}`), 0, eventBusTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	if err := bus.Publish(testAuthorActivityContext(context.Background()), event); err != nil {
		t.Fatal(err)
	}
	if !observer.called {
		t.Fatal("post-commit dispatch lifecycle was not observed")
	}
}

func TestCommittedPublishDispatchDoesNotExposePublicationClaimConnection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	observer := &dispatchContextObserver{t: t}
	bus, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{TestLifecycleProbe: observer})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("task.requested"), "provider", "", []byte(`{}`), 0,
		eventBusTestRunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}
	if !observer.called {
		t.Fatal("post-commit dispatch lifecycle was not observed")
	}
}

func eventBusTestRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), eventBusTestRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
		VALUES ($1::uuid, 'running', $2, $3, $4)
		ON CONFLICT (run_id) DO NOTHING
	`, eventBusTestRunID, authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint); err != nil {
		t.Fatalf("seed event bus test run: %v", err)
	}
	return ctx
}

func TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := pg.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark run cancelled: %v", err)
	}
	assertEventBusTerminalRunRefusal(t, pg, runID, "cancelled", func(eventID string) (string, int, int, error) {
		var status string
		var eventCount, deliveryCount int
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
			return "", 0, 0, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&eventCount); err != nil {
			return "", 0, 0, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveryCount); err != nil {
			return "", 0, 0, err
		}
		return status, eventCount, deliveryCount, nil
	})
}

func TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := sqliteStore.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark run cancelled: %v", err)
	}
	assertEventBusTerminalRunRefusal(t, sqliteStore, runID, "cancelled", func(eventID string) (string, int, int, error) {
		var status string
		var eventCount, deliveryCount int
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
			return "", 0, 0, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&eventCount); err != nil {
			return "", 0, 0, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&deliveryCount); err != nil {
			return "", 0, 0, err
		}
		return status, eventCount, deliveryCount, nil
	})
}

type eventBusExactDuplicateState struct {
	Status        string
	RunEventCount int
	EventRows     int
	DeliveryRows  int
	OutcomeRows   int
}

func TestEventBusExactDuplicateIsOperationNoOpPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	if _, err := newScopedTestEventBus(pg); err != nil {
		t.Fatalf("register author activity catalog: %v", err)
	}
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	evt := exactDuplicateEventBusEvent(runID)
	route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-original"}
	storetest.CommitSemanticEventWithRoutes(t, ctx, pg, evt, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeDirect)
	assertEventBusExactDuplicateIsOperationNoOp(t, pg, evt, func() (eventBusExactDuplicateState, error) {
		var state eventBusExactDuplicateState
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(event_count, 0) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&state.Status, &state.RunEventCount); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, evt.ID()).Scan(&state.EventRows); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, evt.ID()).Scan(&state.DeliveryRows); err != nil {
			return state, err
		}
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_delivery_outcomes o
			JOIN event_deliveries d ON d.delivery_id = o.delivery_id
			WHERE d.event_id = $1::uuid
		`, evt.ID()).Scan(&state.OutcomeRows); err != nil {
			return state, err
		}
		return state, nil
	}, func() error {
		claimed, err := pg.ClaimAgentDelivery(ctx, evt, route)
		if err != nil {
			return err
		}
		if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond); err != nil {
			return err
		}
		_, err = pg.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC())
		return err
	})
}

func TestEventBusExactDuplicateIsOperationNoOpSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	if _, err := newScopedTestEventBus(sqliteStore); err != nil {
		t.Fatalf("register author activity catalog: %v", err)
	}
	ctx := testAuthorActivityContext(context.Background())
	runID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	evt := exactDuplicateEventBusEvent(runID)
	route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-original"}
	storetest.CommitSemanticEventWithRoutes(t, ctx, sqliteStore, evt, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeDirect)
	assertEventBusExactDuplicateIsOperationNoOp(t, sqliteStore, evt, func() (eventBusExactDuplicateState, error) {
		var state eventBusExactDuplicateState
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(event_count, 0) FROM runs WHERE run_id = ?`, runID).Scan(&state.Status, &state.RunEventCount); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, evt.ID()).Scan(&state.EventRows); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, evt.ID()).Scan(&state.DeliveryRows); err != nil {
			return state, err
		}
		if err := sqliteStore.DB.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_delivery_outcomes o
			JOIN event_deliveries d ON d.delivery_id = o.delivery_id
			WHERE d.event_id = ?
		`, evt.ID()).Scan(&state.OutcomeRows); err != nil {
			return state, err
		}
		return state, nil
	}, func() error {
		claimed, err := sqliteStore.ClaimAgentDelivery(ctx, evt, route)
		if err != nil {
			return err
		}
		if _, err := sqliteStore.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond); err != nil {
			return err
		}
		_, err = sqliteStore.MarkRunTerminal(ctx, runID, "cancelled", nil, time.Now().UTC())
		return err
	})
}

func assertEventBusExactDuplicateIsOperationNoOp(
	t *testing.T,
	eventStore runtimebus.EventStore,
	evt events.Event,
	loadState func() (eventBusExactDuplicateState, error),
	markTerminal func() error,
) {
	t.Helper()
	eb, err := newScopedTestEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	original := runtimebustest.Subscribe(t, eb, "agent-original", evt.Type())
	ch := runtimebustest.Subscribe(t, eb, "agent-expansion", evt.Type())
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, event events.Event) error {
			return eb.PublishDirect(ctx, event, []string{"agent-expansion"})
		},
	}
	assertPhase := func(t *testing.T) {
		t.Helper()
		for name, publish := range writers {
			name, publish := name, publish
			t.Run(name, func(t *testing.T) {
				before, err := loadState()
				if err != nil {
					t.Fatalf("load state before duplicate: %v", err)
				}
				if err := publish(testAuthorActivityContext(context.Background()), evt); err != nil {
					t.Fatalf("publish exact duplicate: %v", err)
				}
				after, err := loadState()
				if err != nil {
					t.Fatalf("load state after duplicate: %v", err)
				}
				if after != before {
					t.Fatalf("exact duplicate mutated operation state: before=%+v after=%+v", before, after)
				}
				requireNoBusEvent(t, original, "exact duplicate committed-recipient dispatch")
				requireNoBusEvent(t, ch, "exact duplicate process-local dispatch")
			})
		}
	}
	t.Run("active_run", assertPhase)
	if err := markTerminal(); err != nil {
		t.Fatalf("mark terminal: %v", err)
	}
	t.Run("terminal_run", assertPhase)
}

func exactDuplicateEventBusEvent(runID string) events.Event {
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("custom.exact_duplicate_noop"), "api.v1", "", []byte(`{"attempt":"duplicate"}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond),
	)
}

func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	assertEventBusDiagnosticDirectRefusal(t, pg, func(eventID string) (int, error) {
		var count int
		err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&count)
		return count, err
	})
}

func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerSQLite(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	assertEventBusDiagnosticDirectRefusal(t, sqliteStore, func(eventID string) (int, error) {
		var count int
		err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&count)
		return count, err
	})
}

func assertEventBusDiagnosticDirectRefusal(
	t *testing.T,
	eventStore runtimebus.EventStore,
	loadEventCount func(string) (int, error),
) {
	t.Helper()
	eb, err := newScopedTestEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, evt events.Event) error {
			return eb.PublishDirect(ctx, evt, []string{"agent-1"})
		},
	}
	for _, eventType := range events.DiagnosticDirectEventTypes() {
		eventType := eventType
		t.Run(string(eventType), func(t *testing.T) {
			for name, publish := range writers {
				name, publish := name, publish
				t.Run(name, func(t *testing.T) {
					eventID := uuid.NewString()
					runID := ""
					if eventType != events.EventTypePlatformRuntimeLog {
						runID = uuid.NewString()
					}
					evt := eventtest.DiagnosticDirect(
						eventID, eventType, "runtime", "", []byte(`{"evidence":"typed-owner-only"}`),
						0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
					)
					err := publish(context.Background(), evt)
					if err == nil || !strings.Contains(err.Error(), "closed event type") {
						t.Fatalf("publish error = %v, want closed-event refusal", err)
					}
					count, err := loadEventCount(eventID)
					if err != nil {
						t.Fatalf("load event count: %v", err)
					}
					if count != 0 {
						t.Fatalf("persisted diagnostic-direct events = %d, want 0", count)
					}
				})
			}
		})
	}
}

func assertEventBusTerminalRunRefusal(
	t *testing.T,
	eventStore runtimebus.EventStore,
	runID string,
	wantStatus string,
	loadState func(string) (string, int, int, error),
) {
	t.Helper()
	eb, err := newScopedTestEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := testAuthorActivityContext(context.Background())
	writers := map[string]func(context.Context, events.Event) error{
		"publish":              eb.Publish,
		"publish_acknowledged": eb.PublishAcknowledged,
		"publish_direct": func(ctx context.Context, evt events.Event) error {
			return eb.PublishDirect(ctx, evt, []string{"agent-1"})
		},
	}
	for name, publish := range writers {
		name, publish := name, publish
		t.Run(name, func(t *testing.T) {
			eventID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(
				eventID,
				events.EventType("custom.terminal_refusal"),
				"api.v1",
				"",
				[]byte(`{"attempt":"terminal"}`),
				0,
				runID,
				"",
				events.EventEnvelope{},
				time.Now().UTC(),
			)
			if err := publish(ctx, evt); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("publish error = %v, want inactive-run rejection", err)
			}
			status, eventCount, deliveryCount, err := loadState(eventID)
			if err != nil {
				t.Fatalf("load state: %v", err)
			}
			if status != wantStatus || eventCount != 0 || deliveryCount != 0 {
				t.Fatalf("state after refusal = status=%s events=%d deliveries=%d", status, eventCount, deliveryCount)
			}
		})
	}
}

type fixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	workflowNodes  []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func (m *fixtureWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m *fixtureWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m *fixtureWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNodes...)
}

func (m *fixtureWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guardRegistry
}

func (m *fixtureWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

func newFixtureWorkflowModule(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimepipeline.WorkflowModule {
	t.Helper()
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &fixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}
}

func TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	fixtureRoot := canonicalrouting.CopyTemplateSelectAgentOnlyWithUnrelatedNode(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := eventBusTestRunContext(t, db)
	instanceRoute := runtimeflowidentity.DeriveRoute("account", "one")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'account', 'template', '{}'::jsonb, 'active', NOW())
	`, instanceRoute.InstancePath); err != nil {
		t.Fatalf("seed account flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, 'account', 'active', '{"account_id":"acct-agent"}'::jsonb, NOW(), NOW())
	`, runtimeflowidentity.EntityID(instanceRoute.InstancePath), eventBusTestRunID, instanceRoute.InstancePath); err != nil {
		t.Fatalf("seed account entity state: %v", err)
	}

	var pc *runtimepipeline.PipelineCoordinator
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		DeliveryStore: pg,
	})
	if pc == nil {
		t.Fatal("expected pipeline coordinator")
	}
	if err := eb.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: instanceRoute}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	agentID := "account-agent-one"
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       agentID,
		FlowID:        "account",
		FlowPath:      instanceRoute.InstancePath,
		Subscriptions: []string{"account.ready"},
	})
	if err != nil {
		t.Fatalf("AdmitFlowOwnedAgentSubscriptions: %v", err)
	}
	agentEvents := runtimebustest.SubscribeAdmission(t, eb, admission.CarrierOnly())
	if agentEvents == nil {
		t.Fatal("agent carrier admission returned no channel")
	}
	defer runtimebustest.Unsubscribe(eb, agentID)

	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("producer/account.ready"), "producer", "", json.RawMessage(`{"account_id":"acct-agent"}`), 0, eventBusTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].SubscriberType != "agent" || plan.DeliveryRoutes[0].SubscriberID != agentID {
		t.Fatalf("preflight failure/routes = %q/%#v, want agent-only connect route", plan.TargetFailure, plan.DeliveryRoutes)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case delivered := <-agentEvents:
		if delivered.ID() != evt.ID() {
			t.Fatalf("delivered event = %q, want %q", delivered.ID(), evt.ID())
		}
		if err := delivered.Complete(); err != nil {
			t.Fatalf("complete agent-only connect delivery: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent-only connect delivery")
	}
	var nodeDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'account-setup-node-one'
	`, evt.ID()).Scan(&nodeDeliveries); err != nil {
		t.Fatalf("count node deliveries: %v", err)
	}
	if nodeDeliveries != 0 {
		t.Fatalf("node deliveries = %d, want none without node route authority", nodeDeliveries)
	}
}

type waitInterceptor struct {
	started chan struct{}
	release chan struct{}
}

type providerReachabilityInterceptor struct{ reached chan<- struct{} }

func (i providerReachabilityInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case i.reached <- struct{}{}:
	default:
	}
	return true, nil, nil
}

func assertSelectedForkDispatchNotReached(t *testing.T, deliveries <-chan *runtimebus.LocalDelivery, providerReached <-chan struct{}) {
	t.Helper()
	select {
	case delivery := <-deliveries:
		_ = delivery.Complete()
		t.Fatalf("selected-fork delivery ran before commit: %s", delivery.ID())
	default:
	}
	select {
	case <-providerReached:
		t.Fatal("selected-fork provider boundary ran before commit")
	default:
	}
}

func (w waitInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return true, nil, nil
}

type deferredChainInterceptor struct{}

func (deferredChainInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	next := ""
	switch evt.Type() {
	case events.EventType("custom.root"):
		next = "custom.middle"
	case events.EventType("custom.middle"):
		next = "custom.leaf"
	case events.EventType("custom.leaf"):
		next = "custom.final"
	default:
		return true, nil, nil
	}
	return false, []events.Event{eventtest.RunCreatingRootIngress("", events.EventType(next), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()), time.Now().UTC())}, nil
}

type singleDeferredInterceptor struct{}

func (singleDeferredInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type() != events.EventType("custom.root") {
		return true, nil, nil
	}
	return false, []events.Event{eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.middle"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)}, nil
}

type deliveryContextDeferredInterceptor struct {
	t    *testing.T
	want events.DeliveryContext
}

func (i deliveryContextDeferredInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	switch evt.Type() {
	case events.EventType("custom.root"):
		next := eventtest.ForDelivery(eventtest.RunCreatingRootIngress(
			"",
			events.EventType("custom.middle"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
			time.Now().UTC(),
		), i.want)
		return false, []events.Event{next}, nil
	case events.EventType("custom.middle"):
		if got := events.DeliveryContextFromContext(ctx).ReplyContextID(); got != i.want.ReplyContextID() {
			i.t.Fatalf("deferred publish reply context = %q, want %q", got, i.want.ReplyContextID())
		}
	}
	return true, nil, nil
}

type nonTransactionalPersistedBeforeInterceptor struct {
	t     *testing.T
	store *recordingEventStore
}

func (i nonTransactionalPersistedBeforeInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("non-transactional interceptor unexpectedly ran with sql tx")
	}
	for _, got := range i.store.eventTypes() {
		if got == string(evt.Type()) {
			return true, nil, nil
		}
	}
	i.t.Fatalf("event type %s was not persisted before non-transactional interceptor ran; persisted=%v", evt.Type(), i.store.eventTypes())
	return true, nil, nil
}

type postCommitTxAbsentInterceptor struct {
	t              *testing.T
	store          *store.PostgresStore
	eventID        string
	called         chan struct{}
	wantRecipients []string
	wantScope      runtimereplayclaim.CommittedReplayScope
}

func (i postCommitTxAbsentInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("post-commit interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected event %s to be committed before interceptor ran", i.eventID)
	}
	if i.wantRecipients != nil {
		got, err := i.store.ListEventDeliveryRecipients(ctx, i.eventID)
		if err != nil {
			i.t.Fatalf("ListEventDeliveryRecipients(%s): %v", i.eventID, err)
		}
		assertSortedStringsEqual(i.t, got, i.wantRecipients)
	}
	if i.wantScope != "" {
		got, err := i.store.LoadCommittedReplayScope(ctx, i.eventID)
		if err != nil {
			i.t.Fatalf("LoadCommittedReplayScope(%s): %v", i.eventID, err)
		}
		if got != i.wantScope {
			i.t.Fatalf("committed replay scope = %q, want %q", got, i.wantScope)
		}
	}
	select {
	case i.called <- struct{}{}:
	default:
	}
	return true, nil, nil
}

type postCommitErrorInterceptor struct {
	t       *testing.T
	store   *store.PostgresStore
	eventID string
	err     error
}

func (i postCommitErrorInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("post-commit error interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected event %s to be committed before interceptor error", i.eventID)
	}
	return false, nil, i.err
}

type deferredEventVisibleInterceptor struct {
	t        *testing.T
	store    *store.PostgresStore
	eventID  string
	checkFor events.EventType
}

func (i deferredEventVisibleInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if evt.Type() == events.EventType("custom.root") {
		return false, []events.Event{
			eventtest.RunCreatingRootIngress(i.eventID, i.checkFor, "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		}, nil
	}
	if evt.Type() != i.checkFor {
		return true, nil, nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		i.t.Fatal("deferred event interceptor ran with sql tx still in context")
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected deferred event %s to be persisted before interceptors ran", i.eventID)
	}
	return true, nil, nil
}

type recordingLoggerHook struct {
	entries []recordedLogEntry
}

type recordedLogEntry struct {
	Action     string
	Detail     any
	Lineage    runtimecorrelation.RuntimeLineage
	HasLineage bool
	SourceFact runtimecorrelation.BundleSourceFact
	HasSource  bool
}

func (h *recordingLoggerHook) Log(ctx context.Context, _ diaglog.Level, _, _, action, _, _, _, _, _ string, _ map[string]string, detail any, _ *runtimefailures.Envelope, _ int) error {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	sourceFact, hasSource := runtimecorrelation.BundleSourceFactFromContext(ctx)
	h.entries = append(h.entries, recordedLogEntry{Action: action, Detail: detail, Lineage: lineage, HasLineage: ok, SourceFact: sourceFact, HasSource: hasSource})
	return nil
}

type descriptorAwareEventStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
	listErr     error
}

type routeSetEventStore struct {
	runtimebus.InMemoryEventStore
	mu     sync.Mutex
	routes map[string][]events.DeliveryRoute
}

type replayCapableAtomicStoreMissingScope struct {
	mu         sync.Mutex
	deliveries []string
}

func newRouteSetEventStore() *routeSetEventStore {
	return &routeSetEventStore{
		routes: map[string][]events.DeliveryRoute{},
	}
}

func (s *descriptorAwareEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{finalize: func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.deliveries = s.deliveries[:0]
		for _, route := range req.DeliveryRoutes {
			if route.SubscriberType == "agent" {
				s.deliveries = append(s.deliveries, route.SubscriberID)
			}
		}
		return nil
	}})
}

func (s *descriptorAwareEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return s.persistedDeliveries(), nil
}

func (s *descriptorAwareEventStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (s *descriptorAwareEventStore) persistedDeliveries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...)
}

func (s *routeSetEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{finalize: func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.routes == nil {
			s.routes = map[string][]events.DeliveryRoute{}
		}
		s.routes[req.Event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		return nil
	}})
}

func (s *routeSetEventStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *replayCapableAtomicStoreMissingScope) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{finalize: func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		if req.ReplayScope != "" {
			return runtimereplayclaim.ErrMissingCommittedReplayScope
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.deliveries = s.deliveries[:0]
		for _, route := range req.DeliveryRoutes {
			if route.SubscriberType == "agent" {
				s.deliveries = append(s.deliveries, route.SubscriberID)
			}
		}
		return nil
	}})
}

func (s *replayCapableAtomicStoreMissingScope) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...), nil
}

func (*replayCapableAtomicStoreMissingScope) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return nil, nil
}

func (*replayCapableAtomicStoreMissingScope) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return nil, false, nil
}

func (*replayCapableAtomicStoreMissingScope) ClaimPipelinePublication(context.Context, string) (runtimeownership.Lease, bool, error) {
	return sweeperClaimLease{}, true, nil
}

func (*replayCapableAtomicStoreMissingScope) SupportsPersistedReplay() bool { return true }

func assertSortedStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	slices.Sort(got)
	slices.Sort(want)
	if len(got) != len(want) {
		t.Fatalf("strings = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("strings = %v, want %v", got, want)
		}
	}
}

func seedActiveRuntimeBusAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "observer",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        []byte(`{}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func loadRunStateForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, string, string) {
	t.Helper()
	var runID, runStatus, triggerEventType string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(r.run_id::text, ''),
			COALESCE(r.status, ''),
			COALESCE(r.trigger_event_type, '')
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
	`, eventID).Scan(&runID, &runStatus, &triggerEventType); err != nil {
		t.Fatalf("load run state for %s: %v", eventID, err)
	}
	return runID, runStatus, triggerEventType
}

func countEventDeliveriesForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	return count
}

func countPipelineReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts for %s: %v", eventID, err)
	}
	return count
}

func TestEventBusPublishTransactionalPostCommitReceiptFailureIsRecoverable(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	failing := &failStandalonePipelineReceiptOnceStore{
		PostgresStore: pg,
		err:           errors.New("simulated post-commit receipt failure"),
	}
	logger := &recordingLoggerHook{}
	eb, err := newScopedTestEventBus(failing, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	agentID := "agent-post-commit-receipt"
	eventID := "21000000-0000-0000-0000-000000000011"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := runtimebustest.Subscribe(t, eb, agentID, events.EventType("custom.receipt_failure"))
	defer runtimebustest.Unsubscribe(eb, agentID)

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("custom.receipt_failure"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000012"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000012"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish with post-commit receipt failure: %v", err)
	}
	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted", eventID)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts = %d, want 0 after injected failure", got)
	}
	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	if !containsMissingPipelineReceiptEvent(missing, eventID) {
		t.Fatalf("missing pipeline receipt events = %#v, want %s", missing, eventID)
	}
	if !hasRuntimeLogAction(logger.entries, "pipeline_receipt_persist_failed") {
		t.Fatalf("logger entries = %#v, want pipeline_receipt_persist_failed", logger.entries)
	}
	got := requireBusEvent(t, ch, "post-commit receipt failure delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
}

func TestEventBusPublishTransactionalPostCommitCompletionFailureIsRecoverable(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	failing := &failNormalRunCompletionStore{
		PostgresStore: pg,
		err:           errors.New("simulated normal-run completion failure"),
	}
	logger := &recordingLoggerHook{}
	eb, err := newScopedTestEventBus(failing, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	agentID := "agent-post-commit-completion"
	eventID := "21000000-0000-0000-0000-000000000021"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := runtimebustest.Subscribe(t, eb, agentID, events.EventType("custom.completion_failure"))
	defer runtimebustest.Unsubscribe(eb, agentID)

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("custom.completion_failure"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000022"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000022"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish with post-commit completion failure: %v", err)
	}
	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted", eventID)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries = %d, want 1", got)
	}
	outcome, failure := loadPipelineReceiptOutcomeAndFailure(t, ctx, db, eventID)
	if outcome != "dead_letter" || failure == nil || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "normal_run_completion_failed" {
		t.Fatalf("pipeline receipt outcome=%q failure=%#v, want dead_letter with canonical failure", outcome, failure)
	}
	if !hasRuntimeLogAction(logger.entries, "publish_post_commit_convergence_failed") {
		t.Fatalf("logger entries = %#v, want publish_post_commit_convergence_failed", logger.entries)
	}
	got := requireBusEvent(t, ch, "post-commit completion failure delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
}

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	err error
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	if tx == nil && s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.PostgresStore.UpsertPipelineReceiptTx(ctx, tx, eventID, status, failure)
}

type failNormalRunCompletionStore struct {
	*store.PostgresStore
	err error
}

func (s *failNormalRunCompletionStore) ConvergeNormalRunCompletion(context.Context, string, []string, map[string][]string) error {
	return s.err
}

func loadPipelineReceiptOutcomeAndFailure(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, *runtimefailures.Envelope) {
	t.Helper()
	var outcome string
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, failure
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &raw); err != nil {
		t.Fatalf("load pipeline receipt for %s: %v", eventID, err)
	}
	if len(raw) == 0 {
		return outcome, nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode pipeline receipt failure for %s: %v", eventID, err)
	}
	return outcome, &failure
}

func containsMissingPipelineReceiptEvent(items []events.PersistedReplayEvent, eventID string) bool {
	for _, evt := range items {
		if strings.TrimSpace(evt.Event.ID()) == strings.TrimSpace(eventID) {
			return true
		}
	}
	return false
}

func hasRuntimeLogAction(entries []recordedLogEntry, action string) bool {
	for _, entry := range entries {
		if entry.Action == action {
			return true
		}
	}
	return false
}

func loadAgentDeliveryForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID, agentID string) (string, string) {
	t.Helper()
	var status, runStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(r.status, '')
		FROM event_deliveries d
		INNER JOIN runs r ON r.run_id = d.run_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, eventID, agentID).Scan(&status, &runStatus); err != nil {
		t.Fatalf("load delivery state for %s/%s: %v", eventID, agentID, err)
	}
	return status, runStatus
}

func TestEventBusPublish_LogsQueuedDeliveryLifecycleTransition(t *testing.T) {
	logger := &recordingLoggerHook{}
	bus, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := runtimebustest.Subscribe(t, bus, "agent-1", events.EventType("task.requested"))

	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)

	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = requireBusEvent(t, ch, "queued delivery lifecycle transition")

	var found bool
	for _, entry := range logger.entries {
		if entry.Action != "delivery_lifecycle_transition" {
			continue
		}
		detail, ok := entry.Detail.(map[string]any)
		if !ok {
			t.Fatalf("detail type = %T", entry.Detail)
		}
		if detail["delivery_state"] == "queued" && detail["subscriber_id"] == "agent-1" && detail["delivery_reason"] == "matched_agent_subscription" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("queued delivery lifecycle transition not found in logs: %#v", logger.entries)
	}
}

func TestEventBusPublish_AttachesTypedRuntimeDiagnosticLineage(t *testing.T) {
	logger := &recordingLoggerHook{}
	bus, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRuntimeLineage(context.Background(), runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               runID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	if err := bus.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var published recordedLogEntry
	for _, entry := range logger.entries {
		if entry.Action == "published" {
			published = entry
			break
		}
	}
	if !published.HasLineage {
		t.Fatalf("logger entries = %#v, want typed lineage on published diagnostic", logger.entries)
	}
	if published.Lineage.Owner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" ||
		published.Lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryDiagnostic ||
		published.Lineage.SubjectEventID != eventID ||
		published.Lineage.ParentEventID != eventID ||
		published.Lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal {
		t.Fatalf("published diagnostic lineage = %#v", published.Lineage)
	}
}

func TestEventBusPublish_AttachesBundleSourceFactToRuntimeLogs(t *testing.T) {
	logger := &recordingLoggerHook{}
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      "persisted",
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	bus, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Logger:           logger,
		BundleSourceFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := runtimebustest.Subscribe(t, bus, "agent-1", events.EventType("task.requested"))
	if err := bus.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("task.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		uuid.NewString(),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = requireBusEvent(t, ch, "bundle source fact delivery")
	for _, entry := range logger.entries {
		if entry.HasSource && entry.SourceFact == sourceFact.Normalized() {
			return
		}
	}
	t.Fatalf("bundle source fact not found in runtime logs: %#v", logger.entries)
}

func TestEventBusPublish_UsesPayloadValidator(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(_ context.Context, eventType string, payload []byte) error {
			if strings.TrimSpace(eventType) != "task.completed" {
				t.Fatalf("unexpected event type %q", eventType)
			}
			if string(payload) != `{"ok":true}` {
				t.Fatalf("unexpected payload %s", string(payload))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusPublish_FailsClosedWhenReplayCapableAtomicStoreOmitsCommittedReplayScope(t *testing.T) {
	store := &replayCapableAtomicStoreMissingScope{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.replay.checked"))

	err = eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("custom.replay.checked"), "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()))
	if !errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) {
		t.Fatalf("Publish error = %v, want missing committed replay scope", err)
	}
}

func TestEventBusPublishDirect_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.PublishDirect(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}), []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusCheckDirectRecipients_PayloadValidatorFailureAbortsBeforeRecipientPlanning(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(context.Context, string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	status, err := eb.CheckDirectRecipients(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}), []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
	if !slices.Equal(status.Requested, []string{"agent-a"}) {
		t.Fatalf("requested recipients = %#v, want agent-a", status.Requested)
	}
	if len(status.Recipients) != 0 || len(status.Filtered) != 0 || len(status.Missing) != 0 {
		t.Fatalf("recipient status after validation failure = %#v, want no planning result", status)
	}
}

func TestEventBusCheckPublishRecipientPlanReportsSubscribedPublishWithoutDelivery(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("task.completed"))

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if !slices.Equal(plan.Recipients, []string{"agent-a"}) {
		t.Fatalf("recipients = %#v, want agent-a", plan.Recipients)
	}
	if !slices.Equal(plan.PersistedRecipients, []string{"agent-a"}) {
		t.Fatalf("persisted recipients = %#v, want agent-a", plan.PersistedRecipients)
	}
	if !slices.Equal(plan.SubscriptionRecipients, []string{"agent-a"}) {
		t.Fatalf("subscription recipients = %#v, want agent-a", plan.SubscriptionRecipients)
	}
	select {
	case got := <-ch:
		t.Fatalf("CheckPublishRecipientPlan delivered event %#v", got)
	default:
	}
}

func TestEventBusPublishDirect_PersistsButDoesNotMarkDeliveredBeforeRealFanOut(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	err = eb.PublishDirect(context.Background(), eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	),
		[]string{"agent-a"})
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassTargetUnreachable || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" {
		t.Fatalf("PublishDirect failure = %#v, want authoritative delivery incomplete", failure)
	}
	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDirect_PreservesContextOnPersistedAndLiveDelivery(t *testing.T) {
	store := newRouteSetEventStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "agent-a"})
	ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.direct"))
	eventID := uuid.NewString()
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:direct-context"}}
	ctx := events.WithDeliveryContext(context.Background(), deliveryContext)

	if err := eb.PublishDirect(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	), []string{"agent-a"}); err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}
	got := requireBusEvent(t, ch, "context-carrying direct delivery")
	if got.DeliveryContext().ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("live direct context = %#v, want %#v", got.DeliveryContext(), deliveryContext)
	}
	store.mu.Lock()
	routes := append([]events.DeliveryRoute(nil), store.routes[eventID]...)
	store.mu.Unlock()
	if len(routes) != 1 || routes[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("persisted direct routes = %#v, want one context-carrying route", routes)
	}
}

func TestEventBusPublishDirect_RejectsAnyExplicitRecipientFilteredByMetadata(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: eventtest.UUID(eventtest.UUID("ent-1"))},
			{AgentID: "reviewer-ent-2", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := runtimebustest.Subscribe(t, eb, "control-plane")
	matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1")
	otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")

	err = eb.PublishDirect(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.direct"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	),
		[]string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"})
	if err == nil || !strings.Contains(err.Error(), "direct delivery rejected recipients: reviewer-ent-2, missing-agent") {
		t.Fatalf("PublishDirect error = %v, want exact filtered-recipient rejection", err)
	}
	requireNoBusEvent(t, controlCh, "rejected direct delivery to control-plane")
	requireNoBusEvent(t, matchCh, "rejected direct delivery to matching entity-scoped reviewer")
	requireNoBusEvent(t, otherCh, "direct delivery to filtered entity-scoped reviewer")
	assertSortedStringsEqual(t, store.persistedDeliveries(), nil)
}

func TestEventBusPublish_FiltersEntityScopedRecipientsByExplicitMetadata(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: eventtest.UUID(eventtest.UUID("ent-1"))},
			{AgentID: "reviewer-ent-2", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := runtimebustest.Subscribe(t, eb, "control-plane", events.EventType("custom.trigger"))
	matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1", events.EventType("custom.trigger"))
	otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, controlCh, "explicit metadata delivery to control-plane")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("control event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, matchCh, "explicit metadata delivery to entity-scoped reviewer")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "explicit metadata delivery to filtered entity-scoped reviewer")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"control-plane", "reviewer-ent-1"})
}

func TestEventBusPublish_FiltersEntityScopedRecipientsByTypedEnvelopeNotPayload(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer-ent-1", EntityID: eventtest.UUID(eventtest.UUID("ent-1"))},
			{AgentID: "reviewer-ent-2", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1", events.EventType("custom.trigger"))
	otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2", events.EventType("custom.trigger"))

	err = eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-2"}`),
		0,
		"",
		"",
		events.EventEnvelope{EntityID: eventtest.UUID(eventtest.UUID("ent-1"))},
		time.Now().UTC(),
	))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, matchCh, "typed-envelope delivery to entity-scoped reviewer")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "typed-envelope delivery to filtered entity-scoped reviewer")
	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"reviewer-ent-1"})
}

func TestEventBusPublish_DropsRecipientsMissingExplicitDescriptor(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := runtimebustest.Subscribe(t, eb, "control-plane", events.EventType("custom.trigger"))
	missingCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	_ = requireBusEvent(t, controlCh, "descriptor delivery to control-plane")
	requireNoBusEvent(t, missingCh, "descriptor delivery to missing reviewer")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"control-plane"})
}

func TestEventBusPublish_KeepsInternalSubscribersLiveOnlyUnderDescriptorPlanning(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	workflowCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.trigger"))
	nodeCh := subscribeInternalDeliveriesForTest(t, eb, "scan-orchestrator", events.EventType("custom.trigger"))
	agentCh := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.trigger"))
	missingCh := runtimebustest.Subscribe(t, eb, "agent-missing", events.EventType("custom.trigger"))

	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, workflowCh, "internal workflow-runtime descriptor delivery")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, nodeCh, "internal system-node descriptor delivery")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("system node event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "agent descriptor delivery")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, missingCh, "descriptor delivery to missing agent")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDeferred_UsesCanonicalSubscribedRecipientFiltering(t *testing.T) {
	store := &descriptorAwareEventStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
			{AgentID: "agent-b", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{singleDeferredInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.middle"))
	agentCh := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.middle"))
	otherCh := runtimebustest.Subscribe(t, eb, "agent-b", events.EventType("custom.middle"))

	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.root"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	evt := requireBusEvent(t, workflowCh, "deferred delivery to workflow-runtime")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("workflow-runtime event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "deferred delivery to agent")
	if got := evt.EntityID(); got != eventtest.UUID(eventtest.UUID("ent-1")) {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "deferred delivery to filtered agent")

	assertSortedStringsEqual(t, store.persistedDeliveries(), []string{"agent-a"})
}

func TestEventBusPublishDeferredRestoresEventDeliveryContext(t *testing.T) {
	store := &recordingEventStore{}
	want := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:deferred-publish"}}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deliveryContextDeferredInterceptor{t: t, want: want}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.root"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
}

func TestEventBusPublish_FailsClosedWhenDescriptorLookupFails(t *testing.T) {
	store := &descriptorAwareEventStore{
		listErr: errors.New("descriptor lookup failed"),
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, "reviewer-ent-1", events.EventType("custom.trigger"))

	err = eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	))
	if err == nil || !strings.Contains(err.Error(), "descriptor lookup failed") {
		t.Fatalf("Publish error = %v, want descriptor lookup failure", err)
	}
	requireNoBusEvent(t, ch, "descriptor lookup failure delivery")
	if got := store.persistedDeliveries(); len(got) != 0 {
		t.Fatalf("persisted deliveries = %v, want none", got)
	}
}

func TestEventBusWaitForQuiescenceWaitsForPublishCompletion(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}))
	}()

	requireSignalBefore(t, started, 500*time.Millisecond, "interceptor start")

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence error = %v, want deadline exceeded while publish is blocked", err)
	}

	close(release)
	if err := requireErrorBefore(t, publishDone, 500*time.Millisecond, "publish completion"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitCtx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after publish completion: %v", err)
	}
}

func TestEventBusPublishAcknowledgedReturnsBeforePostCommitDispatchCompletes(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	const eventID = "11111111-1111-1111-1111-111111111136"
	const agentID = "agent-acknowledged-publish"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, agentID, events.EventType("task.completed"))
	defer runtimebustest.Unsubscribe(eb, agentID)

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.PublishAcknowledged(ctx, eventtest.RunCreatingRootIngress(
			eventID,
			events.EventType("task.completed"),
			"api.v1",
			"",
			[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111137"}`),
			0,
			eventBusTestRunID,
			"",
			events.EventEnvelope{EntityID: "11111111-1111-1111-1111-111111111137"},
			time.Now().UTC()))
	}()
	requireSignalBefore(t, started, 5*time.Second, "async post-commit interceptor start")
	if err := requireErrorBefore(t, publishDone, 5*time.Second, "acknowledged publish return before interceptor release"); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}

	ok, err := pg.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists(%s): %v", eventID, err)
	}
	if !ok {
		t.Fatalf("event %s was not persisted before acknowledged publish returned", eventID)
	}
	gotRecipients, err := pg.ListEventDeliveryRecipients(ctx, eventID)
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients(%s): %v", eventID, err)
	}
	assertSortedStringsEqual(t, gotRecipients, []string{agentID})
	gotScope, err := pg.LoadCommittedReplayScope(ctx, eventID)
	if err != nil {
		t.Fatalf("LoadCommittedReplayScope(%s): %v", eventID, err)
	}
	if gotScope != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want %q", gotScope, runtimereplayclaim.CommittedReplayScopeSubscribed)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence while async dispatch blocked = %v, want deadline exceeded", err)
	}
	requireNoBusEvent(t, ch, "acknowledged publish before interceptor release")

	close(release)
	deliveryTimer := time.NewTimer(time.Second)
	defer deliveryTimer.Stop()
	var got events.Event
	select {
	case delivery := <-ch:
		got = delivery.Event()
		_ = delivery.Complete()
	case <-deliveryTimer.C:
		t.Fatal("acknowledged publish delivery: timed out waiting for queued bus event")
	}
	if got.ID() != eventID {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), eventID)
	}
	waitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after acknowledged dispatch completion: %v", err)
	}
}

func TestEventBusForegroundPublicationClaimBlocksSiblingReplayOnSQLiteAndPostgres(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, selected, selected.DB, "?"
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), storetest.AdmitPostgresRuntimeStore(t, db), db, "$1::uuid"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foregroundStore, siblingStore, db, runPlaceholder := tc.open(t)
			runID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if _, err := db.ExecContext(context.Background(), "INSERT INTO runs (run_id, status) VALUES ("+runPlaceholder+", 'running')", runID); err != nil {
				t.Fatalf("insert run: %v", err)
			}
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			foreground, err := newScopedTestEventBus(foregroundStore, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
			})
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := newScopedTestEventBus(siblingStore)
			if err != nil {
				t.Fatal(err)
			}
			publishDone := make(chan error, 1)
			go func() {
				publishDone <- foreground.PublishAcknowledged(context.Background(), eventtest.RunCreatingRootIngress(
					eventID, events.EventType("custom.shared_claim"), "test", "", []byte(`{}`), 0, runID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()))
			}()
			requireSignalBefore(t, started, 5*time.Second, "foreground dispatch start")
			if err := requireErrorBefore(t, publishDone, 5*time.Second, "acknowledged foreground publish"); err != nil {
				t.Fatal(err)
			}

			if got, err := sibling.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 0 {
				t.Fatalf("sibling sweep while foreground owns event = %d, %v; want 0, nil", got, err)
			}
			close(release)
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := foreground.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait foreground completion: %v", err)
			}
			if got, err := sibling.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 0 {
				t.Fatalf("sibling sweep after foreground settlement = %d, %v; want 0, nil", got, err)
			}
		})
	}
}

type publicationClaimBarrierStore struct {
	*store.PostgresStore
	claimed chan<- struct{}
	release <-chan struct{}
}

type replayClaimBarrierStore struct {
	*store.PostgresStore
	eventID string
	claimed chan<- struct{}
	release <-chan struct{}
}

func (s *replayClaimBarrierStore) awaitClaim(ctx context.Context, lease runtimeownership.Lease, claimed bool, err error) (runtimeownership.Lease, bool, error) {
	if err != nil || !claimed {
		return lease, claimed, err
	}
	select {
	case s.claimed <- struct{}{}:
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
	select {
	case <-s.release:
		return lease, true, nil
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
}

func (s *replayClaimBarrierStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelineReplay(ctx, eventID)
	return s.awaitClaim(ctx, lease, claimed, err)
}

func (s *replayClaimBarrierStore) ClaimPipelineSettlement(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelineSettlement(ctx, eventID)
	return s.awaitClaim(ctx, lease, claimed, err)
}

func (s *replayClaimBarrierStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListEventsMissingPipelineReceipt(ctx, since, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func (s *replayClaimBarrierStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListEventsMissingPipelineReceiptForRun(ctx, runID, since, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func (s *replayClaimBarrierStore) ListDueDecisionRouteObligations(ctx context.Context, now time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	records, err := s.PostgresStore.ListDueDecisionRouteObligations(ctx, now, 200)
	return filterReplayPoolRecords(records, s.eventID), err
}

func filterReplayPoolRecords(records []events.PersistedReplayEvent, eventID string) []events.PersistedReplayEvent {
	for _, record := range records {
		if record.Event.ID() == eventID {
			return []events.PersistedReplayEvent{record}
		}
	}
	return nil
}

func (s *publicationClaimBarrierStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	lease, claimed, err := s.PostgresStore.ClaimPipelinePublication(ctx, eventID)
	if err != nil || !claimed {
		return lease, claimed, err
	}
	select {
	case s.claimed <- struct{}{}:
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
	select {
	case <-s.release:
		return lease, true, nil
	case <-ctx.Done():
		_ = lease.Release(context.WithoutCancel(ctx))
		return nil, false, ctx.Err()
	}
}

func TestEventBusPostgresPublicationClaimsDoNotExhaustPersistencePool(t *testing.T) {
	const poolSize = 4
	for _, form := range []string{"synchronous", "acknowledged"} {
		t.Run(form, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			db.SetMaxOpenConns(poolSize)
			db.SetMaxIdleConns(poolSize)

			claimed := make(chan struct{}, poolSize)
			release := make(chan struct{})
			selected := &publicationClaimBarrierStore{PostgresStore: pg, claimed: claimed, release: release}
			bus, err := newScopedTestEventBus(selected)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := make(chan struct{})
			eventIDs := make([]string, poolSize)
			runIDs := make([]string, poolSize)
			errs := make(chan error, poolSize)
			for i := 0; i < poolSize; i++ {
				eventIDs[i] = uuid.NewString()
				runIDs[i] = uuid.NewString()
				if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runIDs[i]); err != nil {
					t.Fatalf("insert run: %v", err)
				}
				eventID := eventIDs[i]
				runID := runIDs[i]
				go func() {
					<-start
					evt := eventtest.RunCreatingRootIngress(
						eventID, events.EventType("custom.pool_saturation"), "test", "", []byte(`{}`), 0, runID, "",
						events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), time.Now().UTC(),
					)
					switch form {
					case "synchronous":
						errs <- bus.Publish(ctx, evt)
					case "acknowledged":
						errs <- bus.PublishAcknowledged(ctx, evt)
					}
				}()
			}
			close(start)
			for i := 0; i < poolSize; i++ {
				requireSignalBefore(t, claimed, 5*time.Second, "aligned PostgreSQL publication claim")
			}
			close(release)
			for i := 0; i < poolSize; i++ {
				if err := requireErrorBefore(t, errs, 10*time.Second, "pool-saturated publication"); err != nil {
					t.Fatal(err)
				}
			}
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer waitCancel()
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait for publication settlement: %v", err)
			}
			for _, eventID := range eventIDs {
				var count int
				if err := db.QueryRowContext(context.Background(), `
					SELECT COUNT(*)
					FROM event_receipts
					WHERE event_id = $1::uuid
					  AND subscriber_type = 'platform'
					  AND subscriber_id = 'pipeline'
				`, eventID).Scan(&count); err != nil {
					t.Fatalf("count pipeline receipt for %s: %v", eventID, err)
				}
				if count != 1 {
					t.Fatalf("pipeline receipt count for %s = %d, want 1", eventID, count)
				}
			}
		})
	}
}

func TestEventBusPostgresReplayClaimsDoNotExhaustPersistencePool(t *testing.T) {
	const poolSize = 4
	for _, surface := range []string{"generic_periodic", "decision_periodic", "run_queue", "startup"} {
		t.Run(surface, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			seedStore := storetest.AdmitPostgresRuntimeStore(t, db)
			if _, err := newScopedTestEventBus(seedStore); err != nil {
				t.Fatalf("prepare replay seed author activity catalog: %v", err)
			}
			eventIDs := make([]string, poolSize)
			runIDs := make([]string, poolSize)
			decisionRoute := surface == "decision_periodic"
			for i := 0; i < poolSize; i++ {
				runIDs[i] = uuid.NewString()
				if _, err := db.ExecContext(context.Background(), `
					INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
					VALUES ($1::uuid, 'running', $2, $3, $4)
				`, runIDs[i], authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint); err != nil {
					t.Fatalf("insert run: %v", err)
				}
				eventIDs[i] = seedReplayPoolEvent(t, seedStore, runIDs[i], decisionRoute)
			}
			db.SetMaxOpenConns(poolSize)
			db.SetMaxIdleConns(poolSize)

			claimed := make(chan struct{}, poolSize)
			release := make(chan struct{})
			start := make(chan struct{})
			errs := make(chan error, poolSize)
			for i := 0; i < poolSize; i++ {
				selected := &replayClaimBarrierStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db), eventID: eventIDs[i], claimed: claimed, release: release,
				}
				bus, err := newScopedTestEventBus(selected)
				if err != nil {
					t.Fatal(err)
				}
				runID := runIDs[i]
				go func() {
					<-start
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					switch surface {
					case "generic_periodic", "decision_periodic":
						_, err := bus.SweepUndispatched(ctx, time.Hour, 10)
						errs <- err
					case "run_queue":
						_, err := bus.ReleaseRunQueue(ctx, runID, time.Hour, 10)
						errs <- err
					case "startup":
						errs <- runtimepipeline.NewRecoveryManagerWith(selected, bus).Recover(ctx)
					}
				}()
			}
			close(start)
			for i := 0; i < poolSize; i++ {
				requireSignalBefore(t, claimed, 5*time.Second, "aligned PostgreSQL replay claim")
			}
			close(release)
			for i := 0; i < poolSize; i++ {
				if err := requireErrorBefore(t, errs, 10*time.Second, "pool-saturated replay"); err != nil {
					t.Fatal(err)
				}
			}
			for _, eventID := range eventIDs {
				var count int
				if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("pipeline receipt count for %s = %d, want 1", eventID, count)
				}
				if decisionRoute {
					var status string
					if err := db.QueryRowContext(context.Background(), `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`, eventID).Scan(&status); err != nil || status != "completed" {
						t.Fatalf("decision route status for %s = %q, %v; want completed", eventID, status, err)
					}
				}
			}
		})
	}
}

func seedReplayPoolEvent(t *testing.T, selected *store.PostgresStore, runID string, decisionRoute bool) string {
	t.Helper()
	eventID, entityID := uuid.NewString(), uuid.NewString()
	eventType := events.EventType("custom.replay_pool_saturation")
	payload := []byte(`{}`)
	if decisionRoute {
		snapshot, err := decisioncard.FreezeSnapshot("launch_review", "", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		})
		if err != nil {
			t.Fatal(err)
		}
		anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
			FlowInstance: "launch/replay-pool", FlowID: "launch", EntityID: entityID,
			Stage: "awaiting_review", StageActivationID: uuid.NewString(),
		})
		if err != nil {
			t.Fatal(err)
		}
		card, err := decisioncard.New(decisioncard.Card{
			CardID: uuid.NewString(), RunID: runID, Anchor: anchor,
			ExecutionMode:    "live",
			Snapshot:         snapshot,
			BundleHash:       "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EffectiveCadence: decisioncard.Cadence{ReminderInterval: "24h", InputDraftTTL: "15m"}, CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := selected.CreateDecisionCard(testAuthorActivityContext(context.Background()), card); err != nil {
			t.Fatal(err)
		}
		if _, err := selected.DecideDecisionCard(testAuthorActivityContext(context.Background()), decisioncard.DecideRequest{
			CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash,
			DecisionEventID: eventID, Now: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		eventType = events.EventType("mailbox.card_decided")
		payload = []byte(`{"card_id":"` + card.CardID + `"}`)
	}
	evt := eventtest.RunCreatingRootIngress(eventID, eventType, "test", "", payload, 0, runID, "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
	storetest.CommitSemanticEventWithRoutes(t, testAuthorActivityContext(context.Background()), selected, evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
	return eventID
}

func TestEventBusPublish_InterceptsMultiHopDeferredChains(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredChainInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("", events.EventType("custom.root"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))), time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.eventTypes(); len(got) < 4 || got[0] != "custom.root" || got[1] != "custom.middle" || got[2] != "custom.leaf" || got[3] != "custom.final" {
		t.Fatalf("persisted event types prefix = %v, want prefix [custom.root custom.middle custom.leaf custom.final]", got)
	}
}

func TestEventBusPublishNonTransactional_PersistsBeforeInterceptorsRun(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{nonTransactionalPersistedBeforeInterceptor{t: t, store: store}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.non_transactional"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublishTransactional_RunsInterceptorsAfterCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eventID := "11111111-1111-1111-1111-111111111111"
	agentID := "agent-post-commit-publish"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	called := make(chan struct{}, 1)
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{postCommitTxAbsentInterceptor{
			t:              t,
			store:          pg,
			eventID:        eventID,
			called:         called,
			wantRecipients: []string{agentID},
			wantScope:      runtimereplayclaim.CommittedReplayScopeSubscribed,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, agentID, events.EventType("task.completed"))
	defer runtimebustest.Unsubscribe(eb, agentID)
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("task.completed"),
		"api.v1",
		"",
		[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111114"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111114"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	requireSignalBefore(t, called, time.Second, "post-commit interceptor")
	got := requireBusEvent(t, ch, "post-commit delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), eventID)
	}
}

func TestSelectedForkCommitFailurePreventsDispatchAndProviderReachability(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	providerReached := make(chan struct{}, 1)
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{providerReachabilityInterceptor{reached: providerReached}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	deliveries := runtimebustest.Subscribe(t, eb, "selected-worker", events.EventType("item.received"))
	defer runtimebustest.Unsubscribe(eb, "selected-worker")

	sourceRunID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkRunID := uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(forkRunID, sourceRunID, sourceEventID, "selection:pre-dispatch-proof", "fork-task", "live")
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.SelectedForkReplay(
		uuid.NewString(), "item.received", eventtest.Producer(events.EventProducerNode, "selected-node"), "fork-task",
		[]byte(`{"selected":true}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC(),
	)
	prepared, err := eb.PrepareSelectedForkPublish(ctx, event)
	if err != nil {
		t.Fatalf("PrepareSelectedForkPublish: %v", err)
	}
	assertSelectedForkDispatchNotReached(t, deliveries, providerReached)

	outcome, err := pg.CommitSelectedForkEvent(ctx, store.CommitSelectedForkEventRequest{
		Commit: prepared.CommitRequest(),
		Lineage: store.RunForkSelectedContractExecutionLineage{
			ForkRunID: forkRunID, SourceRunID: sourceRunID, SourceEventID: sourceEventID,
			ForkEventID: event.ID(), EventName: string(event.Type()), SelectionAuthority: lineage.AuthorityStamp(), CreatedAt: event.CreatedAt(),
		},
	})
	if err == nil || outcome != runtimebus.EventAppendOutcomeUnknown {
		t.Fatalf("commit outcome=%v err=%v, want missing-source rollback", outcome, err)
	}
	eb.AbandonPreparedPublish(ctx, prepared)
	assertSelectedForkDispatchNotReached(t, deliveries, providerReached)
	exists, err := pg.EventExists(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed selected-fork operation left an event visible")
	}
}

func TestEventBusPublishTransactional_ReturnsPostCommitInterceptorErrorAndRecordsReceipt(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eventID := "11111111-1111-1111-1111-111111111112"
	wantErr := errors.New("post-commit interceptor failure")
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{postCommitErrorInterceptor{
			t:       t,
			store:   pg,
			eventID: eventID,
			err:     wantErr,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("task.failed"),
		"",
		"",
		[]byte(`{"entity_id":"11111111-1111-1111-1111-111111111114"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111114"),
		time.Now().UTC(),
	))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Publish error = %v, want %v", err, wantErr)
	}
	var outcome, failureClass, detailCode string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &failureClass, &detailCode); err != nil {
		t.Fatalf("load pipeline receipt: %v", err)
	}
	if outcome != "dead_letter" || failureClass != string(runtimefailures.ClassInternalFailure) || detailCode != "event_interceptor_failed" {
		t.Fatalf("pipeline receipt = outcome:%q class:%q detail:%q, want canonical event_interceptor_failed", outcome, failureClass, detailCode)
	}
}

func TestEventBusPublishTransactional_RecordsTargetFailureDeadLetter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	eventID := uuid.NewString()
	targetEntityID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("child/output.done"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: targetEntityID, FlowInstance: "missing-flow"}),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var reason, targetContext string
	if err := db.QueryRowContext(ctx, `
		SELECT failure->'detail'->>'code', COALESCE((failure->'detail'->'attributes'->'target')::text, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure->>'class' = 'platform.target_unreachable'
		  AND handler_node = 'pin_routing'
	`, eventID).Scan(&reason, &targetContext); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if reason != "target_unreachable_terminated" {
		t.Fatalf("target failure reason = %q, want target_unreachable_terminated", reason)
	}
	if !strings.Contains(targetContext, "missing-flow") {
		t.Fatalf("target context = %s, want missing-flow", targetContext)
	}
}

func TestEventBusPublishInMutationSQLiteRecordsTargetFailureDeadLetter(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	eb, err := newScopedTestEventBus(sqliteStore, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		AgentID:      "live-other",
		EntityID:     uuid.NewString(),
		FlowInstance: "other-flow",
	})
	ctx := context.Background()
	descriptors, err := eb.PinRoutingDescriptors(ctx)
	if err != nil {
		t.Fatalf("PinRoutingDescriptors: %v", err)
	}
	if len(descriptors) == 0 {
		t.Fatal("PinRoutingDescriptors returned no runtime descriptors")
	}
	if descriptors[0].ID != "live-other" {
		t.Fatalf("PinRoutingDescriptors[0].ID = %q, want live-other", descriptors[0].ID)
	}
	_ = runtimebustest.Subscribe(t, eb, "live-other", events.EventType("task.completed"))
	if got := eb.ResolveSubscribedRecipients("task.completed"); !slices.Equal(got, []string{"live-other"}) {
		t.Fatalf("ResolveSubscribedRecipients = %#v, want live-other", got)
	}
	eventID := uuid.NewString()
	targetEntityID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("task.completed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: targetEntityID, FlowInstance: "missing-flow"}),
		time.Now().UTC(),
	)

	if !evt.HasTargetRoute() {
		t.Fatalf("event target route missing after construction: envelope=%#v", evt.NormalizedEnvelope())
	}
	plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "target_unreachable_terminated" {
		t.Fatalf("target failure = %q, want target_unreachable_terminated: plan=%#v", plan.TargetFailure, plan)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var reason, targetContext string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COALESCE(json_extract(failure, '$.detail.code'), ''), COALESCE(json_extract(failure, '$.detail.attributes.target'), '')
		FROM dead_letters
		WHERE original_event_id = ?
		  AND json_extract(failure, '$.class') = 'platform.target_unreachable'
		  AND handler_node = 'pin_routing'
	`, eventID).Scan(&reason, &targetContext); err != nil {
		t.Fatalf("query sqlite dead_letters: %v", err)
	}
	if reason != "target_unreachable_terminated" {
		t.Fatalf("target failure reason = %q, want target_unreachable_terminated", reason)
	}
	if !strings.Contains(targetContext, "missing-flow") {
		t.Fatalf("target context = %s, want missing-flow", targetContext)
	}
	var pipelineReceipts int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&pipelineReceipts); err != nil {
		t.Fatalf("query sqlite pipeline receipt: %v", err)
	}
	if pipelineReceipts != 1 {
		t.Fatalf("sqlite pipeline receipts = %d, want 1", pipelineReceipts)
	}
}

func TestEventBusPublish_ClassifiesCanonicalRunBundleSourceThroughRunLifecycleOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	fingerprint := "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash: "bundle-v1:" + fingerprint, BundleSource: "ephemeral", BundleFingerprint: fingerprint,
	}
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		BundleFingerprint: fingerprint,
		BundleSourceFact:  sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(uuid.NewString(),

		events.EventType("scan.requested"),
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var bundleHash, bundleSource, storedFingerprint string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&bundleHash, &bundleSource, &storedFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if bundleHash != sourceFact.BundleHash || bundleSource != sourceFact.BundleSource || storedFingerprint != fingerprint {
		t.Fatalf("bundle identity = hash:%q source:%q fingerprint:%q, want canonical ephemeral source", bundleHash, bundleSource, storedFingerprint)
	}
}

func TestEventBusPublishDirect_StampsBundleSourceFactOnRunRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444",
		BundleSource:      "persisted",
		BundleFingerprint: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, sourceFact.BundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		BundleSourceFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source, status)
		VALUES ('agent-a', 'bundle-source-test', 'worker', 'regular', TRUE, 'authored', 'active')
	`); err != nil {
		t.Fatalf("seed direct recipient: %v", err)
	}
	runtimebustest.Subscribe(t, eb, "agent-a")
	if err := eb.PublishDirect(context.Background(), eventtest.RunCreatingRootIngress(uuid.NewString(),

		events.EventType("scan.requested"),
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),

		[]string{"agent-a"}); err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}
	var bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if bundleHash != sourceFact.BundleHash || bundleSource != sourceFact.BundleSource || legacyFingerprint != sourceFact.BundleFingerprint {
		t.Fatalf("bundle identity = hash:%q source:%q fingerprint:%q, want canonical source fact %#v", bundleHash, bundleSource, legacyFingerprint, sourceFact)
	}
}

func TestEventBusPublishDeferred_RunsInterceptorsAfterDeferredEventCommit(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eventID := "22222222-2222-2222-2222-222222222222"
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredEventVisibleInterceptor{
			t:        t,
			store:    pg,
			eventID:  eventID,
			checkFor: events.EventType("custom.middle"),
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("11111111-1111-1111-1111-111111111111",
		events.EventType("custom.root"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_DoesNotInferLineageFromInboundContext(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), eventtest.RunCreatingRootIngress(eventtest.UUID("evt-parent"),
		events.EventType("task.started"), "", "", nil, 0, "run-abc", "", events.EventEnvelope{}, time.Time{}))
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(eventtest.UUID(eventtest.UUID("evt-child")),
		events.EventType("task.completed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.events) != 1 {
		found := false
		for _, evt := range store.events {
			if evt.ID() != eventtest.UUID(eventtest.UUID("evt-child")) {
				continue
			}
			found = true
			if got := evt.RunID(); got == "run-abc" {
				t.Fatalf("persisted run_id = %q, inherited ambient inbound run", got)
			}
			if got := evt.ParentEventID(); got != "" {
				t.Fatalf("persisted parent_event_id = %q, want no inferred parent", got)
			}
		}
		if !found {
			t.Fatalf("persisted events = %#v, want child event", store.events)
		}
		return
	}
	if got := store.events[0].RunID(); got == "run-abc" {
		t.Fatalf("persisted run_id = %q, inherited ambient inbound run", got)
	}
	if got := store.events[0].ParentEventID(); got != "" {
		t.Fatalf("persisted parent_event_id = %q, want no inferred parent", got)
	}
}

func TestEventBusPublish_ZeroRecipientsDoesNotEmitContradiction(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(eventtest.UUID(eventtest.UUID("evt-zero")),
		events.EventType("custom.no_subscribers"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "custom.no_subscribers" {
		t.Fatalf("persisted event types = %v, want [custom.no_subscribers]", got)
	}
}

func TestEventBusPublish_RuntimeControlBypassesContradictionRouting(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RuntimeControl(
		uuid.NewString(), "platform.paused", "runtime", "", nil, 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Time{},
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "platform.paused" {
		t.Fatalf("persisted event types = %v, want [platform.paused]", got)
	}
}

func TestEventBusPublish_RuntimeOwnedStandalonePlatformRunsConvergeWithoutPersistedDeliveries(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	testCases := []struct {
		name      string
		eventID   string
		eventType events.EventType
		event     func(id string, eventType events.EventType) events.Event
	}{
		{
			name:      "platform.boot",
			eventID:   "10000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.boot"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeControl(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "platform.recovery_failed",
			eventID:   "10000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.recovery_failed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			internal := subscribeInternalDeliveriesForTest(t, eb, "internal-"+string(tc.eventType), tc.eventType)
			defer runtimebustest.Unsubscribe(eb, "internal-"+string(tc.eventType))

			if err := eb.Publish(ctx, tc.event(tc.eventID, tc.eventType)); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			got := requireBusEvent(t, internal, "standalone platform event internal delivery")
			if got.ID() != tc.eventID {
				t.Fatalf("internal delivery event_id = %q, want %q", got.ID(), tc.eventID)
			}

			runID, runStatus, triggerEventType := loadRunStateForEvent(t, ctx, db, tc.eventID)
			if strings.TrimSpace(runID) == "" {
				t.Fatalf("run_id missing for %s", tc.eventType)
			}
			if runStatus != "completed" {
				t.Fatalf("run status for %s = %q, want completed", tc.eventType, runStatus)
			}
			if triggerEventType != string(tc.eventType) {
				t.Fatalf("trigger_event_type for %s = %q, want %q", tc.eventType, triggerEventType, tc.eventType)
			}
			if got := countEventDeliveriesForEvent(t, ctx, db, tc.eventID); got != 0 {
				t.Fatalf("event_deliveries for %s = %d, want 0", tc.eventType, got)
			}
		})
	}
}

func TestEventBusPublish_RuntimeOwnedStandalonePlatformRunsConvergeAfterFinalReceipt(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentID := "agent-runtime-owned-platform"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)

	testCases := []struct {
		name      string
		eventID   string
		eventType events.EventType
		event     func(id string, eventType events.EventType) events.Event
	}{
		{
			name:      "manager platform.agent_failed",
			eventID:   "20000000-0000-0000-0000-000000000001",
			eventType: events.EventType("platform.agent_failed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "receipts platform.paused",
			eventID:   "20000000-0000-0000-0000-000000000002",
			eventType: events.EventType("platform.paused"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeControl(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "budget platform.budget_threshold_crossed",
			eventID:   "20000000-0000-0000-0000-000000000003",
			eventType: events.EventType("platform.budget_threshold_crossed"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", platformSignalFixturePayload(t, eventType), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
		{
			name:      "run lifecycle platform.run_stalled",
			eventID:   "20000000-0000-0000-0000-000000000004",
			eventType: events.EventType("platform.run_stalled"),
			event: func(id string, eventType events.EventType) events.Event {
				return eventtest.RuntimeDiagnostic(id, eventType, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subscription := runtimebustest.Subscribe(t, eb, agentID, tc.eventType)
			defer runtimebustest.Unsubscribe(eb, agentID)

			if err := eb.Publish(ctx, tc.event(tc.eventID, tc.eventType)); err != nil {
				t.Fatalf("Publish(%s): %v", tc.eventType, err)
			}

			got := requireBusEvent(t, subscription, "standalone agent event delivery")
			if got.ID() != tc.eventID {
				t.Fatalf("delivered event_id = %q, want %q", got.ID(), tc.eventID)
			}

			if got := countEventDeliveriesForEvent(t, ctx, db, tc.eventID); got != 1 {
				t.Fatalf("event_deliveries for %s = %d, want 1", tc.eventType, got)
			}
			if deliveryStatus, runStatus := loadAgentDeliveryForEvent(t, ctx, db, tc.eventID, agentID); deliveryStatus != "pending" || runStatus != "running" {
				t.Fatalf("pre-receipt state for %s = delivery:%q run:%q, want pending/running", tc.eventType, deliveryStatus, runStatus)
			}

			route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}
			claimed, err := pg.ClaimAgentDelivery(ctx, got, route)
			if err != nil {
				t.Fatalf("ClaimAgentDelivery(%s): %v", tc.eventType, err)
			}
			if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, 0); err != nil {
				t.Fatalf("SettleSuccess(%s): %v", tc.eventType, err)
			}
			if err := eb.ConvergeDeliveryRunCompletion(ctx, got); err != nil {
				t.Fatalf("ConvergeDeliveryRunCompletion(%s): %v", tc.eventType, err)
			}

			deliveryStatus, runStatus := loadAgentDeliveryForEvent(t, ctx, db, tc.eventID, agentID)
			if deliveryStatus != "delivered" {
				t.Fatalf("delivery status for %s = %q, want delivered", tc.eventType, deliveryStatus)
			}
			if runStatus != "completed" {
				t.Fatalf("run status for %s = %q, want completed", tc.eventType, runStatus)
			}
		})
	}
}

func platformSignalFixturePayload(t *testing.T, eventType events.EventType) []byte {
	t.Helper()
	payload := map[string]any{}
	switch eventType {
	case events.EventType("platform.agent_failed"), events.EventType("platform.recovery_failed"):
		failure := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassInternalFailure,
			"unclassified_runtime_error",
			"eventbus-test",
			"publish_platform_signal",
			nil,
		), "eventbus-test", "publish_platform_signal")
		payload["failure"] = &failure
	case events.EventType("platform.budget_threshold_crossed"):
		payload["level"] = "warning"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal platform signal fixture %s: %v", eventType, err)
	}
	return raw
}

func TestEventBusRuntimeIngressPauseQueuesAndResumeReleases(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, eb, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	eb.SetRuntimeIngressDispatchGate(controller)

	agentID := "agent-paused-queue"
	eventType := events.EventType("custom.paused")
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := runtimebustest.Subscribe(t, eb, agentID, eventType)
	defer runtimebustest.Unsubscribe(eb, agentID)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	eventID := "21000000-0000-0000-0000-000000000001"
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000002"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish while paused: %v", err)
	}

	requireNoBusEvent(t, ch, "paused runtime before resume")
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries while paused = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireBusEvent(t, ch, "queued event release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
}

func TestEventBusPublish_HumanTaskEventsRouteBySubscriptionOnly(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, "requester")
	defer runtimebustest.Unsubscribe(eb, "requester")

	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress("", events.EventType("human_task.approved"), "", "", []byte(`{"requesting_agent":"requester"}`), 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	requireNoBusEvent(t, ch, "human task event without subscription")
}

func TestEventBusPublish_RecordsNoRoutedDiagnosticsForRetiredSiblingAutoWire(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	eb, err := newScopedTestEventBus(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	runtimebustest.Subscribe(t, eb, "scan-orchestrator")
	defer runtimebustest.Unsubscribe(eb, "scan-orchestrator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress("", "producer/scan.requested", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(eventtest.UUID("ent-1"))), time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if diags[0].EventType != "producer/scan.requested" {
		t.Fatalf("event_type = %q", diags[0].EventType)
	}
	if len(diags[0].RoutedRecipients) != 0 {
		t.Fatalf("routed_recipients = %#v, want none for retired sibling auto-wire", diags[0].RoutedRecipients)
	}
}

func TestEventBusPublish_NestedDescendantCompletionFollowsDeclaredAncestorConnects(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		DeliveryStore: pg,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := runtimepipeline.FlowInstanceEntityID("child")
	grandchildEntityID := runtimepipeline.FlowInstanceEntityID("child/grandchild")
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	ctx := eventBusTestRunContext(t, db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    bundle.WorkflowName(),
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "idle",
			Metadata: map[string]any{
				"entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      childEntityID,
			StorageRef:      "child",
			WorkflowName:    "child",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "delegated",
			Metadata: map[string]any{
				"parent_entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      grandchildEntityID,
			StorageRef:      "child/grandchild",
			WorkflowName:    "grandchild",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "finished",
		},
	} {
		if err := store.Upsert(ctx, instance); err != nil {
			t.Fatalf("seed workflow instance %q: %v", instance.InstanceID, err)
		}
	}

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		"11111111-2222-3333-4444-555555555555",
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+grandchildEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	child, found, err := store.Load(ctx, childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "completed" {
		t.Fatalf("child current_state = %q, want completed", got)
	}

	root, found, err := store.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "done" {
		t.Fatalf("root current_state = %q, want done through declared ancestor connects", got)
	}

	var emitted []string
	rows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		emitted = append(emitted, strings.TrimSpace(name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events: %v", err)
	}
	if contains(emitted, "child/step.result") {
		t.Fatalf("events = %v, do not want child/step.result", emitted)
	}
	if !contains(emitted, "pipeline.complete") {
		t.Fatalf("events = %v, want pipeline.complete through declared ancestor connects", emitted)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func TestEventBusPublish_MixedEmptyAndTargetedNodeRoutesExecuteAndSettle(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := eventBusTestRunContext(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	module, bundle := mixedNodeRouteWorkflowModule(t)
	const eventType = "child/child.start"
	const rootEntityID = "11111111-1111-1111-1111-222222222222"
	const childEntityID = "11111111-1111-1111-1111-333333333333"
	emptyRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "project-observer",
	}
	targetRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "child-intake",
		Target: events.RouteIdentity{
			FlowInstance: "child",
			EntityID:     childEntityID,
		},
	}

	var pc *runtimepipeline.PipelineCoordinator
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		RecipientPlanMaterializer: func(context.Context, events.Event, runtimebus.PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			return []events.DeliveryRoute{emptyRoute, targetRoute}, nil
		},
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		DeliveryStore: pg,
	})
	if _, ok := any(pc).(runtimebus.DeliveryRouteInterceptor); !ok {
		t.Fatal("PipelineCoordinator does not implement DeliveryRouteInterceptor")
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    "mixed-route",
			WorkflowVersion: "v-test",
			CurrentState:    "active",
		},
		{
			InstanceID:      childEntityID,
			StorageRef:      "child",
			WorkflowName:    "child",
			WorkflowVersion: "v-test",
			CurrentState:    "active",
		},
	} {
		if err := workflowStore.Upsert(ctx, instance); err != nil {
			t.Fatalf("seed workflow instance %s: %v", instance.InstanceID, err)
		}
	}

	live := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType(eventType))
	defer runtimebustest.Unsubscribe(eb, "workflow-runtime")
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType(eventType),
		"source",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, emptyRoute) || !deliveryRoutesContain(plan.DeliveryRoutes, targetRoute) {
		t.Fatalf("delivery routes = %#v, want empty route %#v and target route %#v", plan.DeliveryRoutes, emptyRoute, targetRoute)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
	assertNodeDeliveryStatus(t, db, evt.ID(), emptyRoute.SubscriberID, "delivered")
	assertNodeDeliveryStatus(t, db, evt.ID(), targetRoute.SubscriberID, "delivered")
	select {
	case got := <-live:
		t.Fatalf("consumed mixed node route event leaked to workflow-runtime carrier: %#v", got)
	default:
	}
}

func mixedNodeRouteWorkflowModule(t *testing.T) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "accept",
			Condition: "true",
		}},
	}
	child := runtimecontracts.FlowContractView{
		Path:  "child",
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-intake": {
				ID:            "child-intake",
				ExecutionType: "system_node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child.start": handler,
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{
		Path:  "",
		Paths: runtimecontracts.FlowContractPaths{ID: "mixed-route"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child/child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"project-observer": {
				ID:            "project-observer",
				ExecutionType: "system_node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child/child.start": handler,
				},
			},
		},
		Children: []runtimecontracts.FlowContractView{child},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "mixed-route",
			Version: "v-test",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"project-observer": {ID: "project-observer", ExecutionType: "system_node"},
				"child-intake":     {ID: "child-intake", ExecutionType: "system_node"},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"project-observer": {
					"child/child.start": handler,
				},
				"child-intake": {
					"child.start": handler,
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"mixed-route": &root,
				"child":       &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"mixed-route": {},
			"child":       {},
		},
	}
	source := semanticview.Wrap(bundle)
	return &fixtureWorkflowModule{
		source: source,
		workflow: runtimepipeline.NewWorkflowDefinition("mixed-route", []runtimepipeline.WorkflowStage{
			{Name: "active"},
		}, nil),
		workflowNodes: []runtimepipeline.WorkflowNode{
			{
				ID:            "project-observer",
				Subscriptions: []events.EventType{"child/child.start"},
				Policies: map[string]runtimepipeline.WorkflowEventPolicy{
					"child/child.start": {Consume: true},
				},
			},
			{
				ID:            "child-intake",
				Subscriptions: []events.EventType{"child/child.start"},
				Policies: map[string]runtimepipeline.WorkflowEventPolicy{
					"child/child.start": {Consume: true},
				},
			},
		},
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, bundle
}

func assertNodeDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND status = $3
	`, eventID, nodeID, want).Scan(&count); err != nil {
		t.Fatalf("query delivery status for %s: %v", nodeID, err)
	}
	if count != 1 {
		rows, err := db.QueryContext(context.Background(), `
			SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route::text, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			ORDER BY subscriber_id, delivery_target_route::text
		`, eventID)
		if err != nil {
			t.Fatalf("delivery rows for %s status %q = %d, want 1; dump query failed: %v", nodeID, want, count, err)
		}
		defer rows.Close()
		dump := make([]string, 0)
		for rows.Next() {
			var subscriber, status, reason, target string
			if err := rows.Scan(&subscriber, &status, &reason, &target); err != nil {
				t.Fatalf("scan delivery dump: %v", err)
			}
			dump = append(dump, subscriber+" status="+status+" reason="+reason+" target="+target)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate delivery dump: %v", err)
		}
		deadLetters := make([]string, 0)
		deadRows, err := db.QueryContext(context.Background(), `
			SELECT COALESCE(handler_node, ''), COALESCE(failure->>'class', ''), COALESCE(failure::text, '')
			FROM dead_letters
			WHERE original_event_id = $1::uuid
			ORDER BY created_at ASC
		`, eventID)
		if err == nil {
			defer deadRows.Close()
			for deadRows.Next() {
				var node, failure, message string
				if err := deadRows.Scan(&node, &failure, &message); err != nil {
					t.Fatalf("scan dead letter dump: %v", err)
				}
				deadLetters = append(deadLetters, node+" failure="+failure+" detail="+message)
			}
			if err := deadRows.Err(); err != nil {
				t.Fatalf("iterate dead letter dump: %v", err)
			}
		}
		t.Fatalf("delivery rows for %s status %q = %d, want 1; rows=%v dead_letters=%v", nodeID, want, count, dump, deadLetters)
	}
}

func TestEventBusPublish_NestedThreeLevelConnectChainExecutesEndToEnd(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		DeliveryStore: pg,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	ctx := eventBusTestRunContext(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    bundle.WorkflowName(),
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "idle",
		},
		{
			InstanceID:      runtimeflowidentity.EntityID("child"),
			StorageRef:      "child",
			WorkflowName:    "child",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "waiting",
			Metadata: map[string]any{
				"parent_entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      runtimeflowidentity.EntityID("child/grandchild"),
			StorageRef:      "child/grandchild",
			WorkflowName:    "grandchild",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "ready",
		},
	} {
		if err := workflowStore.Upsert(ctx, instance); err != nil {
			t.Fatalf("seed workflow instance %q: %v", instance.InstanceID, err)
		}
	}
	rootConnectProbe := eventtest.RunCreatingRootIngress(
		"bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
		events.EventType("step.begin"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EnvelopeForFlowInstance(events.EventEnvelope{}, rootEntityID), rootEntityID),
		time.Now().UTC(),
	)
	rootConnectPlan, err := eb.CheckPublishRecipientPlan(ctx, rootConnectProbe)
	if err != nil {
		t.Fatalf("root connect preflight: %v", err)
	}
	if rootConnectPlan.TargetFailure != "" || len(rootConnectPlan.DeliveryRoutes) == 0 {
		t.Fatalf("root connect preflight failure=%q routes=%#v", rootConnectPlan.TargetFailure, rootConnectPlan.DeliveryRoutes)
	}
	if got := rootConnectPlan.DeliveryRoutes[0]; got.SubscriberID != "child-relay" {
		t.Fatalf("root connect preflight route=%#v, want child-relay", got)
	}
	childTarget := rootConnectPlan.DeliveryRoutes[0].Target.Normalized()
	previewEnvelope := events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID)
	previewEnvelope = events.EnvelopeForTargetRoute(previewEnvelope, childTarget)
	previewEvent := eventtest.RunCreatingRootIngress(rootConnectProbe.ID(), events.EventType("step.begin"), "cataloge2e", "", []byte(`{"entity_id":"`+rootEntityID+`"}`), 0,
		eventBusTestRunID, "", previewEnvelope, time.Now().UTC())
	if _, err := runtimepipeline.PreviewContractHandlerExecution(ctx, bundle, "child-relay", previewEvent, runtimepipeline.WorkflowState{
		EntityID: childTarget.EntityID,
		Stage:    "waiting",
	}, nil); err != nil {
		t.Fatalf("preview child connect delivery: %v", err)
	}
	grandchildConnectProbe := eventtest.RunCreatingRootIngress(
		"cccccccc-dddd-eeee-ffff-000000000000",
		events.EventType("child/micro.start"),
		"child-relay",
		"",
		nil,
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, childTarget),
		time.Now().UTC(),
	)
	grandchildConnectPlan, err := eb.CheckPublishRecipientPlan(ctx, grandchildConnectProbe)
	if err != nil {
		t.Fatalf("grandchild connect preflight: %v", err)
	}
	if grandchildConnectPlan.TargetFailure != "" || len(grandchildConnectPlan.DeliveryRoutes) == 0 {
		t.Fatalf("grandchild connect preflight failure=%q routes=%#v", grandchildConnectPlan.TargetFailure, grandchildConnectPlan.DeliveryRoutes)
	}
	grandchildTarget := grandchildConnectPlan.DeliveryRoutes[0].Target.Normalized()
	storedGrandchild, found, err := workflowStore.Load(ctx, grandchildTarget.EntityID)
	if err != nil {
		t.Fatalf("load grandchild connect target: %v", err)
	}
	if !found {
		t.Fatalf("grandchild connect target %#v has no stored instance", grandchildTarget)
	}
	storedGrandchildIdentity := runtimepipeline.StoredFlowInstance(semanticview.Wrap(bundle), storedGrandchild)
	if storedGrandchildIdentity.ScopeKey != "child/grandchild" {
		t.Fatalf("grandchild stored identity = %#v, want child/grandchild scope; instance=%#v", storedGrandchildIdentity, storedGrandchild)
	}
	grandchildPreviewEnvelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, grandchildTarget)
	grandchildPreviewEvent := eventtest.RunCreatingRootIngress(grandchildConnectProbe.ID(), events.EventType("micro.start"), "child-relay", "", nil, 0,
		eventBusTestRunID, "", grandchildPreviewEnvelope, time.Now().UTC())
	if _, err := runtimepipeline.PreviewContractHandlerExecution(ctx, bundle, "grandchild-worker", grandchildPreviewEvent, runtimepipeline.WorkflowState{
		EntityID: grandchildTarget.EntityID,
		Stage:    "ready",
	}, nil); err != nil {
		t.Fatalf("preview grandchild connect delivery: %v", err)
	}
	rootReturnEnvelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, childTarget)
	rootReturnEnvelope = events.EnvelopeForTargetRoute(rootReturnEnvelope, events.RouteIdentity{
		EntityID: rootEntityID,
	})
	rootReturnProbe := eventtest.RunCreatingRootIngress(
		"dddddddd-eeee-ffff-0000-111111111111",
		events.EventType("child/micro.relayed"),
		"child-relay",
		"",
		nil,
		0,
		eventBusTestRunID,
		"",
		rootReturnEnvelope,
		time.Now().UTC(),
	)
	rootReturnPlan, err := eb.CheckPublishRecipientPlan(ctx, rootReturnProbe)
	if err != nil {
		t.Fatalf("root return preflight: %v", err)
	}
	if rootReturnPlan.TargetFailure != "" || len(rootReturnPlan.DeliveryRoutes) == 0 {
		t.Fatalf("root return preflight failure=%q routes=%#v", rootReturnPlan.TargetFailure, rootReturnPlan.DeliveryRoutes)
	}
	if got := rootReturnPlan.DeliveryRoutes[0].SubscriberID; got != "root-collector" {
		t.Fatalf("root return preflight subscriber = %q, want root-collector", got)
	}

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		events.EventType("pipeline.start"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
	var stepBeginEventID string
	if err := db.QueryRowContext(ctx, `SELECT event_id::text FROM events WHERE event_name = 'step.begin' ORDER BY created_at DESC LIMIT 1`).Scan(&stepBeginEventID); err != nil {
		t.Fatalf("load step.begin event id: %v", err)
	}
	assertNodeDeliveryStatus(t, db, stepBeginEventID, "child-relay", "delivered")
	var microStartEventID string
	if err := db.QueryRowContext(ctx, `SELECT event_id::text FROM events WHERE event_name = 'child/micro.start' ORDER BY created_at DESC LIMIT 1`).Scan(&microStartEventID); err != nil {
		t.Fatalf("load child/micro.start event id: %v", err)
	}
	assertNodeDeliveryStatus(t, db, microStartEventID, "grandchild-worker", "delivered")
	var microRelayedEventID string
	if err := db.QueryRowContext(ctx, `SELECT event_id::text FROM events WHERE event_name = 'child/micro.relayed' ORDER BY created_at DESC LIMIT 1`).Scan(&microRelayedEventID); err != nil {
		t.Fatalf("load child/micro.relayed event id: %v", err)
	}
	assertNodeDeliveryStatus(t, db, microRelayedEventID, "root-collector", "delivered")

	root, found, err := workflowStore.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "done" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance)
				}
			}
		}
		instances, _ := workflowStore.List(ctx)
		t.Fatalf("root current_state = %q, want done through declared ancestor connects; events=%v instances=%#v", got, dump, instances)
	}

	instances, err := workflowStore.List(ctx)
	if err != nil {
		t.Fatalf("list workflow instances: %v", err)
	}
	var (
		childState      string
		grandchildState string
	)
	var emitted []string
	rows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		emitted = append(emitted, strings.TrimSpace(name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events: %v", err)
	}
	for _, instance := range instances {
		switch strings.TrimSpace(instance.WorkflowName) {
		case "child":
			childState = strings.TrimSpace(instance.CurrentState)
		case "grandchild":
			grandchildState = strings.TrimSpace(instance.CurrentState)
		}
	}
	if childState != "completed" {
		t.Fatalf("child current_state = %q, want completed; events=%v instances=%#v", childState, emitted, instances)
	}
	if grandchildState != "finished" {
		t.Fatalf("grandchild current_state = %q, want finished; events=%v instances=%#v nodes=%#v", grandchildState, emitted, instances, module.WorkflowNodes())
	}
	if contains(emitted, "child/step.result") {
		t.Fatalf("events = %v, do not want child/step.result", emitted)
	}
	if !contains(emitted, "pipeline.complete") {
		t.Fatalf("events = %v, want pipeline.complete through declared ancestor connects", emitted)
	}
}

func TestEventBusPublish_GatedChildFlowCompletionWithoutSubjectLinkFailsClosed(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-gates-in-child-flow")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		DeliveryStore: pg,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	ctx := eventBusTestRunContext(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if err := workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      rootEntityID,
		StorageRef:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	err = eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		"11111111-2222-3333-4444-555555555555",
		events.EventType("validate.requested"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+rootEntityID+`"}`),
		0,
		eventBusTestRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootEntityID),
		time.Now().UTC(),
	))
	if err == nil || !strings.Contains(err.Error(), "no such key: child/g_validated") {
		t.Fatalf("Publish error = %v, want missing child-scoped gate failure", err)
	}
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
	var deadLettered int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.event_name = 'child/validation.done'
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = 'router'
		  AND d.status = 'dead_letter'
	`).Scan(&deadLettered); err != nil {
		t.Fatalf("count dead-lettered exact delivery: %v", err)
	}
	if deadLettered != 1 {
		t.Fatalf("dead-lettered exact deliveries = %d, want 1", deadLettered)
	}

	root, found, err := workflowStore.Load(ctx, rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "pending" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,''), COALESCE(payload::text,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance, payload string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance, &payload); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance+" payload="+payload)
				}
			}
		}
		instances, _ := workflowStore.List(ctx)
		t.Fatalf("root current_state = %q, want pending without subject-link back-propagation; root metadata=%#v events=%v instances=%#v", got, root.Metadata, dump, instances)
	}
}

func TestEventBusPublish_RecordsNestedPackageConnectLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild", PackageKey: "flows/child/flows/grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "micro_done", Event: "micro.done"}}},
			},
		},
		Path: "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Name: "micro_done", Event: "micro.done"}}},
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"step.result"}},
			},
		},
		Path: "child",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-aggregator": {
				ID:           "child-aggregator",
				SubscribesTo: []string{"micro.done"},
			},
		},
		Events:   map[string]runtimecontracts.EventCatalogEntry{"micro.done": {}},
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"child":      child.Schema,
			"grandchild": grandchild.Schema,
		},
		Semantics: runtimecontracts.WorkflowSemanticView{CompositionConnects: []runtimecontracts.FlowPackageConnect{{
			PackageKey: "flows/child",
			SourceFile: "flows/child/package.yaml",
			SourceLine: 10,
			From:       "grandchild.micro_done",
			To:         ".micro_done",
		}}},
	}
	eb, err := newScopedTestEventBus(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	subscribeInternalDeliveriesForTest(t, eb, "child-aggregator")
	defer runtimebustest.Unsubscribe(eb, "child-aggregator")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress("", "child/grandchild/micro.done", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-grandchild")), time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "micro.done" {
		t.Fatalf("localized_event = %q, want child-local micro.done", got)
	}
}

func TestEventBusPublish_RecordsNestedTemplateInstanceLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "child/grandchild",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				SubscribesTo: []string{"micro.done"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Path:     "child",
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
	}
	eb, err := newScopedTestEventBus(newRouteSetEventStore(), runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	runtimebustest.Subscribe(t, eb, "worker-inst-1")
	defer runtimebustest.Unsubscribe(eb, "worker-inst-1")
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress(
		"",
		"child/grandchild/inst-1/micro.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-grandchild")),
		time.Time{},
	)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "micro.done" {
		t.Fatalf("localized_event = %q, want micro.done", got)
	}
}
