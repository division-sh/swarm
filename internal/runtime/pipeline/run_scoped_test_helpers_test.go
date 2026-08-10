package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func newTestWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	runner := &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectPostgres}
	store := newWorkflowPersistenceFixtureStore(runner)
	registerWorkflowPersistenceFixture(store, db, workflowStoreDialectPostgres, runner)
	store.readiness = pipelineTestDynamicFlowRuntimeReadinessPersistence{store: store}
	store.timerActivations = pipelineTestWorkflowTimerPersistence{store: store}
	store.runLifecycle = &unavailablePipelineTestRunLifecycle{}
	store.timerObligations = pipelineTestTimerObligationReader{db: db, dialect: pipelineTestTimerObligationPostgres}
	return installPipelineTestActivityJournal(store)
}

func newTestSQLiteWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	reader := &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite}
	store := newWorkflowPersistenceFixtureStore(reader)
	registerWorkflowPersistenceFixture(store, db, workflowStoreDialectSQLite, reader)
	store.readiness = pipelineTestDynamicFlowRuntimeReadinessPersistence{store: store}
	store.timerActivations = pipelineTestWorkflowTimerPersistence{store: store}
	store.runLifecycle = &unavailablePipelineTestRunLifecycle{}
	store.timerObligations = pipelineTestTimerObligationReader{db: db, dialect: pipelineTestTimerObligationSQLite}
	return installPipelineTestActivityJournal(store)
}

func newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db *sql.DB, runner runtimeMutationRunner) *workflowInstanceStore {
	base := &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite}
	store := newWorkflowPersistenceFixtureStore(base)
	registerWorkflowPersistenceFixture(store, db, workflowStoreDialectSQLite, runner)
	if owner, ok := runner.(DynamicFlowRuntimeReadinessPersistence); ok {
		store.readiness = owner
	} else {
		store.readiness = pipelineTestDynamicFlowRuntimeReadinessPersistence{store: store}
	}
	if owner, ok := runner.(WorkflowTimerActivationPersistence); ok {
		store.timerActivations = owner
	} else {
		store.timerActivations = pipelineTestWorkflowTimerPersistence{store: store}
	}
	if owner, ok := runner.(WorkflowInstancePersistenceReader); ok {
		store.instanceReader = owner
	} else {
		store.instanceReader = pipelineTestWorkflowInstanceReader{db: db, dialect: workflowStoreDialectSQLite}
	}
	if owner, ok := runner.(WorkflowEngineMutationOwner); ok {
		store.engineMutations = owner
	}
	if owner, ok := runner.(WorkflowInitialMaterializationCommitOwner); ok {
		store.initialCommits = owner
	}
	store.timerObligations = pipelineTestTimerObligationReader{db: db, dialect: pipelineTestTimerObligationSQLite}
	if owner, ok := runner.(runtimerunlifecycle.OperationOwner); ok {
		store.runLifecycle = owner
	} else {
		store.runLifecycle = &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite}
	}
	return installPipelineTestActivityJournal(store)
}

func newWorkflowPersistenceFixtureStore(runner *recordingRuntimeMutationRunner) *workflowInstanceStore {
	store := &workflowInstanceStore{runLifecycle: runner}
	if owner, ok := any(runner).(entityquery.Reader); ok {
		store.entityQuery = owner
	}
	if owner, ok := any(runner).(runtimeworkflowroute.RecoveryReader); ok {
		store.routeRecovery = owner
	}
	if owner, ok := any(runner).(runtimeactivityresult.Reader); ok {
		store.activityResults = owner
	}
	if owner, ok := any(runner).(GateRouteAdmissionReader); ok {
		store.gateRoutes = owner
	}
	if owner, ok := any(runner).(WorkflowEngineMutationOwner); ok {
		store.engineMutations = owner
	}
	if owner, ok := any(runner).(DecisionCardMutationOwner); ok {
		store.cardMutations = owner
	} else {
		store.cardMutations = &unavailablePipelineTestDecisionCardMutations{}
	}
	if owner, ok := any(runner).(WorkflowTimerOccurrenceOwner); ok {
		store.timerOccurrences = owner
	}
	if owner, ok := any(runner).(WorkflowDecisionRouteOwner); ok {
		store.decisionRoutes = owner
	}
	if owner, ok := any(runner).(WorkflowInstancePersistenceReader); ok {
		store.instanceReader = owner
	}
	if owner, ok := any(runner).(WorkflowInitialMaterializationCommitOwner); ok {
		store.initialCommits = owner
	}
	store.standingServices = pipelineTestStandingServices{store: store}
	return store
}

type pipelineTestStandingServices struct {
	StandingServicePersistence
	store *workflowInstanceStore
}

func (p pipelineTestStandingServices) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	if p.store == nil || p.store.testDB() == nil {
		return false, errors.New("pipeline test standing-service reader requires selected store")
	}
	query := `SELECT EXISTS (SELECT 1 FROM standing_services WHERE current_run_id = ? AND declaration_present = TRUE AND effective_state IN ('active', 'suspended'))`
	if !p.store.isSQLite() {
		query = `SELECT EXISTS (SELECT 1 FROM standing_services WHERE current_run_id = $1::uuid AND declaration_present = TRUE AND effective_state IN ('active', 'suspended'))`
	}
	var found bool
	if err := p.store.testDB().QueryRowContext(ctx, query, strings.TrimSpace(runID)).Scan(&found); err != nil {
		return false, err
	}
	return found, nil
}

type pipelineTestDynamicFlowRuntimeReadinessPersistence struct {
	store *workflowInstanceStore
}

func (p pipelineTestDynamicFlowRuntimeReadinessPersistence) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	return p.store.legacyReconcileDynamicFlowRuntimeReadinessPlan(ctx, plan, observedAt)
}

func (p pipelineTestDynamicFlowRuntimeReadinessPersistence) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (DynamicFlowRuntimeReadiness, bool, error) {
	return p.store.legacyLoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func (p pipelineTestDynamicFlowRuntimeReadinessPersistence) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	return p.store.legacyListDynamicFlowRuntimeReadiness(ctx)
}

func (p pipelineTestDynamicFlowRuntimeReadinessPersistence) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]DynamicFlowRuntimeReadinessKey, error) {
	return p.store.legacyListDynamicFlowRuntimeReadinessKeys(ctx)
}

func (p pipelineTestDynamicFlowRuntimeReadinessPersistence) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	return p.store.legacyMarkDynamicFlowRuntimeTopologyReady(ctx, plan, readyAt)
}

type pipelineTestWorkflowTimerPersistence struct {
	store *workflowInstanceStore
}

func (p pipelineTestWorkflowTimerPersistence) LoadWorkflowTimerActivation(ctx context.Context, activationID string) (WorkflowTimerActivation, bool, error) {
	return p.store.loadWorkflowTimerActivation(ctx, activationID, false)
}

func (p pipelineTestWorkflowTimerPersistence) ListWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]WorkflowTimerActivation, error) {
	return p.store.listWorkflowTimerActivations(ctx, runID, entityID, activeOnly)
}

func (p pipelineTestWorkflowTimerPersistence) CommitWorkflowTimerReconciliation(ctx context.Context, command WorkflowTimerReconciliationCommand) (CommittedWorkflowLifecycleMutation, error) {
	if err := command.Validate(); err != nil {
		return CommittedWorkflowLifecycleMutation{}, err
	}
	result := CommittedWorkflowLifecycleMutation{}
	err := p.store.runPipelineMutation(ctx, func(txctx context.Context) error {
		for _, mutation := range command.Plan.Timers {
			switch mutation.Kind {
			case WorkflowTimerMutationInsert:
				persisted, _, err := p.store.insertWorkflowTimerActivation(txctx, mutation.Activation)
				if err != nil {
					return err
				}
				result.Wakeups = append(result.Wakeups, persisted.Ref)
			case WorkflowTimerMutationCancel:
				persisted, changed, err := p.store.cancelWorkflowTimerActivation(txctx, mutation.Activation.Ref)
				if err != nil {
					return err
				}
				if changed {
					result.Cancellations = append(result.Cancellations, persisted.Ref)
				}
			}
		}
		if command.Plan.RequestCompletionCandidate {
			return p.store.requestRunCompletionCandidate(txctx, command.RunID)
		}
		return nil
	})
	if err != nil {
		return CommittedWorkflowLifecycleMutation{}, err
	}
	return result, result.Validate()
}

type pipelineTestTimerObligationReader struct {
	db      *sql.DB
	dialect pipelineTestTimerObligationDialect
}

type pipelineTestTimerObligationDialect uint8

const (
	pipelineTestTimerObligationPostgres pipelineTestTimerObligationDialect = iota
	pipelineTestTimerObligationSQLite
)

func (s *workflowInstanceStore) standingRunHasLiveWorkTx(ctx context.Context, tx *sql.Tx, runID string, observedAt time.Time) (bool, error) {
	if s.deliveryStore == nil {
		return false, fmt.Errorf("inspect standing run live work: delivery lifecycle store is required")
	}
	deliverySummary, err := s.deliveryStore.SummarizeRun(ctx, runID)
	if err != nil {
		return false, fmt.Errorf("inspect standing run delivery work: %w", err)
	}
	if !deliverySummary.Settled() {
		return true, nil
	}
	if s.pipelineStore == nil {
		return false, fmt.Errorf("inspect standing run pipeline work: pipeline obligation store is required")
	}
	pipelineSummary, err := s.pipelineStore.SummarizeRun(ctx, runID)
	if err != nil {
		return false, fmt.Errorf("inspect standing run pipeline work: %w", err)
	}
	if pipelineSummary.HasOpenWork() {
		return true, nil
	}
	query := `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = ? AND status IN ('active', 'suspended'))`
	args := []any{runID}
	dialect := pipelineTestTimerObligationSQLite
	if !s.isSQLite() {
		query = `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = $1::uuid AND status IN ('active', 'suspended'))`
		args = []any{runID}
		dialect = pipelineTestTimerObligationPostgres
	}
	var live bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&live); err != nil {
		return false, fmt.Errorf("inspect standing run live work: %w", err)
	}
	if live {
		return true, nil
	}
	scope, err := runtimetimerobligation.Run(runID)
	if err != nil {
		return false, err
	}
	snapshot, err := readPipelineTestTimerObligations(ctx, tx, dialect, scope, observedAt)
	if err != nil {
		return false, fmt.Errorf("inspect standing run timer work: %w", err)
	}
	run, ok := snapshot.Run(runID)
	if !ok {
		return false, fmt.Errorf("inspect standing run timer work: snapshot omitted requested run")
	}
	return run.Totals().ActiveCount > 0, nil
}

func (r pipelineTestTimerObligationReader) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if tx, ok := PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return readPipelineTestTimerObligations(ctx, tx, r.dialect, scope, observedAt)
	}
	return readPipelineTestTimerObligations(ctx, r.db, r.dialect, scope, observedAt)
}

type pipelineTestTimerQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readPipelineTestTimerObligations(ctx context.Context, queryer pipelineTestTimerQueryer, dialect pipelineTestTimerObligationDialect, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	query := `
		SELECT t.task_type, COALESCE(CAST(t.run_id AS TEXT), ''), t.status, t.fire_at, COALESCE(r.status, '')
		FROM timers t
		LEFT JOIN runs r ON r.run_id = t.run_id
		WHERE (? = '' OR t.run_id = ?)
		ORDER BY t.task_type, t.run_id, t.timer_id`
	args := []any{scope.RunID(), scope.RunID()}
	if dialect == pipelineTestTimerObligationPostgres {
		query = `
			SELECT t.task_type, COALESCE(t.run_id::text, ''), t.status, t.fire_at, COALESCE(r.status, '')
			FROM timers t
			LEFT JOIN runs r ON r.run_id = t.run_id
			WHERE (NULLIF($1, '') IS NULL OR t.run_id = NULLIF($1, '')::uuid)
			ORDER BY t.task_type, t.run_id, t.timer_id`
		args = []any{scope.RunID()}
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimetimerobligation.Snapshot{}, err
	}
	defer rows.Close()
	global := runtimetimerobligation.ZeroFamilies()
	runs := map[string][]runtimetimerobligation.FamilyObligation{}
	if scope.RunID() != "" {
		runs[scope.RunID()] = runtimetimerobligation.ZeroFamilies()
	}
	for rows.Next() {
		var familyValue, runID, status, runStatus string
		var fireAtRaw any
		if err := rows.Scan(&familyValue, &runID, &status, &fireAtRaw, &runStatus); err != nil {
			return runtimetimerobligation.Snapshot{}, err
		}
		family, err := runtimetimerobligation.ParseFamily(familyValue)
		if err != nil {
			return runtimetimerobligation.Snapshot{}, err
		}
		fireAt, err := pipelineTestTimerTime(fireAtRaw)
		if err != nil {
			return runtimetimerobligation.Snapshot{}, err
		}
		target := global
		runID = strings.TrimSpace(runID)
		if runID != "" {
			if _, ok := runs[runID]; !ok {
				runs[runID] = runtimetimerobligation.ZeroFamilies()
			}
			target = runs[runID]
		}
		index := pipelineTestTimerFamilyIndex(family)
		if strings.TrimSpace(status) == "active" {
			target[index].ActiveCount++
			if !fireAt.After(observedAt.UTC()) {
				target[index].DueCount++
			}
			runState, stateErr := runtimerunlifecycle.ParseState(strings.TrimSpace(runStatus))
			if runID == "" || (stateErr == nil && runState.Active()) {
				target[index].RecoverableCount++
			}
		}
		if runID == "" {
			global = target
		} else {
			runs[runID] = target
		}
	}
	if err := rows.Err(); err != nil {
		return runtimetimerobligation.Snapshot{}, err
	}
	snapshot := runtimetimerobligation.Snapshot{ObservedAt: observedAt.UTC(), GlobalFamilies: global}
	for _, runID := range runtimetimerobligation.SortedRunIDs(runs) {
		snapshot.Runs = append(snapshot.Runs, runtimetimerobligation.RunObligations{RunID: runID, Families: runs[runID]})
	}
	return snapshot, nil
}

func pipelineTestTimerFamilyIndex(family runtimetimerobligation.Family) int {
	for index, candidate := range runtimetimerobligation.AllFamilies() {
		if candidate == family {
			return index
		}
	}
	panic("validated timer family is absent from canonical ordering")
}

func pipelineTestTimerTime(raw any) (time.Time, error) {
	if value, ok := raw.(time.Time); ok {
		return value.UTC(), nil
	}
	var text string
	switch value := raw.(type) {
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		return time.Time{}, fmt.Errorf("unsupported timer timestamp %T", raw)
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(text)); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timer timestamp %q", text)
}

func workflowPersistenceForTest(store *workflowInstanceStore) WorkflowPersistence {
	return WorkflowPersistence{store: store}
}

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

func seedPipelineEventRecord(t testing.TB, ctx context.Context, db *sql.DB, event events.Event) {
	seedPipelineEventRecordForDialect(t, ctx, db, authoractivityfixture.DialectPostgres, event)
}

func seedPipelineEventRecordForDialect(t testing.TB, ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, event events.Event) {
	t.Helper()
	if runID := strings.TrimSpace(event.RunID()); runID != "" {
		switch dialect {
		case authoractivityfixture.DialectPostgres:
			runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
		case authoractivityfixture.DialectSQLite:
			runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
		default:
			t.Fatalf("seed canonical pipeline event %s: unsupported dialect %q", event.ID(), dialect)
		}
	}
	if err := eventfixture.Insert(ctx, db, dialect, event); err != nil {
		t.Fatalf("seed canonical pipeline event %s: %v", event.ID(), err)
	}
}

func testPipelineRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	if db == nil {
		t.Fatal("test pipeline run context requires db")
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: testPipelineRunID})
	return ctx
}

func testPipelineRunContextNoSeed(t *testing.T) context.Context {
	t.Helper()
	return runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
}

func testWorkflowStoreRunContext(t *testing.T, store *workflowInstanceStore) context.Context {
	t.Helper()
	if store == nil {
		t.Fatal("test workflow store run context requires store")
	}
	if store.testDB() == nil {
		return testPipelineRunContextNoSeed(t)
	}
	return testPipelineRunContext(t, store.testDB())
}

func materializedWorkflowInstanceForTest(instance WorkflowInstance) WorkflowInstance {
	occurredAt := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	instance.Metadata = cloneStringAnyMap(instance.Metadata)
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	if strings.TrimSpace(asString(instance.Metadata["entity_id"])) == "" {
		for _, candidate := range []string{instance.InstanceID, instance.StorageRef} {
			if parsed, err := uuid.Parse(strings.TrimSpace(candidate)); err == nil {
				instance.Metadata["entity_id"] = parsed.String()
				break
			}
		}
	}
	storageRef := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	if _, declared := instance.Metadata["flow_path"]; !declared && strings.Contains(storageRef, "/") {
		instance.Metadata["flow_path"] = storageRef
		instance.Metadata["instance_id"] = runtimeflowidentity.LogicalInstanceID(storageRef)
	} else if !declared {
		canonicalRoute := strings.Trim(strings.TrimSpace(instance.WorkflowName), "/")
		if canonicalRoute != "" {
			instance.StorageRef = canonicalRoute
			instance.InstanceID = runtimeflowidentity.LogicalInstanceID(canonicalRoute)
			instance.Metadata["instance_id"] = instance.InstanceID
		}
	}
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = occurredAt
	}
	if instance.CreatedAt.IsZero() {
		instance.CreatedAt = occurredAt
	}
	return instance
}

func configureWorkflowLifecycleForTest(t testing.TB, pc *PipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.workflowStore == nil {
		return
	}
	if pc.workflowTimers == nil {
		pc.workflowTimers = newWorkflowTimerLifecycle(pc.workflowStore, pc.SemanticSource(), pc.bus, pc.workOwner, pc.timerScheduler, executionposture.Live)
	}
	if pc.workflowStore.lifecycleOwner == nil {
		pc.workflowStore.lifecycleOwner = pipelineWorkflowLifecycleOwner{coordinator: pc}
	}
}

func testPipelineCoordinatorRunContext(t *testing.T, pc *PipelineCoordinator) context.Context {
	t.Helper()
	if pc == nil {
		t.Fatal("test pipeline coordinator run context requires coordinator")
	}
	configureWorkflowLifecycleForTest(t, pc)
	if pc.workflowStore != nil && pc.workflowStore.testDB() != nil {
		configurePipelineTestDeliveryOwner(t, pc)
	}
	var ctx context.Context
	if pc.workflowStore != nil {
		ctx = testWorkflowStoreRunContext(t, pc.workflowStore)
	} else {
		ctx = testPipelineRunContextNoSeed(t)
	}
	if !pc.runtimeReceiver {
		return ctx
	}
	bound, err := pc.receiverExecution.Bind(ctx, executionmode.Live)
	if err != nil {
		t.Fatalf("bind test Pipeline receiver execution: %v", err)
	}
	return bound
}

func testWorkflowSourceEnvelope(flowID, instancePath, entityID string) events.EventEnvelope {
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath)
	return events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: instancePath,
		EntityID:     entityID,
	})
}

func testWorkflowStateTransitionContext(ctx context.Context, route runtimeflowidentity.Route, entityID, eventType string) context.Context {
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), route.InstancePath)
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType(strings.TrimSpace(eventType)), "test", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", envelope, time.Now().UTC(),
	)
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}

func testPersistedWorkflowStateTransitionContext(t *testing.T, store *workflowInstanceStore, ctx context.Context, route runtimeflowidentity.Route, entityID, eventType string) context.Context {
	t.Helper()
	transitionCtx := testWorkflowStateTransitionContext(ctx, route, entityID, eventType)
	evt, ok := runtimecorrelation.InboundEventFromContext(transitionCtx)
	if !ok {
		t.Fatal("test workflow transition context has no inbound event")
	}
	seedExactOnceEvent(t, store, ctx, evt)
	return transitionCtx
}

func seedPipelineNodeDeliveryAuthority(t *testing.T, db *sql.DB, evt events.Event, nodeID string) events.DeliveryRoute {
	t.Helper()
	return seedPipelineNodeDeliveryRouteAuthority(t, db, evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(strings.TrimSpace(nodeID)), Target: evt.TargetRoute()})
}

func seedPipelineNodeDeliveryRouteAuthority(t *testing.T, db *sql.DB, evt events.Event, route events.DeliveryRoute) events.DeliveryRoute {
	t.Helper()
	if db == nil {
		t.Fatal("seed pipeline node delivery authority requires db")
	}
	route = route.Normalized()
	if !route.Recipient.IsNode() {
		t.Fatal("seed pipeline node delivery authority requires nodeID")
	}
	eventID := strings.TrimSpace(evt.ID())
	if _, err := uuid.Parse(eventID); err != nil {
		t.Fatalf("seed pipeline node delivery authority event id = %q: %v", eventID, err)
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		runID = testPipelineRunID
	}
	if _, err := uuid.Parse(runID); err != nil {
		t.Fatalf("seed pipeline node delivery authority run id = %q: %v", runID, err)
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	entityID := ""
	if raw := strings.TrimSpace(evt.EntityID()); raw != "" {
		if _, err := uuid.Parse(raw); err == nil {
			entityID = raw
		}
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = $1::uuid)`, eventID).Scan(&exists); err != nil {
		t.Fatalf("load pipeline node delivery authority event: %v", err)
	}
	deliveryEvent := evt
	if !exists || strings.TrimSpace(evt.RunID()) != runID {
		envelope := events.EventEnvelope{Scope: events.EventScopeGlobal}
		if entityID != "" {
			envelope = events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
			if flowInstance := strings.TrimSpace(evt.FlowInstance()); flowInstance != "" {
				envelope = events.EnvelopeForFlowInstance(envelope, flowInstance)
			}
		}
		fixture := eventtest.PersistedProjectionForProducer(
			eventID, evt.Type(), evt.Producer(), evt.TaskID(), evt.Payload(), evt.ChainDepth(), runID, evt.ParentEventID(), envelope, createdAt,
		)
		deliveryEvent = fixture
		if !exists {
			seedPipelineEventRecord(t, ctx, db, fixture)
		}
	}
	owner := newPipelineTestDeliveryOwnerForDB(t, db)
	if err := owner.commitInitial(ctx, deliveryEvent, route); err != nil {
		t.Fatalf("seed pipeline node delivery authority: %v", err)
	}
	return route
}
