package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type standingSignalCoordinatorDispatcher struct {
	dispatched atomic.Int32
}

func (d *standingSignalCoordinatorDispatcher) DispatchDeliveryContinuation(context.Context, events.Event, events.DeliveryRoute) error {
	d.dispatched.Add(1)
	return nil
}

func TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			ctx := worklifetime.WithRuntimeOccurrence(testAuthorActivityRuntimeContext(), occurrence)
			t.Cleanup(func() {
				retireRunLifecycleExecutorOccurrence(t, occurrence)
				retireRunLifecycleProcess(t, process)
			})

			var (
				db            *sql.DB
				selected      workflowTestSelectedStore
				deliveryStore runtimedelivery.Store
				workflowStore *runtimepipeline.PipelineCoordinator
				dialect       authoractivityfixture.Dialect
				adapter       *deliveryadapter.Adapter
			)
			if backend == "sqlite" {
				sqliteSelected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, deliveryStore = sqliteSelected.backend.ConstructionHandle(), sqliteSelected
				selected = sqliteSelected
				dialect, adapter = authoractivityfixture.DialectSQLite, sqliteDeliveryAdapter
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				postgresSelected := admitTestPostgresStore(t, opened)
				db, deliveryStore = opened, postgresSelected
				selected = postgresSelected
				dialect, adapter = authoractivityfixture.DialectPostgres, postgresDeliveryAdapter
			}
			if backend == "sqlite" {
				workflowStore = newSQLiteWorkflowTestCoordinator(t, db, selected)
			} else {
				workflowStore = newPostgresWorkflowTestCoordinator(t, db, selected)
			}

			candidate := runtimepipeline.StandingServiceCandidate{
				ServiceID:  runtimeflowidentity.StandingServiceID("project", "signal-startup-order"),
				PackageKey: "project", FlowID: "signal-startup-order",
				InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
				Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("7", 64)),
			}
			seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
			created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
			if err != nil || len(created) != 1 {
				t.Fatalf("seed standing service = %#v, %v", created, err)
			}
			authority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-startup-owner", 1)
			if err != nil {
				t.Fatal(err)
			}
			route := testEntitylessNodeDeliveryRoute("standing-startup-node")
			evt := eventtest.RuntimeControl(uuid.NewString(), "standing.signal.startup", "test", candidate.EntityID, []byte(`{}`), 0, created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := eventfixture.Insert(ctx, db, dialect, evt); err != nil {
				t.Fatalf("insert startup-order event: %v", err)
			}
			var proofs []runtimedelivery.DurableHandoffProof
			commit := func(txctx context.Context, tx *sql.Tx) error {
				var err error
				proofs, err = adapter.CommitInitial(txctx, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, authority)
				return err
			}
			if backend == "sqlite" {
				err = deliveryStore.(*SQLiteRuntimeStore).runEventTransaction(ctx, commit)
			} else {
				err = deliveryStore.(*PostgresStore).runEventTransaction(ctx, commit)
			}
			if err != nil {
				t.Fatalf("commit startup-order delivery: %v", err)
			}

			if _, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"}); err != nil {
				t.Fatalf("terminalize before registration: %v", err)
			}

			coordinator, err := runtimedeliverycontinuation.New(deliveryStore, authority, occurrence, &standingSignalCoordinatorDispatcher{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.AcceptCommitted(proofs); err != nil {
				t.Fatalf("accept startup-order handoff: %v", err)
			}
			var signals atomic.Int32
			registration, err := workflowStore.RegisterDeliveryContinuationSignal(authority, func() {
				signals.Add(1)
				coordinator.Signal()
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Release)
			if err := coordinator.Start(ctx); err != nil {
				t.Fatalf("startup scan after pre-registration callback: %v", err)
			}
			t.Cleanup(func() {
				retireCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := coordinator.Retire(retireCtx); err != nil {
					t.Errorf("retire startup-order coordinator: %v", err)
				}
			})
			if got := signals.Load(); got != 0 {
				t.Fatalf("callback executed before registration replayed %d signals", got)
			}
			deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Acquire(deliveryID); err == nil {
				t.Fatal("startup scan retained a terminal delivery continuation")
			}
		})
	}
}

func TestSQLiteStandingServiceReconcileCreatesPublishesAndRepairsRestartAbandon(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
	packageKey := "project"
	flowID := "ingress"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	instanceID := uuid.NewString()
	entityID := uuid.NewString()
	firstHash := "bundle-v1:sha256:" + strings.Repeat("1", 64)
	secondHash := "bundle-v1:sha256:" + strings.Repeat("2", 64)
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: packageKey, FlowID: flowID,
		InstanceID: instanceID, EntityID: entityID,
		Source: mustStoreTestPersistedBundleSourceFact(firstHash),
	}
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), firstHash)

	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService(create): %v", err)
	}
	if created.Transition != "created" || created.Generation != 1 || created.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 1) {
		t.Fatalf("created reconciliation = %#v", created)
	}
	sequence, err := workflowStore.PublishStandingService(ctx, serviceID, created.RunID, created.Generation)
	if err != nil || sequence != 1 {
		t.Fatalf("PublishStandingService = %d, %v", sequence, err)
	}
	if _, err := store.backend.ExecContext(ctx, `
		INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, fields, gates, accumulator, entered_state_at, created_at, updated_at)
		VALUES (?, ?, ?, 'default', 'ready', '{"name":"preserved"}', '{}', '{}', ?, ?, ?)
	`, created.RunID, entityID, "ingress/"+instanceID, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if _, err := markRunTerminalStatusForTest(
		ctx, store, created.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("cancel standing run: %v", err)
	}
	if _, err := store.backend.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, stopped_at, updated_at) VALUES (?, 'stopped', 'server_restart_abandon', 'swarm.serve.abandon_active_runs', ?, ?)`, created.RunID, time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed restart-abandon provenance: %v", err)
	}
	candidate.Source = mustStoreTestPersistedBundleSourceFact(secondHash)
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), secondHash)
	repaired, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService(repair): %v", err)
	}
	if repaired.Transition != "repaired" || repaired.Generation != 2 || repaired.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("repaired reconciliation = %#v", repaired)
	}
	var state, name string
	if err := store.backend.QueryRowContext(ctx, `SELECT current_state, json_extract(fields, '$.name') FROM entity_state WHERE run_id = ? AND entity_id = ?`, repaired.RunID, entityID).Scan(&state, &name); err != nil {
		t.Fatalf("load repaired entity state: %v", err)
	}
	if state != "ready" || name != "preserved" {
		t.Fatalf("repaired entity state = %s/%s", state, name)
	}
	var oldStatus, retiredReason string
	if err := store.backend.QueryRowContext(ctx, `
		SELECT r.status, COALESCE(g.retired_reason, '')
		FROM runs r JOIN standing_service_generations g ON g.run_id = r.run_id
		WHERE r.run_id = ?
	`, created.RunID).Scan(&oldStatus, &retiredReason); err != nil {
		t.Fatalf("load predecessor lineage: %v", err)
	}
	if oldStatus != "cancelled" || retiredReason != "server_restart_abandon" {
		t.Fatalf("predecessor = %s/%s", oldStatus, retiredReason)
	}
}

func TestSQLiteStandingServiceReconcileRejectsUnknownTerminalityWithCommand(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("3", 64)),
	}
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := markRunTerminalStatusForTest(
		ctx, store, created.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	_, err = workflowStore.ReconcileStandingService(ctx, candidate)
	if err == nil || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("error = %v, want teaching reset command", err)
	}
}

func TestSQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore, genericReconciler := newGenericScheduleAwareWorkflowTestCoordinator(t, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("4", 64)),
	}
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflowStore.PublishStandingService(ctx, serviceID, created.RunID, created.Generation); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	agentID := "standing-agent"
	sessionID := uuid.NewString()
	fixtureCtx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(candidate.Source.BundleHash()), created.RunID)
	workEvent := eventtest.PersistedProjection(
		eventID, events.EventType("standing.work"), "test", "", json.RawMessage(`{}`), 0,
		created.RunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	identity := testAgentIdentity(t, agentID, "standing/ingress")
	fields := testAgentIdentityStorageFields(t, identity)
	workRoute := testAgentDeliveryRoute(t, agentID, "standing/ingress")
	if err := commitSemanticEventFixtureWithRoutes(fixtureCtx, store, workEvent, []events.DeliveryRoute{workRoute}); err != nil {
		t.Fatal(err)
	}
	if err := commitSemanticEventFixture(fixtureCtx, store, eventtest.PersistedProjection(
		unsettledEventID, events.EventType("standing.unsettled"), "test", "", json.RawMessage(`{}`), 0,
		created.RunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)); err != nil {
		t.Fatal(err)
	}
	seedTestAgentRow(t, ctx, store.backend.ConstructionHandle(), false, identity, "active")
	if _, err := store.backend.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, conversation, runtime_state, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', '{}', 'active')
	`, sessionID, created.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(fixtureCtx, store, workEvent, workRoute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BindAgentSession(fixtureCtx, claimed.Claim, sessionID); err != nil {
		t.Fatal(err)
	}
	genericActivation, workflowActivation := seedGenericScheduleTimerFamilies(
		t, store, store.backend.ConstructionHandle(), fixtureCtx,
	)
	continuationAuthority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-lifecycle", 1)
	if err != nil {
		t.Fatal(err)
	}
	var continuationSignals atomic.Int32
	registration, err := workflowStore.RegisterDeliveryContinuationSignal(continuationAuthority, func() { continuationSignals.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()

	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" || suspended.Transition != "suspended" || !suspended.DeliveryContinuationRequired {
		t.Fatalf("suspended = %#v", suspended)
	}
	assertGenericScheduleTimerFamilyCancellation(
		t, store, store.backend.ConstructionHandle(), fixtureCtx,
		genericActivation, workflowActivation, suspended.TimerCancellations,
	)
	assertGenericScheduleReconciled(t, genericReconciler, genericActivation.ID)
	if got := continuationSignals.Load(); got != 1 {
		t.Fatalf("suspend continuation signals = %d, want 1", got)
	}
	var runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason string
	if err := store.backend.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.backend.QueryRowContext(ctx, `
		SELECT status, reason_code
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'agent' AND subscriber_id = ?
	`, eventID, agentID).Scan(&deliveryStatus, &deliveryReason); err != nil {
		t.Fatal(err)
	}
	if err := store.backend.QueryRowContext(ctx, `SELECT status, termination_reason FROM agent_sessions WHERE session_id = ?`, sessionID).Scan(&sessionStatus, &sessionReason); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := store.backend.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || deliveryReason != "standing_suspended" || sessionStatus != "terminated" || sessionReason != "cancelled" {
		t.Fatalf("suspend state = run:%s delivery:%s/%s session:%s/%s", runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason)
	}
	if pipelineOutcome != "dead_letter" || pipelineReason != "standing_suspended" {
		t.Fatalf("unsettled pipeline receipt = %s/%s", pipelineOutcome, pipelineReason)
	}
	statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil || len(statuses) != 1 {
		t.Fatalf("ListStandingServiceStatuses = %#v, %v", statuses, err)
	}
	if statuses[0].OverrideActor != "tester" || statuses[0].OverrideReason != "maintenance" || statuses[0].OverrideAt.IsZero() {
		t.Fatalf("suspended status = %#v", statuses[0])
	}
	assertStandingMockOnlyLifecycleRejectsLiveWorkBeforeMutation(
		t, store, store.backend.ConstructionHandle(), fixtureCtx, workflowStore, serviceID, created.RunID, created.Generation,
	)

	resumed, err := workflowStore.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResumeStandingService: %v", err)
	}
	if resumed.EffectiveState != "active" || resumed.RunID != created.RunID || resumed.Generation != created.Generation {
		t.Fatalf("resumed = %#v", resumed)
	}

	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResetStandingService: %v", err)
	}
	if reset.Transition != "reset" || reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) || !reset.DeliveryContinuationRequired {
		t.Fatalf("reset = %#v", reset)
	}
	if got := continuationSignals.Load(); got != 2 {
		t.Fatalf("reset continuation signals = %d, want 2", got)
	}
	if err := store.backend.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "cancelled" {
		t.Fatalf("reset predecessor status = %s, want cancelled", runStatus)
	}
}

func TestSQLiteStandingServiceSetOrphansRemovedDeclaration(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("5", 64)),
	}
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("create set = %#v, %v", created, err)
	}
	results, err := workflowStore.ReconcileStandingServiceSet(ctx, nil)
	if err != nil {
		t.Fatalf("orphan set: %v", err)
	}
	if len(results) != 1 || results[0].Transition != "orphaned" || results[0].EffectiveState != "orphaned" || !results[0].DeliveryContinuationRequired {
		t.Fatalf("orphan results = %#v", results)
	}
	var declarationPresent bool
	var effectiveState, runStatus string
	if err := store.backend.QueryRowContext(ctx, `SELECT declaration_present, effective_state FROM standing_services WHERE service_id = ?`, serviceID).Scan(&declarationPresent, &effectiveState); err != nil {
		t.Fatal(err)
	}
	if err := store.backend.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if declarationPresent || effectiveState != "orphaned" || runStatus != "paused" {
		t.Fatalf("orphan state = declared:%t state:%s run:%s", declarationPresent, effectiveState, runStatus)
	}
}

func TestSQLiteStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
	testStandingServiceReplacementIsScopedAndAtomic(t, store.backend.ConstructionHandle(), workflowStore)
}

func TestPostgresStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := admitTestPostgresStore(t, db)
	workflowStore := newPostgresWorkflowTestCoordinator(t, db, selected)
	testStandingServiceReplacementIsScopedAndAtomic(t, db, workflowStore)
}

func testStandingServiceReplacementIsScopedAndAtomic(t *testing.T, db *sql.DB, workflowStore *runtimepipeline.PipelineCoordinator) {
	t.Helper()
	ctx := testAuthorActivityRuntimeContext()
	makeCandidate := func(flowID, hashDigit string) runtimepipeline.StandingServiceCandidate {
		return runtimepipeline.StandingServiceCandidate{
			ServiceID: runtimeflowidentity.StandingServiceID("project", flowID), PackageKey: "project", FlowID: flowID,
			InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
			Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat(hashDigit, 64)),
		}
	}
	retained := makeCandidate("retained", "1")
	removed := makeCandidate("removed", "2")
	unrelated := makeCandidate("unrelated", "3")
	for _, candidate := range []runtimepipeline.StandingServiceCandidate{retained, removed, unrelated} {
		seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
	}
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed, unrelated})
	if err != nil || len(created) != 3 {
		t.Fatalf("seed standing services = %#v, %v", created, err)
	}
	initialRunID := map[string]string{}
	for _, result := range created {
		initialRunID[result.ServiceID] = result.RunID
	}

	revised := retained
	revised.Source = mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("4", 64))
	seedStoreTestPersistedBundle(t, db, revised.Source.BundleHash())
	missing := makeCandidate("missing", "5")
	if _, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{missing}, []runtimepipeline.StandingServiceCandidate{revised}); err == nil || !strings.Contains(err.Error(), "is not persisted") {
		t.Fatalf("replacement with missing predecessor error = %v", err)
	}
	statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if status.ServiceID == retained.ServiceID && status.BundleHash != retained.Source.BundleHash() {
			t.Fatalf("failed replacement leaked retained revision: %#v", status)
		}
	}

	added := makeCandidate("added", "6")
	seedStoreTestPersistedBundle(t, db, added.Source.BundleHash())
	results, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed}, []runtimepipeline.StandingServiceCandidate{revised, added})
	if err != nil {
		t.Fatalf("ReconcileStandingServiceReplacement: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("replacement results = %#v, want revised, created, and orphaned", results)
	}
	if !results[0].DeliveryContinuationRequired {
		t.Fatalf("replacement omitted committed delivery-continuation evidence: %#v", results)
	}
	statuses, err = workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]runtimepipeline.StandingServiceStatus, len(statuses))
	for _, status := range statuses {
		byID[status.ServiceID] = status
	}
	if got := byID[retained.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != revised.Source.BundleHash() || got.RunID != initialRunID[retained.ServiceID] || got.Transition != "revised" {
		t.Fatalf("retained service = %#v", got)
	}
	if got := byID[removed.ServiceID]; got.DeclarationPresent || got.EffectiveState != "orphaned" || got.Transition != "orphaned" {
		t.Fatalf("removed service = %#v", got)
	}
	if got := byID[added.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.Transition != "created" {
		t.Fatalf("added service = %#v", got)
	}
	if got := byID[unrelated.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != unrelated.Source.BundleHash() || got.RunID != initialRunID[unrelated.ServiceID] {
		t.Fatalf("unrelated service = %#v", got)
	}
}

func TestPostgresStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := admitTestPostgresStore(t, db)
	workflowStore, genericReconciler := newGenericScheduleAwareWorkflowTestCoordinator(t, selected)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("6", 64)),
	}
	seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
	fixtureCtx := testAuthorActivityContextForBundle(candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("ReconcileStandingServiceSet = %#v, %v", created, err)
	}
	eventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	agentID := "standing-agent"
	fixtureCtx = runtimecorrelation.WithRunID(fixtureCtx, created[0].RunID)
	var workEvent events.Event
	for _, fixture := range []struct {
		id        string
		eventType events.EventType
	}{
		{id: eventID, eventType: "standing.work"},
		{id: unsettledEventID, eventType: "standing.unsettled"},
	} {
		event := eventtest.PersistedProjection(
			fixture.id, fixture.eventType, "test", "", json.RawMessage(`{}`), 0,
			created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC(),
		)
		if fixture.id == eventID {
			workEvent = event
			continue
		}
		if err := commitSemanticEventFixture(fixtureCtx, selected, event); err != nil {
			t.Fatal(err)
		}
	}
	identity := testAgentIdentity(t, agentID, "standing/ingress")
	fields := testAgentIdentityStorageFields(t, identity)
	workRoute := testAgentDeliveryRoute(t, agentID, "standing/ingress")
	if err := commitSemanticEventFixtureWithRoutes(fixtureCtx, selected, workEvent, []events.DeliveryRoute{workRoute}); err != nil {
		t.Fatal(err)
	}
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, conversation, runtime_state, status
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', '[]', '{}', 'active')
	`, uuid.NewString(), created[0].RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
		t.Fatal(err)
	}
	if _, err := claimDeliveryFixture(fixtureCtx, selected, workEvent, workRoute); err != nil {
		t.Fatal(err)
	}
	genericActivation, workflowActivation := seedGenericScheduleTimerFamilies(t, selected, db, fixtureCtx)
	continuationAuthority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-lifecycle", 1)
	if err != nil {
		t.Fatal(err)
	}
	var continuationSignals atomic.Int32
	registration, err := workflowStore.RegisterDeliveryContinuationSignal(continuationAuthority, func() { continuationSignals.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" || !suspended.DeliveryContinuationRequired {
		t.Fatalf("suspended = %#v", suspended)
	}
	assertGenericScheduleTimerFamilyCancellation(
		t, selected, db, fixtureCtx, genericActivation, workflowActivation, suspended.TimerCancellations,
	)
	assertGenericScheduleReconciled(t, genericReconciler, genericActivation.ID)
	if got := continuationSignals.Load(); got != 1 {
		t.Fatalf("suspend continuation signals = %d, want 1", got)
	}
	var runStatus, deliveryStatus, sessionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE run_id = $1::uuid`, created[0].RunID).Scan(&sessionStatus); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := db.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || sessionStatus != "terminated" {
		t.Fatalf("suspend state = %s/%s/%s", runStatus, deliveryStatus, sessionStatus)
	}
	if pipelineOutcome != "dead_letter" || pipelineReason != "standing_suspended" {
		t.Fatalf("unsettled pipeline receipt = %s/%s", pipelineOutcome, pipelineReason)
	}
	assertStandingMockOnlyLifecycleRejectsLiveWorkBeforeMutation(
		t, selected, db, fixtureCtx, workflowStore, serviceID, created[0].RunID, created[0].Generation,
	)
	if _, err := workflowStore.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"}); err != nil {
		t.Fatalf("ResumeStandingService: %v", err)
	}
	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResetStandingService: %v", err)
	}
	if reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) || !reset.DeliveryContinuationRequired {
		t.Fatalf("reset = %#v", reset)
	}
	if got := continuationSignals.Load(); got != 2 {
		t.Fatalf("reset continuation signals = %d, want 2", got)
	}
}

func assertStandingMockOnlyLifecycleRejectsLiveWorkBeforeMutation(
	t *testing.T,
	selected workflowTestSelectedStore,
	db *sql.DB,
	ctx context.Context,
	liveCoordinator *runtimepipeline.PipelineCoordinator,
	serviceID, runID string,
	generation int64,
) {
	t.Helper()
	workflowTimer := newWorkflowTimerDDLProofRow(runID)
	if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, workflowTimer); err != nil {
		t.Fatalf("insert standing posture timer fixture: %v", err)
	}

	options := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(selected), selected)
	options.ExecutionPosture = executionposture.MockOnly
	mockCoordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, options)
	if mockCoordinator == nil {
		t.Fatal("construct mock-only standing lifecycle coordinator")
	}

	assertRejected := func(operation string, mutate func() error) {
		t.Helper()
		err := mutate()
		if err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
			t.Fatalf("%s error = %v, want mock-only live-work rejection", operation, err)
		}
		statuses, statusErr := liveCoordinator.ListStandingServiceStatuses(ctx)
		if statusErr != nil {
			t.Fatalf("load standing status after rejected %s: %v", operation, statusErr)
		}
		var found bool
		for _, status := range statuses {
			if status.ServiceID != serviceID {
				continue
			}
			found = true
			if status.RunID != runID || status.Generation != generation || status.EffectiveState != "suspended" || status.OperatorOverride != "suspended" {
				t.Fatalf("standing status after rejected %s = %#v", operation, status)
			}
		}
		if !found {
			t.Fatalf("standing service %s missing after rejected %s", serviceID, operation)
		}
		var runStatus string
		var queryErr error
		if _, postgres := selected.(*PostgresStore); postgres {
			queryErr = db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&runStatus)
		} else {
			queryErr = db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, runID).Scan(&runStatus)
		}
		if queryErr != nil {
			t.Fatalf("load standing run after rejected %s: %v", operation, queryErr)
		}
		if runStatus != "paused" {
			t.Fatalf("standing run status after rejected %s = %q, want paused", operation, runStatus)
		}
	}

	assertRejected("resume", func() error {
		_, err := mockCoordinator.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
		return err
	})
	assertRejected("reset", func() error {
		_, err := mockCoordinator.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
		return err
	})
}

func TestSQLiteRunStopRefusesCurrentStandingGenerationWithTeachingCommand(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress", InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("7", 64)),
	}
	seedStoreTestPersistedBundle(t, store.backend.ConstructionHandle(), candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: created.RunID})
	if err == nil || !strings.Contains(err.Error(), "swarm standing suspend "+serviceID) || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("StopRunControl error = %v", err)
	}
	var status string
	if err := store.backend.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("standing run status = %s, want running", status)
	}
}
