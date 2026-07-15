package store

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteStandingServiceReconcileCreatesPublishesAndRepairsRestartAbandon(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
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
		Source: runtimecorrelation.BundleSourceFact{BundleHash: firstHash, BundleSource: "persisted"},
	}

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
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, fields, gates, accumulator, entered_state_at, created_at, updated_at)
		VALUES (?, ?, ?, 'default', 'ready', '{"name":"preserved"}', '{}', '{}', ?, ?, ?)
	`, created.RunID, entityID, "ingress/"+instanceID, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE runs SET status = 'cancelled', ended_at = ? WHERE run_id = ?`, time.Now().UTC(), created.RunID); err != nil {
		t.Fatalf("cancel standing run: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, stopped_at, updated_at) VALUES (?, 'stopped', 'server_restart_abandon', 'swarm.serve.abandon_active_runs', ?, ?)`, created.RunID, time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed restart-abandon provenance: %v", err)
	}
	candidate.Source.BundleHash = secondHash
	repaired, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService(repair): %v", err)
	}
	if repaired.Transition != "repaired" || repaired.Generation != 2 || repaired.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("repaired reconciliation = %#v", repaired)
	}
	var state, name string
	if err := store.DB.QueryRowContext(ctx, `SELECT current_state, json_extract(fields, '$.name') FROM entity_state WHERE run_id = ? AND entity_id = ?`, repaired.RunID, entityID).Scan(&state, &name); err != nil {
		t.Fatalf("load repaired entity state: %v", err)
	}
	if state != "ready" || name != "preserved" {
		t.Fatalf("repaired entity state = %s/%s", state, name)
	}
	var oldStatus, retiredReason string
	if err := store.DB.QueryRowContext(ctx, `
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
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("3", 64), BundleSource: "persisted"},
	}
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE runs SET status = 'cancelled', ended_at = ? WHERE run_id = ?`, time.Now().UTC(), created.RunID); err != nil {
		t.Fatal(err)
	}
	_, err = workflowStore.ReconcileStandingService(ctx, candidate)
	if err == nil || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("error = %v, want teaching reset command", err)
	}
}

func TestSQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("4", 64), BundleSource: "persisted"},
	}
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
	timerID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO events (event_id, run_id, event_name, payload) VALUES (?, ?, 'standing.work', '{}')`, eventID, created.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO events (event_id, run_id, event_name, payload) VALUES (?, ?, 'standing.unsettled', '{}')`, unsettledEventID, created.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status) VALUES (?, ?, ?, 'agent', ?, 'in_progress')`, uuid.NewString(), created.RunID, eventID, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model, memory_enabled, memory_source) VALUES (?, 'worker', 'test', 1, 'authored')`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, conversation, runtime_state, status) VALUES (?, ?, ?, 'standing/ingress', 1, 'authored', '[]', '{}', 'active')`, sessionID, created.RunID, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status) VALUES (?, 'standing-timer', ?, 'timer.fire', ?, 'active')`, timerID, created.RunID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" || suspended.Transition != "suspended" {
		t.Fatalf("suspended = %#v", suspended)
	}
	var runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason, timerStatus string
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status, reason_code FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&deliveryStatus, &deliveryReason); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status, termination_reason FROM agent_sessions WHERE session_id = ?`, sessionID).Scan(&sessionStatus, &sessionReason); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id = ?`, timerID).Scan(&timerStatus); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := store.DB.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || deliveryReason != "standing_suspended" || sessionStatus != "terminated" || sessionReason != "cancelled" || timerStatus != "cancelled" {
		t.Fatalf("suspend state = run:%s delivery:%s/%s session:%s/%s timer:%s", runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason, timerStatus)
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
	if reset.Transition != "reset" || reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("reset = %#v", reset)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "cancelled" {
		t.Fatalf("reset predecessor status = %s, want cancelled", runStatus)
	}
}

func TestSQLiteStandingServiceSetOrphansRemovedDeclaration(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("5", 64), BundleSource: "persisted"},
	}
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("create set = %#v, %v", created, err)
	}
	results, err := workflowStore.ReconcileStandingServiceSet(ctx, nil)
	if err != nil {
		t.Fatalf("orphan set: %v", err)
	}
	if len(results) != 1 || results[0].Transition != "orphaned" || results[0].EffectiveState != "orphaned" {
		t.Fatalf("orphan results = %#v", results)
	}
	var declarationPresent bool
	var effectiveState, runStatus string
	if err := store.DB.QueryRowContext(ctx, `SELECT declaration_present, effective_state FROM standing_services WHERE service_id = ?`, serviceID).Scan(&declarationPresent, &effectiveState); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if declarationPresent || effectiveState != "orphaned" || runStatus != "paused" {
		t.Fatalf("orphan state = declared:%t state:%s run:%s", declarationPresent, effectiveState, runStatus)
	}
}

func TestSQLiteStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	testStandingServiceReplacementIsScopedAndAtomic(t, runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store))
}

func TestPostgresStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	testStandingServiceReplacementIsScopedAndAtomic(t, runtimepipeline.NewWorkflowInstanceStore(db))
}

func testStandingServiceReplacementIsScopedAndAtomic(t *testing.T, workflowStore *runtimepipeline.WorkflowInstanceStore) {
	t.Helper()
	ctx := context.Background()
	makeCandidate := func(flowID, hashDigit string) runtimepipeline.StandingServiceCandidate {
		return runtimepipeline.StandingServiceCandidate{
			ServiceID: runtimeflowidentity.StandingServiceID("project", flowID), PackageKey: "project", FlowID: flowID,
			InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
			Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat(hashDigit, 64), BundleSource: "persisted"},
		}
	}
	retained := makeCandidate("retained", "1")
	removed := makeCandidate("removed", "2")
	unrelated := makeCandidate("unrelated", "3")
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed, unrelated})
	if err != nil || len(created) != 3 {
		t.Fatalf("seed standing services = %#v, %v", created, err)
	}
	initialRunID := map[string]string{}
	for _, result := range created {
		initialRunID[result.ServiceID] = result.RunID
	}

	revised := retained
	revised.Source.BundleHash = "bundle-v1:sha256:" + strings.Repeat("4", 64)
	missing := makeCandidate("missing", "5")
	if _, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{missing}, []runtimepipeline.StandingServiceCandidate{revised}); err == nil || !strings.Contains(err.Error(), "is not persisted") {
		t.Fatalf("replacement with missing predecessor error = %v", err)
	}
	statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if status.ServiceID == retained.ServiceID && status.BundleHash != retained.Source.BundleHash {
			t.Fatalf("failed replacement leaked retained revision: %#v", status)
		}
	}

	added := makeCandidate("added", "6")
	results, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed}, []runtimepipeline.StandingServiceCandidate{revised, added})
	if err != nil {
		t.Fatalf("ReconcileStandingServiceReplacement: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("replacement results = %#v, want revised, created, and orphaned", results)
	}
	statuses, err = workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]runtimepipeline.StandingServiceStatus, len(statuses))
	for _, status := range statuses {
		byID[status.ServiceID] = status
	}
	if got := byID[retained.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != revised.Source.BundleHash || got.RunID != initialRunID[retained.ServiceID] || got.Transition != "revised" {
		t.Fatalf("retained service = %#v", got)
	}
	if got := byID[removed.ServiceID]; got.DeclarationPresent || got.EffectiveState != "orphaned" || got.Transition != "orphaned" {
		t.Fatalf("removed service = %#v", got)
	}
	if got := byID[added.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.Transition != "created" {
		t.Fatalf("added service = %#v", got)
	}
	if got := byID[unrelated.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != unrelated.Source.BundleHash || got.RunID != initialRunID[unrelated.ServiceID] {
		t.Fatalf("unrelated service = %#v", got)
	}
}

func TestPostgresStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("6", 64), BundleSource: "persisted"},
	}
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("ReconcileStandingServiceSet = %#v, %v", created, err)
	}
	eventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	agentID := "standing-agent"
	if _, err := db.ExecContext(ctx, `INSERT INTO events (event_id, run_id, event_name, payload) VALUES ($1::uuid, $2::uuid, 'standing.work', '{}')`, eventID, created[0].RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events (event_id, run_id, event_name, payload) VALUES ($1::uuid, $2::uuid, 'standing.unsettled', '{}')`, unsettledEventID, created[0].RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status) VALUES ($1::uuid, $2::uuid, $3::uuid, 'agent', $4, 'in_progress')`, uuid.NewString(), created[0].RunID, eventID, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model, memory_enabled, memory_source) VALUES ($1, 'worker', 'test', TRUE, 'authored')`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, conversation, runtime_state, status) VALUES ($1::uuid, $2::uuid, $3, 'standing/ingress', TRUE, 'authored', '[]', '{}', 'active')`, uuid.NewString(), created[0].RunID, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status) VALUES ($1::uuid, 'standing-timer', $2::uuid, 'timer.fire', $3, 'active')`, uuid.NewString(), created[0].RunID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" {
		t.Fatalf("suspended = %#v", suspended)
	}
	var runStatus, deliveryStatus, sessionStatus, timerStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE run_id = $1::uuid`, created[0].RunID).Scan(&sessionStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM timers WHERE run_id = $1::uuid`, created[0].RunID).Scan(&timerStatus); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := db.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || sessionStatus != "terminated" || timerStatus != "cancelled" {
		t.Fatalf("suspend state = %s/%s/%s/%s", runStatus, deliveryStatus, sessionStatus, timerStatus)
	}
	if pipelineOutcome != "dead_letter" || pipelineReason != "standing_suspended" {
		t.Fatalf("unsettled pipeline receipt = %s/%s", pipelineOutcome, pipelineReason)
	}
	if _, err := workflowStore.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"}); err != nil {
		t.Fatalf("ResumeStandingService: %v", err)
	}
	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResetStandingService: %v", err)
	}
	if reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("reset = %#v", reset)
	}
}

func TestSQLiteRunStopRefusesCurrentStandingGenerationWithTeachingCommand(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	created, err := workflowStore.ReconcileStandingService(ctx, runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress", InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("7", 64), BundleSource: "persisted"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: created.RunID})
	if err == nil || !strings.Contains(err.Error(), "swarm standing suspend "+serviceID) || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("StopRunControl error = %v", err)
	}
	var status string
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("standing run status = %s, want running", status)
	}
}
