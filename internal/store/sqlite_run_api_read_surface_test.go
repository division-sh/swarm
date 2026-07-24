package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestSQLiteRunAPIReadSurface_LoadListAndDiagnoseEvidence(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Unix(1700000000, 0).UTC()
	newer := uuid.NewString()
	older := uuid.NewString()
	newerEvent := uuid.NewString()
	newerMiddleEvent := uuid.NewString()
	newerLatestEvent := uuid.NewString()
	olderEvent := uuid.NewString()
	newerEntityA := uuid.NewString()
	newerEntityB := uuid.NewString()
	olderEntity := uuid.NewString()
	olderEventOnly := uuid.NewString()
	bundleA := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bundleB := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	runtimeLogFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, "proof_failure", nil))
	runtimeLogPayload := `{"log_level":"error","message":"boom","details":{"component":"runtime","action":"proof","failure":` + runtimeLogFailure + `}}`

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, trigger_event_id, trigger_event_type,
			forked_from_run_id, entity_count, event_count, failure, started_at, ended_at
		)
		VALUES
			(?, 'running', ?, 'ephemeral', ?, 'scan.requested', NULL, 3, 0, NULL, ?, NULL),
			(?, 'running', ?, 'ephemeral', ?, 'scan.completed', ?, 5, 0, NULL, ?, NULL)
	`, newer, bundleA, newerEvent, now, older, bundleB, olderEvent, newer, now.Add(-time.Hour)); err != nil {
		t.Fatalf("seed sqlite runs: %v", err)
	}
	for _, fixture := range []struct {
		id, runID, name, entityID string
		at                        time.Time
	}{
		{newerEvent, newer, "scan.requested", "", now.Add(time.Second)},
		{newerMiddleEvent, newer, "scan.progressed", "", now.Add(2 * time.Second)},
		{newerLatestEvent, newer, "scan.finished", "", now.Add(3 * time.Second)},
		{olderEvent, older, "scan.completed", olderEntity, now.Add(-time.Hour + time.Second)},
		{uuid.NewString(), older, "scan.replayed", olderEventOnly, now.Add(-time.Hour + 2*time.Second)},
	} {
		envelope := events.EventEnvelope{}
		if fixture.entityID != "" {
			envelope = events.EnvelopeForEntityID(envelope, fixture.entityID)
		}
		if err := commitSemanticEventFixture(ctx, sqliteStore, eventtest.PersistedProjection(
			fixture.id, events.EventType(fixture.name), "test", "", json.RawMessage(`{}`), 0,
			fixture.runID, "", envelope, fixture.at,
		)); err != nil {
			t.Fatalf("seed sqlite event %s: %v", fixture.name, err)
		}
	}
	if err := commitDiagnosticRuntimeLogFixture(ctx, sqliteStore, eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(runtimeLogPayload), 0,
		newer, "", events.EventEnvelope{}, now.Add(4*time.Second),
	)); err != nil {
		t.Fatalf("seed sqlite runtime log: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		UPDATE runs SET status = 'completed', ended_at = ? WHERE run_id = ?
	`, now.Add(-30*time.Minute), older); err != nil {
		t.Fatalf("terminalize older sqlite run: %v", err)
	}
	seedSQLiteEntityStateRows(t, sqliteStore.DB, ctx, newer, newerEntityA, newerEntityB)
	seedSQLiteEntityStateRows(t, sqliteStore.DB, ctx, older, olderEntity)
	rootEvent := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.DB, newerEvent)
	pendingDelivery := seedDeliveryStateFixture(t, ctx, sqliteStore, rootEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-1",
	}, runtimedelivery.StateQueued, nil)
	setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.DB, pendingDelivery, now.Add(3*time.Second), now.Add(3*time.Second))

	middleEvent := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.DB, newerMiddleEvent)
	agentFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "agent_failure", nil)
	agentFailedDelivery := seedDeliveryStateFixture(t, ctx, sqliteStore, middleEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-failed",
	}, runtimedelivery.StateRetrying, &agentFailure)
	setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.DB, agentFailedDelivery, now.Add(4*time.Second), now.Add(5*time.Second))
	agentFailedDeliveryID := agentFailedDelivery.DeliveryID

	latestEvent := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.DB, newerLatestEvent)
	deadRoute := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberNode), SubscriberID: "node-dead"}
	if err := commitDeliveryObligationFixture(ctx, sqliteStore, latestEvent, deadRoute); err != nil {
		t.Fatalf("commit sqlite exhausted delivery: %v", err)
	}
	nodeFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "node_failure", nil)
	claimed, err := sqliteStore.ClaimNodeDelivery(ctx, latestEvent, deadRoute)
	if err != nil {
		t.Fatalf("claim sqlite exhausted delivery: %v", err)
	}
	var nodeDeadDelivery runtimedelivery.Snapshot
	for attempt := 0; attempt <= 3; attempt++ {
		nodeDeadDelivery, err = sqliteStore.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     &nodeFailure,
			RetryBase:   time.Second,
		})
		if err != nil {
			t.Fatalf("settle sqlite exhausted delivery attempt %d: %v", attempt+1, err)
		}
		if attempt == 3 {
			break
		}
		setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.DB, nodeDeadDelivery, now.Add(6*time.Second), now.Add(6*time.Second))
		claimed, err = sqliteStore.ClaimNodeDelivery(ctx, latestEvent, deadRoute)
		if err != nil {
			t.Fatalf("reclaim sqlite exhausted delivery attempt %d: %v", attempt+2, err)
		}
	}
	setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.DB, nodeDeadDelivery, now.Add(6*time.Second), now.Add(8*time.Second))
	nodeDeadDeliveryID := nodeDeadDelivery.DeliveryID

	successfulDelivery := seedDeliveryStateFixture(t, ctx, sqliteStore, middleEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberNode),
		SubscriberID:   "node-success",
	}, runtimedelivery.StateDelivered, nil)
	setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.DB, successfulDelivery, now.Add(5*time.Second), now.Add(7*time.Second))
	successDeliveryID := successfulDelivery.DeliveryID
	header, err := sqliteStore.LoadRunHeader(ctx, older)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.RunID != older || header.Status != "completed" || header.TriggerEventID != olderEvent || header.ForkedFromRunID != newer {
		t.Fatalf("header = %#v", header)
	}
	if header.EndedAt == nil {
		t.Fatal("header.EndedAt = nil, want terminal timestamp")
	}
	if header.EntityCount != 1 {
		t.Fatalf("header.EntityCount = %d, want entity_state count 1 despite stale run counter and event overcount", header.EntityCount)
	}

	firstPage, cursor, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListRunHeaders first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].RunID != newer {
		t.Fatalf("first page = %#v, want newer run", firstPage)
	}
	if firstPage[0].EntityCount != 2 {
		t.Fatalf("first page entity_count = %d, want entity_state count 2 despite event undercount", firstPage[0].EntityCount)
	}
	if cursor == "" {
		t.Fatal("cursor empty for truncated sqlite run list")
	}
	secondPage, next, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 1, Cursor: cursor})
	if err != nil {
		t.Fatalf("ListRunHeaders second page: %v", err)
	}
	if len(secondPage) != 1 || secondPage[0].RunID != older || next != "" {
		t.Fatalf("second page = %#v cursor=%q, want older only and no next cursor", secondPage, next)
	}
	filtered, _, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Status: "running", BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders filtered: %v", err)
	}
	if len(filtered) != 1 || filtered[0].RunID != newer {
		t.Fatalf("filtered = %#v, want newer only", filtered)
	}

	report, err := sqliteStore.LoadRunDebugReport(ctx, newer, RunDebugQueryOptions{EventLimit: 2})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if report.RunID != newer || report.RootEventID != newerEvent || report.WarnErrorLogCount != 1 {
		t.Fatalf("report = %#v", report)
	}
	if report.EntityCount != 2 {
		t.Fatalf("report.EntityCount = %d, want entity_state count 2", report.EntityCount)
	}
	if len(report.Deliveries) != 4 {
		t.Fatalf("report deliveries = %#v, want pending/delivered/failed/dead_letter delivery count groups", report.Deliveries)
	}
	if len(report.FailedDeliveries) != 2 {
		t.Fatalf("report failed deliveries = %#v, want 2", report.FailedDeliveries)
	}
	for _, got := range report.FailedDeliveries {
		if got.DeliveryID == successDeliveryID {
			t.Fatalf("successful delivered/node_processed delivery appeared in FailedDeliveries: %#v", report.FailedDeliveries)
		}
	}
	if got := report.FailedDeliveries[0]; got.DeliveryID != nodeDeadDeliveryID || got.SubscriberType != "node" || got.RetryCount != 3 || got.RetryEligible || !got.Terminal || len(got.DeadLetters) != 1 {
		t.Fatalf("node failed delivery evidence = %#v", got)
	}
	if got := report.FailedDeliveries[1]; got.DeliveryID != agentFailedDeliveryID || got.SubscriberType != "agent" || got.RetryCount != 1 || !got.RetryEligible || got.Terminal || got.Failure == nil || got.Failure.Detail.Code != "agent_failure" {
		t.Fatalf("agent failed delivery evidence = %#v", got)
	}
	traceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, newer, RunDebugTraceQueryOptions{
		Limit: 10,
		Filter: RunDebugTraceFilter{
			DeliveryStatuses: []string{"failed"},
		},
	})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage sqlite failed filter: %v", err)
	}
	if len(traceRows) != 1 {
		t.Fatalf("sqlite failed trace rows = %#v, want one failed delivery row", traceRows)
	}
	if got := traceRows[0]; got.DeliveryID != agentFailedDeliveryID || got.DeliveryFailure == nil || got.DeliveryFailure.Detail.Code != "agent_failure" || got.DeliveryRetryCount != 1 || !got.DeliveryRetryEligible || got.DeliveryTerminal {
		t.Fatalf("sqlite trace delivery failure evidence = %#v", got)
	}
	if len(report.Events) != 2 || report.Events[0].EventID != newerLatestEvent || report.Events[1].EventID != newerMiddleEvent {
		t.Fatalf("report events = %#v, want latest non-log events first", report.Events)
	}
	full, err := sqliteStore.LoadOperatorEvent(ctx, newerLatestEvent)
	if err != nil {
		t.Fatalf("LoadOperatorEvent sqlite latest: %v", err)
	}
	if len(full.DeadLetters) != 1 || full.DeadLetters[0].DeadLetterID == "" {
		t.Fatalf("sqlite event dead letters = %#v", full.DeadLetters)
	}
	if len(full.Deliveries) != 1 || len(full.Deliveries[0].DeadLetters) != 1 || !full.Deliveries[0].Terminal {
		t.Fatalf("sqlite event delivery evidence = %#v", full.Deliveries)
	}
	if len(report.RuntimeLogs) != 1 || report.RuntimeLogs[0].Component != "runtime" || report.RuntimeLogs[0].Action != "proof" {
		t.Fatalf("runtime logs = %#v, want runtime proof log", report.RuntimeLogs)
	}
}

func TestSQLiteRunAPIReadSurface_LoadRunDebugReportProjectsTestQuiescenceCounts(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	sqliteStore.nowFn = func() time.Time { return now }

	blockedRunID := uuid.NewString()
	readyRunID := uuid.NewString()
	activeEventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	runtimeLogEventID := uuid.NewString()
	readyEventID := uuid.NewString()

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES
			(?, 'running', ?),
			(?, 'running', ?)
	`, blockedRunID, now.Add(-time.Minute), readyRunID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed sqlite runs: %v", err)
	}
	for _, fixture := range []struct {
		id, runID, name string
		at              time.Time
	}{
		{activeEventID, blockedRunID, "quiescence.active_delivery", now.Add(-50 * time.Second)},
		{unsettledEventID, blockedRunID, "quiescence.missing_pipeline_receipt", now.Add(-40 * time.Second)},
		{readyEventID, readyRunID, "quiescence.ready", now.Add(-20 * time.Second)},
	} {
		if err := commitSemanticEventFixture(ctx, sqliteStore, eventtest.PersistedProjection(
			fixture.id, events.EventType(fixture.name), "test", "", json.RawMessage(`{}`), 0,
			fixture.runID, "", events.EventEnvelope{}, fixture.at,
		)); err != nil {
			t.Fatalf("seed sqlite event %s: %v", fixture.name, err)
		}
	}
	if err := commitDiagnosticRuntimeLogFixture(ctx, sqliteStore, eventtest.DiagnosticDirect(
		runtimeLogEventID, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{}`), 0,
		blockedRunID, "", events.EventEnvelope{}, now.Add(-30*time.Second),
	)); err != nil {
		t.Fatalf("seed sqlite runtime log: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, sqliteStore, activeEventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt active event: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, sqliteStore, readyEventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt ready event: %v", err)
	}
	activeEvent := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.DB, activeEventID)
	seedDeliveryStateFixture(t, ctx, sqliteStore, activeEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-active",
	}, runtimedelivery.StateQueued, nil)
	readyEvent := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.DB, readyEventID)
	seedDeliveryStateFixture(t, ctx, sqliteStore, readyEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-done",
	}, runtimedelivery.StateDelivered, nil)
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, fire_event, fire_payload,
			fire_at, owner_agent, task_type, status, created_at
		)
		VALUES
			(?, ?, 'due', 'quiescence.timeout', '{}', ?, 'timer-agent', 'timer', 'active', ?),
			(?, ?, 'settled', 'quiescence.timeout', '{}', ?, 'timer-agent', 'timer', 'fired', ?)
	`, uuid.NewString(), blockedRunID, now.Add(-time.Minute), now,
		uuid.NewString(), readyRunID, now.Add(-time.Minute), now); err != nil {
		t.Fatalf("seed sqlite timers: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
			config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, created_at
		)
		VALUES (
			'quiescence-agent', 'quiescence', 'worker', 'regular', 'mock', 1, 'authored',
			'{}', '[]', '[]', '[]', '{}', '{}', 'active', ?
		)
	`, now); err != nil {
		t.Fatalf("seed sqlite agent: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES
			(?, ?, 'quiescence-agent', 'quiescence', 1, 'authored', '{}',
				'worker-1', ?, 'active', ?, ?),
			(?, ?, 'quiescence-agent', 'quiescence', 1, 'authored', '{}',
				'worker-1', ?, 'active', ?, ?)
	`, uuid.NewString(), blockedRunID, now.Add(time.Minute), now, now,
		uuid.NewString(), readyRunID, now.Add(-time.Minute), now, now); err != nil {
		t.Fatalf("seed sqlite sessions: %v", err)
	}

	blocked, err := sqliteStore.LoadRunDebugReport(ctx, blockedRunID, RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport blocked: %v", err)
	}
	assertRunTestQuiescence(t, blocked.TestQuiescence, RunTestQuiescence{
		Ready:                   false,
		ActiveDeliveries:        1,
		UnsettledPipelineEvents: 1,
		DueTimers:               1,
		ActiveSessionLeases:     1,
	})

	ready, err := sqliteStore.LoadRunDebugReport(ctx, readyRunID, RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport ready: %v", err)
	}
	assertRunTestQuiescence(t, ready.TestQuiescence, RunTestQuiescence{Ready: true})
}

func TestSQLiteRunAPIReadSurface_LoadRunHeaderNotFound(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	_, err := sqliteStore.LoadRunHeader(testAuthorActivityContext(), uuid.NewString())
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("LoadRunHeader error = %v, want ErrRunNotFound", err)
	}
}
