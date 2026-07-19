package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorObservabilityEventOwnerFiltersDetailsAndCursor(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	entityID := uuid.NewString()
	olderEventID := uuid.NewString()
	newerEventID := uuid.NewString()
	base := time.Unix(1700000000, 0).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedOperatorObservabilityEvent(t, ctx, pg, olderEventID, runID, "task.failed", events.EventProducerAgent, "agent-a", json.RawMessage(`{"entity_id":"`+entityID+`","n":1}`), entityID, base)
	seedOperatorObservabilityEvent(t, ctx, pg, newerEventID, runID, "task.completed", events.EventProducerAgent, "agent-b", json.RawMessage(`{"entity_id":"`+entityID+`","n":2}`), entityID, base.Add(time.Minute))
	if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, created_at)
			VALUES
				($1::uuid, $2::uuid, 'agent', 'agent-a', 'dead_letter', 3, 'retry_exhausted', $3::jsonb, $4),
				($1::uuid, $2::uuid, 'node', 'node-a', 'failed', 1, 'handler_error', $5::jsonb, $6)
		`, runID, olderEventID,
		mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "retry_exhausted", nil)), base.Add(time.Second),
		mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "node_failed", nil)), base.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			original_event_id, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		) VALUES (
			$1::uuid, 'task.failed', '{}'::jsonb, $2::uuid, 'flow-1',
			$3::jsonb, 3, 1, 'handler-a', $4
		)
	`, olderEventID, entityID, mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "retry_exhausted", nil)), base.Add(2*time.Second)); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}

	hasDead := true
	filtered, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{
		Filter: OperatorEventListFilter{
			RunID:          runID,
			DeliveryStatus: "dead_letter",
			ReasonCode:     "retry_exhausted",
			HasDeadLetter:  &hasDead,
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListOperatorEvents filtered: %v", err)
	}
	if len(filtered.Events) != 1 {
		t.Fatalf("filtered events len = %d, want 1: %#v", len(filtered.Events), filtered.Events)
	}
	got := filtered.Events[0]
	if got.EventID != olderEventID || got.Source != "agent-a" || got.Deliveries[0].ReasonCode != "retry_exhausted" || len(got.DeadLetters) != 1 {
		t.Fatalf("filtered event = %#v", got)
	}
	if len(got.Deliveries) != 2 {
		t.Fatalf("deliveries len = %d, want 2", len(got.Deliveries))
	}
	agentDelivery := got.Deliveries[0]
	if agentDelivery.SubscriberType != "agent" || agentDelivery.RetryCount != 3 || agentDelivery.RetryEligible || !agentDelivery.Terminal || len(agentDelivery.DeadLetters) != 1 {
		t.Fatalf("agent delivery evidence = %#v", agentDelivery)
	}
	nodeDelivery := got.Deliveries[1]
	if nodeDelivery.SubscriberType != "node" || nodeDelivery.RetryCount != 1 || !nodeDelivery.RetryEligible || nodeDelivery.Terminal || nodeDelivery.Failure == nil || nodeDelivery.Failure.Detail.Code != "node_failed" {
		t.Fatalf("node delivery evidence = %#v", nodeDelivery)
	}

	page1, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{Filter: OperatorEventListFilter{RunID: runID}, Limit: 1})
	if err != nil {
		t.Fatalf("ListOperatorEvents page1: %v", err)
	}
	if len(page1.Events) != 1 || page1.Events[0].EventID != newerEventID || page1.NextCursor == "" {
		t.Fatalf("page1 = %#v", page1)
	}
	page2, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{Filter: OperatorEventListFilter{RunID: runID}, Limit: 1, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("ListOperatorEvents page2: %v", err)
	}
	if len(page2.Events) != 1 || page2.Events[0].EventID != olderEventID {
		t.Fatalf("page2 = %#v", page2)
	}
	ascPage1, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{Filter: OperatorEventListFilter{RunID: runID}, Limit: 1, Order: "asc"})
	if err != nil {
		t.Fatalf("ListOperatorEvents asc page1: %v", err)
	}
	if len(ascPage1.Events) != 1 || ascPage1.Events[0].EventID != olderEventID || ascPage1.NextCursor == "" {
		t.Fatalf("asc page1 = %#v", ascPage1)
	}
	ascPage2, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{Filter: OperatorEventListFilter{RunID: runID}, Limit: 1, Order: "asc", Cursor: ascPage1.NextCursor})
	if err != nil {
		t.Fatalf("ListOperatorEvents asc page2: %v", err)
	}
	if len(ascPage2.Events) != 1 || ascPage2.Events[0].EventID != newerEventID {
		t.Fatalf("asc page2 = %#v", ascPage2)
	}
	sinceBase := base
	afterBase, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{
		Filter: OperatorEventListFilter{RunID: runID},
		Since:  &sinceBase,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListOperatorEvents since: %v", err)
	}
	if len(afterBase.Events) != 1 || afterBase.Events[0].EventID != newerEventID {
		t.Fatalf("since events = %#v, want only newer event", afterBase.Events)
	}

	if _, err := pg.LoadOperatorEvent(ctx, uuid.NewString()); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("LoadOperatorEvent missing error = %v, want ErrEventNotFound", err)
	}
}

func TestOperatorObservabilityEventOwnerDoesNotPromotePayloadEntityIdentity(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	targetEntityID := uuid.NewString()
	payloadOnlyEventID := uuid.NewString()
	canonicalEventID := uuid.NewString()
	base := time.Unix(1700001200, 0).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedOperatorObservabilityEvent(t, ctx, pg, payloadOnlyEventID, runID, "task.payload_only", events.EventProducerAgent, "agent-a", json.RawMessage(`{"entity_id":"`+targetEntityID+`","marker":"payload-only"}`), "", base)
	seedOperatorObservabilityEvent(t, ctx, pg, canonicalEventID, runID, "task.canonical_entity", events.EventProducerAgent, "agent-b", json.RawMessage(`{"entity_id":"payload-business-value","marker":"canonical"}`), targetEntityID, base.Add(time.Second))

	filtered, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{
		Filter: OperatorEventListFilter{EntityID: targetEntityID},
		Limit:  10,
		Order:  "asc",
	})
	if err != nil {
		t.Fatalf("ListOperatorEvents entity filter: %v", err)
	}
	if len(filtered.Events) != 1 || filtered.Events[0].EventID != canonicalEventID {
		t.Fatalf("filtered events = %#v, want only canonical event %s", filtered.Events, canonicalEventID)
	}
	if filtered.Events[0].EntityID != targetEntityID {
		t.Fatalf("canonical event entity_id = %q, want %s", filtered.Events[0].EntityID, targetEntityID)
	}

	payloadOnly, err := pg.LoadOperatorEvent(ctx, payloadOnlyEventID)
	if err != nil {
		t.Fatalf("LoadOperatorEvent payload-only: %v", err)
	}
	if payloadOnly.EntityID != "" {
		t.Fatalf("payload-only top-level entity_id = %q, want empty", payloadOnly.EntityID)
	}
	if got := readStoreString(payloadOnly.Payload["entity_id"]); got != targetEntityID {
		t.Fatalf("payload entity_id = %q, want preserved payload value %s", got, targetEntityID)
	}

	canonical, err := pg.LoadOperatorEvent(ctx, canonicalEventID)
	if err != nil {
		t.Fatalf("LoadOperatorEvent canonical: %v", err)
	}
	if canonical.EntityID != targetEntityID {
		t.Fatalf("canonical top-level entity_id = %q, want %s", canonical.EntityID, targetEntityID)
	}
	if got := readStoreString(canonical.Payload["entity_id"]); got != "payload-business-value" {
		t.Fatalf("canonical payload entity_id = %q, want payload-business-value", got)
	}
}

func TestOperatorRuntimeObservabilityOwnerLogsIncidentsAndCursor(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	insertLog := func(code string, createdAt time.Time) string {
		t.Helper()
		eventID := uuid.NewString()
		failure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, code, nil))
		payload := `{
			"log_level":"error",
			"message":"runtime failed",
			"details":{
				"component":"mcp-gateway",
				"action":"request_failed",
				"agent_id":"agent-1",
				"entity_id":"` + uuid.NewString() + `",
				"failure":` + failure + `
			}
		}`
		seedOperatorRuntimeLog(t, ctx, pg, eventID, runID, "runtime", json.RawMessage(payload), createdAt)
		return eventID
	}
	olderLog := insertLog("old_code", base)
	newerLog := insertLog("new_code", base.Add(time.Minute))

	page1, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "mcp-gateway",
		Level:     "error",
		Limit:     1,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs page1: %v", err)
	}
	if len(page1.Logs) != 1 || page1.Logs[0].LogID != newerLog || page1.NextCursor == "" {
		t.Fatalf("runtime log page1 = %#v", page1)
	}
	page2, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "mcp-gateway",
		Level:     "error",
		Limit:     1,
		Order:     "desc",
		Cursor:    page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs page2: %v", err)
	}
	if len(page2.Logs) != 1 || page2.Logs[0].LogID != olderLog {
		t.Fatalf("runtime log page2 = %#v", page2)
	}
	sinceBase := base
	afterBase, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "mcp-gateway",
		Level:     "error",
		Since:     &sinceBase,
		Limit:     10,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs since: %v", err)
	}
	if len(afterBase.Logs) != 1 || afterBase.Logs[0].LogID != newerLog {
		t.Fatalf("since logs = %#v, want only newer log", afterBase.Logs)
	}

	incidents, err := pg.ListOperatorRuntimeIncidents(ctx, OperatorRuntimeIncidentListOptions{
		SinceHours: 2,
		MCPOnly:    true,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeIncidents: %v", err)
	}
	if len(incidents.Incidents) != 2 {
		t.Fatalf("incidents len = %d, want 2: %#v", len(incidents.Incidents), incidents.Incidents)
	}
	if incidents.Incidents[0].ErrorCode != "new_code" || len(incidents.Incidents[0].SampleLogIDs) != 1 {
		t.Fatalf("first incident = %#v", incidents.Incidents[0])
	}

	bulkFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, "bulk_code", nil))
	bulkPayload := json.RawMessage(`{"log_level":"error","message":"bulk runtime failed","details":{"component":"mcp-gateway","action":"request_failed","agent_id":"agent-1","failure":` + bulkFailure + `}}`)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin bulk runtime-log fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin bulk runtime-log author activity: %v", err)
	}
	for i := 1; i <= 1005; i++ {
		event := eventtest.DiagnosticDirect(
			uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", bulkPayload, 0, runID, "", events.EventEnvelope{}, base.Add(2*time.Minute+time.Duration(i)*time.Millisecond),
		)
		if err := commitDiagnosticRuntimeLogFixtureTx(txctx, pg, tx, event); err != nil {
			t.Fatalf("seed bulk runtime log %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit bulk runtime-log fixture: %v", err)
	}
	bulkIncidents, err := pg.ListOperatorRuntimeIncidents(ctx, OperatorRuntimeIncidentListOptions{
		SinceHours: 2,
		MCPOnly:    true,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeIncidents bulk: %v", err)
	}
	var bulk *OperatorRuntimeIncident
	for idx := range bulkIncidents.Incidents {
		if bulkIncidents.Incidents[idx].ErrorCode == "bulk_code" {
			bulk = &bulkIncidents.Incidents[idx]
			break
		}
	}
	if bulk == nil || bulk.Count != 1005 {
		t.Fatalf("bulk incident = %#v, want count 1005 in %#v", bulk, bulkIncidents.Incidents)
	}
}

func TestPostgresRuntimeLogSourceFilterUsesCanonicalAgentOrRuntime(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	insertLog := func(message, details string, createdAt time.Time) string {
		t.Helper()
		eventID := uuid.NewString()
		payload := `{
			"log_level":"warn",
			"message":"` + message + `",
			"details":` + details + `
		}`
		seedOperatorRuntimeLog(t, ctx, pg, eventID, runID, "runtime", json.RawMessage(payload), createdAt)
		return eventID
	}
	runtimeFallbackID := insertLog("runtime fallback", `{"component":"source-parity","action":"runtime_fallback"}`, base.Add(time.Second))
	agentID := insertLog("agent source", `{"component":"source-parity","action":"agent_source","agent_id":"agent-1"}`, base.Add(2*time.Second))
	blankAgentID := insertLog("blank agent fallback", `{"component":"source-parity","action":"blank_agent_fallback","agent_id":"   "}`, base.Add(3*time.Second))

	all, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Limit:     10,
		Order:     "asc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs all: %v", err)
	}
	if len(all.Logs) != 3 {
		t.Fatalf("all logs = %#v, want three", all.Logs)
	}
	assertRuntimeLogIDsAndSources(t, all.Logs, map[string]string{
		runtimeFallbackID: "runtime",
		agentID:           "agent-1",
		blankAgentID:      "runtime",
	})

	runtimeRows, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "runtime",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs runtime source: %v", err)
	}
	assertRuntimeLogIDsAndSources(t, runtimeRows.Logs, map[string]string{
		runtimeFallbackID: "runtime",
		blankAgentID:      "runtime",
	})

	agentRows, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "agent-1",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs agent source: %v", err)
	}
	assertRuntimeLogIDsAndSources(t, agentRows.Logs, map[string]string{agentID: "agent-1"})

	missingRows, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "missing-source",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs missing source: %v", err)
	}
	if len(missingRows.Logs) != 0 {
		t.Fatalf("missing source rows = %#v, want none", missingRows.Logs)
	}
}

func assertRuntimeLogIDsAndSources(t *testing.T, logs []OperatorRuntimeLogEntry, want map[string]string) {
	t.Helper()
	if len(logs) != len(want) {
		t.Fatalf("runtime logs = %#v, want %d rows", logs, len(want))
	}
	for _, log := range logs {
		wantSource, ok := want[log.LogID]
		if !ok {
			t.Fatalf("unexpected runtime log row = %#v; want ids %#v", log, want)
		}
		if got := strings.TrimSpace(log.Source); got != wantSource {
			t.Fatalf("runtime log %s source = %q, want %q", log.LogID, got, wantSource)
		}
	}
}

func TestOperatorRuntimeLogsFilterBySessionAndTimeWindow(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	insertLog := func(sessionID string, createdAt time.Time) string {
		t.Helper()
		eventID := uuid.NewString()
		payload := `{
			"log_level":"warn",
			"message":"runtime warning",
			"details":{
				"component":"agent-runtime",
				"action":"turn_progress",
				"session_id":"` + sessionID + `"
			}
		}`
		seedOperatorRuntimeLog(t, ctx, pg, eventID, runID, "runtime", json.RawMessage(payload), createdAt)
		return eventID
	}
	inWindow := insertLog("sess-1", base.Add(1*time.Second))
	_ = insertLog("sess-2", base.Add(2*time.Second))
	_ = insertLog("sess-1", base.Add(3*time.Second))

	since := base
	until := base.Add(2500 * time.Millisecond)
	result, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		SessionID: "sess-1",
		Since:     &since,
		Until:     &until,
		Limit:     10,
		Order:     "asc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs: %v", err)
	}
	if len(result.Logs) != 1 {
		t.Fatalf("runtime logs len=%d logs=%#v, want one session/time-window row", len(result.Logs), result.Logs)
	}
	if got := result.Logs[0]; got.LogID != inWindow || got.SessionID != "sess-1" || got.Component != "agent-runtime" {
		t.Fatalf("runtime log row = %#v", got)
	}
}

func TestOperatorRuntimeObservabilityFiltersByBundleHash(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	bundleA := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bundleB := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	runA := uuid.NewString()
	runB := uuid.NewString()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', $3),
			($4::uuid, 'running', $5, 'persisted', $3)
	`, runA, bundleA, base, runB, bundleB); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	insertLog := func(runID, code string, createdAt time.Time) string {
		t.Helper()
		eventID := uuid.NewString()
		failure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, code, nil))
		payload := `{
			"log_level":"error",
			"message":"runtime failed",
			"details":{
				"component":"mcp-gateway",
				"action":"request_failed",
				"agent_id":"agent-1",
				"failure":` + failure + `
			}
		}`
		seedOperatorRuntimeLog(t, ctx, pg, eventID, runID, "runtime", json.RawMessage(payload), createdAt)
		return eventID
	}
	logA := insertLog(runA, "bundle_a_code", base.Add(time.Second))
	_ = insertLog(runB, "bundle_b_code", base.Add(2*time.Second))

	logs, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		BundleHash: bundleA,
		Limit:      10,
		Order:      "asc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs bundle_hash: %v", err)
	}
	if len(logs.Logs) != 1 || logs.Logs[0].LogID != logA || logs.Logs[0].RunID != runA {
		t.Fatalf("bundle-filtered logs = %#v, want only run A log", logs.Logs)
	}

	mismatched, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:      runB,
		BundleHash: bundleA,
		Limit:      10,
		Order:      "asc",
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs run_id+bundle_hash mismatch: %v", err)
	}
	if len(mismatched.Logs) != 0 {
		t.Fatalf("mismatched run_id+bundle_hash logs = %#v, want none", mismatched.Logs)
	}

	incidents, err := pg.ListOperatorRuntimeIncidents(ctx, OperatorRuntimeIncidentListOptions{
		BundleHash: bundleA,
		SinceHours: 2,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeIncidents bundle_hash: %v", err)
	}
	if len(incidents.Incidents) != 1 || incidents.Incidents[0].ErrorCode != "bundle_a_code" {
		t.Fatalf("bundle-filtered incidents = %#v, want only bundle A aggregate", incidents.Incidents)
	}
}

func TestRunDebugTracePageCursorAndRunNotFound(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	base := time.Unix(1700000300, 0).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	firstEvent := uuid.NewString()
	secondEvent := uuid.NewString()
	seedOperatorObservabilityEvent(t, ctx, pg, firstEvent, runID, "first.event", events.EventProducerPlatform, "runtime", json.RawMessage(`{}`), "", base)
	seedOperatorObservabilityEvent(t, ctx, pg, secondEvent, runID, "second.event", events.EventProducerPlatform, "runtime", json.RawMessage(`{}`), "", base.Add(time.Second))

	page1, next, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 1})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page1: %v", err)
	}
	if len(page1) != 1 || page1[0].EventID != firstEvent || next == "" {
		t.Fatalf("trace page1 rows=%#v next=%q", page1, next)
	}
	page2, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 1, Cursor: next})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page2: %v", err)
	}
	if len(page2) != 1 || page2[0].EventID != secondEvent {
		t.Fatalf("trace page2 = %#v", page2)
	}
	until := base.Add(time.Second)
	boundedPage1, boundedNext, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 1, Until: &until})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage bounded page1: %v", err)
	}
	if len(boundedPage1) != 1 || boundedPage1[0].EventID != firstEvent || boundedNext == "" {
		t.Fatalf("bounded trace page1 rows=%#v next=%q", boundedPage1, boundedNext)
	}
	boundedPage2, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 1, Cursor: boundedNext, Until: &until})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage bounded page2: %v", err)
	}
	if len(boundedPage2) != 1 || boundedPage2[0].EventID != secondEvent {
		t.Fatalf("bounded trace page2 = %#v", boundedPage2)
	}
	sinceRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Since: &base})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage since: %v", err)
	}
	if len(sinceRows) != 1 || sinceRows[0].EventID != secondEvent {
		t.Fatalf("trace since rows = %#v, want only second event", sinceRows)
	}
	untilRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Until: &base})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage until: %v", err)
	}
	if len(untilRows) != 1 || untilRows[0].EventID != firstEvent {
		t.Fatalf("trace until rows = %#v, want only first event", untilRows)
	}
	emptyWindowRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Since: &base, Until: &base})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage empty window: %v", err)
	}
	if len(emptyWindowRows) != 0 {
		t.Fatalf("trace equal since/until rows = %#v, want empty exclusive/inclusive window", emptyWindowRows)
	}
	windowRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Since: &base, Until: &until})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage bounded window: %v", err)
	}
	if len(windowRows) != 1 || windowRows[0].EventID != secondEvent {
		t.Fatalf("trace bounded window rows = %#v, want only second event", windowRows)
	}
	if _, _, err := pg.LoadRunDebugTracePage(ctx, uuid.NewString(), RunDebugTraceQueryOptions{}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("missing run error = %v, want ErrRunNotFound", err)
	}
}

func TestRunDebugTracePageExcludeRuntimeLogs(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	businessEvent := uuid.NewString()
	runtimeLogEvent := uuid.NewString()
	base := time.Unix(1700000600, 0).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedOperatorObservabilityEvent(t, ctx, pg, businessEvent, runID, "item.received", events.EventProducerPlatform, "runtime", json.RawMessage(`{}`), "", base)
	seedOperatorRuntimeLog(t, ctx, pg, runtimeLogEvent, runID, "runtime", json.RawMessage(`{}`), base.Add(time.Millisecond))

	allRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage all: %v", err)
	}
	if got := traceEventIDs(allRows); !sameStrings(got, []string{businessEvent, runtimeLogEvent}) {
		t.Fatalf("all trace rows = %#v, want business and runtime_log", got)
	}
	filteredRows, _, err := pg.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, ExcludeRuntimeLogs: true})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage filtered: %v", err)
	}
	if got := traceEventIDs(filteredRows); !sameStrings(got, []string{businessEvent}) {
		t.Fatalf("filtered trace rows = %#v, want business row only", got)
	}
}

func TestRunDebugTracePageTypedFilters(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runID := uuid.NewString()
	entityOne := uuid.NewString()
	entityTwo := uuid.NewString()
	firstEvent := uuid.NewString()
	secondEvent := uuid.NewString()
	firstDelivery := uuid.NewString()
	secondDelivery := uuid.NewString()
	base := time.Unix(1700000400, 0).UTC()
	until := base
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedOperatorObservabilityEvent(t, ctx, pg, firstEvent, runID, "first.event", events.EventProducerPlatform, "runtime", json.RawMessage(`{}`), entityOne, base)
	seedOperatorObservabilityEvent(t, ctx, pg, secondEvent, runID, "second.event", events.EventProducerPlatform, "runtime", json.RawMessage(`{}`), entityTwo, base.Add(time.Second))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at)
		VALUES
			($1::uuid, $2::uuid, $3::uuid, 'node', 'node-1', 'pending', '', $4),
			($5::uuid, $2::uuid, $6::uuid, 'agent', 'agent-2', 'dead_letter', 'handler_error', $7)
	`, firstDelivery, runID, firstEvent, base, secondDelivery, secondEvent, base.Add(time.Second)); err != nil {
		t.Fatalf("seed trace deliveries: %v", err)
	}

	for _, tc := range []struct {
		name string
		opts RunDebugTraceQueryOptions
		want string
	}{
		{
			name: "event name multi-value",
			opts: RunDebugTraceQueryOptions{Limit: 10, Filter: RunDebugTraceFilter{EventNames: []string{"missing.event", "second.event"}}},
			want: secondEvent,
		},
		{
			name: "entity id",
			opts: RunDebugTraceQueryOptions{Limit: 10, Filter: RunDebugTraceFilter{EntityIDs: []string{entityOne}}},
			want: firstEvent,
		},
		{
			name: "delivery status",
			opts: RunDebugTraceQueryOptions{Limit: 10, Filter: RunDebugTraceFilter{DeliveryStatuses: []string{"dead_letter"}}},
			want: secondEvent,
		},
		{
			name: "subscriber identity",
			opts: RunDebugTraceQueryOptions{Limit: 10, Filter: RunDebugTraceFilter{SubscriberIDs: []string{"agent-2"}, SubscriberTypes: []string{"agent"}}},
			want: secondEvent,
		},
		{
			name: "and composition",
			opts: RunDebugTraceQueryOptions{Limit: 10, Filter: RunDebugTraceFilter{EventNames: []string{"first.event", "second.event"}, EntityIDs: []string{entityTwo}, DeliveryStatuses: []string{"dead_letter"}, SubscriberIDs: []string{"agent-2"}, SubscriberTypes: []string{"agent"}}},
			want: secondEvent,
		},
		{
			name: "filter and until composition",
			opts: RunDebugTraceQueryOptions{Limit: 10, Until: &until, Filter: RunDebugTraceFilter{EventNames: []string{"first.event", "second.event"}}},
			want: firstEvent,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, _, err := pg.LoadRunDebugTracePage(ctx, runID, tc.opts)
			if err != nil {
				t.Fatalf("LoadRunDebugTracePage: %v", err)
			}
			if len(rows) != 1 || rows[0].EventID != tc.want {
				t.Fatalf("trace rows = %#v, want only %s", rows, tc.want)
			}
		})
	}
}

func seedOperatorObservabilityEvent(
	t *testing.T,
	ctx context.Context,
	pg *PostgresStore,
	eventID, runID, eventName string,
	producerType events.EventProducerType,
	producerID string,
	payload json.RawMessage,
	entityID string,
	createdAt time.Time,
) {
	t.Helper()
	envelope := events.EventEnvelope{}
	if entityID != "" {
		envelope = events.EnvelopeForEntityID(envelope, entityID)
	}
	producer := eventtest.Producer(producerType, producerID)
	var event events.Event
	if producerType == events.EventProducerPlatform {
		event = eventtest.PersistedRuntimeControlForProducer(eventID, events.EventType(eventName), producer, "", payload, 0, runID, "", envelope, createdAt)
	} else {
		event = eventtest.RootIngress(eventID, events.EventType(eventName), producerID, "", payload, 0, runID, "", envelope, createdAt)
	}
	if err := commitSemanticEventFixture(ctx, pg, event); err != nil {
		t.Fatalf("seed operator observability event %s: %v", eventName, err)
	}
}

func seedOperatorRuntimeLog(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, runID, producerID string, payload json.RawMessage, createdAt time.Time) {
	t.Helper()
	event := eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformRuntimeLog, producerID, "", payload, 0, runID, "", events.EventEnvelope{}, createdAt,
	)
	if err := commitDiagnosticRuntimeLogFixture(ctx, pg, event); err != nil {
		t.Fatalf("seed operator runtime log: %v", err)
	}
}
