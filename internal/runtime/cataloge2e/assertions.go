package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func assertCatalogRuntimeOutcome(t testing.TB, h *runtimeHarness, expected catalogExpectedDocument) {
	t.Helper()
	if len(expected.Expected.Entities) > 0 {
		assertCatalogRuntimeEntities(t, h, expected.Expected.Entities)
		return
	}
	if len(h.publishedIDs) > 0 && strings.TrimSpace(expected.Expected.HandlerOutcome) != "" {
		assertHandlerOutcome(t, h, expected.Expected.HandlerOutcome)
	}
	entityID := ""
	for _, step := range expected.triggerSequence() {
		if entityID = triggerPayloadEntityID(step.Payload); entityID != "" {
			break
		}
	}
	if entityID != "" && strings.TrimSpace(expected.Expected.EntityState) != "" {
		assertEntityState(t, h.db, h.workflow, entityID, expected.Expected.EntityState)
	}
	if entityID != "" {
		assertEntityFields(t, h.workflow, entityID, expected.Expected.EntityFields)
		assertGates(t, h.workflow, entityID, expected.Expected.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, expected.Expected.EmittedEvents)
		assertDeadLetter(t, h.db, h.startedAt, entityID, expected.Expected.DeadLetter)
	}
	assertAgentReceived(t, h.db, h.startedAt, expected.Expected.AgentReceived)
	if len(expected.Expected.FlowInstanceCreated) > 0 {
		assertFlowInstanceCreated(t, h.db, h.startedAt)
	}
	if expected.Expected.TemplateInstances != nil {
		assertFlowInstanceCount(t, h.db, h.startedAt, *expected.Expected.TemplateInstances)
	}
}

func assertCatalogRuntimeEntities(t testing.TB, h *runtimeHarness, expected map[string]catalogEntityExpected) {
	t.Helper()
	for entityID, want := range expected {
		entityID = strings.TrimSpace(entityID)
		if entityID == "" {
			continue
		}
		if strings.TrimSpace(want.HandlerOutcome) != "" {
			assertHandlerOutcomeForEntity(t, h, want.HandlerOutcome, entityID)
		}
		if strings.TrimSpace(want.EntityState) != "" {
			assertEntityState(t, h.db, h.workflow, entityID, want.EntityState)
		}
		assertEntityFields(t, h.workflow, entityID, want.EntityFields)
		assertGates(t, h.workflow, entityID, want.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, want.EmittedEvents)
		assertDeadLetter(t, h.db, h.startedAt, entityID, want.DeadLetter)
	}
}

func assertGates(t testing.TB, workflow *runtimepipeline.WorkflowInstanceStore, entityID string, want map[string]bool) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if workflow == nil {
		t.Fatal("workflow instance store is required for gates assertions")
	}
	instance, ok, err := workflow.Load(context.Background(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("load workflow instance %s for gates: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for gates assertion", entityID)
	}
	raw, _ := instance.Metadata["gates"].(map[string]any)
	for key, wantValue := range want {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		gotValue, ok := raw[key]
		if !ok {
			if !wantValue {
				continue
			}
			t.Fatalf("gate %q missing from metadata.gates; have keys=%v", key, metadataKeys(raw))
		}
		if boolFromAny(gotValue) != wantValue {
			t.Fatalf("gate %q = %v, want %v", key, boolFromAny(gotValue), wantValue)
		}
	}
}

func assertEntityFields(t testing.TB, workflow *runtimepipeline.WorkflowInstanceStore, entityID string, want map[string]any) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if workflow == nil {
		t.Fatal("workflow instance store is required for entity_fields assertions")
	}
	instance, ok, err := workflow.Load(context.Background(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("load workflow instance %s for entity_fields: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for entity_fields assertion", entityID)
	}
	for key, wantValue := range want {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		gotValue, ok := instance.Metadata[key]
		if !ok {
			t.Fatalf("entity field %q missing from metadata; have keys=%v", key, metadataKeys(instance.Metadata))
		}
		if strings.TrimSpace(asString(wantValue)) == "computed_value" {
			continue
		}
		gotCanonical, err := canonicalJSONValue(gotValue)
		if err != nil {
			t.Fatalf("canonicalize entity field %q got value: %v", key, err)
		}
		wantCanonical, err := canonicalJSONValue(wantValue)
		if err != nil {
			t.Fatalf("canonicalize entity field %q expected value: %v", key, err)
		}
		if gotCanonical != wantCanonical {
			t.Fatalf("entity field %q = %s, want %s", key, gotCanonical, wantCanonical)
		}
	}
}

func assertEntityState(t testing.TB, db *sql.DB, workflow *runtimepipeline.WorkflowInstanceStore, entityID, wantState string) {
	t.Helper()
	if workflow == nil {
		t.Fatal("workflow instance store is required")
	}
	instance, ok, err := workflow.Load(context.Background(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("load workflow instance %s: %v", entityID, err)
	}
	if !ok {
		rows, dumpErr := workflowStateDebugRows(db)
		if dumpErr != nil {
			t.Fatalf("workflow instance %s not found (debug dump failed: %v)", entityID, dumpErr)
		}
		t.Fatalf("workflow instance %s not found; entity_state rows=%s", entityID, rows)
	}
	if got := strings.TrimSpace(instance.CurrentState); got != strings.TrimSpace(wantState) {
		t.Fatalf("entity_state = %q, want %q", got, strings.TrimSpace(wantState))
	}
}

func workflowStateDebugRows(db *sql.DB) (string, error) {
	if db == nil {
		return "", nil
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT entity_id::text, COALESCE(flow_instance, ''), current_state
		FROM entity_state
		ORDER BY created_at ASC
	`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var rowID, flowInstance, state string
		if err := rows.Scan(&rowID, &flowInstance, &state); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprintf("{entity_id:%s flow_instance:%s state:%s}", rowID, flowInstance, state))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(out) == 0 {
		return "[]", nil
	}
	return "[" + strings.Join(out, ", ") + "]", nil
}

func assertEmittedEvents(t testing.TB, db *sql.DB, since time.Time, publishedIDs map[string]struct{}, entityID string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT event_id::text, event_name, COALESCE(payload->>'entity_id', '')
		FROM events
		WHERE created_at >= $1
		ORDER BY created_at ASC, event_id ASC
	`, since)
	if err != nil {
		t.Fatalf("query emitted events: %v", err)
	}
	defer rows.Close()
	got := make([]string, 0, 8)
	dedup := !hasDuplicateStrings(want)
	seen := make(map[string]struct{}, 8)
	for rows.Next() {
		var eventID, eventName, payloadEntityID string
		if err := rows.Scan(&eventID, &eventName, &payloadEntityID); err != nil {
			t.Fatalf("scan emitted event: %v", err)
		}
		if _, ok := publishedIDs[strings.TrimSpace(eventID)]; ok {
			continue
		}
		if strings.TrimSpace(payloadEntityID) != strings.TrimSpace(entityID) {
			continue
		}
		eventName = strings.TrimSpace(eventName)
		if shouldIgnoreCatalogE2EEvent(eventName) {
			continue
		}
		if dedup {
			if _, ok := seen[eventName]; ok {
				continue
			}
			seen[eventName] = struct{}{}
		}
		got = append(got, eventName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read emitted events: %v", err)
	}
	if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
		t.Fatalf("emitted_events = %v, want %v", got, want)
	}
}

func shouldIgnoreCatalogE2EEvent(eventName string) bool {
	eventName = strings.TrimSpace(eventName)
	switch eventName {
	case "platform.runtime_log", "spec.contradiction_detected":
		return true
	default:
		return false
	}
}

func assertDeadLetter(t testing.TB, db *sql.DB, since time.Time, entityID string, want bool) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_receipts r
		JOIN events e ON e.event_id = r.event_id
		WHERE r.processed_at >= $1
		  AND r.outcome = 'dead_letter'
		  AND COALESCE(e.payload->>'entity_id', '') = $2
	`, since, strings.TrimSpace(entityID)).Scan(&count); err != nil {
		t.Fatalf("query dead_letter receipts: %v", err)
	}
	got := count > 0
	if got != want {
		t.Fatalf("dead_letter = %v, want %v", got, want)
	}
}

func assertAgentReceived(t testing.TB, db *sql.DB, since time.Time, want map[string][]string) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	for agentID, expectedEvents := range want {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		rows, err := db.QueryContext(context.Background(), `
			SELECT e.event_name
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE d.created_at >= $1
			  AND d.subscriber_id = $2
			ORDER BY d.created_at ASC, e.event_id ASC
		`, since, agentID)
		if err != nil {
			t.Fatalf("query agent_received for %s: %v", agentID, err)
		}
		got := make([]string, 0, len(expectedEvents))
		for rows.Next() {
			var eventName string
			if err := rows.Scan(&eventName); err != nil {
				_ = rows.Close()
				t.Fatalf("scan agent_received for %s: %v", agentID, err)
			}
			got = append(got, strings.TrimSpace(eventName))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("read agent_received for %s: %v", agentID, err)
		}
		_ = rows.Close()
		if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", expectedEvents) {
			t.Fatalf("agent_received[%s] = %v, want %v", agentID, got, expectedEvents)
		}
	}
}

func assertFlowInstanceCreated(t testing.TB, db *sql.DB, since time.Time) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM flow_instances
		WHERE created_at >= $1
	`, since).Scan(&count); err != nil {
		t.Fatalf("query flow_instances: %v", err)
	}
	if count == 0 {
		t.Fatal("expected flow instance to be created")
	}
}

func assertFlowInstanceCount(t testing.TB, db *sql.DB, since time.Time, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM flow_instances
		WHERE created_at >= $1
	`, since).Scan(&count); err != nil {
		t.Fatalf("query flow_instances count: %v", err)
	}
	if count != want {
		t.Fatalf("flow_instances count = %d, want %d", count, want)
	}
}

func assertHandlerOutcome(t testing.TB, h *runtimeHarness, want string) {
	t.Helper()
	assertHandlerOutcomeForEntity(t, h, want, "")
}

func assertHandlerOutcomeForEntity(t testing.TB, h *runtimeHarness, want, entityID string) {
	t.Helper()
	if h == nil || h.db == nil {
		t.Fatal("database is required for handler_outcome assertions")
	}
	want = strings.TrimSpace(strings.ToLower(want))
	entityID = strings.TrimSpace(entityID)
	if want == "" {
		return
	}
	if want != "success" && len(h.previews) > 0 {
		for eventID := range h.publishedIDs {
			if entityID != "" && strings.TrimSpace(h.eventEntityIDs[eventID]) != entityID {
				continue
			}
			if preview, ok := h.previews[eventID]; ok {
				got := strings.TrimSpace(strings.ToLower(string(preview.Status)))
				if got != want {
					t.Fatalf("handler_outcome = %q, want %q", got, want)
				}
				return
			}
		}
	}
	for eventID := range h.publishedIDs {
		if entityID != "" && strings.TrimSpace(h.eventEntityIDs[eventID]) != entityID {
			continue
		}
		var outcome string
		err := h.db.QueryRowContext(context.Background(), `
			SELECT outcome
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'platform'
			  AND (
				subscriber_id = 'pipeline'
				OR subscriber_id LIKE 'pipeline:%'
			  )
			ORDER BY processed_at DESC
			LIMIT 1
		`, strings.TrimSpace(eventID)).Scan(&outcome)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			t.Fatalf("query handler_outcome for event %s: %v", eventID, err)
		}
		got := strings.TrimSpace(strings.ToLower(outcome))
		switch want {
		case "success":
			if got != "success" {
				t.Fatalf("handler_outcome = %q, want %q", got, want)
			}
		case "reject":
			if got != "reject" {
				t.Fatalf("handler_outcome = %q, want %q", got, want)
			}
		case "escalate":
			if got != "escalate" {
				t.Fatalf("handler_outcome = %q, want %q", got, want)
			}
		case "discard":
			if got != "discard" {
				t.Fatalf("handler_outcome = %q, want %q", got, want)
			}
		case "error", "kill", "dead_letter":
			if got != "dead_letter" {
				t.Fatalf("handler_outcome = %q, want %q", got, want)
			}
		default:
			t.Fatalf("handler_outcome assertion for %q is not wired in cataloge2e yet", want)
		}
		return
	}
	t.Fatalf("handler_outcome %q could not be asserted: no platform pipeline receipt found", want)
}

func metadataKeys(in map[string]any) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for key := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out = append(out, key)
	}
	return out
}

func canonicalJSONValue(v any) (string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func hasDuplicateStrings(in []string) bool {
	if len(in) < 2 {
		return false
	}
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			return true
		}
		seen[item] = struct{}{}
	}
	return false
}
