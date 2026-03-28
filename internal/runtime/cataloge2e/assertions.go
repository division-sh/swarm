package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func assertCatalogRuntimeOutcome(t testing.TB, h *runtimeHarness, expected catalogExpectedDocument) {
	t.Helper()
	flowPrefix := expected.triggerFlowPrefix()
	if len(expected.Expected.Entities) > 0 {
		assertCatalogRuntimeEntities(t, h, expected.Expected.Entities, flowPrefix)
		return
	}
	entityID := ""
	for _, step := range expected.triggerSequence() {
		if entityID = triggerPayloadEntityID(step.Payload); entityID != "" {
			break
		}
	}
	if len(h.publishedIDs) > 0 && strings.TrimSpace(expected.Expected.HandlerOutcome) != "" {
		assertHandlerOutcome(t, h, expected.Expected.HandlerOutcome, entityID, expected.Expected.ChainDepthExceeded)
	}
	if entityID != "" && strings.TrimSpace(expected.Expected.EntityState) != "" {
		if flowPrefix != "" {
			assertFlowState(t, h.workflow, h.bundle, entityID, flowPrefix, expected.Expected.EntityState)
		} else {
			assertEntityState(t, h.db, h.workflow, entityID, expected.Expected.EntityState)
		}
	}
	if entityID != "" {
		assertFlowState(t, h.workflow, h.bundle, entityID, "", expected.Expected.ParentState)
		assertFlowState(t, h.workflow, h.bundle, entityID, "flow-b", expected.Expected.FlowBState)
		assertEntityFields(t, h.workflow, entityID, expected.Expected.EntityFields)
		assertGates(t, h.workflow, entityID, expected.Expected.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, expected.Expected.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
		assertDeadLetter(t, h.db, h.startedAt, entityID, expected.Expected.DeadLetter)
		assertChainDepthExceeded(t, h.db, h.startedAt, entityID, expected.Expected.ChainDepthExceeded)
	}
	assertAgentReceived(t, h.db, h.startedAt, expected.Expected.AgentReceived)
	if len(expected.Expected.FlowInstanceCreated) > 0 {
		assertFlowInstanceCreated(t, h.db, h.startedAt, expected.Expected.FlowInstanceCreated)
	}
	if expected.Expected.TemplateInstances != nil {
		assertFlowInstanceCount(t, h.db, h.startedAt, *expected.Expected.TemplateInstances)
	}
}

func assertCatalogRuntimeEntities(t testing.TB, h *runtimeHarness, expected map[string]catalogEntityExpected, flowPrefix string) {
	t.Helper()
	for entityID, want := range expected {
		entityID = strings.TrimSpace(entityID)
		if entityID == "" {
			continue
		}
		if strings.TrimSpace(want.HandlerOutcome) != "" {
			assertHandlerOutcomeForEntity(t, h, want.HandlerOutcome, entityID, false)
		}
		if strings.TrimSpace(want.EntityState) != "" {
			assertEntityState(t, h.db, h.workflow, entityID, want.EntityState)
		}
		assertEntityFields(t, h.workflow, entityID, want.EntityFields)
		assertGates(t, h.workflow, entityID, want.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, want.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
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

func assertFlowState(t testing.TB, workflow *runtimepipeline.WorkflowInstanceStore, bundle *runtimecontracts.WorkflowContractBundle, entityID, flowID, wantState string) {
	t.Helper()
	wantState = strings.TrimSpace(wantState)
	if wantState == "" {
		return
	}
	if workflow == nil {
		t.Fatal("workflow instance store is required")
	}
	instance, ok, err := workflow.Load(context.Background(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("load workflow instance %s for flow state: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for flow state assertion", entityID)
	}
	var semanticSource = runtimepipeline.DefaultWorkflowSemanticSourceOrNil()
	if bundle != nil {
		semanticSource = semanticview.Wrap(bundle)
	}
	got := string(runtimepipeline.NormalizeWorkflowStateID(instance.CurrentState))
	if semanticSource != nil {
		got = strings.TrimSpace(catalogFlowScopedStateValue(semanticSource, bundle, strings.TrimSpace(flowID), cloneStringAnyMap(instance.Metadata), instance.CurrentState))
	}
	if strings.TrimSpace(got) != wantState {
		t.Fatalf("flow state %q = %q, want %q", strings.TrimSpace(flowID), strings.TrimSpace(got), wantState)
	}
}

func catalogFlowScopedStateValue(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle, flowID string, metadata map[string]any, fallback string) string {
	fallback = strings.TrimSpace(string(runtimepipeline.NormalizeWorkflowStateID(fallback)))
	stateKey := catalogFlowScopedStateKey(source, flowID)
	if scoped := strings.TrimSpace(catalogFlowScopedStateFromMetadata(metadata, stateKey)); scoped != "" {
		return scoped
	}
	if initial := strings.TrimSpace(catalogFlowScopedInitialState(source, bundle, flowID)); initial != "" {
		return initial
	}
	return fallback
}

func catalogFlowScopedStateKey(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "$root"
	}
	if source != nil {
		if flowPath := strings.Trim(source.FlowPath(flowID), "/"); flowPath != "" {
			return flowPath
		}
	}
	return flowID
}

func catalogFlowScopedInitialState(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		if bundle != nil {
			return strings.TrimSpace(bundle.WorkflowInitialStage())
		}
		return ""
	}
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.FlowInitialStage(flowID))
}

func catalogFlowScopedStateFromMetadata(metadata map[string]any, stateKey string) string {
	stateKey = strings.TrimSpace(stateKey)
	if stateKey == "" || len(metadata) == 0 {
		return ""
	}
	raw := catalogPayloadMap(metadata["flow_states"])
	if len(raw) == 0 {
		return ""
	}
	return strings.TrimSpace(asString(raw[stateKey]))
}

func catalogPayloadMap(v any) map[string]any {
	switch typed := v.(type) {
	case map[string]any:
		return cloneStringAnyMap(typed)
	default:
		return nil
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

func assertEmittedEvents(t testing.TB, db *sql.DB, since time.Time, publishedIDs map[string]struct{}, entityID string, want []string, flowPrefix string, source semanticview.Source) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT event_id::text, event_name, COALESCE(NULLIF(payload->>'entity_id', ''), COALESCE(entity_id::text, ''))
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
	wantNames := make(map[string]struct{}, len(want))
	for _, name := range want {
		name = strings.TrimSpace(name)
		if name != "" {
			wantNames[name] = struct{}{}
		}
	}
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
		eventName = normalizeCatalogObservedEventName(eventName, flowPrefix, source, wantNames)
		if flowPrefix == "" && strings.Contains(eventName, "/") {
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

func normalizeCatalogObservedEventName(eventName, flowPrefix string, source semanticview.Source, want map[string]struct{}) string {
	eventName = strings.Trim(strings.TrimSpace(eventName), "/")
	flowPrefix = strings.Trim(strings.TrimSpace(flowPrefix), "/")
	if eventName == "" {
		return ""
	}
	if flowPrefix != "" {
		prefix := flowPrefix + "/"
		if strings.HasPrefix(eventName, prefix) {
			eventName = strings.TrimPrefix(eventName, prefix)
		}
	}
	if source == nil || !strings.Contains(eventName, "/") {
		return eventName
	}
	for _, scope := range source.FlowScopes() {
		scopePath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if scopePath == "" {
			continue
		}
		prefix := scopePath + "/"
		if !strings.HasPrefix(eventName, prefix) {
			continue
		}
		localEvent := strings.TrimPrefix(eventName, prefix)
		for _, candidate := range scope.OutputEvents {
			if strings.TrimSpace(candidate) == localEvent {
				if flowPrefix == "" {
					if _, ok := want[localEvent]; !ok {
						return eventName
					}
				} else if !catalogRootEventExists(source, localEvent) {
					return eventName
				}
				return localEvent
			}
		}
	}
	return eventName
}

func catalogRootEventExists(source semanticview.Source, eventName string) bool {
	eventName = strings.TrimSpace(eventName)
	if source == nil || eventName == "" {
		return false
	}
	for _, scope := range source.ProjectScopes() {
		if _, ok := scope.Events[eventName]; ok {
			return true
		}
	}
	return false
}

func shouldIgnoreCatalogE2EEvent(eventName string) bool {
	eventName = strings.TrimSpace(eventName)
	switch eventName {
	case "platform.runtime_log":
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
		FROM (
			SELECT 1
			FROM dead_letters dl
			WHERE COALESCE(NULLIF(dl.original_payload->>'entity_id', ''), COALESCE(dl.entity_id::text, '')) = $1
			UNION ALL
			SELECT 1
			FROM events e
			WHERE e.event_name = 'platform.dead_letter'
			  AND COALESCE(NULLIF(e.payload->>'entity_id', ''), COALESCE(e.entity_id::text, '')) = $1
		) hits
	`, strings.TrimSpace(entityID)).Scan(&count); err != nil {
		t.Fatalf("query dead_letters: %v", err)
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

func assertFlowInstanceCreated(t testing.TB, db *sql.DB, since time.Time, want map[string]any) {
	t.Helper()
	if db == nil {
		t.Fatal("database is required for flow_instance_created assertions")
	}
	templateID := strings.TrimSpace(asString(want["template"]))
	instanceID := strings.TrimSpace(asString(want["instance_id"]))
	if templateID == "" || instanceID == "" {
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
		return
	}
	instancePath := strings.Trim(strings.TrimSpace(templateID+"/"+instanceID), "/")
	var routeCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE flow_instance = $1
		  AND is_materialized = true
		  AND status = 'active'
	`, instancePath).Scan(&routeCount); err != nil {
		t.Fatalf("query flow instance routes: %v", err)
	}
	if routeCount == 0 {
		t.Fatalf("expected flow instance to be created")
	}
	if config, ok := want["config"].(map[string]any); ok && len(config) > 0 {
		var raw []byte
		err := db.QueryRowContext(context.Background(), `
			SELECT config
			FROM flow_instances
			WHERE instance_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, instancePath).Scan(&raw)
		if err == sql.ErrNoRows {
			t.Fatalf("expected flow instance config for %s", instancePath)
		}
		if err != nil {
			t.Fatalf("query flow instance config: %v", err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("decode flow instance config: %v", err)
		}
		for key, wantValue := range config {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			gotValue, ok := got[key]
			if !ok {
				t.Fatalf("flow instance config missing %q; have keys=%v", key, metadataKeys(got))
			}
			gotCanonical, err := canonicalJSONValue(gotValue)
			if err != nil {
				t.Fatalf("canonicalize flow instance config %q got value: %v", key, err)
			}
			wantCanonical, err := canonicalJSONValue(wantValue)
			if err != nil {
				t.Fatalf("canonicalize flow instance config %q expected value: %v", key, err)
			}
			if gotCanonical != wantCanonical {
				t.Fatalf("flow instance config %q = %s, want %s", key, gotCanonical, wantCanonical)
			}
		}
	}
	if autoEmitted := strings.TrimSpace(asString(want["auto_emitted"])); autoEmitted != "" {
		var count int
		if err := db.QueryRowContext(context.Background(), `
			SELECT COUNT(*)
			FROM events
			WHERE event_name = $1
		`, autoEmitted).Scan(&count); err != nil {
			t.Fatalf("query flow auto-emitted event: %v", err)
		}
		if count == 0 {
			t.Fatalf("expected auto-emitted event %q for flow instance", autoEmitted)
		}
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

func assertHandlerOutcome(t testing.TB, h *runtimeHarness, want, entityID string, chainDepthExceeded bool) {
	t.Helper()
	assertHandlerOutcomeForEntity(t, h, want, entityID, chainDepthExceeded)
}

func assertHandlerOutcomeForEntity(t testing.TB, h *runtimeHarness, want, entityID string, chainDepthExceeded bool) {
	t.Helper()
	if h == nil || h.db == nil {
		t.Fatal("database is required for handler_outcome assertions")
	}
	want = strings.TrimSpace(strings.ToLower(want))
	entityID = strings.TrimSpace(entityID)
	if want == "" {
		return
	}
	if chainDepthExceeded && entityID != "" && (want == "error" || want == "kill" || want == "dead_letter") {
		if assertEntityDeadLetterOutcome(t, h.db, h.startedAt, entityID) {
			return
		}
	}
	eventIDs := h.publishedEventIDs(entityID)
	if want != "success" && len(h.previews) > 0 {
		for _, eventID := range eventIDs {
			if preview, ok := h.previews[eventID]; ok {
				got := strings.TrimSpace(strings.ToLower(string(preview.Status)))
				if got != want {
					t.Fatalf("handler_outcome = %q, want %q", got, want)
				}
				return
			}
		}
	}
	for _, eventID := range eventIDs {
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

func assertEntityDeadLetterOutcome(t testing.TB, db *sql.DB, since time.Time, entityID string) bool {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM (
			SELECT 1
			FROM dead_letters dl
			WHERE COALESCE(NULLIF(dl.original_payload->>'entity_id', ''), COALESCE(dl.entity_id::text, '')) = $1
			UNION ALL
			SELECT 1
			FROM events e
			WHERE e.event_name = 'platform.dead_letter'
			  AND COALESCE(NULLIF(e.payload->>'entity_id', ''), COALESCE(e.entity_id::text, '')) = $1
		) hits
	`, entityID).Scan(&count); err != nil {
		t.Fatalf("query dead_letter outcomes: %v", err)
	}
	return count > 0
}

func assertChainDepthExceeded(t testing.TB, db *sql.DB, since time.Time, entityID string, want bool) {
	t.Helper()
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		if want {
			t.Fatalf("chain_depth_exceeded = true requires entity_id in trigger payload")
		}
		return
	}
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM (
			SELECT 1
			FROM dead_letters dl
			WHERE COALESCE(NULLIF(dl.original_payload->>'entity_id', ''), COALESCE(dl.entity_id::text, '')) = $1
			  AND dl.failure_type = 'chain_depth_exceeded'
			UNION ALL
			SELECT 1
			FROM events e
			WHERE e.event_name = 'platform.dead_letter'
			  AND COALESCE(NULLIF(e.payload->>'entity_id', ''), COALESCE(e.entity_id::text, '')) = $1
			  AND COALESCE(e.payload->>'failure_type', '') = 'chain_depth_exceeded'
		) hits
	`, entityID).Scan(&count); err != nil {
		t.Fatalf("query chain_depth_exceeded dead_letters: %v", err)
	}
	got := count > 0
	if got != want {
		t.Fatalf("chain_depth_exceeded = %v, want %v", got, want)
	}
}

func (h *runtimeHarness) publishedEventIDs(entityID string) []string {
	if h == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.publishedOrder) == 0 {
		return nil
	}
	out := make([]string, 0, len(h.publishedOrder))
	for i := len(h.publishedOrder) - 1; i >= 0; i-- {
		eventID := strings.TrimSpace(h.publishedOrder[i])
		if eventID == "" {
			continue
		}
		if entityID != "" && strings.TrimSpace(h.eventEntityIDs[eventID]) != entityID {
			continue
		}
		out = append(out, eventID)
	}
	return out
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
