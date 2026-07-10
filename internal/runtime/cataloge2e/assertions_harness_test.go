package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func catalogRuntimeContext() context.Context {
	return runtimecorrelation.WithRunID(context.Background(), catalogRuntimeRunID)
}

func assertCatalogRuntimeOutcome(t testing.TB, h *runtimeHarness, expected catalogExpectedDocument) {
	t.Helper()
	flowPrefix := expected.triggerFlowPrefix()
	if len(expected.Expected.Entities) > 0 {
		assertCatalogRuntimeEntities(t, h, expected.Expected.Entities, flowPrefix)
		return
	}
	entityID := h.expectedTriggerEntityID(expected)
	assertCatalogRecognizedHandlerOutcome(t, expected.Expected.HandlerOutcome)
	if len(h.publishedIDs) > 0 && catalogAssertsAuthoritativeHandlerOutcome(expected.Expected.HandlerOutcome) {
		assertHandlerOutcome(t, h, expected.Expected.HandlerOutcome, entityID, expected.Expected.ChainDepthExceeded)
	}
	if entityID != "" && strings.TrimSpace(expected.Expected.EntityState) != "" {
		if flowPrefix != "" {
			assertFlowState(t, h, entityID, flowPrefix, expected.Expected.EntityState)
		} else {
			assertEntityState(t, h.db, h.workflow, entityID, expected.Expected.EntityState)
		}
	}
	if entityID != "" {
		assertFlowState(t, h, entityID, "", expected.Expected.ParentState)
		assertFlowState(t, h, entityID, "flow-b", expected.Expected.FlowBState)
		assertCausalFlowEntities(t, h, entityID, expected.Expected.FlowEntities)
		assertEntityFields(t, h.workflow, entityID, expected.Expected.EntityFields)
		assertGates(t, h.workflow, entityID, expected.Expected.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, expected.Expected.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
		assertCausalEvents(t, h, expected.Expected.CausalEvents, flowPrefix)
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

func assertCausalFlowEntities(t testing.TB, h *runtimeHarness, rootEntityID string, want map[string]catalogEntityExpected) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if h == nil {
		t.Fatal("runtime harness is required for flow entity assertions")
	}
	candidateEntityIDs := catalogCausalEntityIDs(t, h.db, h.startedAt, h.publishedIDs, rootEntityID)
	for flowID, expected := range want {
		flowID = strings.TrimSpace(flowID)
		got, found, err := catalogFlowInstanceForCausalFlow(h.workflow, semanticview.Wrap(h.bundle), candidateEntityIDs, flowID, true)
		if err != nil {
			t.Fatalf("load causal flow instance %s: %v", flowID, err)
		}
		if expected.Exists != nil && !*expected.Exists {
			if found {
				t.Fatalf("causal flow instance %s unexpectedly exists: entity_id=%s state=%s", flowID, got.InstanceID, got.CurrentState)
			}
			continue
		}
		if !found {
			t.Fatalf("causal flow instance for %s not found; causal entity ids=%v", flowID, mapKeys(candidateEntityIDs))
		}
		if wantState := strings.TrimSpace(expected.EntityState); wantState != "" {
			if gotState := strings.TrimSpace(got.CurrentState); gotState != wantState {
				t.Fatalf("causal flow instance %s state = %q, want %q", flowID, gotState, wantState)
			}
		}
		if len(expected.EntityFields) > 0 {
			for key, wantValue := range expected.EntityFields {
				if gotValue := got.Metadata[strings.TrimSpace(key)]; fmt.Sprintf("%#v", gotValue) != fmt.Sprintf("%#v", wantValue) {
					t.Fatalf("causal flow instance %s field %s = %#v, want %#v", flowID, key, gotValue, wantValue)
				}
			}
		}
		if len(expected.Gates) > 0 {
			gates := catalogBoolGates(got.Metadata)
			for key, wantValue := range expected.Gates {
				if gotValue := gates[strings.TrimSpace(key)]; gotValue != wantValue {
					t.Fatalf("causal flow instance %s gate %s = %v, want %v", flowID, key, gotValue, wantValue)
				}
			}
		}
	}
}

func assertCatalogRuntimeEntities(t testing.TB, h *runtimeHarness, expected map[string]catalogEntityExpected, flowPrefix string) {
	t.Helper()
	for entityID, want := range expected {
		entityID = h.resolveExpectedEntityID(strings.TrimSpace(entityID))
		if entityID == "" {
			continue
		}
		assertCatalogRecognizedHandlerOutcome(t, want.HandlerOutcome)
		if catalogAssertsAuthoritativeHandlerOutcome(want.HandlerOutcome) {
			assertHandlerOutcomeForEntity(t, h, want.HandlerOutcome, entityID, false)
		}
		if strings.TrimSpace(want.EntityState) != "" {
			assertEntityState(t, h.db, h.workflow, entityID, want.EntityState)
		}
		assertEntityFields(t, h.workflow, entityID, want.EntityFields)
		assertGates(t, h.workflow, entityID, want.Gates)
		assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, want.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
		assertCausalEvents(t, h, want.CausalEvents, flowPrefix)
		assertDeadLetter(t, h.db, h.startedAt, entityID, want.DeadLetter)
	}
}

func (h *runtimeHarness) resolveExpectedEntityID(entityID string) string {
	entityID = strings.TrimSpace(entityID)
	switch strings.ToLower(entityID) {
	case "null", "unknown":
		return h.firstPublishedEntityID()
	default:
		return entityID
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
	instance, ok, err := workflow.Load(catalogRuntimeContext(), strings.TrimSpace(entityID))
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
	instance, ok, err := workflow.Load(catalogRuntimeContext(), strings.TrimSpace(entityID))
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
	instance, ok, err := workflow.Load(catalogRuntimeContext(), strings.TrimSpace(entityID))
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

func assertFlowState(t testing.TB, h *runtimeHarness, entityID, flowID, wantState string) {
	t.Helper()
	wantState = strings.TrimSpace(wantState)
	if wantState == "" {
		return
	}
	if h == nil || h.workflow == nil {
		t.Fatal("workflow instance store is required")
	}
	instance, ok, err := h.workflow.Load(catalogRuntimeContext(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("load workflow instance %s for flow state: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for flow state assertion", entityID)
	}
	got := strings.TrimSpace(instance.CurrentState)
	if flowID != "" {
		candidateEntityIDs := catalogCausalEntityIDs(t, h.db, h.startedAt, h.publishedIDs, entityID)
		matched, found, err := catalogFlowInstanceForCausalFlow(h.workflow, semanticview.Wrap(h.bundle), candidateEntityIDs, strings.TrimSpace(flowID), false)
		if err != nil {
			t.Fatalf("load causal flow instance %s: %v", flowID, err)
		}
		if !found {
			t.Fatalf("causal flow instance for %s not found", flowID)
		}
		got = strings.TrimSpace(matched.CurrentState)
	}
	if strings.TrimSpace(got) != wantState {
		t.Fatalf("flow state %q = %q, want %q", strings.TrimSpace(flowID), strings.TrimSpace(got), wantState)
	}
}

func catalogFlowInstanceForCausalFlow(workflow *runtimepipeline.WorkflowInstanceStore, source semanticview.Source, candidateEntityIDs map[string]struct{}, flowID string, requireCausal bool) (runtimepipeline.WorkflowInstance, bool, error) {
	if workflow == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	rows, err := workflow.List(catalogRuntimeContext())
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	flowID = strings.TrimSpace(flowID)
	candidates := map[string]struct{}{}
	if flowID != "" {
		candidates[flowID] = struct{}{}
		if source != nil {
			for _, scope := range source.FlowScopes() {
				if strings.TrimSpace(scope.ID) == flowID || strings.Trim(strings.TrimSpace(scope.Path), "/") == flowID {
					if id := strings.TrimSpace(scope.ID); id != "" {
						candidates[id] = struct{}{}
					}
					if path := strings.Trim(strings.TrimSpace(scope.Path), "/"); path != "" {
						candidates[path] = struct{}{}
					}
				}
			}
		}
	}
	matchFlow := func(row runtimepipeline.WorkflowInstance) bool {
		if len(candidates) == 0 {
			return false
		}
		rowWorkflow := strings.TrimSpace(row.WorkflowName)
		rowStorage := strings.Trim(strings.TrimSpace(row.StorageRef), "/")
		for candidate := range candidates {
			candidate = strings.Trim(candidate, "/")
			switch {
			case candidate == "":
			case rowWorkflow == candidate:
				return true
			case rowStorage == candidate:
				return true
			}
		}
		return false
	}
	matchCausal := func(row runtimepipeline.WorkflowInstance) bool {
		if len(candidateEntityIDs) > 0 {
			rowEntityIDs := []string{
				strings.TrimSpace(asString(row.Metadata["entity_id"])),
				strings.TrimSpace(asString(row.Metadata["parent_entity_id"])),
				strings.TrimSpace(row.InstanceID),
				runtimepipeline.FlowInstanceEntityID(row.StorageRef),
			}
			for _, rowEntityID := range rowEntityIDs {
				if _, ok := candidateEntityIDs[rowEntityID]; ok {
					return true
				}
			}
			return false
		}
		return true
	}
	for _, row := range rows {
		if matchCausal(row) && matchFlow(row) {
			return row, true, nil
		}
	}
	if !requireCausal {
		for _, row := range rows {
			if matchFlow(row) {
				return row, true, nil
			}
		}
	}
	return runtimepipeline.WorkflowInstance{}, false, nil
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
	if want == nil {
		return
	}
	relevantEventIDs := catalogCausalEventIDs(t, db, since, publishedIDs)
	relevantEntityIDs := catalogCausalEntityIDs(t, db, since, publishedIDs, entityID)
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
		eventID = strings.TrimSpace(eventID)
		payloadEntityID = strings.TrimSpace(payloadEntityID)
		_, causalEvent := relevantEventIDs[eventID]
		_, causalEntity := relevantEntityIDs[payloadEntityID]
		if !causalEvent && !causalEntity {
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

type catalogStoredEvent struct {
	ID              string
	Name            string
	SourceEventID   string
	PayloadEntityID string
}

func catalogEventsSince(t testing.TB, db *sql.DB, since time.Time) []catalogStoredEvent {
	t.Helper()
	if db == nil {
		return nil
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT
			event_id::text,
			event_name,
			COALESCE(source_event_id::text, ''),
			COALESCE(NULLIF(payload->>'entity_id', ''), COALESCE(entity_id::text, ''))
		FROM events
		WHERE created_at >= $1
		ORDER BY created_at ASC, event_id ASC
	`, since)
	if err != nil {
		t.Fatalf("query causal events: %v", err)
	}
	defer rows.Close()
	out := []catalogStoredEvent{}
	for rows.Next() {
		var row catalogStoredEvent
		if err := rows.Scan(&row.ID, &row.Name, &row.SourceEventID, &row.PayloadEntityID); err != nil {
			t.Fatalf("scan causal event: %v", err)
		}
		row.ID = strings.TrimSpace(row.ID)
		row.Name = strings.TrimSpace(row.Name)
		row.SourceEventID = strings.TrimSpace(row.SourceEventID)
		row.PayloadEntityID = strings.TrimSpace(row.PayloadEntityID)
		if row.ID != "" {
			out = append(out, row)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate causal events: %v", err)
	}
	return out
}

func catalogCausalEventIDs(t testing.TB, db *sql.DB, since time.Time, publishedIDs map[string]struct{}) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	for eventID := range publishedIDs {
		if eventID = strings.TrimSpace(eventID); eventID != "" {
			out[eventID] = struct{}{}
		}
	}
	if len(out) == 0 {
		return out
	}
	rows := catalogEventsSince(t, db, since)
	changed := true
	for changed {
		changed = false
		for _, row := range rows {
			if _, seen := out[row.ID]; seen {
				continue
			}
			if _, parentSeen := out[row.SourceEventID]; parentSeen {
				out[row.ID] = struct{}{}
				changed = true
			}
		}
	}
	return out
}

func catalogCausalEntityIDs(t testing.TB, db *sql.DB, since time.Time, publishedIDs map[string]struct{}, fallbackEntityID string) map[string]struct{} {
	t.Helper()
	eventIDs := catalogCausalEventIDs(t, db, since, publishedIDs)
	out := map[string]struct{}{}
	if fallbackEntityID = strings.TrimSpace(fallbackEntityID); fallbackEntityID != "" {
		out[fallbackEntityID] = struct{}{}
	}
	if len(eventIDs) == 0 {
		return out
	}
	for _, row := range catalogEventsSince(t, db, since) {
		if _, ok := eventIDs[row.ID]; !ok {
			continue
		}
		if row.PayloadEntityID != "" {
			out[row.PayloadEntityID] = struct{}{}
		}
	}
	return out
}

func mapKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		key = strings.TrimSpace(key)
		if key != "" {
			out = append(out, key)
		}
	}
	return out
}

func assertCausalEvents(t testing.TB, h *runtimeHarness, want []string, flowPrefix string) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if h == nil || h.db == nil {
		t.Fatal("runtime harness database is required for causal_events assertions")
	}
	wantNames := make(map[string]struct{}, len(want))
	for _, name := range want {
		name = strings.TrimSpace(name)
		if name != "" {
			wantNames[name] = struct{}{}
		}
	}
	rows := catalogEventsSince(t, h.db, h.startedAt)
	source := semanticview.Wrap(h.bundle)
	parentIDs := map[string]struct{}{}
	for eventID := range h.publishedIDs {
		if eventID = strings.TrimSpace(eventID); eventID != "" {
			parentIDs[eventID] = struct{}{}
		}
	}
	if len(parentIDs) == 0 {
		t.Fatalf("causal_events = %v requires a published root event", want)
	}
	for index, wantName := range want {
		wantName = strings.TrimSpace(wantName)
		if wantName == "" {
			continue
		}
		var matched catalogStoredEvent
		found := false
		for _, row := range rows {
			observedName := normalizeCatalogObservedEventName(row.Name, flowPrefix, source, wantNames)
			_, isRoot := parentIDs[row.ID]
			_, isChild := parentIDs[row.SourceEventID]
			if observedName == wantName && (isChild || (index == 0 && isRoot)) {
				matched = row
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("causal_events[%d] = %q not found from parents=%v", index, wantName, mapKeys(parentIDs))
		}
		parentIDs = map[string]struct{}{matched.ID: {}}
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

func catalogBoolGates(metadata map[string]any) map[string]bool {
	raw, _ := metadata["gates"].(map[string]any)
	out := make(map[string]bool, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if b, ok := value.(bool); ok {
			out[key] = b
		}
	}
	return out
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
	var instanceCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM flow_instances
		WHERE instance_id = $1
		  AND created_at >= $2
	`, instancePath, since).Scan(&instanceCount); err != nil {
		t.Fatalf("query flow instance row: %v", err)
	}
	if instanceCount != 1 {
		t.Fatalf("flow instance %q count = %d, want 1", instancePath, instanceCount)
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
		if count != 1 {
			t.Fatalf("auto-emitted event %q count = %d, want 1", autoEmitted, count)
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

func (h *runtimeHarness) assertTriggerReceipt(step catalogTriggerStep) {
	wantOutcome := strings.TrimSpace(strings.ToLower(step.ReceiptOutcome))
	wantClass := strings.TrimSpace(step.ReceiptFailureClass)
	wantDetail := strings.TrimSpace(step.ReceiptFailureDetail)
	if wantOutcome == "" && wantClass == "" && wantDetail == "" {
		return
	}
	h.t.Helper()
	if h == nil || h.db == nil {
		h.t.Fatal("database is required for trigger receipt assertions")
	}
	eventIDs := h.publishedEventIDs(triggerPayloadEntityID(step.Payload))
	for _, eventID := range eventIDs {
		var subscriberID, outcome, sideEffects string
		var rawFailure []byte
		err := h.db.QueryRowContext(context.Background(), `
			SELECT subscriber_id, outcome, COALESCE(side_effects::text, ''), COALESCE(failure, 'null'::jsonb)
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'platform'
			  AND (
				subscriber_id = 'pipeline'
				OR subscriber_id LIKE 'pipeline:%'
			  )
			ORDER BY processed_at DESC
			LIMIT 1
		`, strings.TrimSpace(eventID)).Scan(&subscriberID, &outcome, &sideEffects, &rawFailure)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			h.t.Fatalf("query trigger receipt for event %s: %v", eventID, err)
		}
		if wantOutcome != "" {
			gotOutcome := strings.TrimSpace(strings.ToLower(outcome))
			if gotOutcome != wantOutcome {
				h.t.Fatalf("trigger receipt outcome for %s/%s = %q, want %q", eventID, subscriberID, gotOutcome, wantOutcome)
			}
		}
		if wantClass != "" || wantDetail != "" {
			failure, decodeErr := runtimefailures.UnmarshalEnvelope(rawFailure)
			if decodeErr != nil {
				h.t.Fatalf("decode trigger receipt failure for %s/%s: %v raw=%s", eventID, subscriberID, decodeErr, rawFailure)
			}
			if string(failure.Class) != wantClass || failure.Detail.Code != wantDetail {
				h.t.Fatalf("trigger receipt failure for %s/%s = %#v, want %s/%s", eventID, subscriberID, failure, wantClass, wantDetail)
			}
			for key, want := range step.ReceiptFailureAttributes {
				if got := failure.Detail.Attributes[key]; fmt.Sprint(got) != fmt.Sprint(want) {
					h.t.Fatalf("trigger receipt failure attribute %s = %#v, want %#v", key, got, want)
				}
			}
		}
		return
	}
	h.t.Fatalf("trigger receipt %q could not be asserted: no platform pipeline receipt found", wantOutcome)
}

func assertHandlerOutcomeForEntity(t testing.TB, h *runtimeHarness, want, entityID string, chainDepthExceeded bool) {
	t.Helper()
	if h == nil || h.db == nil {
		t.Fatal("database is required for handler_outcome assertions")
	}
	_ = chainDepthExceeded
	want = strings.TrimSpace(strings.ToLower(want))
	entityID = strings.TrimSpace(entityID)
	if want == "" {
		return
	}
	if !catalogAssertsAuthoritativeHandlerOutcome(want) {
		t.Fatalf("cataloge2e does not authoritatively assert handler_outcome %q; assert runtime/store evidence instead", want)
	}
	eventIDs := h.publishedEventIDs(entityID)
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
		if got != "success" {
			t.Fatalf("handler_outcome = %q, want %q", got, want)
		}
		return
	}
	t.Fatalf("handler_outcome %q could not be asserted: no platform pipeline receipt found", want)
}

func catalogAssertsAuthoritativeHandlerOutcome(raw string) bool {
	return strings.TrimSpace(strings.ToLower(raw)) == "success"
}

func assertCatalogRecognizedHandlerOutcome(t testing.TB, raw string) {
	t.Helper()
	if !catalogRecognizesHandlerOutcome(raw) {
		t.Fatalf("cataloge2e does not recognize handler_outcome %q; supported values are success or explicit local-only non-success outcomes", strings.TrimSpace(raw))
	}
}

func catalogRecognizesHandlerOutcome(raw string) bool {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "success", "reject", "discard", "escalate", "kill", "dead_letter", "error", "terminal_reject", "blocked":
		return true
	default:
		return false
	}
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
			  AND dl.failure->>'class' = 'platform.chain_depth_exceeded'
			UNION ALL
			SELECT 1
			FROM events e
			WHERE e.event_name = 'platform.dead_letter'
			  AND COALESCE(NULLIF(e.payload->>'entity_id', ''), COALESCE(e.entity_id::text, '')) = $1
			  AND COALESCE(e.payload->'failure'->>'class', '') = 'platform.chain_depth_exceeded'
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
