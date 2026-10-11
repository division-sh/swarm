package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func catalogRuntimeContext() context.Context {
	return runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), catalogRuntimeRunID)
}

func assertCatalogRuntimeOutcome(t testing.TB, h *runtimeHarness, expected catalogExpectedDocument) {
	t.Helper()
	if len(expected.Expected.FlowInstanceCreated) > 0 {
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		assertFlowInstanceCreated(t, reader, h.startedAt, expected.Expected.FlowInstanceCreated)
	}
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
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			assertEntityState(t, reader, h.workflow, entityID, expected.Expected.EntityState)
		}
	}
	if entityID != "" {
		assertFlowState(t, h, entityID, "", expected.Expected.ParentState)
		assertFlowState(t, h, entityID, "flow-b", expected.Expected.FlowBState)
		assertCausalFlowEntities(t, h, entityID, expected.Expected.FlowEntities)
		assertEntityFields(t, h.workflow, entityID, expected.Expected.EntityFields)
		assertGates(t, h, entityID, expected.Expected.Gates)
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		assertEmittedEvents(t, reader, h.startedAt, h.publishedIDs, entityID, expected.Expected.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
		assertCausalEvents(t, h, expected.Expected.CausalEvents, flowPrefix)
		if !expected.Expected.ChainDepthExceeded {
			assertPublishedEventDeadLetter(t, reader, h.publishedIDs, expected.Expected.DeadLetter)
		}
		assertChainDepthExceeded(t, reader, entityID, expected.Expected.ChainDepthExceeded)
	}
	assertAgentReceived(t, h, h.startedAt, expected.Expected.AgentReceived)
	if expected.Expected.TemplateInstances != nil {
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		assertFlowInstanceCount(t, reader, h.startedAt, *expected.Expected.TemplateInstances)
	}
}

func assertPublishedEventDeadLetter(t testing.TB, selected catalogOperatorEventLister, publishedEventIDs map[string]struct{}, want bool) {
	t.Helper()
	count := 0
	for eventID := range publishedEventIDs {
		eventCount, err := storetest.CountDeadLettersForOriginalEvent(testAuthorActivityContext(context.Background()), selected, strings.TrimSpace(eventID))
		if err != nil {
			t.Fatalf("query published-event dead letters for %s: %v", eventID, err)
		}
		count += eventCount
	}
	if got := count > 0; got != want {
		t.Fatalf("published-event dead_letter = %v, want %v", got, want)
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
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	candidateEntityIDs := catalogCausalEntityIDs(t, reader, h.startedAt, h.publishedIDs, rootEntityID)
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
				if gotValue := got.Fields[strings.TrimSpace(key)]; fmt.Sprintf("%#v", gotValue) != fmt.Sprintf("%#v", wantValue) {
					t.Fatalf("causal flow instance %s field %s = %#v, want %#v", flowID, key, gotValue, wantValue)
				}
			}
		}
		if len(expected.Gates) > 0 {
			gates := catalogBoolGates(got.Gates)
			for key, wantValue := range expected.Gates {
				key = h.catalogGateKey(flowID, key)
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
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			assertEntityState(t, reader, h.workflow, entityID, want.EntityState)
		}
		assertEntityFields(t, h.workflow, entityID, want.EntityFields)
		assertGates(t, h, entityID, want.Gates)
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		assertEmittedEvents(t, reader, h.startedAt, h.publishedIDs, entityID, want.EmittedEvents, flowPrefix, semanticview.Wrap(h.bundle))
		assertCausalEvents(t, h, want.CausalEvents, flowPrefix)
		assertDeadLetter(t, reader, h.startedAt, entityID, want.DeadLetter)
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

func assertGates(t testing.TB, h *runtimeHarness, entityID string, want map[string]bool) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if h == nil || h.workflow == nil {
		t.Fatal("workflow instance store is required for gates assertions")
	}
	instance, ok, err := catalogWorkflowInstanceForEntity(h.workflow, entityID)
	if err != nil {
		t.Fatalf("load workflow instance %s for gates: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for gates assertion", entityID)
	}
	for key, wantValue := range want {
		flowID := ""
		if h.bundle != nil {
			flowID = h.bundle.WorkflowName()
		}
		key = h.catalogGateKey(flowID, key)
		if key == "" {
			continue
		}
		gotValue, ok := instance.Gates[key]
		if !ok {
			if !wantValue {
				continue
			}
			t.Fatalf("gate %q missing; have keys=%v", key, boolMapKeys(instance.Gates))
		}
		if gotValue != wantValue {
			t.Fatalf("gate %q = %v, want %v", key, gotValue, wantValue)
		}
	}
}

func assertEntityFields(t testing.TB, workflow catalogWorkflowPersistence, entityID string, want map[string]any) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	if workflow == nil {
		t.Fatal("workflow instance store is required for entity_fields assertions")
	}
	instance, ok, err := catalogWorkflowInstanceForEntity(workflow, entityID)
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
		gotValue, ok := instance.Fields[key]
		if !ok {
			t.Fatalf("entity field %q missing; have keys=%v", key, metadataKeys(instance.Fields))
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

func assertEntityState(t testing.TB, selected catalogOperatorEventLister, workflow catalogWorkflowPersistence, entityID, wantState string) {
	t.Helper()
	if workflow == nil {
		t.Fatal("workflow instance store is required")
	}
	instance, ok, err := catalogWorkflowInstanceForEntity(workflow, entityID)
	if err != nil {
		t.Fatalf("load workflow instance %s: %v", entityID, err)
	}
	if !ok {
		rows, dumpErr := workflowStateDebugRows(selected)
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
	instance, ok, err := catalogWorkflowInstanceForEntity(h.workflow, entityID)
	if err != nil {
		t.Fatalf("load workflow instance %s for flow state: %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflow instance %s not found for flow state assertion", entityID)
	}
	got := strings.TrimSpace(instance.CurrentState)
	if flowID != "" {
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		candidateEntityIDs := catalogCausalEntityIDs(t, reader, h.startedAt, h.publishedIDs, entityID)
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

func catalogWorkflowInstanceForEntity(workflow catalogWorkflowPersistence, entityID string) (runtimepipeline.WorkflowInstance, bool, error) {
	if workflow == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	instances, err := workflow.ListWorkflowInstances(catalogRuntimeContext(), catalogRuntimeRunID)
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	var match runtimepipeline.WorkflowInstance
	found := false
	for _, instance := range instances {
		if strings.TrimSpace(instance.EntityID) != entityID {
			continue
		}
		if found {
			return runtimepipeline.WorkflowInstance{}, false, fmt.Errorf("entity %s has multiple workflow instance owners", entityID)
		}
		match = instance
		found = true
	}
	return match, found, nil
}

func catalogFlowInstanceForCausalFlow(workflow catalogWorkflowPersistence, source semanticview.Source, candidateEntityIDs map[string]struct{}, flowID string, requireCausal bool) (runtimepipeline.WorkflowInstance, bool, error) {
	if workflow == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
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
				strings.TrimSpace(row.EntityID),
				strings.TrimSpace(row.ParentEntityID),
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
	instances, err := workflow.ListWorkflowInstances(catalogRuntimeContext(), catalogRuntimeRunID)
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	for _, row := range instances {
		if matchCausal(row) && matchFlow(row) {
			return row, true, nil
		}
	}
	if !requireCausal {
		for _, row := range instances {
			if matchFlow(row) {
				return row, true, nil
			}
		}
	}
	return runtimepipeline.WorkflowInstance{}, false, nil
}

func workflowStateDebugRows(selected catalogOperatorEventLister) (string, error) {
	if selected == nil {
		return "", nil
	}
	rows, err := storetest.ReadWorkflowStateObservationRows(testAuthorActivityContext(context.Background()), selected)
	if err != nil {
		return "", err
	}
	out := []string{}
	for _, row := range rows {
		out = append(out, fmt.Sprintf("{entity_id:%s flow_instance:%s state:%s}", row.EntityID, row.FlowInstance, row.CurrentState))
	}
	if len(out) == 0 {
		return "[]", nil
	}
	return "[" + strings.Join(out, ", ") + "]", nil
}

func assertEmittedEvents(t testing.TB, selected catalogOperatorEventLister, since time.Time, publishedIDs map[string]struct{}, entityID string, want []string, flowPrefix string, source semanticview.Source) {
	t.Helper()
	if want == nil {
		return
	}
	relevantEventIDs := catalogCausalEventIDs(t, selected, since, publishedIDs)
	relevantEntityIDs := catalogCausalEntityIDs(t, selected, since, publishedIDs, entityID)
	rows := catalogCausalOrder(t, catalogEventsSince(t, selected, since))
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
	for _, row := range rows {
		eventID, eventName, payloadEntityID := row.ID, row.Name, row.PayloadEntityID
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
			if _, explicitlyExpected := wantNames[eventName]; !explicitlyExpected {
				continue
			}
		}
		if dedup {
			if _, ok := seen[eventName]; ok {
				continue
			}
			seen[eventName] = struct{}{}
		}
		got = append(got, eventName)
	}
	if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
		t.Fatalf("emitted_events = %v, want %v", got, want)
	}
}

type catalogStoredEvent = storetest.CausalEventStorageRow

// Creation occurrences retain their trigger's logical timestamp. UUID ordering
// therefore cannot establish parent-before-child execution order.
func catalogCausalOrder(t testing.TB, rows []catalogStoredEvent) []catalogStoredEvent {
	t.Helper()
	byID := make(map[string]catalogStoredEvent, len(rows))
	for _, row := range rows {
		if _, duplicate := byID[row.ID]; duplicate {
			t.Fatalf("duplicate catalog event %s", row.ID)
		}
		byID[row.ID] = row
	}
	out := make([]catalogStoredEvent, 0, len(rows))
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(catalogStoredEvent)
	visit = func(row catalogStoredEvent) {
		if visited[row.ID] {
			return
		}
		if visiting[row.ID] {
			t.Fatalf("cyclic catalog causality at %s", row.ID)
		}
		visiting[row.ID] = true
		if parent, exists := byID[row.SourceEventID]; exists {
			visit(parent)
		}
		delete(visiting, row.ID)
		visited[row.ID] = true
		out = append(out, row)
	}
	for _, row := range rows {
		visit(row)
	}
	return out
}

func catalogEventsSince(t testing.TB, selected catalogOperatorEventLister, since time.Time) []catalogStoredEvent {
	t.Helper()
	if selected == nil {
		return nil
	}
	rows, err := storetest.ReadCausalEventStorageSince(testAuthorActivityContext(context.Background()), selected, since)
	if err != nil {
		t.Fatalf("query causal events: %v", err)
	}
	out := []catalogStoredEvent{}
	for _, row := range rows {
		row.ID = strings.TrimSpace(row.ID)
		row.Name = strings.TrimSpace(row.Name)
		row.SourceEventID = strings.TrimSpace(row.SourceEventID)
		row.PayloadEntityID = strings.TrimSpace(row.PayloadEntityID)
		if row.ID != "" {
			out = append(out, row)
		}
	}
	return out
}

func catalogCausalEventIDs(t testing.TB, selected catalogOperatorEventLister, since time.Time, publishedIDs map[string]struct{}) map[string]struct{} {
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
	rows := catalogEventsSince(t, selected, since)
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

func catalogCausalEntityIDs(t testing.TB, selected catalogOperatorEventLister, since time.Time, publishedIDs map[string]struct{}, fallbackEntityID string) map[string]struct{} {
	t.Helper()
	eventIDs := catalogCausalEventIDs(t, selected, since, publishedIDs)
	out := map[string]struct{}{}
	if fallbackEntityID = strings.TrimSpace(fallbackEntityID); fallbackEntityID != "" {
		out[fallbackEntityID] = struct{}{}
	}
	if len(eventIDs) == 0 {
		return out
	}
	for _, row := range catalogEventsSince(t, selected, since) {
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
	if h == nil {
		t.Fatal("runtime harness is required for causal_events assertions")
	}
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := make(map[string]struct{}, len(want))
	for _, name := range want {
		name = strings.TrimSpace(name)
		if name != "" {
			wantNames[name] = struct{}{}
		}
	}
	rows := catalogEventsSince(t, reader, h.startedAt)
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

func catalogBoolGates(gates map[string]bool) map[string]bool {
	out := make(map[string]bool, len(gates))
	for key, value := range gates {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func boolMapKeys(in map[string]bool) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	return keys
}

func catalogRootEventExists(source semanticview.Source, eventName string) bool {
	eventName = strings.TrimSpace(eventName)
	if source == nil || eventName == "" {
		return false
	}
	scope, ok := source.FlowScopeByID(".")
	if !ok {
		return false
	}
	_, ok = scope.Events[eventName]
	return ok
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

func assertDeadLetter(t testing.TB, selected catalogOperatorEventLister, since time.Time, entityID string, want bool) {
	t.Helper()
	got := catalogHasDeadLetterRelation(t, selected, since, entityID)
	if got != want {
		rows, readErr := storetest.ReadDeadLetterObservationRows(testAuthorActivityContext(context.Background()), selected)
		var evidence []string
		for _, row := range rows {
			evidence = append(evidence, fmt.Sprintf("event=%s stored_entity=%q payload_entity=%q node=%q", row.OriginalEventID, row.StoredEntityID, row.PayloadEntityID, row.HandlerNode))
		}
		t.Fatalf("dead_letter = %v, want %v for entity %q; evidence=%v read_error=%v", got, want, entityID, evidence, readErr)
	}
}

func catalogHasDeadLetterRelation(t testing.TB, selected catalogOperatorEventLister, since time.Time, entityID string) bool {
	t.Helper()
	count, err := storetest.CountDeadLetterEntityRelationsSince(testAuthorActivityContext(context.Background()), selected, since.UTC(), strings.TrimSpace(entityID))
	if err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	return count > 0
}

func assertAgentReceived(t testing.TB, h *runtimeHarness, since time.Time, want map[string][]string) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	for agentID, expectedEvents := range want {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		reader, err := h.catalogOperatorEventLister()
		if err != nil {
			t.Fatalf("query agent_received for %s: %v", agentID, err)
		}
		rows, err := storetest.ReadSubscriberEventNamesCreatedSince(testAuthorActivityContext(context.Background()), reader, since, agentID)
		if err != nil {
			t.Fatalf("query agent_received for %s: %v", agentID, err)
		}
		got := make([]string, 0, len(expectedEvents))
		for _, eventName := range rows {
			got = append(got, strings.TrimSpace(eventName))
		}
		if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", expectedEvents) {
			t.Fatalf("agent_received[%s] = %v, want %v", agentID, got, expectedEvents)
		}
	}
}

func validateFlowInstanceCreatedExpectation(want map[string]any) error {
	for key, value := range want {
		switch key {
		case "template", "instance_id", "auto_emitted":
			text, ok := value.(string)
			if !ok || strings.TrimSpace(text) == "" {
				return fmt.Errorf("flow_instance_created.%s must be a nonempty string", key)
			}
		case "fields":
			fields, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("flow_instance_created.fields must be an object")
			}
			for name, field := range fields {
				if strings.TrimSpace(name) == "" {
					return fmt.Errorf("flow_instance_created.fields has an empty field name")
				}
				if _, err := canonicalJSONValue(field); err != nil {
					return fmt.Errorf("flow_instance_created.fields.%s: %w", name, err)
				}
			}
		default:
			return fmt.Errorf("unknown key flow_instance_created.%s", key)
		}
	}
	if len(want) != 0 {
		template, _ := want["template"].(string)
		instance, _ := want["instance_id"].(string)
		if template == "" || instance == "" {
			return fmt.Errorf("flow_instance_created requires template and instance_id")
		}
	}
	return nil
}

func assertFlowInstanceCreated(t testing.TB, selected catalogOperatorEventLister, since time.Time, want map[string]any) {
	t.Helper()
	if err := validateFlowInstanceCreatedExpectation(want); err != nil {
		t.Fatal(err)
	}
	if selected == nil {
		t.Fatal("selected reader is required for flow_instance_created assertions")
	}
	templateID := strings.TrimSpace(asString(want["template"]))
	instanceID := strings.TrimSpace(asString(want["instance_id"]))
	if templateID == "" || instanceID == "" {
		count, err := storetest.CountWorkflowHeadersCreatedSince(testAuthorActivityContext(context.Background()), selected, since)
		if err != nil {
			t.Fatalf("query flow_instances: %v", err)
		}
		if count == 0 {
			t.Fatal("expected flow instance to be created")
		}
		return
	}
	instancePath := strings.Trim(strings.TrimSpace(templateID+"/"+instanceID), "/")
	instanceCount, err := storetest.CountWorkflowHeadersForPathCreatedSince(testAuthorActivityContext(context.Background()), selected, instancePath, since)
	if err != nil {
		t.Fatalf("query flow instance row: %v", err)
	}
	if instanceCount != 1 {
		t.Fatalf("flow instance %q count = %d, want 1", instancePath, instanceCount)
	}
	if fields, ok := want["fields"].(map[string]any); ok && len(fields) > 0 {
		raw, found, err := storetest.ReadLatestWorkflowFieldsForPath(testAuthorActivityContext(context.Background()), selected, instancePath)
		if err == nil && !found {
			t.Fatalf("expected flow instance fields for %s", instancePath)
		}
		if err != nil {
			t.Fatalf("query flow instance fields: %v", err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("decode flow instance fields: %v", err)
		}
		for key, wantValue := range fields {
			gotValue, ok := got[key]
			if !ok {
				t.Fatalf("flow instance fields missing %q; have keys=%v", key, metadataKeys(got))
			}
			gotCanonical, err := canonicalJSONValue(gotValue)
			if err != nil {
				t.Fatalf("canonicalize flow instance fields %q got value: %v", key, err)
			}
			wantCanonical, err := canonicalJSONValue(wantValue)
			if err != nil {
				t.Fatalf("canonicalize flow instance fields %q expected value: %v", key, err)
			}
			if gotCanonical != wantCanonical {
				t.Fatalf("flow instance fields %q = %s, want %s", key, gotCanonical, wantCanonical)
			}
		}
	}
	if autoEmitted := strings.TrimSpace(asString(want["auto_emitted"])); autoEmitted != "" {
		count, err := storetest.CountEventNameStorage(testAuthorActivityContext(context.Background()), selected, autoEmitted)
		if err != nil {
			t.Fatalf("query flow auto-emitted event: %v", err)
		}
		if count != 1 {
			t.Fatalf("auto-emitted event %q count = %d, want 1", autoEmitted, count)
		}
	}
}

func assertFlowInstanceCount(t testing.TB, selected catalogOperatorEventLister, since time.Time, want int) {
	t.Helper()
	count, err := storetest.CountWorkflowHeadersCreatedSince(testAuthorActivityContext(context.Background()), selected, since)
	if err != nil {
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
	if h == nil {
		h.t.Fatal("runtime harness is required for trigger receipt assertions")
	}
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		h.t.Fatal(err)
	}
	eventIDs := h.publishedEventIDs(triggerPayloadEntityID(step.Payload))
	for _, eventID := range eventIDs {
		receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(testAuthorActivityContext(context.Background()), reader, strings.TrimSpace(eventID))
		if err == nil && !receipt.Found {
			err = sql.ErrNoRows
		}
		subscriberID, outcome := receipt.SubscriberID, receipt.Outcome
		rawFailure := []byte(receipt.Failure)
		if rawFailure == nil {
			rawFailure = []byte("null")
		}
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
	if h == nil {
		t.Fatal("runtime harness is required for handler_outcome assertions")
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
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	eventIDs := h.publishedEventIDs(entityID)
	for _, eventID := range eventIDs {
		receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(testAuthorActivityContext(context.Background()), reader, strings.TrimSpace(eventID))
		if err == nil && !receipt.Found {
			err = sql.ErrNoRows
		}
		outcome := receipt.Outcome
		failure := []byte(receipt.Failure)
		if failure == nil {
			failure = []byte("{}")
		}
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			t.Fatalf("query handler_outcome for event %s: %v", eventID, err)
		}
		got := strings.TrimSpace(strings.ToLower(outcome))
		if got != "success" {
			diagnostic, readErr := storetest.ReadLatestHandlerErrorLog(testAuthorActivityContext(context.Background()), reader)
			t.Fatalf("handler_outcome = %q, want %q; failure=%s; diagnostic=%s; diagnostic_read_error=%v", got, want, failure, diagnostic, readErr)
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

func assertEntityDeadLetterOutcome(t testing.TB, selected catalogOperatorEventLister, since time.Time, entityID string) bool {
	t.Helper()
	return catalogHasDeadLetterRelation(t, selected, since, entityID)
}

func assertChainDepthExceeded(t testing.TB, selected catalogOperatorEventLister, entityID string, want bool) {
	t.Helper()
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		if want {
			t.Fatalf("chain_depth_exceeded = true requires entity_id in trigger payload")
		}
		return
	}
	handlerNodeID := identitytest.RootNode(t, "node-6").Key()
	relation, err := storetest.ReadChainDepthDeadLetterStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID, entityID)
	if err != nil {
		t.Fatalf("query chain_depth_exceeded dead-letter relation: %v", err)
	}
	relationCount, chainDepth := relation.Count, relation.Depth
	handlerNode, failureClass, originalEvent := relation.HandlerNode, relation.FailureClass, relation.OriginalEvent

	diagnosticCount, err := storetest.CountChainDepthDiagnosticStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID, entityID, handlerNodeID)
	if err != nil {
		t.Fatalf("query chain_depth_exceeded diagnostic: %v", err)
	}

	activityCount, err := storetest.CountRecordedDeadLetterStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID)
	if err != nil {
		t.Fatalf("query chain_depth_exceeded author activity: %v", err)
	}

	got := relationCount == 1 && diagnosticCount == 1 && activityCount == 1 && chainDepth == 6 && handlerNode == handlerNodeID+":chain.e6" && failureClass == "platform.chain_depth_exceeded" && originalEvent == "chain.e6"
	if got != want {
		t.Fatalf("chain_depth_exceeded facts = relation:%d diagnostic:%d activity:%d depth:%d handler:%q class:%q original:%q, want exact=%v", relationCount, diagnosticCount, activityCount, chainDepth, handlerNode, failureClass, originalEvent, want)
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
