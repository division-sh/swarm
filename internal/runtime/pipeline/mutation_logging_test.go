package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/testutil"
)

func TestUpdateEntityState_LogsMutationRowForStateTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)

	entityID := uuid.NewString()
	pc := &PipelineCoordinator{
		workflowStore: NewWorkflowInstanceStore(db),
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					Name:    "mutation-flow",
					Version: "1.0.0",
				},
			},
		},
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	if err := pc.updateEntityState(context.Background(), entityID, "done", "flow.transitioned"); err != nil {
		t.Fatalf("updateEntityState: %v", err)
	}

	var (
		field      string
		oldValue   string
		newValue   string
		writerType string
		step       string
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT
			COALESCE(field, ''),
			COALESCE(old_value::text, ''),
			COALESCE(new_value::text, ''),
			COALESCE(writer_type, ''),
			COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid AND field = 'current_state'
		ORDER BY created_at DESC
		LIMIT 1
	`, entityID).Scan(&field, &oldValue, &newValue, &writerType, &step); err != nil {
		t.Fatalf("load entity mutation: %v", err)
	}
	if field != "current_state" {
		t.Fatalf("mutation field = %q, want current_state", field)
	}
	if oldValue != `"queued"` {
		t.Fatalf("mutation old_value = %s, want \"queued\"", oldValue)
	}
	if newValue != `"done"` {
		t.Fatalf("mutation new_value = %s, want \"done\"", newValue)
	}
	if writerType == "" {
		t.Fatal("mutation writer_type is empty")
	}
	if step == "" {
		t.Fatal("mutation handler_step is empty")
	}
}

func TestWorkflowInstanceStore_UpsertTracksFieldsGatesAndAccumulatorInMutationLog(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()

	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"status": "open",
			"gates": map[string]any{
				"g_ready": true,
			},
		},
		StateBuckets: map[string]any{
			"evidence": map[string]any{"score": 1},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"status": "closed",
			"gates": map[string]any{
				"g_done": true,
			},
		},
		StateBuckets: map[string]any{
			"evidence": map[string]any{"score": 2},
			"notes":    map[string]any{"count": 1},
		},
	}); err != nil {
		t.Fatalf("update workflow instance: %v", err)
	}

	fields := mutationFieldsForEntity(t, db, entityID)
	for _, want := range []string{
		"current_state",
		"status",
		"gates.g_ready",
		"gates.g_done",
		"accumulator.evidence",
		"accumulator.notes",
	} {
		if !containsMutationField(fields, want) {
			t.Fatalf("mutation fields missing %q: %v", want, fields)
		}
	}

	assertMutationLogMatchesTrackedEntityState(t, db, entityID)
}

func mutationFieldsForEntity(t *testing.T, db *sql.DB, entityID string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT field
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY created_at ASC, mutation_id ASC
	`, entityID)
	if err != nil {
		t.Fatalf("query mutation fields: %v", err)
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			t.Fatalf("scan mutation field: %v", err)
		}
		out = append(out, strings.TrimSpace(field))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read mutation fields: %v", err)
	}
	return out
}

func assertMutationLogMatchesTrackedEntityState(t *testing.T, db *sql.DB, entityID string) {
	t.Helper()
	var (
		currentState string
		fieldsRaw    []byte
		gatesRaw     []byte
		accRaw       []byte
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT
			COALESCE(current_state, ''),
			COALESCE(fields, '{}'::jsonb),
			COALESCE(gates, '{}'::jsonb),
			COALESCE(accumulator, '{}'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw); err != nil {
		t.Fatalf("load entity_state projection: %v", err)
	}

	want := trackedMutationState{
		CurrentState: strings.TrimSpace(currentState),
		Fields:       decodeJSONMap(t, fieldsRaw),
		Gates:        decodeJSONMap(t, gatesRaw),
		Accumulator:  decodeJSONMap(t, accRaw),
	}
	got := trackedMutationState{
		Fields:      map[string]any{},
		Gates:       map[string]any{},
		Accumulator: map[string]any{},
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT field, old_value, new_value
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY created_at ASC, mutation_id ASC
	`, entityID)
	if err != nil {
		t.Fatalf("query mutations: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			field    string
			oldValue []byte
			newValue []byte
		)
		if err := rows.Scan(&field, &oldValue, &newValue); err != nil {
			t.Fatalf("scan mutation: %v", err)
		}
		applyTrackedMutation(&got, strings.TrimSpace(field), decodeJSONValue(t, newValue))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read mutations: %v", err)
	}

	if !trackedStatesEqual(got, want) {
		t.Fatalf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(t, got), mustCanonicalJSON(t, want))
	}
}

type trackedMutationState struct {
	CurrentState string         `json:"current_state"`
	Fields       map[string]any `json:"fields"`
	Gates        map[string]any `json:"gates"`
	Accumulator  map[string]any `json:"accumulator"`
}

func applyTrackedMutation(state *trackedMutationState, field string, value any) {
	if state == nil {
		return
	}
	switch {
	case field == "current_state":
		state.CurrentState = strings.TrimSpace(trackedAsString(value))
	case strings.HasPrefix(field, "gates."):
		applyTrackedMapValue(state.Gates, strings.TrimPrefix(field, "gates."), value)
	case strings.HasPrefix(field, "accumulator."):
		applyTrackedMapValue(state.Accumulator, strings.TrimPrefix(field, "accumulator."), value)
	default:
		applyTrackedMapValue(state.Fields, field, value)
	}
}

func applyTrackedMapValue(target map[string]any, key string, value any) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	if value == nil {
		delete(target, key)
		return
	}
	target[key] = value
}

func decodeJSONMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode json map: %v", err)
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func decodeJSONValue(t *testing.T, raw []byte) any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode json value: %v", err)
	}
	return out
}

func trackedStatesEqual(left, right trackedMutationState) bool {
	return mustCanonicalJSONForCompare(left) == mustCanonicalJSONForCompare(right)
}

func mustCanonicalJSON(t *testing.T, value any) string {
	t.Helper()
	return mustCanonicalJSONForCompare(value)
}

func mustCanonicalJSONForCompare(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func containsMutationField(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func trackedAsString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	default:
		return ""
	}
}
