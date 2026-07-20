package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
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
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	ctx := testPipelineCoordinatorRunContext(t, pc)
	transitionCtx := testPersistedWorkflowStateTransitionContext(t, pc.workflowStore, ctx, entityID, "flow.transitioned")
	if err := pc.updateEntityState(transitionCtx, entityID, "done", "flow.transitioned"); err != nil {
		t.Fatalf("updateEntityState: %v", err)
	}

	var (
		field      string
		oldValue   string
		newValue   string
		writerType string
		step       string
	)
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
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

	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"status":          "open",
			"business_status": "pending",
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

	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"status":          "closed",
			"business_status": "approved",
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
		"business_status",
		"gates.g_ready",
		"gates.g_done",
		"accumulator.evidence",
		"accumulator.notes",
	} {
		if !containsMutationField(fields, want) {
			t.Fatalf("mutation fields missing %q: %v", want, fields)
		}
	}

	if err := trackedMutationStateMatchesEntityState(t, db, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(upsert): %v", err)
	}
}

func TestWorkflowInstanceStore_ReplaysContainedStateMapListProjection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()

	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "contained-state-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"verticals": map[string]any{
				"north": map[string]any{
					"status":      "active",
					"active_jobs": []any{},
				},
			},
			"tags": []any{"new"},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	contained := map[string]any{
		"north": map[string]any{
			"status": "busy",
			"active_jobs": []any{
				map[string]any{"id": "job-1", "title": "Build"},
			},
		},
	}
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "contained-state-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"verticals": contained,
			"tags":      []any{"new", "vip"},
		},
	}); err != nil {
		t.Fatalf("update workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(testWorkflowStoreRunContext(t, store), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after contained state update")
	}
	if got := mustCanonicalJSON(t, loaded.Metadata["verticals"]); got != mustCanonicalJSON(t, contained) {
		t.Fatalf("loaded verticals = %s, want %s", got, mustCanonicalJSON(t, contained))
	}
	if got := mustCanonicalJSON(t, loaded.Metadata["tags"]); got != `["new","vip"]` {
		t.Fatalf("loaded tags = %s, want [new,vip]", got)
	}

	fields := mutationFieldsForEntity(t, db, entityID)
	for _, want := range []string{"verticals", "tags"} {
		if !containsMutationField(fields, want) {
			t.Fatalf("mutation fields missing %q: %v", want, fields)
		}
	}
	if err := trackedMutationStateMatchesEntityState(t, db, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(contained state): %v", err)
	}
}

func TestApplyWorkflowGateMutation_LogsMutationRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	entityID := uuid.NewString()
	pc := testMutationLoggingCoordinator(db)
	seedMutationLoggingInstance(t, pc.workflowStore, entityID)

	if err := pc.applyWorkflowGateMutation(testPipelineCoordinatorRunContext(t, pc), entityID, "workflow.ready", "g_ready", false); err != nil {
		t.Fatalf("applyWorkflowGateMutation: %v", err)
	}

	fields := mutationFieldsForEntity(t, db, entityID)
	if !containsMutationField(fields, "gates.g_ready") {
		t.Fatalf("mutation fields missing gates.g_ready: %v", fields)
	}
	if err := trackedMutationStateMatchesEntityState(t, db, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(gate): %v", err)
	}
}

func TestRecordWorkflowEvidence_LogsMutationRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	entityID := uuid.NewString()
	pc := testMutationLoggingCoordinator(db)
	seedMutationLoggingInstance(t, pc.workflowStore, entityID)

	if err := pc.recordWorkflowEvidence(testPipelineCoordinatorRunContext(t, pc), entityID, "", "research", map[string]any{"summary": "done"}); err != nil {
		t.Fatalf("recordWorkflowEvidence: %v", err)
	}

	fields := mutationFieldsForEntity(t, db, entityID)
	if !containsMutationField(fields, "accumulator.evidence") {
		t.Fatalf("mutation fields missing accumulator.evidence: %v", fields)
	}
	if err := trackedMutationStateMatchesEntityState(t, db, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(evidence): %v", err)
	}
}

func TestMutationLogTrackedStateFailsOnMalformedCanonicalMutationField(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	entityID := uuid.NewString()
	pc := testMutationLoggingCoordinator(db)
	seedMutationLoggingInstance(t, pc.workflowStore, entityID)

	var runID string
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `SELECT run_id::text FROM runs ORDER BY started_at DESC LIMIT 1`).Scan(&runID); err != nil {
		t.Fatalf("load run_id: %v", err)
	}
	if _, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, writer_type, writer_id, handler_step
		) VALUES (
			$1::uuid, $2::uuid, 'accumulator.', 'null'::jsonb, '{"bad":true}'::jsonb, 'platform', 'test', 'seed'
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed malformed mutation: %v", err)
	}

	err := trackedMutationStateMatchesEntityState(t, db, entityID)
	if err == nil || !strings.Contains(err.Error(), "accumulator mutation key is required") {
		t.Fatalf("trackedMutationStateMatchesEntityState error = %v, want malformed accumulator failure", err)
	}
}

func TestMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTable(t *testing.T) {
	t.Run("state transition", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		entityID := uuid.NewString()
		pc := testMutationLoggingCoordinator(db)
		seedMutationLoggingInstance(t, pc.workflowStore, entityID)
		dropEntityMutationsTable(t, db)

		ctx := testPipelineCoordinatorRunContext(t, pc)
		transitionCtx := testPersistedWorkflowStateTransitionContext(t, pc.workflowStore, ctx, entityID, "flow.transitioned")
		err := pc.updateEntityState(transitionCtx, entityID, "done", "flow.transitioned")
		if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
			t.Fatalf("updateEntityState err = %v, want entity_mutations failure", err)
		}
		assertCurrentState(t, db, entityID, "queued")
	})

	t.Run("gate mutation", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		entityID := uuid.NewString()
		pc := testMutationLoggingCoordinator(db)
		seedMutationLoggingInstance(t, pc.workflowStore, entityID)
		dropEntityMutationsTable(t, db)

		err := pc.applyWorkflowGateMutation(testPipelineCoordinatorRunContext(t, pc), entityID, "workflow.ready", "g_ready", false)
		if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
			t.Fatalf("applyWorkflowGateMutation err = %v, want entity_mutations failure", err)
		}
		assertEntityGates(t, db, entityID, map[string]any{})
	})

	t.Run("evidence write", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		entityID := uuid.NewString()
		pc := testMutationLoggingCoordinator(db)
		seedMutationLoggingInstance(t, pc.workflowStore, entityID)
		dropEntityMutationsTable(t, db)

		err := pc.recordWorkflowEvidence(testPipelineCoordinatorRunContext(t, pc), entityID, "", "research", map[string]any{"summary": "done"})
		if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
			t.Fatalf("recordWorkflowEvidence err = %v, want entity_mutations failure", err)
		}
		assertAccumulatorBucketMissing(t, db, entityID, "evidence")
	})
}

func testMutationLoggingCoordinator(db *sql.DB) *PipelineCoordinator {
	return &PipelineCoordinator{
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
}

func seedMutationLoggingInstance(t *testing.T, store *WorkflowInstanceStore, entityID string) {
	t.Helper()
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
}

func dropEntityMutationsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), `DROP TABLE entity_mutations`); err != nil {
		t.Fatalf("drop entity_mutations: %v", err)
	}
}

func assertCurrentState(t *testing.T, db *sql.DB, entityID, want string) {
	t.Helper()
	var currentState string
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(current_state, '')
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&currentState); err != nil {
		t.Fatalf("load current_state: %v", err)
	}
	if got := strings.TrimSpace(currentState); got != want {
		t.Fatalf("current_state = %q, want %q", got, want)
	}
}

func assertEntityGates(t *testing.T, db *sql.DB, entityID string, want map[string]any) {
	t.Helper()
	var gatesRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(gates, '{}'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&gatesRaw); err != nil {
		t.Fatalf("load gates: %v", err)
	}
	got := decodeJSONMap(t, gatesRaw)
	if mustCanonicalJSON(t, got) != mustCanonicalJSON(t, want) {
		t.Fatalf("gates = %s, want %s", mustCanonicalJSON(t, got), mustCanonicalJSON(t, want))
	}
}

func assertAccumulatorBucketMissing(t *testing.T, db *sql.DB, entityID, bucket string) {
	t.Helper()
	var accRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(accumulator, '{}'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&accRaw); err != nil {
		t.Fatalf("load accumulator: %v", err)
	}
	acc := decodeJSONMap(t, accRaw)
	if _, ok := acc[strings.TrimSpace(bucket)]; ok {
		t.Fatalf("accumulator = %s, expected bucket %q to be absent", mustCanonicalJSON(t, acc), bucket)
	}
}

func mutationFieldsForEntity(t *testing.T, db *sql.DB, entityID string) []string {
	t.Helper()
	rows, err := db.QueryContext(testAuthorActivityContext(t, context.Background()), `
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

func trackedMutationStateMatchesEntityState(t *testing.T, db *sql.DB, entityID string) error {
	t.Helper()
	var (
		currentState string
		fieldsRaw    []byte
		gatesRaw     []byte
		accRaw       []byte
	)
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT
			COALESCE(current_state, ''),
			COALESCE(fields, '{}'::jsonb),
			COALESCE(gates, '{}'::jsonb),
			COALESCE(accumulator, '{}'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw); err != nil {
		return fmt.Errorf("load entity_state projection: %w", err)
	}

	want := runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState),
		Fields:       decodeJSONMap(t, fieldsRaw),
		Gates:        decodeJSONMap(t, gatesRaw),
		Accumulator:  decodeJSONMap(t, accRaw),
	}
	records := make([]runtimemutationlog.ProjectionMutation, 0, 8)
	rows, err := db.QueryContext(testAuthorActivityContext(t, context.Background()), `
		SELECT field, old_value, new_value
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY created_at ASC, mutation_id ASC
	`, entityID)
	if err != nil {
		return fmt.Errorf("query mutations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			field    string
			oldValue []byte
			newValue []byte
		)
		if err := rows.Scan(&field, &oldValue, &newValue); err != nil {
			return fmt.Errorf("scan mutation: %w", err)
		}
		records = append(records, runtimemutationlog.ProjectionMutation{
			Field:    strings.TrimSpace(field),
			NewValue: decodeJSONValue(t, newValue),
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read mutations: %w", err)
	}
	got, err := runtimemutationlog.ReconstructEntityStateProjection(records)
	if err != nil {
		return fmt.Errorf("reconstruct mutation state: %w", err)
	}

	if !trackedStatesEqual(got, want) {
		return fmt.Errorf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(t, got), mustCanonicalJSON(t, want))
	}
	return nil
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

func trackedStatesEqual(left, right runtimemutationlog.EntityStateProjection) bool {
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
