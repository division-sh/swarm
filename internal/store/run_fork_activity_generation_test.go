package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedContractForkRemintsActivityRequestAndReusesRecordedWriteEvidence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	sourceRunID, entityID := uuid.NewString(), uuid.NewString()
	sourceEventID := uuid.NewString()
	activation, err := loopruntime.New(sourceRunID, entityID, "flow-a", "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	sourceGeneration := activation.Generation()
	fact := activityidentity.Fact{
		RunID: sourceRunID, SourceEventID: sourceEventID, EntityID: entityID, FlowID: "flow-a",
		NodeID: "writer", HandlerEventKey: "review.accepted", ActivityID: "commit", Tool: "provider.write",
		Attempt: 1, RevisionID: sourceGeneration.RevisionID,
	}
	requestEventID := activityidentity.RequestEventID(fact)
	at := time.Unix(1700003100, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, requestEventID, at)
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	accumulator, _ := json.Marshal(buckets)
	requestPayload := map[string]any{
		"activity_id": "commit", "tool": "provider.write", "input": map[string]any{"value": "x"},
		"effect_class":  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		"success_event": "write.succeeded", "failure_event": "write.failed",
		"fork_policy": string(runtimecontracts.ActivityForkReuseRecordedResult),
		"entity_id":   entityID, "node_id": "writer", "flow_id": "flow-a", "handler_event_key": "review.accepted",
		"source_event_id": sourceEventID, "source_run_id": sourceRunID, "attempt": 1,
		"loop_generation": sourceGeneration, "loop_stage": "review",
	}
	requestJSON, _ := json.Marshal(requestPayload)
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET accumulator = $3::jsonb WHERE run_id = $1::uuid AND entity_id = $2::uuid`, sourceRunID, entityID, string(accumulator)); err != nil {
		t.Fatal(err)
	}
	handlerLoops, _ := json.Marshal(buckets[loopruntime.BucketKey])
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event,
			writer_type, writer_id, handler_step, created_at
		) VALUES ($1::uuid, $2::uuid, 'accumulator.handler_loops', 'null'::jsonb, $3::jsonb, $4::uuid, 'platform', 'loop-test', 'seed', $5)
	`, sourceRunID, entityID, string(handlerLoops), requestEventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_name = 'platform.activity_requested', flow_instance = '', payload = $3::jsonb WHERE run_id = $1::uuid AND event_id = $2::uuid`, sourceRunID, requestEventID, string(requestJSON)); err != nil {
		t.Fatal(err)
	}
	resultPayload, _ := json.Marshal(map[string]any{
		"activity_id": "commit", "tool": "provider.write", "effect_class": "non_idempotent_write", "attempt": 1,
		"result": map[string]any{"ok": true}, "revision_id": sourceGeneration.RevisionID,
	})
	resultEventID := activityidentity.ResultEventID(fact, "write.succeeded")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO activity_attempts (
			request_event_id, run_id, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
			activity_id, tool, effect_class, attempt, status, success_event, failure_event,
			result_event_id, result_event_type, result_payload, input_hash, loop_generation, loop_stage,
			started_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, 'flow-a/1', 'writer', 'review.accepted',
			'commit', 'provider.write', 'non_idempotent_write', 1, 'succeeded', 'write.succeeded', 'write.failed',
			$5::uuid, 'write.succeeded', $6::jsonb, 'input-hash', $7::jsonb, 'review', $8, $8, $8
		)
	`, requestEventID, sourceRunID, sourceEventID, entityID, resultEventID, string(resultPayload), forkTestJSON(t, sourceGeneration), at); err != nil {
		t.Fatal(err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID, At: requestEventID,
		ContractSelection: RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "flow", WorkflowVersion: "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := pg.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, materialized.ForkRunID, []string{requestEventID})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("prepared events = %#v", events)
	}
	var forkPayload runForkActivityRequestPayload
	if err := json.Unmarshal(events[0].Payload, &forkPayload); err != nil {
		t.Fatal(err)
	}
	if forkPayload.SourceRunID != materialized.ForkRunID || forkPayload.Generation.RevisionID == sourceGeneration.RevisionID || !forkPayload.Generation.Valid() {
		t.Fatalf("fork payload = %#v, fork_run=%s source_generation=%#v valid=%v", forkPayload, materialized.ForkRunID, sourceGeneration, forkPayload.Generation.Valid())
	}
	forkFact := activityidentity.Fact{
		RunID: materialized.ForkRunID, SourceEventID: forkPayload.SourceEventID, ParentEventID: forkPayload.ParentEventID,
		EntityID: entityID, FlowID: "flow-a", NodeID: "writer", HandlerEventKey: "review.accepted",
		ActivityID: "commit", Tool: "provider.write", Attempt: 1, RevisionID: forkPayload.Generation.RevisionID,
	}
	forkRequestID := activityidentity.RequestEventID(forkFact)
	var forkRun, status, forkResultID string
	var forkGenerationRaw, forkResultRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT run_id::text, status, result_event_id::text, loop_generation, result_payload FROM activity_attempts WHERE request_event_id = $1::uuid`, forkRequestID).
		Scan(&forkRun, &status, &forkResultID, &forkGenerationRaw, &forkResultRaw); err != nil {
		t.Fatal(err)
	}
	var forkGeneration attemptgeneration.Generation
	if err := json.Unmarshal(forkGenerationRaw, &forkGeneration); err != nil {
		t.Fatal(err)
	}
	var forkResult map[string]any
	if err := json.Unmarshal(forkResultRaw, &forkResult); err != nil {
		t.Fatal(err)
	}
	if forkRun != materialized.ForkRunID || status != "succeeded" || !forkGeneration.Equal(forkPayload.Generation) || forkResult["revision_id"] != forkPayload.Generation.RevisionID {
		t.Fatalf("fork attempt = run:%s status:%s generation:%#v payload:%#v", forkRun, status, forkGeneration, forkResult)
	}
	if forkResultID != activityidentity.ResultEventID(forkFact, "write.succeeded") || forkResultID == resultEventID {
		t.Fatalf("fork result id = %s, source = %s", forkResultID, resultEventID)
	}
}

func TestSelectedContractForkRemintsReadOnlyActivityForReexecution(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	sourceRunID, entityID, sourceEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	activation, err := loopruntime.New(sourceRunID, entityID, "flow-a", "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	sourceGeneration := activation.Generation()
	fact := activityidentity.Fact{
		RunID: sourceRunID, SourceEventID: sourceEventID, EntityID: entityID, FlowID: "flow-a",
		NodeID: "reader", HandlerEventKey: "review.inspect", ActivityID: "inspect", Tool: "provider.read",
		Attempt: 1, RevisionID: sourceGeneration.RevisionID,
	}
	requestEventID := activityidentity.RequestEventID(fact)
	at := time.Unix(1700003200, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, requestEventID, at)
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	accumulator, _ := json.Marshal(buckets)
	handlerLoops, _ := json.Marshal(buckets[loopruntime.BucketKey])
	payload, _ := json.Marshal(map[string]any{
		"activity_id": "inspect", "tool": "provider.read", "input": map[string]any{"id": "x"},
		"effect_class":  string(runtimecontracts.ActivityEffectClassReadOnly),
		"success_event": "read.succeeded", "failure_event": "read.failed",
		"fork_policy": string(runtimecontracts.ActivityForkReexecuteRead),
		"entity_id":   entityID, "node_id": "reader", "flow_id": "flow-a", "handler_event_key": "review.inspect",
		"source_event_id": sourceEventID, "source_run_id": sourceRunID, "attempt": 1,
		"loop_generation": sourceGeneration, "loop_stage": "review",
	})
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET accumulator = $3::jsonb WHERE run_id = $1::uuid AND entity_id = $2::uuid`, sourceRunID, entityID, string(accumulator)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at)
		VALUES ($1::uuid, $2::uuid, 'accumulator.handler_loops', 'null'::jsonb, $3::jsonb, $4::uuid, 'platform', 'loop-test', 'seed', $5)
	`, sourceRunID, entityID, string(handlerLoops), requestEventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_name = 'platform.activity_requested', flow_instance = '', payload = $3::jsonb WHERE run_id = $1::uuid AND event_id = $2::uuid`, sourceRunID, requestEventID, string(payload)); err != nil {
		t.Fatal(err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID, At: requestEventID,
		ContractSelection: RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "flow", WorkflowVersion: "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := pg.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, materialized.ForkRunID, []string{requestEventID})
	if err != nil {
		t.Fatal(err)
	}
	var forkPayload runForkActivityRequestPayload
	if err := json.Unmarshal(prepared[0].Payload, &forkPayload); err != nil {
		t.Fatal(err)
	}
	if forkPayload.SourceRunID != materialized.ForkRunID || forkPayload.Generation.RevisionID == sourceGeneration.RevisionID {
		t.Fatalf("fork read payload = %#v", forkPayload)
	}
	forkFact := activityidentity.Fact{
		RunID: materialized.ForkRunID, SourceEventID: forkPayload.SourceEventID, ParentEventID: forkPayload.ParentEventID,
		EntityID: entityID, FlowID: "flow-a", NodeID: "reader", HandlerEventKey: "review.inspect",
		ActivityID: "inspect", Tool: "provider.read", Attempt: 1, RevisionID: forkPayload.Generation.RevisionID,
	}
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE request_event_id = $1::uuid`, activityidentity.RequestEventID(forkFact)).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("fork read attempts = %d, want runtime reexecution rather than copied evidence", attempts)
	}
}

func TestSelectedContractForkPreservesTypedFailedWriteEvidence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	sourceRunID, entityID, sourceEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	activation, err := loopruntime.New(sourceRunID, entityID, "flow-a", "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	generation := activation.Generation()
	fact := activityidentity.Fact{
		RunID: sourceRunID, SourceEventID: sourceEventID, EntityID: entityID, FlowID: "flow-a",
		NodeID: "writer", HandlerEventKey: "review.accepted", ActivityID: "commit", Tool: "provider.write",
		Attempt: 1, RevisionID: generation.RevisionID,
	}
	requestEventID := activityidentity.RequestEventID(fact)
	at := time.Unix(1700003300, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, requestEventID, at)
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	accumulator, _ := json.Marshal(buckets)
	handlerLoops, _ := json.Marshal(buckets[loopruntime.BucketKey])
	requestPayload, _ := json.Marshal(map[string]any{
		"activity_id": "commit", "tool": "provider.write", "input": map[string]any{"value": "x"},
		"effect_class":  string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		"success_event": "write.succeeded", "failure_event": "write.failed",
		"fork_policy": string(runtimecontracts.ActivityForkReuseRecordedResult),
		"entity_id":   entityID, "node_id": "writer", "flow_id": "flow-a", "handler_event_key": "review.accepted",
		"source_event_id": sourceEventID, "source_run_id": sourceRunID, "attempt": 1,
		"loop_generation": generation, "loop_stage": "review",
	})
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET accumulator = $3::jsonb WHERE run_id = $1::uuid AND entity_id = $2::uuid`, sourceRunID, entityID, string(accumulator)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at)
		VALUES ($1::uuid, $2::uuid, 'accumulator.handler_loops', 'null'::jsonb, $3::jsonb, $4::uuid, 'platform', 'loop-test', 'seed', $5)
	`, sourceRunID, entityID, string(handlerLoops), requestEventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_name = 'platform.activity_requested', flow_instance = '', payload = $3::jsonb WHERE run_id = $1::uuid AND event_id = $2::uuid`, sourceRunID, requestEventID, string(requestPayload)); err != nil {
		t.Fatal(err)
	}
	failure := `{"class":"dependency_unavailable","code":"provider_unavailable","owner":"activity-runtime","operation":"execute","details":{"provider":"provider.write"}}`
	resultPayload, _ := json.Marshal(map[string]any{
		"activity_id": "commit", "tool": "provider.write", "effect_class": "non_idempotent_write", "attempt": 1,
		"failure": map[string]any{"code": "provider_unavailable"}, "revision_id": generation.RevisionID,
	})
	resultEventID := activityidentity.ResultEventID(fact, "write.failed")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO activity_attempts (
			request_event_id, run_id, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
			activity_id, tool, effect_class, attempt, status, success_event, failure_event,
			result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage,
			started_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, 'flow-a/1', 'writer', 'review.accepted',
			'commit', 'provider.write', 'non_idempotent_write', 1, 'failed', 'write.succeeded', 'write.failed',
			$5::uuid, 'write.failed', $6::jsonb, $7::jsonb, 'input-hash', $8::jsonb, 'review', $9, $9, $9
		)
	`, requestEventID, sourceRunID, sourceEventID, entityID, resultEventID, string(resultPayload), failure, forkTestJSON(t, generation), at); err != nil {
		t.Fatal(err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID, At: requestEventID,
		ContractSelection: RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "flow", WorkflowVersion: "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := pg.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, materialized.ForkRunID, []string{requestEventID})
	if err != nil {
		t.Fatal(err)
	}
	var forkPayload runForkActivityRequestPayload
	if err := json.Unmarshal(prepared[0].Payload, &forkPayload); err != nil {
		t.Fatal(err)
	}
	forkFact := activityidentity.Fact{
		RunID: materialized.ForkRunID, SourceEventID: forkPayload.SourceEventID, ParentEventID: forkPayload.ParentEventID,
		EntityID: entityID, FlowID: "flow-a", NodeID: "writer", HandlerEventKey: "review.accepted",
		ActivityID: "commit", Tool: "provider.write", Attempt: 1, RevisionID: forkPayload.Generation.RevisionID,
	}
	var rawFailure []byte
	if err := db.QueryRowContext(ctx, `SELECT failure FROM activity_attempts WHERE request_event_id = $1::uuid`, activityidentity.RequestEventID(forkFact)).Scan(&rawFailure); err != nil {
		t.Fatal(err)
	}
	var typed map[string]any
	if err := json.Unmarshal(rawFailure, &typed); err != nil {
		t.Fatalf("fork failure is not a typed JSON object: %v (%s)", err, rawFailure)
	}
	if typed["code"] != "provider_unavailable" || typed["class"] != "dependency_unavailable" {
		t.Fatalf("fork failure = %#v", typed)
	}
}

func forkTestJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
