package runtimepersistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestHistoricalActivityForkSetupKeepsClocksCutsAndCapturedFrontierBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			entity, at := uuid.NewString(), time.Unix(1700002710, 123456000).UTC()
			if err := SeedSelectedSourceEntityForTest(ctx, selected, SelectedSourceEntityFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: entity, EntityType: "activity_history", CreatedAt: at.Add(-time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			activation, err := loopruntime.New(event.RunID(), entity, "flow-a", "revision", "revision_id", event.ID(), "pending", 3, at.Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedHistoricalActivityForkLoopForTest(ctx, selected, event.RunID(), entity, event.ID(), activation, at); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 2 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("historical loop lost its two independent unrevisioned commits: %+v", counts)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			assertActivityHistoryOnlyTablesChanged(t, before, after, "entity_state", "entity_mutations")
			oldEntity, oldErr := DecodeSelectedForkSnapshotRowsForTest(before["entity_state"])
			newEntity, newErr := DecodeSelectedForkSnapshotRowsForTest(after["entity_state"])
			if oldErr != nil || newErr != nil || len(oldEntity) != 1 || len(newEntity) != 1 {
				t.Fatalf("historical entity inventory changed: %d/%d/%v/%v", len(oldEntity), len(newEntity), oldErr, newErr)
			}
			newEntity[0]["accumulator"] = oldEntity[0]["accumulator"]
			if !reflect.DeepEqual(newEntity, oldEntity) {
				t.Fatal("historical loop changed an entity column or clock outside its exact accumulator")
			}
			row, err := ReadActivityForkSourceStorageForTest(ctx, selected, event.RunID(), entity, event.ID())
			if err != nil {
				t.Fatal(err)
			}
			var buckets map[string]map[string]any
			if err := json.Unmarshal(row.Accumulator, &buckets); err != nil {
				t.Fatal(err)
			}
			loops, found, err := loopruntime.Load(buckets, activation.FlowID, activation.LoopID)
			if err != nil || !found || !reflect.DeepEqual(loops, activation) {
				t.Fatalf("historical loop identity, bounds or clocks changed: %+v/%v", loops, err)
			}
			assertActivityHistoryMutation(t, after, event.RunID(), entity, event.ID(), buckets[loopruntime.BucketKey], at)
			if revision, err := CaptureSelectedRunForkRevisionForTest(ctx, selected, event.RunID()); err != nil || revision <= 0 {
				t.Fatalf("capture before recorded attempt: %d/%v", revision, err)
			}
			before, err = ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			input := sha256.Sum256([]byte(`{"value":"x"}`))
			record := pipeline.ActivityAttemptRecord{
				RequestEventID: event.ID(), RunID: event.RunID(), ExecutionMode: executionmode.Live,
				SourceEventID: event.ID(), EntityID: entity, FlowInstance: "flow-a/1", NodeID: "history-node", HandlerEventKey: "review.requested",
				ActivityID: "connector", Tool: "provider.connector", Attempt: 1,
				Status: pipeline.ActivityAttemptStatusSucceeded, SuccessEvent: "connector.succeeded", FailureEvent: "connector.failed",
				ResultEventID: uuid.NewString(), ResultEventType: "connector.succeeded", ResultPayload: map[string]any{"value": "recorded-result"},
				InputHash: fmt.Sprintf("sha256:%x", input), Generation: activation.Generation(), LoopStage: "pending",
				StartedAt: at, CompletedAt: &at, UpdatedAt: at,
			}
			cuts := probe.Snapshot()
			if err := SeedHistoricalActivityForkAttemptForTest(ctx, selected, record); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != cuts.Total.WriteCommits+1 || counts.Total.Revision.Finalizations != cuts.Total.Revision.Finalizations || counts.Active != 0 {
				t.Fatalf("recorded attempt minted another source revision or escaped selected ownership: %+v", counts)
			}
			after, err = ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			assertActivityHistoryOnlyTablesChanged(t, before, after, "activity_attempts")
			assertActivityHistoryJournalRow(t, after["activity_attempts"], record)
			if err := SeedHistoricalActivityForkAttemptForTest(ctx, selected, record); err == nil {
				t.Fatal("historical insert silently overwrote or replayed an existing journal row")
			}
			again, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(again, after) {
				t.Fatalf("duplicate journal insert changed captured or live facts: %v", err)
			}
			assertActivityHistoryRefusals(t, ctx, selected, record, activation, after)
		})
	}
}

func assertActivityHistoryJournalRow(t testing.TB, table SelectedForkStorageTableSnapshot, record pipeline.ActivityAttemptRecord) {
	t.Helper()
	for _, column := range table.Columns {
		if column == "effect_class" {
			t.Fatal("historical journal still exposes the retired authored-policy column")
		}
	}
	assertSelectedSourceEntityFixtureRow(t, table, map[string]string{
		"request_event_id": record.RequestEventID, "run_id": record.RunID, "execution_mode": string(record.ExecutionMode),
		"source_event_id": record.SourceEventID, "parent_event_id": "", "entity_id": record.EntityID, "flow_instance": record.FlowInstance,
		"node_id": record.NodeID, "handler_event_key": record.HandlerEventKey, "activity_id": record.ActivityID, "tool": record.Tool,
		"status": record.Status, "success_event": record.SuccessEvent, "failure_event": record.FailureEvent,
		"result_event_id": record.ResultEventID, "result_event_type": record.ResultEventType, "input_hash": record.InputHash,
		"loop_stage": record.LoopStage, "reply_context_id": "",
	}, record.StartedAt, "started_at", "completed_at", "updated_at")
	rows, err := DecodeSelectedForkSnapshotRowsForTest(table)
	if err != nil || len(rows) != 1 {
		t.Fatalf("historical journal physical row inventory: %d/%v", len(rows), err)
	}
	row := rows[0]
	payload, payloadErr := row["result_payload"].Text()
	generation, generationErr := row["loop_generation"].Text()
	var actualPayload map[string]any
	var actualGeneration pipeline.ActivityAttemptRecord
	if payloadErr != nil || generationErr != nil || json.Unmarshal([]byte(payload), &actualPayload) != nil || json.Unmarshal([]byte(generation), &actualGeneration.Generation) != nil ||
		!reflect.DeepEqual(actualPayload, record.ResultPayload) || actualGeneration.Generation != record.Generation || string(row["attempt"].Value) != "1" || string(row["failure"].Value) != "null" {
		t.Fatalf("historical journal changed native payload, full generation, attempt or SQL NULL failure: %+v", row)
	}
}

func assertActivityHistoryOnlyTablesChanged(t testing.TB, before, after map[string]SelectedForkStorageTableSnapshot, allowed ...string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("historical fixture changed the schema table inventory")
	}
	for table, rows := range before {
		permitted := false
		for _, name := range allowed {
			permitted = permitted || table == name
		}
		if !permitted && !reflect.DeepEqual(rows, after[table]) {
			t.Fatalf("historical setup changed unrelated table %s, including source frontier or history", table)
		}
	}
}

func assertActivityHistoryMutation(t testing.TB, snapshot map[string]SelectedForkStorageTableSnapshot, run, entity, event string, loops map[string]any, at time.Time) {
	t.Helper()
	rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["entity_mutations"])
	if err != nil {
		t.Fatal(err)
	}
	var matched int
	for _, row := range rows {
		text := func(key string) string {
			value, err := row[key].Text()
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
		if text("run_id") != run || text("path") != "handler_loops" {
			continue
		}
		matched++
		var stored map[string]any
		if err := json.Unmarshal([]byte(text("new_value")), &stored); err != nil {
			t.Fatal(err)
		}
		cell := row["created_at"]
		var clock time.Time
		clockMatches := cell.Type == "string" && text("created_at") == at.String()
		if cell.Type == "time.Time" {
			clockMatches = json.Unmarshal(cell.Value, &clock) == nil && clock.Equal(at)
		}
		if !clockMatches || text("entity_id") != entity || text("domain") != "accumulator" || text("old_value") != "null" || text("caused_by_event") != event || text("writer_type") != "platform" || text("writer_id") != "activity-fork-proof" || text("handler_step") != "seed" || !reflect.DeepEqual(stored, loops) {
			t.Fatalf("historical mutation changed exact original fields or clock: %+v", row)
		}
	}
	if matched != 1 {
		t.Fatalf("historical loop mutation inventory=%d want1", matched)
	}
}

func assertActivityHistoryRefusals(t testing.TB, ctx context.Context, selected any, record pipeline.ActivityAttemptRecord, activation loopruntime.Activation, before map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := SeedHistoricalActivityForkLoopForTest(cancelled, selected, record.RunID, record.EntityID, record.RequestEventID, activation, record.StartedAt); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled historical loop accepted: %v", err)
	}
	if err := SeedHistoricalActivityForkAttemptForTest(cancelled, selected, record); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled historical attempt accepted: %v", err)
	}
	for _, owner := range []any{nil, &sql.DB{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
		if err := SeedHistoricalActivityForkLoopForTest(ctx, owner, record.RunID, record.EntityID, record.RequestEventID, activation, record.StartedAt); err == nil {
			t.Fatalf("historical loop accepted raw or uninitialized owner %T", owner)
		}
		if err := SeedHistoricalActivityForkAttemptForTest(ctx, owner, record); err == nil {
			t.Fatalf("historical attempt accepted raw or uninitialized owner %T", owner)
		}
	}
	if err := SeedHistoricalActivityForkLoopForTest(ctx, selected, record.RunID, record.EntityID, record.RequestEventID, loopruntime.Activation{}, record.StartedAt); err == nil {
		t.Fatal("invalid loop activation was persisted")
	}
	for _, change := range []func(*pipeline.ActivityAttemptRecord){
		func(row *pipeline.ActivityAttemptRecord) { row.RequestEventID = "invalid" },
		func(row *pipeline.ActivityAttemptRecord) { row.Status = "started" },
		func(row *pipeline.ActivityAttemptRecord) { row.CompletedAt = nil },
		func(row *pipeline.ActivityAttemptRecord) { row.UpdatedAt = row.UpdatedAt.Add(time.Second) },
		func(row *pipeline.ActivityAttemptRecord) { row.ExecutionMode = "unknown" },
		func(row *pipeline.ActivityAttemptRecord) { row.Generation.RevisionID = "" },
	} {
		invalid := record
		change(&invalid)
		if err := SeedHistoricalActivityForkAttemptForTest(ctx, selected, invalid); err == nil {
			t.Fatalf("invalid recorded journal was persisted: %+v", invalid)
		}
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused historical setup mutated durable facts: %v", err)
	}
	if err := selected.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if err := SeedHistoricalActivityForkLoopForTest(ctx, selected, record.RunID, record.EntityID, record.RequestEventID, activation, record.StartedAt); err == nil {
		t.Fatal("closed loop owner accepted")
	}
	if err := SeedHistoricalActivityForkAttemptForTest(ctx, selected, record); err == nil {
		t.Fatal("closed journal owner accepted")
	}
}

func TestHistoricalActivityForkLoopLateFailurePreservesFirstCommittedCutBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			entity, at := uuid.NewString(), time.Unix(1700002750, 0).UTC()
			if err := SeedSelectedSourceEntityForTest(ctx, selected, SelectedSourceEntityFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: entity, EntityType: "activity_history", CreatedAt: at,
			}); err != nil {
				t.Fatal(err)
			}
			activation, err := loopruntime.New(event.RunID(), entity, "flow-a", "revision", "revision_id", event.ID(), "pending", 3, at)
			if err != nil {
				t.Fatal(err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE entity_mutations RENAME TO unavailable_activity_mutations`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restoreTable := func() {
				if restored {
					return
				}
				if err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), selected, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_activity_mutations RENAME TO entity_mutations`)
					return err
				}); err != nil {
					t.Error(err)
					return
				}
				restored = true
			}
			defer restoreTable()
			probe, detach, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer detach()
			if err := SeedHistoricalActivityForkLoopForTest(ctx, selected, event.RunID(), entity, event.ID(), activation, at); err == nil {
				t.Fatal("missing second-statement storage falsely reported completed historical setup")
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Failed != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("late failure changed the first commit cut or leaked the failed native transaction: %+v", counts)
			}
			restoreTable()
			if !restored {
				t.Fatal("could not restore historical mutation table")
			}
			row, err := ReadActivityForkSourceStorageForTest(ctx, selected, event.RunID(), entity, event.ID())
			var buckets map[string]map[string]any
			if err != nil || json.Unmarshal(row.Accumulator, &buckets) != nil {
				t.Fatalf("first committed accumulator was lost: %s/%v", row.Accumulator, err)
			}
			actual, found, err := loopruntime.Load(buckets, activation.FlowID, activation.LoopID)
			if err != nil || !found || !reflect.DeepEqual(actual, activation) {
				t.Fatalf("late failure rolled back or changed the first original cut: %+v/%t/%v", actual, found, err)
			}
			snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || len(snapshot["entity_mutations"].Rows) != 1 {
				t.Fatalf("failed second cut left a mutation row or lost original history: %d/%v", len(snapshot["entity_mutations"].Rows), err)
			}
		})
	}
}
