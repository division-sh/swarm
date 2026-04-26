package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestRunForkMaterializer_CreatesPausedForkRunAndSnapshotWithoutResuming(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	firstEventID := uuid.NewString()
	secondEventID := uuid.NewString()
	at := time.Unix(1700000500, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'fork.before', $4::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $5),
			($1::uuid, $3::uuid, 'fork.after', $4::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $6)
	`, sourceRunID, firstEventID, secondEventID, entityID, at, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'title', 'null'::jsonb, '"before-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'slug', 'null'::jsonb, '"before-slug"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Before Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'gates.ready', 'null'::jsonb, 'true'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'accumulator.score', 'null'::jsonb, '7'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $5),
			($1::uuid, $2::uuid, 'current_state', '"queued"'::jsonb, '"done"'::jsonb, $4::uuid, 'platform', 'materializer-test', 'after', $6),
			($1::uuid, $2::uuid, 'title', '"before-title"'::jsonb, '"after-title"'::jsonb, $4::uuid, 'platform', 'materializer-test', 'after', $6),
			($1::uuid, $2::uuid, 'slug', '"before-slug"'::jsonb, '"after-slug"'::jsonb, $4::uuid, 'platform', 'materializer-test', 'after', $6),
			($1::uuid, $2::uuid, 'name', '"Before Name"'::jsonb, '"After Name"'::jsonb, $4::uuid, 'platform', 'materializer-test', 'after', $6)
	`, sourceRunID, entityID, firstEventID, secondEventID, at, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'after-slug', 'After Name',
			'done', '{"ready": true}'::jsonb, '{"title": "after-title", "slug": "after-slug", "name": "After Name"}'::jsonb, '{"score": 9}'::jsonb, 4,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}

	result, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: firstEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if result.ForkRunID == "" {
		t.Fatal("ForkRunID is empty")
	}
	if result.ForkRunStatus != RunForkMaterializedStatus {
		t.Fatalf("ForkRunStatus = %q, want %q", result.ForkRunStatus, RunForkMaterializedStatus)
	}
	if !result.DeliveryResumeBlocked || !result.SourceRunStatusUnchanged {
		t.Fatalf("boundary flags = resume_blocked:%v source_unchanged:%v", result.DeliveryResumeBlocked, result.SourceRunStatusUnchanged)
	}

	var forkStatus, forkedFromRun, forkedFromEvent string
	if err := db.QueryRowContext(ctx, `
		SELECT status, forked_from_run_id::text, forked_from_event_id::text
		FROM runs
		WHERE run_id = $1::uuid
	`, result.ForkRunID).Scan(&forkStatus, &forkedFromRun, &forkedFromEvent); err != nil {
		t.Fatalf("load fork run: %v", err)
	}
	if forkStatus != "paused" || forkedFromRun != sourceRunID || forkedFromEvent != firstEventID {
		t.Fatalf("fork run = status:%s from:%s event:%s", forkStatus, forkedFromRun, forkedFromEvent)
	}
	var sourceStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source run status: %v", err)
	}
	if sourceStatus != "running" {
		t.Fatalf("source status = %q, want running", sourceStatus)
	}

	var sourceState, forkState, sourceTitle, forkTitle string
	var sourceRevision, forkRevision int
	if err := db.QueryRowContext(ctx, `
		SELECT current_state, fields->>'title', revision
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceState, &sourceTitle, &sourceRevision); err != nil {
		t.Fatalf("load source entity_state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT current_state, fields->>'title', revision
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkState, &forkTitle, &forkRevision); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if sourceState != "done" || sourceTitle != "after-title" {
		t.Fatalf("source state/title = %s/%s, want done/after-title", sourceState, sourceTitle)
	}
	if sourceRevision != 4 {
		t.Fatalf("source revision = %d, want 4", sourceRevision)
	}
	if forkState != "queued" || forkTitle != "before-title" {
		t.Fatalf("fork state/title = %s/%s, want queued/before-title", forkState, forkTitle)
	}
	if forkRevision != 1 {
		t.Fatalf("fork revision = %d, want fork-local revision 1", forkRevision)
	}
	var forkSlug, forkName string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(slug, ''), COALESCE(name, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkSlug, &forkName); err != nil {
		t.Fatalf("load fork display metadata: %v", err)
	}
	if forkSlug != "before-slug" || forkName != "Before Name" {
		t.Fatalf("fork display metadata = %s/%s, want before-slug/Before Name", forkSlug, forkName)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET fields = jsonb_set(fields, '{title}', '"fork-title"'::jsonb, true)
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID); err != nil {
		t.Fatalf("diverge fork entity_state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT fields->>'title'
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceTitle); err != nil {
		t.Fatalf("reload source title: %v", err)
	}
	if sourceTitle != "after-title" {
		t.Fatalf("source title after fork divergence = %q, want after-title", sourceTitle)
	}

	for _, field := range []string{"current_state", "title", "slug", "name", "gates.ready", "accumulator.score"} {
		var count int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND field = $3
			  AND writer_type = 'platform'
			  AND writer_id = 'run_fork_materializer'
		`, result.ForkRunID, entityID, field).Scan(&count); err != nil {
			t.Fatalf("count mutation %s: %v", field, err)
		}
		if count != 1 {
			t.Fatalf("mutation count for %s = %d, want 1", field, count)
		}
	}

	sideEffectQueries := []struct {
		name  string
		query string
		args  []any
	}{
		{name: "event_deliveries", query: `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, args: []any{result.ForkRunID}},
		{name: "timers", query: `SELECT COUNT(*) FROM timers WHERE entity_id = $1::uuid`, args: []any{entityID}},
		{name: "agent_sessions", query: `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid`, args: []any{result.ForkRunID}},
		{name: "agent_turns", query: `SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid`, args: []any{result.ForkRunID}},
	}
	for _, check := range sideEffectQueries {
		var count int
		if err := db.QueryRowContext(ctx, check.query, check.args...).Scan(&count); err != nil {
			t.Fatalf("count side-effect rows %s: %v", check.name, err)
		}
		if count != 0 {
			t.Fatalf("side-effect row count for %s = %d, want 0", check.name, count)
		}
	}
}

func TestRunForkMaterializer_FailsClosedOnRepeatAndUnsupportedBlockers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000600, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.pending', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'ready',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'pending-node', 'pending', $3)
	`, sourceRunID, eventID, at); err != nil {
		t.Fatalf("seed pending delivery: %v", err)
	}

	_, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), "delivery_history_unproven") {
		t.Fatalf("MaterializeRunFork error = %v, want delivery blocker", err)
	}
	var forkCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&forkCount); err != nil {
		t.Fatalf("count blocked fork rows: %v", err)
	}
	if forkCount != 0 {
		t.Fatalf("blocked fork rows = %d, want 0", forkCount)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM event_deliveries WHERE run_id = $1::uuid`, sourceRunID); err != nil {
		t.Fatalf("clear pending delivery: %v", err)
	}
	first, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork first: %v", err)
	}
	_, err = pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("MaterializeRunFork repeat error = %v, want already exists", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
		  AND forked_from_event_id = $2::uuid
	`, sourceRunID, eventID).Scan(&forkCount); err != nil {
		t.Fatalf("count fork rows after repeat: %v", err)
	}
	if forkCount != 1 {
		t.Fatalf("fork rows after repeat = %d, want 1", forkCount)
	}
	if first.ForkRunID == "" {
		t.Fatal("first ForkRunID is empty")
	}
}
