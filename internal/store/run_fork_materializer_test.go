package store

import (
	"context"
	"database/sql"
	"fmt"
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
	thirdEventID := uuid.NewString()
	at := time.Unix(1700000500, 0).UTC()
	fieldOnlyAt := at.Add(30 * time.Second)
	afterAt := at.Add(time.Minute)
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
			($1::uuid, $3::uuid, 'fork.field_only', $4::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $6),
			($1::uuid, $7::uuid, 'fork.after', $4::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $8)
	`, sourceRunID, firstEventID, secondEventID, entityID, at, fieldOnlyAt, thirdEventID, afterAt); err != nil {
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
			($1::uuid, $2::uuid, 'title', '"before-title"'::jsonb, '"fork-title"'::jsonb, $4::uuid, 'platform', 'materializer-test', 'field-only', $6),
			($1::uuid, $2::uuid, 'current_state', '"queued"'::jsonb, '"done"'::jsonb, $7::uuid, 'platform', 'materializer-test', 'after', $8),
			($1::uuid, $2::uuid, 'title', '"fork-title"'::jsonb, '"after-title"'::jsonb, $7::uuid, 'platform', 'materializer-test', 'after', $8),
			($1::uuid, $2::uuid, 'slug', '"before-slug"'::jsonb, '"after-slug"'::jsonb, $7::uuid, 'platform', 'materializer-test', 'after', $8),
			($1::uuid, $2::uuid, 'name', '"Before Name"'::jsonb, '"After Name"'::jsonb, $7::uuid, 'platform', 'materializer-test', 'after', $8)
	`, sourceRunID, entityID, firstEventID, secondEventID, at, fieldOnlyAt, thirdEventID, afterAt); err != nil {
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
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}

	result, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: secondEventID})
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
	if forkStatus != "paused" || forkedFromRun != sourceRunID || forkedFromEvent != secondEventID {
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
	var forkEnteredStateAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT current_state, fields->>'title', revision
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceState, &sourceTitle, &sourceRevision); err != nil {
		t.Fatalf("load source entity_state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT current_state, fields->>'title', revision, entered_state_at
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkState, &forkTitle, &forkRevision, &forkEnteredStateAt); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if sourceState != "done" || sourceTitle != "after-title" {
		t.Fatalf("source state/title = %s/%s, want done/after-title", sourceState, sourceTitle)
	}
	if sourceRevision != 4 {
		t.Fatalf("source revision = %d, want 4", sourceRevision)
	}
	if forkState != "queued" || forkTitle != "fork-title" {
		t.Fatalf("fork state/title = %s/%s, want queued/fork-title", forkState, forkTitle)
	}
	if forkRevision != 1 {
		t.Fatalf("fork revision = %d, want fork-local revision 1", forkRevision)
	}
	if !forkEnteredStateAt.Equal(at) {
		t.Fatalf("fork entered_state_at = %s, want state-entry timestamp %s", forkEnteredStateAt, at)
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
		SET fields = jsonb_set(fields, '{title}', '"fork-local-title"'::jsonb, true)
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

func TestRunForkActivation_ActivatesMaterializedForkAndFreezesSource(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000800, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	activated, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if !activated.Activated || !activated.SourceFrozen {
		t.Fatalf("activation flags = activated:%v frozen:%v", activated.Activated, activated.SourceFrozen)
	}
	if activated.SourceRunID != sourceRunID || activated.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation lineage = %#v", activated)
	}
	if activated.ForkRunStatus != RunForkActivatedStatus || activated.SourceRunStatus != RunForkSourceFrozenStatus {
		t.Fatalf("activation statuses = fork:%s source:%s", activated.ForkRunStatus, activated.SourceRunStatus)
	}

	var sourceStatus, forkStatus string
	var sourceEndedAt sqlNullTime
	if err := db.QueryRowContext(ctx, `
		SELECT status, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, sourceRunID).Scan(&sourceStatus, &sourceEndedAt); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != RunForkSourceFrozenStatus || !sourceEndedAt.Valid {
		t.Fatalf("source status/ended_at = %s/%v, want forked/valid", sourceStatus, sourceEndedAt.Valid)
	}
	if forkStatus != RunForkActivatedStatus {
		t.Fatalf("fork status = %q, want running", forkStatus)
	}

	var sourceState, forkState string
	if err := db.QueryRowContext(ctx, `
		SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceState); err != nil {
		t.Fatalf("load source state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, materialized.ForkRunID, entityID).Scan(&forkState); err != nil {
		t.Fatalf("load fork state: %v", err)
	}
	if sourceState != "ready" || forkState != "ready" {
		t.Fatalf("source/fork state = %s/%s, want ready/ready", sourceState, forkState)
	}
}

func TestRunForkActivation_FailsClosedForSourceAdvancedAndRepeat(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700000900, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.after', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, afterEventID, entityID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed post-fork event: %v", err)
	}
	_, err = pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "source_events_advanced_after_fork_point") {
		t.Fatalf("ActivateRunFork advanced source error = %v, want source advanced blocker", err)
	}
	var forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status after blocked activation: %v", err)
	}
	if forkStatus != RunForkMaterializedStatus {
		t.Fatalf("fork status after blocked activation = %q, want paused", forkStatus)
	}

	cleanSourceRunID := uuid.NewString()
	cleanEntityID := uuid.NewString()
	cleanEventID := uuid.NewString()
	seedActivationReadySourceRun(t, db, cleanSourceRunID, cleanEntityID, cleanEventID, at.Add(time.Minute))
	cleanMaterialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: cleanSourceRunID, At: cleanEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork clean: %v", err)
	}
	if _, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: cleanMaterialized.ForkRunID}); err != nil {
		t.Fatalf("ActivateRunFork clean: %v", err)
	}
	_, err = pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: cleanMaterialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "requires materialized fork status") {
		t.Fatalf("ActivateRunFork repeat error = %v, want materialized-status failure", err)
	}
}

func TestRunForkActivation_FailsClosedForPendingDeliveryAndMissingLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700001000, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'blocked-node', 'pending', $3)
	`, sourceRunID, eventID, at); err != nil {
		t.Fatalf("seed pending delivery: %v", err)
	}
	_, err = pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "delivery_history_unproven") {
		t.Fatalf("ActivateRunFork pending delivery error = %v, want delivery blocker", err)
	}

	orphanRunID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'paused', $2)`, orphanRunID, at); err != nil {
		t.Fatalf("seed orphan paused run: %v", err)
	}
	_, err = pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: orphanRunID})
	if err == nil || !strings.Contains(err.Error(), "requires fork lineage") {
		t.Fatalf("ActivateRunFork orphan error = %v, want lineage failure", err)
	}
}

type sqlNullTime struct {
	Time  time.Time
	Valid bool
}

type execContextDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (n *sqlNullTime) Scan(value any) error {
	if value == nil {
		n.Valid = false
		return nil
	}
	tm, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("sqlNullTime expected time.Time, got %T", value)
	}
	n.Time = tm
	n.Valid = true
	return nil
}

func seedActivationReadySourceRun(t *testing.T, db execContextDB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
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
		VALUES ($1::uuid, $2::uuid, 'fork.ready', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'activation-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Activation Entity"'::jsonb, $3::uuid, 'platform', 'activation-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Activation Entity',
			'ready', '{}'::jsonb, '{"name":"Activation Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
}
