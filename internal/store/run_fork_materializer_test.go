package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkMaterializer_CreatesPausedForkRunAndSnapshotWithoutResuming(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', 'bundle-source-fingerprint', $2)
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
			$3, $4, $3
		)
	`, sourceRunID, entityID, afterAt, at); err != nil {
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
	if result.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", result.ReplayResumeAdmission.Owner, RunForkReplayResumeAdmissionOwner)
	}
	if !result.ReplayResumeAdmission.StateOnlyExecutionReady || result.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("taxonomy flags = state_only:%v bounded_supported:%v, want true/false",
			result.ReplayResumeAdmission.StateOnlyExecutionReady,
			result.ReplayResumeAdmission.BoundedReplaySupported,
		)
	}

	var forkStatus, forkedFromRun, forkedFromEvent, forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT status, forked_from_run_id::text, forked_from_event_id::text, COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, result.ForkRunID).Scan(&forkStatus, &forkedFromRun, &forkedFromEvent, &forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run: %v", err)
	}
	if forkStatus != "paused" || forkedFromRun != sourceRunID || forkedFromEvent != secondEventID {
		t.Fatalf("fork run = status:%s from:%s event:%s", forkStatus, forkedFromRun, forkedFromEvent)
	}
	if forkBundleHash != "" || forkBundleSource != "legacy" || forkBundleFingerprint != "bundle-source-fingerprint" {
		t.Fatalf("fork bundle identity = hash:%q source:%q fingerprint:%q, want legacy with compatibility fingerprint", forkBundleHash, forkBundleSource, forkBundleFingerprint)
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
	var forkFlow, forkType, forkSlug, forkName string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, COALESCE(slug, ''), COALESCE(name, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkFlow, &forkType, &forkSlug, &forkName); err != nil {
		t.Fatalf("load fork display metadata: %v", err)
	}
	if forkFlow != "flow-a/1" || forkType != "default" {
		t.Fatalf("fork owner metadata = flow:%s type:%s, want flow-a/1/default", forkFlow, forkType)
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

func TestRunForkMaterializer_UsesSourceCurrentStateSnapshotMetadataWhenEventFlowAbsent(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	postEventID := uuid.NewString()
	at := time.Unix(1700000505, 0).UTC()
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
			($1::uuid, $2::uuid, 'fork.no_event_flow', $4::uuid, '', 'entity', '{}'::jsonb, 'test', 'platform', $5),
			($1::uuid, $3::uuid, 'fork.post_flow', $4::uuid, 'post-flow/ignored', 'entity', '{}'::jsonb, 'test', 'platform', $6)
	`, sourceRunID, eventID, postEventID, entityID, at, afterAt); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Fork Point Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'current_state', '"pending"'::jsonb, '"done"'::jsonb, $5::uuid, 'platform', 'materializer-test', 'after', $6)
	`, sourceRunID, entityID, eventID, at, postEventID, afterAt); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'state-flow/at-T', 'validation_case', 'Current Name',
			'done', '{}'::jsonb, '{"name": "Current Name"}'::jsonb, '{}'::jsonb, 2,
			$4, $3, $4
		)
	`, sourceRunID, entityID, at, afterAt); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, blockers=%#v", plan.UnsupportedBlockers)
	}
	if len(plan.Entities) != 1 || plan.Entities[0].MaterializationMetadata == nil {
		t.Fatalf("plan entities = %#v, want materialization metadata", plan.Entities)
	}
	metadata := plan.Entities[0].MaterializationMetadata
	if metadata.Owner != RunForkMaterializedEntitySnapshotMetadataOwner ||
		metadata.Source != RunForkMaterializedEntitySnapshotMetadataSourceEntityState ||
		metadata.FlowInstance != "state-flow/at-T" ||
		metadata.EntityType != "validation_case" {
		t.Fatalf("materialization metadata = %#v", metadata)
	}

	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	var flowInstance, entityType, state, name string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, current_state, COALESCE(name, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, materialized.ForkRunID, entityID).Scan(&flowInstance, &entityType, &state, &name); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if flowInstance != "state-flow/at-T" || entityType != "validation_case" || state != "pending" || name != "Fork Point Name" {
		t.Fatalf("fork snapshot = flow:%s type:%s state:%s name:%s", flowInstance, entityType, state, name)
	}
}

func TestRunForkPlanner_FailsClosedWithoutSourceAtTEntitySnapshotMetadata(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000507, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.no_metadata', $3::uuid, '', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'post-state-flow', 'default',
			'pending', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("seed post-T entity_state: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for missing metadata")
	}
	if !runForkTestHasPlanBlocker(plan, RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, RunForkBlockerEntitySnapshotMetadataUnproven)
	}
	if _, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID}); err == nil || !strings.Contains(err.Error(), RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("MaterializeRunFork error = %v, want metadata blocker", err)
	}
	var forks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forks); err != nil {
		t.Fatalf("count fork runs: %v", err)
	}
	if forks != 0 {
		t.Fatalf("fork runs = %d, want 0", forks)
	}
}

func TestRunForkPlanner_FailsClosedWhenFieldEntityTypeHasNoSourceMetadataAuthority(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000508, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.event_flow_only', $3::uuid, 'event-flow/at-T', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'entity_type', 'null'::jsonb, '"field_case"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for field-only entity_type authority")
	}
	if !runForkTestHasPlanBlocker(plan, RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, RunForkBlockerEntitySnapshotMetadataUnproven)
	}
}

func TestRunForkPlanner_FailsClosedWhenFieldEntityTypeConflictsWithSourceMetadata(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000509, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.conflicting_entity_type', $3::uuid, 'event-flow/at-T', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'entity_type', 'null'::jsonb, '"field_case"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'state-flow/at-T', 'source_case',
			'pending', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for conflicting entity_type authority")
	}
	if !runForkTestHasPlanBlocker(plan, RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, RunForkBlockerEntitySnapshotMetadataUnproven)
	}
}

func TestRunForkSelectedContractBinding_MaterializesDurableForkRunBinding(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000510, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

	selection := RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/selected-contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		ContractSelection: &selection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if materialized.SelectedContractBinding == nil {
		t.Fatalf("SelectedContractBinding = nil")
	}
	if materialized.SelectedContractBinding.Owner != RunForkSelectedContractBindingOwner ||
		materialized.SelectedContractBinding.ForkRunID != materialized.ForkRunID ||
		materialized.SelectedContractBinding.SourceRunID != sourceRunID ||
		materialized.SelectedContractBinding.ForkEventID != eventID {
		t.Fatalf("materialized selected binding = %#v", materialized.SelectedContractBinding)
	}

	loaded, err := pg.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatalf("RequireRunForkSelectedContractBinding: %v", err)
	}
	if loaded.Owner != RunForkSelectedContractBindingOwner ||
		loaded.ContractSelection.ContractsRoot != selection.ContractsRoot ||
		loaded.ContractSelection.WorkflowName != selection.WorkflowName ||
		loaded.ContractSelection.WorkflowVersion != selection.WorkflowVersion {
		t.Fatalf("loaded selected binding = %#v", loaded)
	}

	activated, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if activated.SelectedContractBinding == nil ||
		activated.SelectedContractBinding.Owner != RunForkSelectedContractBindingOwner ||
		activated.SelectedContractBinding.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activated selected binding = %#v", activated.SelectedContractBinding)
	}

	var forkEventCount, forkDeliveryCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkEventCount); err != nil {
		t.Fatalf("count fork events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkDeliveryCount); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if forkEventCount != 0 || forkDeliveryCount != 0 {
		t.Fatalf("fork executable work = events:%d deliveries:%d, want 0/0", forkEventCount, forkDeliveryCount)
	}
}

func TestRunForkSelectedContractBinding_MaterializesDurableBundleHashBinding(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000515, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

	targetHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	selection := RunForkContractSelection{
		Mode:            RunForkContractSelectionModeBundleHash,
		BundleHash:      targetHash,
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		BundleHash:        targetHash,
		BundleSource:      "persisted",
		ContractSelection: &selection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	loaded, err := pg.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatalf("RequireRunForkSelectedContractBinding: %v", err)
	}
	if loaded.ContractSelection.Mode != RunForkContractSelectionModeBundleHash ||
		loaded.ContractSelection.BundleHash != targetHash ||
		loaded.ContractSelection.ContractsRoot != "" ||
		loaded.ContractSelection.WorkflowName != selection.WorkflowName ||
		loaded.ContractSelection.WorkflowVersion != selection.WorkflowVersion {
		t.Fatalf("loaded bundle_hash binding = %#v", loaded)
	}
	var forkBundleHash, forkBundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), COALESCE(bundle_source, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkBundleHash, &forkBundleSource); err != nil {
		t.Fatalf("load fork bundle identity: %v", err)
	}
	if forkBundleHash != targetHash || forkBundleSource != "persisted" {
		t.Fatalf("fork bundle identity = %s/%s, want %s/persisted", forkBundleHash, forkBundleSource, targetHash)
	}
}

func TestRunForkSelectedContractBinding_FailsClosedOnMissingDuplicateAndInvalidSelection(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := pg.RequireRunForkSelectedContractBinding(ctx, uuid.NewString()); err == nil || !strings.Contains(err.Error(), "selected contract binding") {
		t.Fatalf("RequireRunForkSelectedContractBinding error = %v, want missing binding failure", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000520, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	invalidSelection := RunForkContractSelection{
		Mode:            "selected_contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	if _, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		ContractSelection: &invalidSelection,
	}); err == nil || !strings.Contains(err.Error(), "contracts_root") {
		t.Fatalf("MaterializeRunFork invalid selection error = %v, want contracts_root failure", err)
	}

	validSelection := RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/selected-contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		ContractSelection: &validSelection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (
			fork_run_id, source_run_id, fork_event_id,
			mode, contracts_root, workflow_name, workflow_version
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid,
			'selected_contracts', '/tmp/duplicate', 'workflow', 'v1'
		)
	`, materialized.ForkRunID, sourceRunID, eventID)
	if err == nil {
		t.Fatalf("duplicate selected contract binding insert succeeded, want unique failure")
	}
}

func TestRunForkMaterializer_FailsClosedOnRepeatAndUnsupportedBlockers(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, started_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'in-progress-node', 'in_progress', $4::uuid, $3, $3)
	`, sourceRunID, eventID, at, uuid.NewString()); err != nil {
		t.Fatalf("seed in-progress delivery: %v", err)
	}

	blocked, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork error = %v, want non-agent delivery blocker", err)
	}
	if blocked.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner || !blocked.ReplayResumeAdmission.ReplayResumeFactsPresent {
		t.Fatalf("blocked taxonomy = %#v, want owner and historical replay required", blocked.ReplayResumeAdmission)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, RunForkReplayResumeFactDeliveryInProgressHistory) {
		t.Fatalf("blocked taxonomy missing in-progress delivery disposition: %#v", blocked.ReplayResumeAdmission)
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
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
	if !activated.ReplayResumeBlocked {
		t.Fatal("ReplayResumeBlocked = false, want true for activation-only boundary")
	}
	if activated.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner || !activated.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("activation taxonomy = %#v, want owner and state-only ready", activated.ReplayResumeAdmission)
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

func TestRunForkActivation_ReplaysSafePendingDeliveryWithForkLocalLineage(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000850, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

	var sourceDeliveryID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'matched_agent_subscription', $3)
		RETURNING delivery_id::text
	`, sourceRunID, eventID, at).Scan(&sourceDeliveryID); err != nil {
		t.Fatalf("seed safe pending delivery: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		t.Fatalf("plan replay readiness = execution:%v admission:%#v", plan.ExecutionReady, plan.ReplayResumeAdmission)
	}
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), RunForkHistoricalReplayExecutionOwner) {
		t.Fatalf("ActivateRunFork without historical replay owner error = %v, want %s", err, RunForkHistoricalReplayExecutionOwner)
	}
	if blocked.Activated || blocked.SourceFrozen {
		t.Fatalf("blocked activation mutated lifecycle: %#v", blocked)
	}
	var sourceStatusAfterBlocked string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatusAfterBlocked); err != nil {
		t.Fatalf("load source status after blocked activation: %v", err)
	}
	if sourceStatusAfterBlocked != "running" {
		t.Fatalf("source status after blocked activation = %q, want running", sourceStatusAfterBlocked)
	}

	admitter := &fakeRunForkHistoricalReplayExecutionAdmitter{}
	activated, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{
		ForkRunID:                         materialized.ForkRunID,
		HistoricalReplayExecutionAdmitter: admitter,
	})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if !admitter.called {
		t.Fatal("historical replay execution admitter was not called")
	}
	if admitter.request.ForkRunID != materialized.ForkRunID ||
		admitter.request.SourceRunID != sourceRunID ||
		admitter.request.ForkEventID != eventID ||
		!admitter.request.ReplayResumeAdmission.DeliveryEventReplayReady ||
		len(admitter.request.PendingWork) != 1 ||
		admitter.request.PendingWork[0].DeliveryID != sourceDeliveryID {
		t.Fatalf("historical replay execution request = %#v", admitter.request)
	}
	if activated.HistoricalReplayExecution == nil ||
		activated.HistoricalReplayExecution.Owner != RunForkHistoricalReplayExecutionOwner ||
		activated.HistoricalReplayExecution.AdmissionOwner != RunForkHistoricalReplayExecutionAdmissionOwner ||
		len(activated.HistoricalReplayExecution.DeliveryEventReplayWork) != 1 ||
		activated.HistoricalReplayExecution.DeliveryEventReplay == nil {
		t.Fatalf("HistoricalReplayExecution = %#v", activated.HistoricalReplayExecution)
	}
	if activated.DeliveryEventReplay == nil {
		t.Fatalf("DeliveryEventReplay = nil, want fork-local replay result: %#v", activated)
	}
	if activated.DeliveryEventReplay.Owner != RunForkDeliveryEventReplayOwner ||
		activated.DeliveryEventReplay.ReplayedEventCount != 1 ||
		activated.DeliveryEventReplay.ReplayedDeliveryCount != 1 {
		t.Fatalf("DeliveryEventReplay = %#v", activated.DeliveryEventReplay)
	}

	var forkEventID, forkRunID, eventName, forkEntityID, flowInstance, payload string
	var sourceEventNull bool
	var chainDepth int
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text, run_id::text, event_name, entity_id::text, COALESCE(flow_instance, ''), payload::text, source_event_id IS NULL, chain_depth
		FROM events
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkEventID, &forkRunID, &eventName, &forkEntityID, &flowInstance, &payload, &sourceEventNull, &chainDepth); err != nil {
		t.Fatalf("load fork replay event: %v", err)
	}
	if forkEventID == eventID || forkRunID != materialized.ForkRunID || eventName != "fork.ready" || forkEntityID != entityID || flowInstance != "flow-a/1" {
		t.Fatalf("fork event = id:%s run:%s name:%s entity:%s flow:%s", forkEventID, forkRunID, eventName, forkEntityID, flowInstance)
	}
	if !sourceEventNull || chainDepth != 0 {
		t.Fatalf("fork event source/chain = null:%v depth:%d, want fresh fork-local event tree", sourceEventNull, chainDepth)
	}
	if payload != "{}" {
		t.Fatalf("fork event payload = %s, want {}", payload)
	}

	var forkDeliveryID, deliveryRunID, deliveryEventID, subscriberType, subscriberID, status, reasonCode string
	var retryCount int
	var activeSessionNull, startedNull, deliveredNull bool
	if err := db.QueryRowContext(ctx, `
		SELECT delivery_id::text, run_id::text, event_id::text, subscriber_type, subscriber_id, status, retry_count,
		       COALESCE(reason_code, ''), active_session_id IS NULL, started_at IS NULL, delivered_at IS NULL
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'safe-agent'
	`, materialized.ForkRunID).Scan(&forkDeliveryID, &deliveryRunID, &deliveryEventID, &subscriberType, &subscriberID, &status, &retryCount, &reasonCode, &activeSessionNull, &startedNull, &deliveredNull); err != nil {
		t.Fatalf("load fork replay delivery: %v", err)
	}
	if deliveryRunID != materialized.ForkRunID || deliveryEventID != forkEventID || subscriberType != "agent" || subscriberID != "safe-agent" || status != "pending" || retryCount != 0 {
		t.Fatalf("fork delivery = run:%s event:%s subscriber:%s/%s status:%s retry:%d", deliveryRunID, deliveryEventID, subscriberType, subscriberID, status, retryCount)
	}
	if reasonCode != "fork_replay:matched_agent_subscription" || !activeSessionNull || !startedNull || !deliveredNull {
		t.Fatalf("fork delivery replay fields = reason:%q activeNull:%v startedNull:%v deliveredNull:%v", reasonCode, activeSessionNull, startedNull, deliveredNull)
	}
	var forkSessionCount, forkTurnCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkSessionCount); err != nil {
		t.Fatalf("count fork sessions: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkTurnCount); err != nil {
		t.Fatalf("count fork turns: %v", err)
	}
	if forkSessionCount != 0 || forkTurnCount != 0 {
		t.Fatalf("fork replay created session/turn rows = %d/%d, want 0/0", forkSessionCount, forkTurnCount)
	}

	var lineageCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_delivery_event_replays
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
		  AND source_delivery_id = $4::uuid
		  AND fork_event_id = $5::uuid
		  AND fork_delivery_id = $6::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'safe-agent'
	`, materialized.ForkRunID, sourceRunID, eventID, sourceDeliveryID, forkEventID, forkDeliveryID).Scan(&lineageCount); err != nil {
		t.Fatalf("count fork replay lineage: %v", err)
	}
	if lineageCount != 1 {
		t.Fatalf("lineage rows = %d, want 1", lineageCount)
	}

	var sourceDeliveryRun, sourceDeliveryStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, status
		FROM event_deliveries
		WHERE delivery_id = $1::uuid
	`, sourceDeliveryID).Scan(&sourceDeliveryRun, &sourceDeliveryStatus); err != nil {
		t.Fatalf("load source delivery after activation: %v", err)
	}
	if sourceDeliveryRun != sourceRunID || sourceDeliveryStatus != "pending" {
		t.Fatalf("source delivery mutated = run:%s status:%s", sourceDeliveryRun, sourceDeliveryStatus)
	}

	scope, err := pg.LoadCommittedReplayScope(ctx, forkEventID)
	if err != nil {
		t.Fatalf("LoadCommittedReplayScope(fork event): %v", err)
	}
	if scope != runtimereplayclaim.CommittedReplayScopeDirect {
		t.Fatalf("fork replay scope = %q, want direct", scope)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventID, "processed", nil); err != nil {
		t.Fatalf("mark source event pipeline receipt: %v", err)
	}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("safe-agent", events.EventType("fork.ready"))
	currentOnly := eb.Subscribe("current-only-agent", events.EventType("fork.ready"))
	replayed, err := eb.SweepUndispatched(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("SweepUndispatched replayed = %d, want 1", replayed)
	}
	select {
	case evt := <-ch:
		if evt.ID() != forkEventID || evt.RunID() != materialized.ForkRunID {
			t.Fatalf("delivered fork replay event = id:%s run:%s", evt.ID(), evt.RunID())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fork replay event delivery")
	}
	select {
	case evt := <-currentOnly:
		t.Fatalf("current-only subscription should not receive direct fork replay: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}
	var pipelineOutcome, pipelineReason string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, forkEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatalf("load fork replay pipeline receipt: %v", err)
	}
	if pipelineOutcome != "success" || pipelineReason != "pipeline_persisted" {
		t.Fatalf("fork replay pipeline receipt = outcome:%s reason:%s", pipelineOutcome, pipelineReason)
	}
}

func TestRunForkActivation_RejectsOwnerWorkOutsideCurrentSafePendingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		seed     func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string
		work     func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork
		wantText string
	}{
		{
			name: "stale missing delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return uuid.NewString()
			},
			work: func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork {
				item := req.PendingWork[0]
				item.DeliveryID = targetDeliveryID
				return []RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)}
			},
			wantText: "is not in current pending evidence",
		},
		{
			name: "foreign delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				foreignRunID := uuid.NewString()
				foreignEventID := uuid.NewString()
				if _, err := db.ExecContext(ctx, `
					INSERT INTO runs (run_id, status, started_at)
					VALUES ($1::uuid, 'running', $2)
				`, foreignRunID, at.Add(-time.Minute)); err != nil {
					t.Fatalf("seed foreign run: %v", err)
				}
				if _, err := db.ExecContext(ctx, `
					INSERT INTO events (
						run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
					)
					VALUES ($1::uuid, $2::uuid, 'foreign.ready', 'global', '{}'::jsonb, 'test', 'platform', $3)
				`, foreignRunID, foreignEventID, at); err != nil {
					t.Fatalf("seed foreign event: %v", err)
				}
				var deliveryID string
				if err := db.QueryRowContext(ctx, `
					INSERT INTO event_deliveries (
						run_id, event_id, subscriber_type, subscriber_id, status, retry_count, created_at
					)
					VALUES ($1::uuid, $2::uuid, 'agent', 'foreign-agent', 'pending', 0, $3)
					RETURNING delivery_id::text
				`, foreignRunID, foreignEventID, at).Scan(&deliveryID); err != nil {
					t.Fatalf("seed foreign delivery: %v", err)
				}
				return deliveryID
			},
			work: func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork {
				item := req.PendingWork[0]
				item.DeliveryID = targetDeliveryID
				return []RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)}
			},
			wantText: "is not in current pending evidence",
		},
		{
			name: "delivered delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				var deliveryID string
				if err := db.QueryRowContext(ctx, `
					INSERT INTO event_deliveries (
						run_id, event_id, subscriber_type, subscriber_id, status, retry_count, delivered_at, created_at
					)
					VALUES ($1::uuid, $2::uuid, 'agent', 'done-agent', 'delivered', 0, $3, $3)
					RETURNING delivery_id::text
				`, sourceRunID, eventID, at).Scan(&deliveryID); err != nil {
					t.Fatalf("seed delivered delivery: %v", err)
				}
				return deliveryID
			},
			work: func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork {
				return []RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkForDelivery(req, targetDeliveryID)}
			},
			wantText: "is not replayable pending agent work",
		},
		{
			name: "duplicate owner work",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return ""
			},
			work: func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork {
				item := runForkHistoricalReplayWorkFromPending(req.PendingWork[0])
				return []RunForkHistoricalReplayExecutableWork{item, item}
			},
			wantText: "duplicate source delivery",
		},
		{
			name: "subscriber mismatch",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return ""
			},
			work: func(req RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []RunForkHistoricalReplayExecutableWork {
				item := runForkHistoricalReplayWorkFromPending(req.PendingWork[0])
				item.SubscriberID = "wrong-agent"
				return []RunForkHistoricalReplayExecutableWork{item}
			},
			wantText: "does not exactly match current pending evidence",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			pg := &PostgresStore{DB: db}
			ctx := context.Background()

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700000875, 0).UTC()
			seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

			if _, err := db.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
				)
				VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'matched_agent_subscription', $3)
			`, sourceRunID, eventID, at); err != nil {
				t.Fatalf("seed safe pending delivery: %v", err)
			}
			targetDeliveryID := tc.seed(t, ctx, db, sourceRunID, eventID, at)
			materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
			if err != nil {
				t.Fatalf("MaterializeRunFork: %v", err)
			}

			admitter := &fakeRunForkHistoricalReplayExecutionAdmitter{
				work: func(req RunForkHistoricalReplayExecutionRequest) []RunForkHistoricalReplayExecutableWork {
					return tc.work(req, targetDeliveryID)
				},
			}
			blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{
				ForkRunID:                         materialized.ForkRunID,
				HistoricalReplayExecutionAdmitter: admitter,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("ActivateRunFork error = %v, want %q", err, tc.wantText)
			}
			if blocked.Activated || blocked.SourceFrozen {
				t.Fatalf("blocked activation mutated lifecycle: %#v", blocked)
			}
			assertRunForkActivationReplayMutationAbsent(t, db, sourceRunID, materialized.ForkRunID)
		})
	}
}

func TestRunForkDeliveryEventReplayValidationRejectsUnsafeCurrentEvidence(t *testing.T) {
	base := RunForkPendingWork{
		EventID:        uuid.NewString(),
		DeliveryID:     uuid.NewString(),
		SubscriberType: "agent",
		SubscriberID:   "safe-agent",
		Classification: RunForkPendingClassificationPending,
		Status:         "pending",
		RetryCount:     0,
		CreatedAt:      time.Unix(1700000880, 0).UTC(),
	}
	for _, tc := range []struct {
		name   string
		mutate func(*RunForkPendingWork)
	}{
		{
			name: "in-progress",
			mutate: func(item *RunForkPendingWork) {
				started := item.CreatedAt
				item.Status = "in_progress"
				item.ActiveSessionID = uuid.NewString()
				item.StartedAt = &started
				item.Classification = RunForkPendingClassificationInProgress
			},
		},
		{
			name: "non-agent",
			mutate: func(item *RunForkPendingWork) {
				item.SubscriberType = "node"
				item.SubscriberID = "node-worker"
			},
		},
		{
			name: "retry",
			mutate: func(item *RunForkPendingWork) {
				item.RetryCount = 1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := base
			tc.mutate(&item)
			err := validateRunForkDeliveryEventReplayWorkAgainstPlan(
				[]RunForkPendingWork{item},
				[]RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)},
			)
			if err == nil || !strings.Contains(err.Error(), "is not replayable pending agent work") {
				t.Fatalf("validateRunForkDeliveryEventReplayWorkAgainstPlan error = %v, want unsafe pending-agent rejection", err)
			}
		})
	}
}

func TestRunForkActivation_FailsClosedForSourceAdvancedAndRepeat(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
	blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "source_events_advanced_after_fork_point") {
		t.Fatalf("ActivateRunFork advanced source error = %v, want source advanced blocker", err)
	}
	if !blocked.SourceAdvancedAfterFork || !runForkTestHasActivationBlocker(blocked, "source_events_advanced_after_fork_point") {
		t.Fatalf("advanced-source activation result = %#v, want taxonomy-backed source advanced blocker", blocked)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, RunForkReplayResumeFactSourceAdvanced) {
		t.Fatalf("advanced-source taxonomy missing source-advanced disposition: %#v", blocked.ReplayResumeAdmission)
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

func TestRunForkActivation_FailsClosedForInProgressDeliveryAndMissingLineage(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, started_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'blocked-node', 'in_progress', $4::uuid, $3, $3)
	`, sourceRunID, eventID, at, uuid.NewString()); err != nil {
		t.Fatalf("seed in-progress delivery: %v", err)
	}
	blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("ActivateRunFork in-progress delivery error = %v, want non-agent delivery blocker", err)
	}
	if blocked.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner || !blocked.ReplayResumeAdmission.ReplayResumeFactsPresent {
		t.Fatalf("blocked activation taxonomy = %#v, want owner and historical replay required", blocked.ReplayResumeAdmission)
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

func TestRunForkActivation_FailsClosedForForkReplayStateWithTaxonomy(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	forkEventID := uuid.NewString()
	at := time.Unix(1700001050, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.replay_state', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, materialized.ForkRunID, forkEventID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed fork event: %v", err)
	}

	blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "fork_events_already_exist") {
		t.Fatalf("ActivateRunFork fork replay state error = %v, want fork event blocker", err)
	}
	if !runForkTestHasActivationBlocker(blocked, "fork_events_already_exist") {
		t.Fatalf("activation blockers = %#v, want fork_events_already_exist", blocked.UnsupportedBlockers)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, RunForkReplayResumeFactForkReplayState) {
		t.Fatalf("fork replay-state taxonomy missing disposition: %#v", blocked.ReplayResumeAdmission)
	}
}

func TestRunForkActivation_FailsClosedForForkSessionAndTurnReplayState(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seed      func(context.Context, *sql.DB, string, time.Time) error
		wantCode  string
		wantError string
	}{
		{
			name: "fork session",
			seed: func(ctx context.Context, db *sql.DB, forkRunID string, at time.Time) error {
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_sessions (
						session_id, run_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, 'agent-a', 'global', 'global', 'session', 'active', $3, $3)
				`, uuid.NewString(), forkRunID, at)
				return err
			},
			wantCode:  "fork_sessions_already_exist",
			wantError: "fork_sessions_already_exist",
		},
		{
			name: "fork conversation audit",
			seed: func(ctx context.Context, db *sql.DB, forkRunID string, at time.Time) error {
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_conversation_audits (
						session_id, run_id, agent_id, scope_key, scope, runtime_mode, runtime_state, status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, 'agent-task', 'global', 'global', 'task', '{}'::jsonb, 'active', $3, $3)
				`, uuid.NewString(), forkRunID, at)
				return err
			},
			wantCode:  "fork_conversation_audits_already_exist",
			wantError: "fork_conversation_audits_already_exist",
		},
		{
			name: "fork turn",
			seed: func(ctx context.Context, db *sql.DB, forkRunID string, at time.Time) error {
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_turns (
						turn_id, run_id, agent_id, session_id, runtime_mode, created_at
					)
					VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'session', $4)
				`, uuid.NewString(), forkRunID, uuid.NewString(), at)
				return err
			},
			wantCode:  "fork_turns_already_exist",
			wantError: "fork_turns_already_exist",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			pg := &PostgresStore{DB: db}
			ctx := context.Background()

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700001060, 0).UTC()
			seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
			materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
			if err != nil {
				t.Fatalf("MaterializeRunFork: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO agents (
					agent_id, role, model, llm_backend, conversation_mode, created_at
				)
				VALUES ('agent-a', 'worker', 'standard', 'mock', 'session', $1)
			`, at); err != nil {
				t.Fatalf("seed agent: %v", err)
			}
			if err := tc.seed(ctx, db, materialized.ForkRunID, at.Add(time.Second)); err != nil {
				t.Fatalf("seed %s: %v", tc.name, err)
			}

			blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("ActivateRunFork error = %v, want %s", err, tc.wantError)
			}
			if !runForkTestHasActivationBlocker(blocked, tc.wantCode) {
				t.Fatalf("activation blockers = %#v, want %s", blocked.UnsupportedBlockers, tc.wantCode)
			}
			if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, RunForkReplayResumeFactForkReplayState) {
				t.Fatalf("fork replay-state taxonomy missing disposition: %#v", blocked.ReplayResumeAdmission)
			}
		})
	}
}

func TestRunForkActivation_ToleratesOptionalLegacyReplaySchemas(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700001100, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS timers CASCADE`); err != nil {
		t.Fatalf("drop timers: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS routing_rules CASCADE`); err != nil {
		t.Fatalf("drop routing_rules: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	for name, ddl := range map[string]string{
		"timers":         `CREATE TABLE timers (timer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), entity_id UUID, flow_instance TEXT)`,
		"routing_rules":  `CREATE TABLE routing_rules (rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), flow_instance TEXT)`,
		"agent_sessions": `CREATE TABLE agent_sessions (session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
		"agent_turns":    `CREATE TABLE agent_turns (turn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_id UUID NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create legacy %s: %v", name, err)
		}
	}

	materialized, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork with optional legacy schemas: %v", err)
	}
	activated, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err != nil {
		t.Fatalf("ActivateRunFork with optional legacy schemas: %v", err)
	}
	if !activated.Activated || activated.ForkRunStatus != RunForkActivatedStatus || activated.SourceRunStatus != RunForkSourceFrozenStatus {
		t.Fatalf("activation result = %#v", activated)
	}
}

type sqlNullTime struct {
	Time  time.Time
	Valid bool
}

type fakeRunForkHistoricalReplayExecutionAdmitter struct {
	called  bool
	request RunForkHistoricalReplayExecutionRequest
	err     error
	work    func(RunForkHistoricalReplayExecutionRequest) []RunForkHistoricalReplayExecutableWork
}

func (a *fakeRunForkHistoricalReplayExecutionAdmitter) AdmitRunForkHistoricalReplayExecution(_ context.Context, req RunForkHistoricalReplayExecutionRequest) (RunForkHistoricalReplayExecution, error) {
	a.called = true
	a.request = req
	if a.err != nil {
		return RunForkHistoricalReplayExecution{}, a.err
	}
	deliveryEventReplayWork := []RunForkHistoricalReplayExecutableWork{
		runForkHistoricalReplayWorkFromPending(req.PendingWork[0]),
	}
	if a.work != nil {
		deliveryEventReplayWork = a.work(req)
	}
	return RunForkHistoricalReplayExecution{
		Owner:                      RunForkHistoricalReplayExecutionOwner,
		AdmissionOwner:             RunForkHistoricalReplayExecutionAdmissionOwner,
		ReplayResumeAdmissionOwner: req.ReplayResumeAdmission.Owner,
		ForkRunID:                  req.ForkRunID,
		SourceRunID:                req.SourceRunID,
		ForkEventID:                req.ForkEventID,
		ClosureLevel:               "canonical_owner_promotion_with_delivery_event_replay_ready_only",
		DeliveryEventReplayReady:   true,
		EventDeliveriesAdmission: RunForkHistoricalReplayFactAdmission{
			Fact:        RunForkHistoricalReplayFactEventDeliveries,
			Admission:   RunForkHistoricalReplayAdmissionExecutableForkWork,
			SourceOwner: RunForkReplayResumeAdmissionOwner,
			Message:     "test admission",
		},
		DeliveryEventReplayWork: deliveryEventReplayWork,
	}, nil
}

func runForkHistoricalReplayWorkFromPending(item RunForkPendingWork) RunForkHistoricalReplayExecutableWork {
	return RunForkHistoricalReplayExecutableWork{
		Fact:             RunForkHistoricalReplayFactEventDeliveries,
		SourceEventID:    item.EventID,
		SourceDeliveryID: item.DeliveryID,
		SubscriberType:   item.SubscriberType,
		SubscriberID:     item.SubscriberID,
		ReasonCode:       item.ReasonCode,
		Classification:   item.Classification,
	}
}

func runForkHistoricalReplayWorkForDelivery(req RunForkHistoricalReplayExecutionRequest, deliveryID string) RunForkHistoricalReplayExecutableWork {
	for _, item := range req.PendingWork {
		if item.DeliveryID == deliveryID {
			return runForkHistoricalReplayWorkFromPending(item)
		}
	}
	return RunForkHistoricalReplayExecutableWork{
		Fact:             RunForkHistoricalReplayFactEventDeliveries,
		SourceEventID:    req.PendingWork[0].EventID,
		SourceDeliveryID: deliveryID,
		SubscriberType:   req.PendingWork[0].SubscriberType,
		SubscriberID:     req.PendingWork[0].SubscriberID,
		ReasonCode:       req.PendingWork[0].ReasonCode,
		Classification:   req.PendingWork[0].Classification,
	}
}

func assertRunForkActivationReplayMutationAbsent(t *testing.T, db *sql.DB, sourceRunID, forkRunID string) {
	t.Helper()
	ctx := context.Background()
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status after blocked activation: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, forkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status after blocked activation: %v", err)
	}
	if sourceStatus != "running" || forkStatus != RunForkMaterializedStatus {
		t.Fatalf("blocked activation lifecycle = source:%s fork:%s, want running/%s", sourceStatus, forkStatus, RunForkMaterializedStatus)
	}
	for name, query := range map[string]string{
		"fork events":     `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`,
		"fork deliveries": `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`,
		"lineage rows":    `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, forkRunID).Scan(&count); err != nil {
			t.Fatalf("count %s after blocked activation: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s after blocked activation = %d, want 0", name, count)
		}
	}
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

func runForkTestHasActivationBlocker(result RunForkActivation, code string) bool {
	for _, blocker := range result.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}
