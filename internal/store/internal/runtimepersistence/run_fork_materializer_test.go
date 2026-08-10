package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestRunForkMaterializer_CreatesPausedForkRunAndSnapshotWithoutResuming(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	firstEventID := uuid.NewString()
	secondEventID := uuid.NewString()
	thirdEventID := uuid.NewString()
	at := time.Unix(1700000500, 0).UTC()
	fieldOnlyAt := at.Add(30 * time.Second)
	afterAt := at.Add(time.Minute)
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, firstEventID, sourceRunID, "fork.before", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'title', 'null'::jsonb, '"before-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'slug', 'null'::jsonb, '"before-slug"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Before Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'gates.ready', 'null'::jsonb, 'true'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'accumulator.score', 'null'::jsonb, '7'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, firstEventID, at); err != nil {
		t.Fatalf("seed first mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'before-slug', 'Before Name',
			'queued', '{"ready": true}'::jsonb, '{"title": "before-title", "slug": "before-slug", "name": "Before Name"}'::jsonb, '{"score": 7}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	seedPostgresSemanticEventRecordFixture(t, ctx, db, secondEventID, sourceRunID, "fork.field_only", events.EventProducerPlatform, "test", entityID, "", fieldOnlyAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'title', '"before-title"'::jsonb, '"fork-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'field-only', $4)
	`, sourceRunID, entityID, secondEventID, fieldOnlyAt); err != nil {
		t.Fatalf("seed selected mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET fields = jsonb_set(fields, '{title}', '"fork-title"'::jsonb, true),
		    revision = 2,
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, fieldOnlyAt); err != nil {
		t.Fatalf("update source state at selected event: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	seedPostgresSemanticEventRecordFixture(t, ctx, db, thirdEventID, sourceRunID, "fork.after", events.EventProducerPlatform, "test", entityID, "", afterAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', '"queued"'::jsonb, '"done"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
			($1::uuid, $2::uuid, 'title', '"fork-title"'::jsonb, '"after-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
			($1::uuid, $2::uuid, 'slug', '"before-slug"'::jsonb, '"after-slug"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
			($1::uuid, $2::uuid, 'name', '"Before Name"'::jsonb, '"After Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4)
	`, sourceRunID, entityID, thirdEventID, afterAt); err != nil {
		t.Fatalf("seed later mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET current_state = 'done',
		    slug = 'after-slug',
		    name = 'After Name',
		    fields = '{"title": "after-title", "slug": "after-slug", "name": "After Name"}'::jsonb,
		    accumulator = '{"score": 9}'::jsonb,
		    revision = 4,
		    entered_state_at = $3,
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("update later source state: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	result, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: secondEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if result.ForkRunID == "" {
		t.Fatal("ForkRunID is empty")
	}
	if result.ForkRunStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("ForkRunStatus = %q, want %q", result.ForkRunStatus, runfork.RunForkMaterializedStatus)
	}
	if !result.DeliveryResumeBlocked || !result.SourceRunStatusUnchanged {
		t.Fatalf("boundary flags = resume_blocked:%v source_unchanged:%v", result.DeliveryResumeBlocked, result.SourceRunStatusUnchanged)
	}
	if result.ReplayResumeAdmission.Owner != runfork.RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", result.ReplayResumeAdmission.Owner, runfork.RunForkReplayResumeAdmissionOwner)
	}
	if !result.ReplayResumeAdmission.StateOnlyExecutionReady || result.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("taxonomy flags = state_only:%v bounded_supported:%v, want true/false",
			result.ReplayResumeAdmission.StateOnlyExecutionReady,
			result.ReplayResumeAdmission.BoundedReplaySupported,
		)
	}

	var forkStatus, forkedFromRun, forkedFromEvent, forkBundleHash, forkBundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT status, forked_from_run_id::text, forked_from_event_id::text, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, result.ForkRunID).Scan(&forkStatus, &forkedFromRun, &forkedFromEvent, &forkBundleHash, &forkBundleSource); err != nil {
		t.Fatalf("load fork run: %v", err)
	}
	if forkStatus != "paused" || forkedFromRun != sourceRunID || forkedFromEvent != secondEventID {
		t.Fatalf("fork run = status:%s from:%s event:%s", forkStatus, forkedFromRun, forkedFromEvent)
	}
	if forkBundleHash != authorActivityTestBundleHash || forkBundleSource != storerunlifecycle.BundleSourceEphemeral {
		t.Fatalf("fork bundle identity = hash:%q source:%q, want inherited canonical identity", forkBundleHash, forkBundleSource)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	postEventID := uuid.NewString()
	at := time.Unix(1700000505, 0).UTC()
	afterAt := at.Add(time.Minute)
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.no_event_flow", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Fork Point Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed selected mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'state-flow/at-T', 'validation_case', 'Fork Point Name',
			'pending', '{}'::jsonb, '{"name": "Fork Point Name"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}

	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, postEventID, sourceRunID, "fork.post_flow", events.EventProducerPlatform, "test", entityID, "post-flow/ignored", afterAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', '"pending"'::jsonb, '"done"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4)
	`, sourceRunID, entityID, postEventID, afterAt); err != nil {
		t.Fatalf("seed later mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET name = 'Current Name',
		    current_state = 'done',
		    fields = '{"name": "Current Name"}'::jsonb,
		    revision = 2,
		    entered_state_at = $3,
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("update later source state: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
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
	if metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
		metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState ||
		metadata.FlowInstance != "state-flow/at-T" ||
		metadata.EntityType != "validation_case" {
		t.Fatalf("materialization metadata = %#v", metadata)
	}

	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
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
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000507, 0).UTC()
	afterAt := at.Add(time.Minute)
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.no_metadata", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations)
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

	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for missing metadata")
	}
	if !runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, runfork.RunForkBlockerEntitySnapshotMetadataUnproven)
	}
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID}); err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
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
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000508, 0).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute)})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.event_flow_only", events.EventProducerPlatform, "test", entityID, "event-flow/at-T", at)
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

	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for field-only entity_type authority")
	}
	if !runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, runfork.RunForkBlockerEntitySnapshotMetadataUnproven)
	}
}

func TestRunForkPlanner_FailsClosedWhenFieldEntityTypeConflictsWithSourceMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000509, 0).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute)})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.conflicting_entity_type", events.EventProducerPlatform, "test", entityID, "event-flow/at-T", at)
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

	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want false for conflicting entity_type authority")
	}
	if !runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, runfork.RunForkBlockerEntitySnapshotMetadataUnproven)
	}
}

func TestRunForkSelectedContractBinding_MaterializesDurableForkRunBinding(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000510, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)

	selection := runfork.RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/selected-contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
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
	if materialized.SelectedContractBinding.Owner != runfork.RunForkSelectedContractBindingOwner ||
		materialized.SelectedContractBinding.ForkRunID != materialized.ForkRunID ||
		materialized.SelectedContractBinding.SourceRunID != sourceRunID ||
		materialized.SelectedContractBinding.ForkEventID != eventID {
		t.Fatalf("materialized selected binding = %#v", materialized.SelectedContractBinding)
	}

	loaded, err := pg.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatalf("RequireRunForkSelectedContractBinding: %v", err)
	}
	if loaded.Owner != runfork.RunForkSelectedContractBindingOwner ||
		loaded.ContractSelection.ContractsRoot != selection.ContractsRoot ||
		loaded.ContractSelection.WorkflowName != selection.WorkflowName ||
		loaded.ContractSelection.WorkflowVersion != selection.WorkflowVersion {
		t.Fatalf("loaded selected binding = %#v", loaded)
	}

	activated, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if activated.SelectedContractBinding == nil ||
		activated.SelectedContractBinding.Owner != runfork.RunForkSelectedContractBindingOwner ||
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
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000515, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)

	targetHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	seedStoreTestPersistedBundle(t, db, targetHash)
	selection := runfork.RunForkContractSelection{
		Mode:            runfork.RunForkContractSelectionModeBundleHash,
		BundleHash:      targetHash,
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	targetSource, err := runtimecorrelation.NewPersistedBundleSourceFact(targetHash)
	if err != nil {
		t.Fatalf("NewPersistedBundleSourceFact: %v", err)
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		BundleSourceFact:  targetSource,
		ContractSelection: &selection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	loaded, err := pg.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatalf("RequireRunForkSelectedContractBinding: %v", err)
	}
	if loaded.ContractSelection.Mode != runfork.RunForkContractSelectionModeBundleHash ||
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
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	if _, err := pg.RequireRunForkSelectedContractBinding(ctx, uuid.NewString()); err == nil || !strings.Contains(err.Error(), "selected contract binding") {
		t.Fatalf("RequireRunForkSelectedContractBinding error = %v, want missing binding failure", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000520, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	invalidSelection := runfork.RunForkContractSelection{
		Mode:            "selected_contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		ContractSelection: &invalidSelection,
	}); err == nil || !strings.Contains(err.Error(), "contracts_root") {
		t.Fatalf("MaterializeRunFork invalid selection error = %v, want contracts_root failure", err)
	}

	validSelection := runfork.RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/selected-contracts",
		WorkflowName:    "selected-workflow",
		WorkflowVersion: "v2",
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
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

func TestRunForkMaterializer_ReplaysExactAndFailsClosedOnUnsupportedBlockers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	clearEventID := uuid.NewString()
	at := time.Unix(1700000600, 0).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	sourceEvent := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.pending", events.EventProducerPlatform, "test", entityID, "", at)
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
	seedDeliveryStateFixture(t, ctx, pg, sourceEvent, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("in-progress-node")}, runtimedelivery.StateLaunching, nil)
	captureRunForkTestRevision(t, db, sourceRunID)

	blocked, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork error = %v, want non-agent delivery blocker", err)
	}
	if blocked.ReplayResumeAdmission.Owner != runfork.RunForkReplayResumeAdmissionOwner || !blocked.ReplayResumeAdmission.ReplayResumeFactsPresent {
		t.Fatalf("blocked taxonomy = %#v, want owner and historical replay required", blocked.ReplayResumeAdmission)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, runfork.RunForkReplayResumeFactDeliveryInProgressHistory) {
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

	deletePostgresDeliveryFixturesForRun(t, ctx, db, sourceRunID)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, clearEventID, sourceRunID, "fork.delivery_cleared", events.EventProducerPlatform, "test", entityID, "", at.Add(time.Second))
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventDeliveries)
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID}); err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork original frontier after delete error = %v, want immutable non-agent delivery blocker", err)
	}
	first, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: clearEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork first: %v", err)
	}
	repeated, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: clearEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork exact replay: %v", err)
	}
	if !reflect.DeepEqual(repeated, first) {
		t.Fatalf("MaterializeRunFork exact replay = %#v, want %#v", repeated, first)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
		  AND forked_from_event_id = $2::uuid
	`, sourceRunID, clearEventID).Scan(&forkCount); err != nil {
		t.Fatalf("count fork rows after repeat: %v", err)
	}
	if forkCount != 1 {
		t.Fatalf("fork rows after repeat = %d, want 1", forkCount)
	}
	if first.ForkRunID == "" {
		t.Fatal("first ForkRunID is empty")
	}
	runlifecyclefixture.CorruptPostgresOrigin(
		t, ctx, db, first.ForkRunID, storerunlifecycle.ScenarioSetupRunOrigin(),
	)
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          clearEventID,
	}); err == nil || !strings.Contains(err.Error(), "conflicts with persisted lifecycle state") {
		t.Fatalf("MaterializeRunFork conflicting replay error = %v", err)
	}
}

func TestRunForkActivation_ActivatesMaterializedForkAndFreezesSource(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000800, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	forkOrigin, err := storerunlifecycle.ForkMaterializationRunOrigin(sourceRunID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	requireRunOriginHeader(t, ctx, pg, materialized.ForkRunID, forkOrigin, 0)
	requireListedRunOrigin(t, ctx, pg, materialized.ForkRunID, forkOrigin)
	activated, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if !activated.Activated || !activated.SourceFrozen {
		t.Fatalf("activation flags = activated:%v frozen:%v", activated.Activated, activated.SourceFrozen)
	}
	if !activated.ReplayResumeBlocked {
		t.Fatal("ReplayResumeBlocked = false, want true for activation-only boundary")
	}
	if activated.ReplayResumeAdmission.Owner != runfork.RunForkReplayResumeAdmissionOwner || !activated.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("activation taxonomy = %#v, want owner and state-only ready", activated.ReplayResumeAdmission)
	}
	if activated.SourceRunID != sourceRunID || activated.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation lineage = %#v", activated)
	}
	if activated.ForkRunStatus != runfork.RunForkActivatedStatus || activated.SourceRunStatus != runfork.RunForkSourceFrozenStatus {
		t.Fatalf("activation statuses = fork:%s source:%s", activated.ForkRunStatus, activated.SourceRunStatus)
	}
	requireRunOriginHeader(t, ctx, pg, materialized.ForkRunID, forkOrigin, 0)
	requireRunOriginHeader(t, ctx, pg, sourceRunID, storerunlifecycle.ScenarioSetupRunOrigin(), 1)

	later := eventtest.ExistingRunRootIngress(
		uuid.NewString(), "fork.after_activation", "fork-test", "", json.RawMessage(`{}`), 0,
		materialized.ForkRunID, events.EventEnvelope{}, at.Add(time.Minute),
	)
	if err := commitSemanticEventFixture(ctx, pg, later); err != nil {
		t.Fatalf("commit post-activation fork event: %v", err)
	}
	requireRunOriginHeader(t, ctx, pg, materialized.ForkRunID, forkOrigin, 1)

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
	if sourceStatus != runfork.RunForkSourceFrozenStatus || !sourceEndedAt.Valid {
		t.Fatalf("source status/ended_at = %s/%v, want forked/valid", sourceStatus, sourceEndedAt.Valid)
	}
	if forkStatus != runfork.RunForkActivatedStatus {
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

type runForkReplaySettlementFixture struct {
	ctx          context.Context
	db           *sql.DB
	store        *PostgresStore
	sourceRunID  string
	forkRunID    string
	sourceEvent  events.Event
	sourceRoute  events.DeliveryRoute
	sourceID     string
	deliveryID   string
	entityID     string
	materialized runfork.RunForkMaterialization
}

func newRunForkReplaySettlementFixture(t *testing.T) runForkReplaySettlementFixture {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	store := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	rootEventID := uuid.NewString()
	at := time.Unix(1700000840, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, rootEventID, at)

	sourceID := uuid.NewString()
	sourceEvent := eventtest.ChildWithLineage(
		sourceID,
		events.EventType("fork.ready"),
		"declarative-node",
		"event-owned-task",
		json.RawMessage(`{"task_id":"payload-owned-task","topic":"fork-ready"}`),
		1,
		events.EventLineage{
			RunID: sourceRunID, ParentEventID: rootEventID, TaskID: "event-owned-task", ExecutionMode: executionmode.Mock,
		},
		events.EventEnvelope{
			EntityID: entityID, FlowInstance: "flow-a/1", Scope: events.EventScopeEntity,
			Target: events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/1", EntityID: entityID},
		},
		at.Add(time.Second),
	)
	agentIdentity := runtimebustest.Identity(t, "safe-agent", "flow-a/1")
	sourceRoute := events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient(agentIdentity.AgentID()), AgentIdentity: agentIdentity,
		Target: events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/1", EntityID: entityID},
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, sourceEvent, []events.DeliveryRoute{sourceRoute}); err != nil {
		t.Fatalf("commit historical replay source event and delivery: %v", err)
	}
	var deliveryID string
	if err := db.QueryRowContext(ctx, `
		SELECT delivery_id::text
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'agent' AND subscriber_id = 'safe-agent'
	`, sourceID).Scan(&deliveryID); err != nil {
		t.Fatalf("load historical replay source delivery: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: sourceID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady || len(plan.PendingWork) != 1 {
		t.Fatalf("historical replay plan = %#v, want one executable pending delivery", plan)
	}
	materialized, err := store.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: sourceID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	return runForkReplaySettlementFixture{
		ctx: ctx, db: db, store: store, sourceRunID: sourceRunID, forkRunID: materialized.ForkRunID,
		sourceEvent: sourceEvent, sourceRoute: sourceRoute, sourceID: sourceID, deliveryID: deliveryID,
		entityID: entityID, materialized: materialized,
	}
}

func (f runForkReplaySettlementFixture) activate(t *testing.T, confirm bool) (runfork.RunForkActivation, error) {
	t.Helper()
	return f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{
		ForkRunID: f.forkRunID, ConfirmSourceFreeze: confirm,
		HistoricalReplayExecutionAdmitter: &fakeRunForkHistoricalReplayExecutionAdmitter{},
	})
}

func (f runForkReplaySettlementFixture) replaySnapshot(t *testing.T) (string, string, string, string) {
	t.Helper()
	var eventID, settlement, deliveryID, target string
	if err := f.db.QueryRowContext(f.ctx, `
		SELECT e.event_id::text, e.route_settlement::text, d.delivery_id::text, d.target_route::text
		FROM events e
		JOIN event_deliveries d ON d.event_id = e.event_id
		WHERE e.run_id = $1::uuid
	`, f.forkRunID).Scan(&eventID, &settlement, &deliveryID, &target); err != nil {
		t.Fatalf("load historical replay settlement snapshot: %v", err)
	}
	return eventID, settlement, deliveryID, target
}

func TestPostgresRunForkDeliveryEventReplayCommitsSettlementAtomically(t *testing.T) {
	fixture := newRunForkReplaySettlementFixture(t)
	result, err := fixture.activate(t, false)
	if err == nil || !strings.Contains(err.Error(), "confirmation") {
		t.Fatalf("ActivateRunFork without freeze confirmation = result:%#v err:%v", result, err)
	}
	if result.Activated || result.SourceFrozen {
		t.Fatalf("failed activation mutated lifecycle: %#v", result)
	}
	assertRunForkActivationReplayMutationAbsent(t, fixture.db, fixture.sourceRunID, fixture.forkRunID)
}

func TestPostgresRunForkDeliveryEventReplayExactDuplicatePreservesSettlement(t *testing.T) {
	fixture := newRunForkReplaySettlementFixture(t)
	result, err := fixture.activate(t, true)
	if err != nil || !result.Activated || result.DeliveryEventReplay == nil {
		t.Fatalf("ActivateRunFork = result:%#v err:%v", result, err)
	}
	beforeEvent, beforeSettlement, beforeDelivery, beforeTarget := fixture.replaySnapshot(t)
	repeated, err := fixture.activate(t, true)
	if err == nil || !strings.Contains(err.Error(), "requires materialized fork status") || !repeated.RepeatedActivationFailed {
		t.Fatalf("repeat ActivateRunFork = result:%#v err:%v", repeated, err)
	}
	afterEvent, afterSettlement, afterDelivery, afterTarget := fixture.replaySnapshot(t)
	if beforeEvent != afterEvent || beforeSettlement != afterSettlement || beforeDelivery != afterDelivery || beforeTarget != afterTarget {
		t.Fatalf("repeat activation changed replay settlement\nbefore: %s %s %s %s\nafter:  %s %s %s %s", beforeEvent, beforeSettlement, beforeDelivery, beforeTarget, afterEvent, afterSettlement, afterDelivery, afterTarget)
	}
	for name, query := range map[string]string{
		"events":     `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`,
		"deliveries": `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`,
		"lineage":    `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`,
	} {
		var count int
		if err := fixture.db.QueryRowContext(fixture.ctx, query, fixture.forkRunID).Scan(&count); err != nil {
			t.Fatalf("count replay %s: %v", name, err)
		}
		if count != 1 {
			t.Fatalf("replay %s after exact retry = %d, want 1", name, count)
		}
	}
}

func TestPostgresOperatorEventReadbackProjectsRunForkReplaySettlement(t *testing.T) {
	fixture := newRunForkReplaySettlementFixture(t)
	result, err := fixture.activate(t, true)
	if err != nil || !result.Activated {
		t.Fatalf("ActivateRunFork = result:%#v err:%v", result, err)
	}
	eventID, _, _, _ := fixture.replaySnapshot(t)
	full, err := fixture.store.LoadOperatorEvent(fixture.ctx, eventID)
	if err != nil {
		t.Fatalf("LoadOperatorEvent: %v", err)
	}
	if full.RunID != fixture.forkRunID || full.NoDelivery != nil || len(full.Deliveries) != 1 {
		t.Fatalf("operator replay settlement = %#v", full)
	}
	delivery := full.Deliveries[0]
	if delivery.SubscriberType != "agent" || delivery.SubscriberID != "safe-agent" || delivery.Status != "pending" || delivery.Terminal {
		t.Fatalf("operator replay delivery = %#v", delivery)
	}
	want := fixture.sourceRoute.Target.Normalized()
	if delivery.Target.FlowID != want.FlowID || delivery.Target.FlowInstance != want.FlowInstance || delivery.Target.EntityID != want.EntityID {
		t.Fatalf("operator replay target = %#v, want %#v", delivery.Target, want)
	}
}

func TestRunForkActivation_ReplaysSafePendingDeliveryWithForkLocalLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	rootEventID := uuid.NewString()
	at := time.Unix(1700000850, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, rootEventID, at)
	eventID := uuid.NewString()
	// Route-bearing replay is gated by historical route proof; this fixture isolates direct pending-delivery replay.
	sourceEnvelope := events.EventEnvelope{
		EntityID: entityID,
		Scope:    events.EventScopeEntity,
		Target:   events.RouteIdentity{EntityID: entityID},
	}
	sourceEvent := eventtest.ChildWithLineage(
		eventID,
		events.EventType("fork.ready"),
		"declarative-node",
		"event-owned-task",
		json.RawMessage(`{"task_id":"payload-owned-task","topic":"fork-ready"}`),
		3,
		events.EventLineage{
			RunID:         sourceRunID,
			ParentEventID: rootEventID,
			TaskID:        "event-owned-task",
			ExecutionMode: executionmode.Mock,
		},
		sourceEnvelope,
		at,
	)
	if err := insertPostgresCanonicalEventRecordFixture(ctx, db, sourceEvent); err != nil {
		t.Fatalf("seed complete historical replay source event: %v", err)
	}
	sourceRow, found, err := loadPostgresEventIdentity(ctx, db, eventID)
	if err != nil || !found {
		t.Fatalf("load complete historical replay source = found:%v err:%v", found, err)
	}
	sourceAdmitted, err := decodeEventRecord(sourceRow)
	if err != nil {
		t.Fatalf("decode complete historical replay source: %v", err)
	}
	sourceEvent = sourceAdmitted.Event()

	safeAgentIdentity := runtimebustest.Identity(t, "safe-agent", "")
	sourceDelivery := seedDeliveryStateFixture(t, ctx, pg, sourceEvent, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(safeAgentIdentity.AgentID()), AgentIdentity: safeAgentIdentity}, runtimedelivery.StateQueued, nil)
	sourceDeliveryID := sourceDelivery.DeliveryID
	captureRunForkTestRevision(t, db, sourceRunID)

	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		t.Fatalf("plan replay readiness = execution:%v admission:%#v", plan.ExecutionReady, plan.ReplayResumeAdmission)
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkHistoricalReplayExecutionOwner) {
		t.Fatalf("ActivateRunFork without historical replay owner error = %v, want %s", err, runfork.RunForkHistoricalReplayExecutionOwner)
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
	activated, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
		ForkRunID:                         materialized.ForkRunID,
		ConfirmSourceFreeze:               true,
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
		activated.HistoricalReplayExecution.Owner != runfork.RunForkHistoricalReplayExecutionOwner ||
		activated.HistoricalReplayExecution.AdmissionOwner != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		len(activated.HistoricalReplayExecution.DeliveryEventReplayWork) != 1 ||
		activated.HistoricalReplayExecution.DeliveryEventReplay == nil {
		t.Fatalf("HistoricalReplayExecution = %#v", activated.HistoricalReplayExecution)
	}
	if activated.DeliveryEventReplay == nil {
		t.Fatalf("DeliveryEventReplay = nil, want fork-local replay result: %#v", activated)
	}
	if activated.DeliveryEventReplay.Owner != runfork.RunForkDeliveryEventReplayOwner ||
		activated.DeliveryEventReplay.ReplayedEventCount != 1 ||
		activated.DeliveryEventReplay.ReplayedDeliveryCount != 1 {
		t.Fatalf("DeliveryEventReplay = %#v", activated.DeliveryEventReplay)
	}

	var forkEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text
		FROM events
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkEventID); err != nil {
		t.Fatalf("load fork replay event: %v", err)
	}
	forkRow, found, err := loadPostgresEventIdentity(ctx, db, forkEventID)
	if err != nil || !found {
		t.Fatalf("load canonical fork replay event = found:%v err:%v", found, err)
	}
	forkAdmitted, err := decodeEventRecord(forkRow)
	if err != nil {
		t.Fatalf("decode canonical fork replay event: %v", err)
	}
	forkEvent := forkAdmitted.Event()
	if forkEvent.ID() == sourceEvent.ID() || forkEvent.RunID() != materialized.ForkRunID || forkEvent.Type() != sourceEvent.Type() ||
		!forkEvent.Producer().Equal(sourceEvent.Producer()) || forkEvent.TaskID() != sourceEvent.TaskID() ||
		forkEvent.ExecutionMode() != sourceEvent.ExecutionMode() || forkEvent.ChainDepth() != 0 || forkEvent.ParentEventID() != "" ||
		!jsonSemanticallyEqual(forkEvent.Payload(), sourceEvent.Payload()) || !reflect.DeepEqual(forkEvent.Envelope(), sourceEvent.Envelope()) {
		t.Fatalf("complete historical replay projection changed\n source: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s payload=%s envelope=%#v\n replay: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s payload=%s envelope=%#v",
			sourceEvent.ID(), sourceEvent.Type(), sourceEvent.ProducerType(), sourceEvent.SourceAgent(), sourceEvent.TaskID(), sourceEvent.ChainDepth(), sourceEvent.RunID(), sourceEvent.ParentEventID(), sourceEvent.ExecutionMode(), sourceEvent.Payload(), sourceEvent.Envelope(),
			forkEvent.ID(), forkEvent.Type(), forkEvent.ProducerType(), forkEvent.SourceAgent(), forkEvent.TaskID(), forkEvent.ChainDepth(), forkEvent.RunID(), forkEvent.ParentEventID(), forkEvent.ExecutionMode(), forkEvent.Payload(), forkEvent.Envelope())
	}

	var forkDeliveryID, deliveryRunID, deliveryEventID, subscriberType, subscriberID, status, reasonCode string
	var retryCount int
	var activeSessionNull, startedNull, settledNull bool
	if err := db.QueryRowContext(ctx, `
		SELECT delivery_id::text, run_id::text, event_id::text, subscriber_type, subscriber_id, status, retry_count,
		       COALESCE(reason_code, ''), current_attempt_version IS NULL, started_at IS NULL, settled_at IS NULL
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'safe-agent'
	`, materialized.ForkRunID).Scan(&forkDeliveryID, &deliveryRunID, &deliveryEventID, &subscriberType, &subscriberID, &status, &retryCount, &reasonCode, &activeSessionNull, &startedNull, &settledNull); err != nil {
		t.Fatalf("load fork replay delivery: %v", err)
	}
	if deliveryRunID != materialized.ForkRunID || deliveryEventID != forkEventID || subscriberType != "agent" || subscriberID != "safe-agent" || status != "pending" || retryCount != 0 {
		t.Fatalf("fork delivery = run:%s event:%s subscriber:%s/%s status:%s retry:%d", deliveryRunID, deliveryEventID, subscriberType, subscriberID, status, retryCount)
	}
	if reasonCode != "" || !activeSessionNull || !startedNull || !settledNull {
		t.Fatalf("fork delivery replay fields = reason:%q activeNull:%v startedNull:%v settledNull:%v", reasonCode, activeSessionNull, startedNull, settledNull)
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

	var sourceDeliveryRun, sourceDeliveryStatus, sourceDeliveryReason string
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE delivery_id = $1::uuid
	`, sourceDeliveryID).Scan(&sourceDeliveryRun, &sourceDeliveryStatus, &sourceDeliveryReason); err != nil {
		t.Fatalf("load source delivery after activation: %v", err)
	}
	if sourceDeliveryRun != sourceRunID || sourceDeliveryStatus != "dead_letter" || sourceDeliveryReason != "run_forked" {
		t.Fatalf("source delivery terminalization = run:%s status:%s reason:%s", sourceDeliveryRun, sourceDeliveryStatus, sourceDeliveryReason)
	}

	var rawScope string
	if err := db.QueryRowContext(ctx, `SELECT scope FROM committed_replay_scopes WHERE event_id = $1::uuid`, forkEventID).Scan(&rawScope); err != nil {
		t.Fatalf("load committed pipeline scope for fork event: %v", err)
	}
	scope, err := runtimepipelineobligation.ParseCommittedScope(rawScope)
	if err != nil {
		t.Fatalf("parse committed pipeline scope for fork event: %v", err)
	}
	if scope != runtimepipelineobligation.ScopeDirect {
		t.Fatalf("fork replay scope = %q, want direct", scope)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, eventID); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
		t.Fatalf("post-freeze source pipeline receipt error = %v, want ErrIneligible", err)
	}
	eb, err := newStoreTestEventBus(t, pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	continuationOwner := runtimebustest.NewDeliveryContinuationOwner(false)
	proof, err := pg.ProveHandoff(
		ctx,
		forkEventID,
		events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(safeAgentIdentity.AgentID()), AgentIdentity: safeAgentIdentity},
	)
	if err != nil {
		t.Fatalf("prove fork replay delivery handoff: %v", err)
	}
	if err := continuationOwner.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatalf("accept fork replay delivery handoff: %v", err)
	}
	if err := eb.SetDeliveryContinuationOwner(continuationOwner); err != nil {
		t.Fatalf("install fork replay delivery continuation owner: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, "safe-agent", events.EventType("fork.ready"))
	currentOnly := runtimebustest.Subscribe(t, eb, "current-only-agent", events.EventType("fork.ready"))
	eventtestsql.CorruptEventStore(t, ctx, db, authoractivityfixture.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.exact_persistence",
		Reason:    "prove historical replay refuses a malformed durable route object",
	}, "", `UPDATE events SET target_route = $1::jsonb WHERE event_id = $2::uuid`, `"bad"`, forkEventID)
	if result, err := eb.SweepPipelineObligations(ctx, 10); err == nil || !strings.Contains(err.Error(), "target_route") {
		t.Fatalf("SweepPipelineObligations corrupt replay = count:%d err:%v, want target_route failure", result.Settled, err)
	}
	select {
	case evt := <-ch:
		t.Fatalf("corrupt historical replay dispatched: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}
	var corruptReceiptCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&corruptReceiptCount); err != nil {
		t.Fatalf("count corrupt historical replay receipts: %v", err)
	}
	if corruptReceiptCount != 0 {
		t.Fatalf("corrupt historical replay receipts = %d, want 0", corruptReceiptCount)
	}
	targetRoute, err := json.Marshal(forkEvent.Envelope().Target)
	if err != nil {
		t.Fatalf("marshal fork replay target route: %v", err)
	}
	eventtestsql.CorruptEventStore(t, ctx, db, authoractivityfixture.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.exact_persistence",
		Reason:    "restore the exact target route after the corruption proof",
	}, "", `UPDATE events SET target_route = $1::jsonb WHERE event_id = $2::uuid`, string(targetRoute), forkEventID)
	result, err := eb.SweepPipelineObligations(ctx, 10)
	if err != nil {
		t.Fatalf("SweepPipelineObligations: %v", err)
	}
	if result.Settled != 1 {
		t.Fatalf("SweepPipelineObligations replayed = %d, want 1", result.Settled)
	}
	select {
	case delivery := <-ch:
		evt := delivery.Event()
		_ = delivery.Complete()
		assertRunForkCompleteEventSnapshot(t, evt, forkEvent)
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

func assertRunForkCompleteEventSnapshot(t *testing.T, got, want events.Event) {
	t.Helper()
	if got.ID() != want.ID() || got.Type() != want.Type() || !got.Producer().Equal(want.Producer()) ||
		got.TaskID() != want.TaskID() || got.ChainDepth() != want.ChainDepth() || got.RunID() != want.RunID() ||
		got.ParentEventID() != want.ParentEventID() || got.ExecutionMode() != want.ExecutionMode() ||
		!got.CreatedAt().Truncate(time.Microsecond).Equal(want.CreatedAt().Truncate(time.Microsecond)) ||
		!jsonSemanticallyEqual(got.Payload(), want.Payload()) || !reflect.DeepEqual(got.Envelope(), want.Envelope()) {
		t.Fatalf("dispatched historical replay snapshot changed\n got: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v\nwant: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v",
			got.ID(), got.Type(), got.ProducerType(), got.SourceAgent(), got.TaskID(), got.ChainDepth(), got.RunID(), got.ParentEventID(), got.ExecutionMode(), got.CreatedAt(), got.Payload(), got.Envelope(),
			want.ID(), want.Type(), want.ProducerType(), want.SourceAgent(), want.TaskID(), want.ChainDepth(), want.RunID(), want.ParentEventID(), want.ExecutionMode(), want.CreatedAt(), want.Payload(), want.Envelope())
	}
}

func TestRunForkActivation_RejectsOwnerWorkOutsideCurrentSafePendingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		seed     func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string
		work     func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork
		wantText string
	}{
		{
			name: "stale missing delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return uuid.NewString()
			},
			work: func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork {
				item := req.PendingWork[0]
				item.DeliveryID = targetDeliveryID
				return []runfork.RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)}
			},
			wantText: "is not in current pending evidence",
		},
		{
			name: "foreign delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				foreignRunID := uuid.NewString()
				foreignEventID := uuid.NewString()
				requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: foreignRunID, StartedAt: at.Add(-time.Minute)})
				foreignEvent := seedPostgresSemanticEventRecordFixture(t, ctx, db, foreignEventID, foreignRunID, "foreign.ready", events.EventProducerPlatform, "test", "", "", at)
				return seedDeliveryStateFixture(t, ctx, postgresDeliveryFixtureStore(db), foreignEvent, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("foreign-agent")}, runtimedelivery.StateQueued, nil).DeliveryID
			},
			work: func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork {
				item := req.PendingWork[0]
				item.DeliveryID = targetDeliveryID
				return []runfork.RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)}
			},
			wantText: "is not in current pending evidence",
		},
		{
			name: "delivered delivery",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
				return seedDeliveryStateFixture(t, ctx, postgresDeliveryFixtureStore(db), event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("done-agent")}, runtimedelivery.StateDelivered, nil).DeliveryID
			},
			work: func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork {
				return []runfork.RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkForDelivery(req, targetDeliveryID)}
			},
			wantText: "is not replayable pending agent work",
		},
		{
			name: "duplicate owner work",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return ""
			},
			work: func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork {
				item := runForkHistoricalReplayWorkFromPending(req.PendingWork[0])
				return []runfork.RunForkHistoricalReplayExecutableWork{item, item}
			},
			wantText: "duplicate source delivery",
		},
		{
			name: "subscriber mismatch",
			seed: func(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, eventID string, at time.Time) string {
				t.Helper()
				return ""
			},
			work: func(req runfork.RunForkHistoricalReplayExecutionRequest, targetDeliveryID string) []runfork.RunForkHistoricalReplayExecutableWork {
				item := runForkHistoricalReplayWorkFromPending(req.PendingWork[0])
				item.SubscriberID = "wrong-agent"
				return []runfork.RunForkHistoricalReplayExecutableWork{item}
			},
			wantText: "does not exactly match current pending evidence",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := newTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700000875, 0).UTC()
			seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)

			sourceEvent := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
			seedDeliveryStateFixture(t, ctx, pg, sourceEvent, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("safe-agent")}, runtimedelivery.StateQueued, nil)
			targetDeliveryID := tc.seed(t, ctx, db, sourceRunID, eventID, at)
			captureRunForkTestRevision(t, db, sourceRunID)
			materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
			if err != nil {
				t.Fatalf("MaterializeRunFork: %v", err)
			}

			admitter := &fakeRunForkHistoricalReplayExecutionAdmitter{
				work: func(req runfork.RunForkHistoricalReplayExecutionRequest) []runfork.RunForkHistoricalReplayExecutableWork {
					return tc.work(req, targetDeliveryID)
				},
			}
			blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
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
	base := runfork.RunForkPendingWork{
		EventID:        uuid.NewString(),
		DeliveryID:     uuid.NewString(),
		SubscriberType: "agent",
		SubscriberID:   "safe-agent",
		Classification: runfork.RunForkPendingClassificationPending,
		Status:         "pending",
		RetryCount:     0,
		CreatedAt:      time.Unix(1700000880, 0).UTC(),
	}
	for _, tc := range []struct {
		name   string
		mutate func(*runfork.RunForkPendingWork)
	}{
		{
			name: "in-progress",
			mutate: func(item *runfork.RunForkPendingWork) {
				started := item.CreatedAt
				item.Status = "in_progress"
				item.ActiveSessionID = uuid.NewString()
				item.StartedAt = &started
				item.Classification = runfork.RunForkPendingClassificationInProgress
			},
		},
		{
			name: "non-agent",
			mutate: func(item *runfork.RunForkPendingWork) {
				item.SubscriberType = "node"
				item.SubscriberID = "node-worker"
			},
		},
		{
			name: "retry",
			mutate: func(item *runfork.RunForkPendingWork) {
				item.RetryCount = 1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := base
			tc.mutate(&item)
			err := validateRunForkDeliveryEventReplayWorkAgainstPlan(
				[]runfork.RunForkPendingWork{item},
				[]runfork.RunForkHistoricalReplayExecutableWork{runForkHistoricalReplayWorkFromPending(item)},
			)
			if err == nil || !strings.Contains(err.Error(), "is not replayable pending agent work") {
				t.Fatalf("validateRunForkDeliveryEventReplayWorkAgainstPlan error = %v, want unsafe pending-agent rejection", err)
			}
		})
	}
}

func TestRunForkActivation_IgnoresExcludedSourceSessionColumnChanges(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700000890, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	seedRunForkSessionProjection(t, db, sourceRunID, "generic-session-agent", sessionID, "terminated", at)
	selectedRevision := captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	mutateRunForkSessionExcludedColumns(t, db, sourceRunID, sessionID, at.Add(time.Minute))
	var afterExcluded int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, sourceRunID).Scan(&afterExcluded); err != nil {
		t.Fatalf("load source revision after excluded session update: %v", err)
	}
	if afterExcluded != selectedRevision {
		t.Fatalf("source revision after excluded session update = %d, want %d", afterExcluded, selectedRevision)
	}

	activation, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true})
	if err != nil {
		t.Fatalf("ActivateRunFork after excluded session update: %v", err)
	}
	if !activation.Activated || activation.SourceAdvancedAfterFork || runForkTestHasActivationBlocker(activation, "source_sessions_advanced_after_fork_point") {
		t.Fatalf("activation = %#v, want activation without session advancement", activation)
	}
}

func TestRunForkActivation_FailsClosedForSourceAdvancedAndRepeat(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700000900, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, afterEventID, sourceRunID, "fork.after", events.EventProducerPlatform, "test", entityID, "flow-a/1", at.Add(time.Second))
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents)
	blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "source_events_advanced_after_fork_point") {
		t.Fatalf("ActivateRunFork advanced source error = %v, want source advanced blocker", err)
	}
	if !blocked.SourceAdvancedAfterFork || !runForkTestHasActivationBlocker(blocked, "source_events_advanced_after_fork_point") {
		t.Fatalf("advanced-source activation result = %#v, want taxonomy-backed source advanced blocker", blocked)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, runfork.RunForkReplayResumeFactSourceAdvanced) {
		t.Fatalf("advanced-source taxonomy missing source-advanced disposition: %#v", blocked.ReplayResumeAdmission)
	}
	var forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status after blocked activation: %v", err)
	}
	if forkStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("fork status after blocked activation = %q, want paused", forkStatus)
	}

	cleanSourceRunID := uuid.NewString()
	cleanEntityID := uuid.NewString()
	cleanEventID := uuid.NewString()
	seedActivationReadySourceRun(t, db, cleanSourceRunID, cleanEntityID, cleanEventID, at.Add(time.Minute))
	captureRunForkTestRevision(t, db, cleanSourceRunID)
	cleanMaterialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: cleanSourceRunID, At: cleanEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork clean: %v", err)
	}
	if _, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: cleanMaterialized.ForkRunID, ConfirmSourceFreeze: true}); err != nil {
		t.Fatalf("ActivateRunFork clean: %v", err)
	}
	_, err = pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: cleanMaterialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "requires materialized fork status") {
		t.Fatalf("ActivateRunFork repeat error = %v, want materialized-status failure", err)
	}
}

func TestRunForkActivation_FailsClosedForDeliveryAdvancementAndUsesTypedOriginLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700001000, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	sourceEvent := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	seedDeliveryStateFixture(t, ctx, pg, sourceEvent, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("blocked-node")}, runtimedelivery.StateLaunching, nil)
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEventDeliveries)
	blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "source_deliveries_advanced_after_fork_point") {
		t.Fatalf("ActivateRunFork delivery advancement error = %v, want source delivery advancement blocker", err)
	}
	if !blocked.SourceAdvancedAfterFork || !runForkTestHasActivationBlocker(blocked, "source_deliveries_advanced_after_fork_point") {
		t.Fatalf("blocked activation = %#v, want source delivery advancement", blocked)
	}

	orphanSourceRunID := uuid.NewString()
	orphanEntityID := uuid.NewString()
	orphanEventID := uuid.NewString()
	seedActivationReadySourceRun(t, db, orphanSourceRunID, orphanEntityID, orphanEventID, at.Add(time.Minute))
	captureRunForkTestRevision(t, db, orphanSourceRunID)
	orphanRunID := uuid.NewString()
	orphanOrigin, err := storerunlifecycle.ForkMaterializationRunOrigin(orphanSourceRunID, orphanEventID)
	if err != nil {
		t.Fatalf("construct orphan fork origin: %v", err)
	}
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{
		Origin:    orphanOrigin,
		RunID:     orphanRunID,
		State:     storerunlifecycle.StatePaused,
		StartedAt: at,
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Orphan Fork Entity',
			'pending', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, orphanRunID, orphanEntityID, at); err != nil {
		t.Fatalf("seed orphan fork entity_state: %v", err)
	}
	activated, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: orphanRunID, ConfirmSourceFreeze: true})
	if err != nil {
		t.Fatalf("ActivateRunFork typed-origin lineage: %v", err)
	}
	if !activated.Activated || activated.SourceRunID != orphanSourceRunID || activated.ForkPoint.EventID != orphanEventID {
		t.Fatalf("typed-origin activation = %#v, want source %s event %s", activated, orphanSourceRunID, orphanEventID)
	}
}

func TestRunForkActivation_FailsClosedForForkReplayStateWithTaxonomy(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	forkEventID := uuid.NewString()
	at := time.Unix(1700001050, 0).UTC()
	seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkEventID, materialized.ForkRunID, "fork.replay_state", events.EventProducerPlatform, "test", "", "", at.Add(time.Second))

	blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "fork_events_already_exist") {
		t.Fatalf("ActivateRunFork fork replay state error = %v, want fork event blocker", err)
	}
	if !runForkTestHasActivationBlocker(blocked, "fork_events_already_exist") {
		t.Fatalf("activation blockers = %#v, want fork_events_already_exist", blocked.UnsupportedBlockers)
	}
	if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, runfork.RunForkReplayResumeFactForkReplayState) {
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
				fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, "agent-a", "fork-state"))
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_sessions (
						session_id, run_id, agent_id, agent_name_owner, agent_name_source,
						agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
						memory_enabled, memory_source, status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', 'active', $10, $10)
				`, uuid.NewString(), forkRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, at)
				return err
			},
			wantCode:  "fork_sessions_already_exist",
			wantError: "fork_sessions_already_exist",
		},
		{
			name: "fork conversation audit",
			seed: func(ctx context.Context, db *sql.DB, forkRunID string, at time.Time) error {
				fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, "agent-task", "fork-state"))
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_conversation_audits (
						session_id, run_id, agent_id, agent_name_owner, agent_name_source,
						agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
						memory_enabled, memory_source, runtime_state, status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, FALSE, 'authored', '{}'::jsonb, 'active', $10, $10)
				`, uuid.NewString(), forkRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, at)
				return err
			},
			wantCode:  "fork_conversation_audits_already_exist",
			wantError: "fork_conversation_audits_already_exist",
		},
		{
			name: "fork turn",
			seed: func(ctx context.Context, db *sql.DB, forkRunID string, at time.Time) error {
				turnID := uuid.NewString()
				sessionID := uuid.NewString()
				identity := testAgentIdentity(t, "agent-a", "fork-state")
				fields := testAgentIdentityStorageFields(t, identity)
				capabilitySurfaceID := seedManagedAgentTurnCapabilitySurface(
					t, admitTestPostgresStore(t, db), forkRunID, identity,
					sessionID, turnID, "session", "global",
				)
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_turns (
						turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
						agent_route_presence, flow_scope_key, flow_instance_id,
						session_id, flow_instance, memory_enabled, memory_source,
						capability_surface_id, execution_mode, created_at
					)
					VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
						$9::uuid, $10, FALSE, 'platform_default', $11::uuid, 'live', $12)
				`, turnID, forkRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, sessionID,
					fields.FlowInstancePath, capabilitySurfaceID, at)
				return err
			},
			wantCode:  "fork_turns_already_exist",
			wantError: "fork_turns_already_exist",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := newTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700001060, 0).UTC()
			seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
			captureRunForkTestRevision(t, db, sourceRunID)
			materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
			if err != nil {
				t.Fatalf("MaterializeRunFork: %v", err)
			}
			seedTestAgentRow(t, ctx, db, true, testAgentIdentity(t, "agent-a", "fork-state"), "active")
			if err := tc.seed(ctx, db, materialized.ForkRunID, at.Add(time.Second)); err != nil {
				t.Fatalf("seed %s: %v", tc.name, err)
			}

			blocked, err := pg.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("ActivateRunFork error = %v, want %s", err, tc.wantError)
			}
			if !runForkTestHasActivationBlocker(blocked, tc.wantCode) {
				t.Fatalf("activation blockers = %#v, want %s", blocked.UnsupportedBlockers, tc.wantCode)
			}
			if !runForkTestHasDisposition(blocked.ReplayResumeAdmission, runfork.RunForkReplayResumeFactForkReplayState) {
				t.Fatalf("fork replay-state taxonomy missing disposition: %#v", blocked.ReplayResumeAdmission)
			}
		})
	}
}

type sqlNullTime struct {
	Time  time.Time
	Valid bool
}

type fakeRunForkHistoricalReplayExecutionAdmitter struct {
	called  bool
	request runfork.RunForkHistoricalReplayExecutionRequest
	err     error
	work    func(runfork.RunForkHistoricalReplayExecutionRequest) []runfork.RunForkHistoricalReplayExecutableWork
}

func (a *fakeRunForkHistoricalReplayExecutionAdmitter) AdmitRunForkHistoricalReplayExecution(_ context.Context, req runfork.RunForkHistoricalReplayExecutionRequest) (runfork.RunForkHistoricalReplayExecution, error) {
	a.called = true
	a.request = req
	if a.err != nil {
		return runfork.RunForkHistoricalReplayExecution{}, a.err
	}
	deliveryEventReplayWork := []runfork.RunForkHistoricalReplayExecutableWork{
		runForkHistoricalReplayWorkFromPending(req.PendingWork[0]),
	}
	if a.work != nil {
		deliveryEventReplayWork = a.work(req)
	}
	return runfork.RunForkHistoricalReplayExecution{
		Owner:                      runfork.RunForkHistoricalReplayExecutionOwner,
		AdmissionOwner:             runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
		ReplayResumeAdmissionOwner: req.ReplayResumeAdmission.Owner,
		ForkRunID:                  req.ForkRunID,
		SourceRunID:                req.SourceRunID,
		ForkEventID:                req.ForkEventID,
		ClosureLevel:               "canonical_owner_promotion_with_delivery_event_replay_ready_only",
		DeliveryEventReplayReady:   true,
		EventDeliveriesAdmission: runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   runfork.RunForkHistoricalReplayAdmissionExecutableForkWork,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			Message:     "test admission",
		},
		DeliveryEventReplayWork: deliveryEventReplayWork,
	}, nil
}

func runForkHistoricalReplayWorkFromPending(item runfork.RunForkPendingWork) runfork.RunForkHistoricalReplayExecutableWork {
	return runfork.RunForkHistoricalReplayExecutableWork{
		Fact:             runfork.RunForkHistoricalReplayFactEventDeliveries,
		SourceEventID:    item.EventID,
		SourceDeliveryID: item.DeliveryID,
		SubscriberType:   item.SubscriberType,
		SubscriberID:     item.SubscriberID,
		ReasonCode:       item.ReasonCode,
		Classification:   item.Classification,
	}
}

func runForkHistoricalReplayWorkForDelivery(req runfork.RunForkHistoricalReplayExecutionRequest, deliveryID string) runfork.RunForkHistoricalReplayExecutableWork {
	for _, item := range req.PendingWork {
		if item.DeliveryID == deliveryID {
			return runForkHistoricalReplayWorkFromPending(item)
		}
	}
	return runfork.RunForkHistoricalReplayExecutableWork{
		Fact:             runfork.RunForkHistoricalReplayFactEventDeliveries,
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
	ctx := testAuthorActivityContext()
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status after blocked activation: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, forkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status after blocked activation: %v", err)
	}
	if sourceStatus != "running" || forkStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("blocked activation lifecycle = source:%s fork:%s, want running/%s", sourceStatus, forkStatus, runfork.RunForkMaterializedStatus)
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

func seedActivationReadySourceRun(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := testAuthorActivityContext()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.ready", events.EventProducerPlatform, "test", entityID, "", at)
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

func runForkTestHasActivationBlocker(result runfork.RunForkActivation, code string) bool {
	for _, blocker := range result.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}
