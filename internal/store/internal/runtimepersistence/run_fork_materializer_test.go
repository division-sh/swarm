package runtimepersistence

import (
	"bytes"
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

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"

	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestRunForkProfileInheritanceRequiresExactEffectiveSourceBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000490, 0).UTC()
	seedActivationReadySourceRun(t, ctx, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("test bundle source fact is missing")
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "derived:flow-a/input", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := scenarioexecutionpersistence.EnsurePostgres(ctx, tx, sourceRunID, profile, at); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	different, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: sourceRunID, At: eventID, EffectiveSourceIdentity: different, RootAdmission: rooted.Admission(),
	}); err == nil || !strings.Contains(err.Error(), "effective source mismatch") {
		t.Fatalf("different effective source fork error = %v", err)
	}
	var forkRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forkRows); err != nil {
		t.Fatal(err)
	}
	if forkRows != 0 {
		t.Fatalf("fork rows after rejected effective source = %d, want 0", forkRows)
	}

	request := runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID, EffectiveSourceIdentity: identity, RootAdmission: rooted.Admission()}
	materialized, err := pg.MaterializeRunFork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	inherited, found, err := pg.LoadScenarioExecutionProfile(ctx, materialized.ForkRunID)
	if err != nil || !found {
		t.Fatalf("load inherited profile: found=%t err=%v", found, err)
	}
	if inherited.Digest() != profile.Digest() || !bytes.Equal(inherited.CanonicalBytes(), profile.CanonicalBytes()) {
		t.Fatal("fork did not inherit exact scenario execution profile bytes")
	}
	if _, err := pg.RequireMaterializedRunFork(ctx, materialized.ForkRunID, request); err != nil {
		t.Fatalf("exact fork replay: %v", err)
	}
}

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
	creating := semanticEventRecordFixture(firstEventID, sourceRunID, "fork.before", eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{"gates":{"review":"authored-review"},"accumulator":{"total":"authored-total"},"bookkeeping":{"activation":"manual"}}`), semanticEventRecordFixtureEnvelope(entityID, ""), at)
	native := prepareFixedForkHistoryFixture(t, pg, "materializer-state", sourceRunID, entityID, creating)
	ctx = native.ctx
	persistFixedForkHistoryFixture(t, native)
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	seedPostgresSemanticEventRecordFixture(t, ctx, db, secondEventID, sourceRunID, "fork.field_only", events.EventProducerPlatform, "test", entityID, "", fieldOnlyAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
			VALUES
				($1::uuid, $2::uuid, 'authored_field', 'title', '"before-title"'::jsonb, '"fork-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'field-only', $4),
				($1::uuid, $2::uuid, 'authored_field', 'current_state', 'null'::jsonb, '"business-state"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'field-only', $4)
	`, sourceRunID, entityID, secondEventID, fieldOnlyAt); err != nil {
		t.Fatalf("seed selected mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
			SET fields = jsonb_set(jsonb_set(fields, '{title}', '"fork-title"'::jsonb, true), '{current_state}', '"business-state"'::jsonb, true),
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, fieldOnlyAt); err != nil {
		t.Fatalf("update source state at selected event: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	seedPostgresSemanticEventRecordFixture(t, ctx, db, thirdEventID, sourceRunID, "fork.after", events.EventProducerPlatform, "test", entityID, "", afterAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'lifecycle_state', '', '"queued"'::jsonb, '"done"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
			($1::uuid, $2::uuid, 'authored_field', 'title', '"fork-title"'::jsonb, '"after-title"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
				($1::uuid, $2::uuid, 'authored_field', 'slug', '"before-slug"'::jsonb, '"after-slug"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4),
			($1::uuid, $2::uuid, 'authored_field', 'name', '"Before Name"'::jsonb, '"After Name"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4)
	`, sourceRunID, entityID, thirdEventID, afterAt); err != nil {
		t.Fatalf("seed later mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET slug = 'after-slug',
		    name = 'After Name',
		    fields = '{"title": "after-title", "slug": "after-slug", "name": "After Name"}'::jsonb,
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("update later source state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE flow_instances SET current_state='done',slug='after-slug',name='After Name',accumulator='{"score":9}'::jsonb,revision=4,entered_state_at=$3,updated_at=$3 WHERE run_id=$1 AND entity_id=$2`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatal(err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: secondEventID, RootAdmission: rooted.Admission()})
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

	var forkStatus, forkedFromRun, forkedFromEvent, forkBundleHash string
	if err := db.QueryRowContext(ctx, `
		SELECT status, forked_from_run_id::text, forked_from_event_id::text, bundle_hash
		FROM runs
		WHERE run_id = $1::uuid
	`, result.ForkRunID).Scan(&forkStatus, &forkedFromRun, &forkedFromEvent, &forkBundleHash); err != nil {
		t.Fatalf("load fork run: %v", err)
	}
	if forkStatus != "paused" || forkedFromRun != sourceRunID || forkedFromEvent != secondEventID {
		t.Fatalf("fork run = status:%s from:%s event:%s", forkStatus, forkedFromRun, forkedFromEvent)
	}
	if forkBundleHash != native.construction.Child.Readiness.BundleHash {
		t.Fatalf("fork bundle identity = hash:%q, want inherited canonical identity", forkBundleHash)
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
		SELECT f.current_state, e.fields->>'title', f.revision
		FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.entity_id=e.entity_id AND f.instance_path=e.flow_instance AND f.entity_type=e.entity_type
		WHERE e.run_id = $1::uuid AND e.entity_id = $2::uuid
	`, sourceRunID, entityID).Scan(&sourceState, &sourceTitle, &sourceRevision); err != nil {
		t.Fatalf("load source entity_state: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT f.current_state, e.fields->>'title', f.revision, f.entered_state_at
		FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.entity_id=e.entity_id AND f.instance_path=e.flow_instance AND f.entity_type=e.entity_type
		WHERE e.run_id = $1::uuid AND e.entity_id = $2::uuid
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
	var forkFieldsJSON, forkBookkeepingJSON, forkGatesJSON, forkAccumulatorJSON []byte
	if err := db.QueryRowContext(ctx, `
			SELECT e.fields, f.bookkeeping, f.gates, f.accumulator
			FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.entity_id=e.entity_id AND f.instance_path=e.flow_instance AND f.entity_type=e.entity_type
			WHERE e.run_id = $1::uuid AND e.entity_id = $2::uuid
		`, result.ForkRunID, entityID).Scan(&forkFieldsJSON, &forkBookkeepingJSON, &forkGatesJSON, &forkAccumulatorJSON); err != nil {
		t.Fatalf("load fork semantic domains: %v", err)
	}
	var forkFields, forkBookkeeping, forkGates, forkAccumulator map[string]any
	for label, item := range map[string]struct {
		raw    []byte
		target *map[string]any
	}{
		"fields":      {forkFieldsJSON, &forkFields},
		"bookkeeping": {forkBookkeepingJSON, &forkBookkeeping},
		"gates":       {forkGatesJSON, &forkGates},
		"accumulator": {forkAccumulatorJSON, &forkAccumulator},
	} {
		if err := json.Unmarshal(item.raw, item.target); err != nil {
			t.Fatalf("decode fork %s: %v", label, err)
		}
	}
	if forkFields["current_state"] != "business-state" ||
		runtimeProjectionNestedValue(t, forkFields, "gates", "review") != "authored-review" ||
		runtimeProjectionNestedValue(t, forkFields, "accumulator", "total") != "authored-total" ||
		runtimeProjectionNestedValue(t, forkFields, "bookkeeping", "activation") != "manual" {
		t.Fatalf("fork authored collision fields = %#v", forkFields)
	}
	if forkBookkeeping["activation"] != "standing" || forkGates["review"] != true || forkAccumulator["total"] != float64(7) {
		t.Fatalf("fork typed semantic domains = bookkeeping:%#v gates:%#v accumulator:%#v", forkBookkeeping, forkGates, forkAccumulator)
	}
	var forkFlow, forkType, forkSlug, forkName string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, COALESCE(slug, ''), COALESCE(name, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkFlow, &forkType, &forkSlug, &forkName); err != nil {
		t.Fatalf("load fork display metadata: %v", err)
	}
	if forkFlow != "flow-a/1" || forkType != "state" {
		t.Fatalf("fork owner metadata = flow:%s type:%s, want flow-a/1/state", forkFlow, forkType)
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

	for _, mutation := range []struct{ domain, path string }{
		{"lifecycle_state", ""},
		{"authored_field", "title"},
		{"authored_field", "slug"},
		{"authored_field", "name"},
		{"authored_field", "current_state"},
		{"authored_field", "gates"},
		{"authored_field", "accumulator"},
		{"authored_field", "bookkeeping"},
		{"bookkeeping", "activation"},
		{"gate", "review"},
		{"accumulator", "total"},
	} {
		var count int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND domain = $3
			  AND path = $4
			  AND writer_type = 'platform'
			  AND writer_id = 'run_fork_materializer'
		`, result.ForkRunID, entityID, mutation.domain, mutation.path).Scan(&count); err != nil {
			t.Fatalf("count mutation %s:%s: %v", mutation.domain, mutation.path, err)
		}
		if count != 1 {
			t.Fatalf("mutation count for %s:%s = %d, want 1", mutation.domain, mutation.path, count)
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

func runtimeProjectionNestedValue(t *testing.T, root map[string]any, objectKey, valueKey string) any {
	t.Helper()
	object, ok := root[objectKey].(map[string]any)
	if !ok {
		t.Fatalf("projection %s = %#v, want object", objectKey, root[objectKey])
	}
	return object[valueKey]
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
	creating := semanticEventRecordFixture(eventID, sourceRunID, "fork.no_event_flow", eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`), semanticEventRecordFixtureEnvelope(entityID, ""), at)
	native := prepareFixedForkHistoryFixture(t, pg, "source-metadata", sourceRunID, entityID, creating)
	ctx = native.ctx
	persistFixedForkHistoryFixture(t, native)

	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, postEventID, sourceRunID, "fork.post_flow", events.EventProducerPlatform, "test", entityID, "post-flow/ignored", afterAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'lifecycle_state', '', '"pending"'::jsonb, '"done"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'after', $4)
	`, sourceRunID, entityID, postEventID, afterAt); err != nil {
		t.Fatalf("seed later mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET name = 'Current Name',
		    fields = '{"name": "Current Name"}'::jsonb,
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatalf("update later source state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE flow_instances SET name='Current Name',current_state='done',revision=2,entered_state_at=$3,updated_at=$3 WHERE run_id=$1 AND entity_id=$2`, sourceRunID, entityID, afterAt); err != nil {
		t.Fatal(err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations, runforkrevision.FamilyEntityMetadata)

	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, blockers=%#v", plan.UnsupportedBlockers)
	}
	metadata := fixedForkBusinessEntity(t, plan, native).MaterializationMetadata
	if metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
		metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance ||
		metadata.FlowInstance != "state-flow/at-T" ||
		metadata.EntityType != "state" {
		t.Fatalf("materialization metadata = %#v", metadata)
	}

	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID, RootAdmission: rooted.Admission()})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	var flowInstance, entityType, state, name string
	if err := db.QueryRowContext(ctx, `
		SELECT e.flow_instance, e.entity_type, f.current_state, COALESCE(e.name, '')
		FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.entity_id=e.entity_id AND f.instance_path=e.flow_instance AND f.entity_type=e.entity_type
		WHERE e.run_id = $1::uuid AND e.entity_id = $2::uuid
	`, materialized.ForkRunID, entityID).Scan(&flowInstance, &entityType, &state, &name); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if flowInstance != "state-flow/at-T" || entityType != "state" || state != "pending" || name != "Fork Point Name" {
		t.Fatalf("fork snapshot = flow:%s type:%s state:%s name:%s", flowInstance, entityType, state, name)
	}
}

func TestRunForkPlanner_FailsClosedWithoutSourceAtTEntitySnapshotMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000507, 0).UTC()
	afterAt := at.Add(time.Minute)
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.no_metadata", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'lifecycle_state', '', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations)
	seedWorkflowHeaderProjectionFixture(t, ctx, db, sourceRunID, entityID, "post-state-flow", "post-state-flow", "default", "pending", "{}", afterAt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, fields, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'post-state-flow', 'default',
			'{}'::jsonb, $3, $3
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
	ctx = originalStoreFixtureContext(t, ctx, pg)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000508, 0).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute)})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.event_flow_only", events.EventProducerPlatform, "test", entityID, "event-flow/at-T", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'lifecycle_state', '', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4),
			($1::uuid, $2::uuid, 'authored_field', 'entity_type', 'null'::jsonb, '"field_case"'::jsonb, $3::uuid, 'platform', 'materializer-test', 'before', $4)
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

func TestRunForkPlanner_TypedSourceMetadataWinsOverAuthoredEntityTypeCollision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000509, 0).UTC()
	creating := semanticEventRecordFixture(eventID, sourceRunID, "fork.conflicting_entity_type", eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`), semanticEventRecordFixtureEnvelope(entityID, "event-flow/at-T"), at)
	native := prepareFixedForkHistoryFixture(t, pg, "typed-collision", sourceRunID, entityID, creating)
	ctx = native.ctx
	persistFixedForkHistoryFixture(t, native)

	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
		t.Fatalf("plan blockers = %#v, authored entity_type must not challenge typed source metadata", plan.UnsupportedBlockers)
	}
	entity := fixedForkBusinessEntity(t, plan, native)
	if entity.MaterializationMetadata.EntityType != "state" || entity.MaterializationMetadata.FlowInstance != "state-flow/at-T" ||
		entity.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
		t.Fatalf("typed source metadata = %#v", entity.MaterializationMetadata)
	}
	if entity.Fields["entity_type"] != "field_case" {
		t.Fatalf("authored entity_type collision = %#v, want preserved authored value", entity.Fields)
	}
}

func TestRunForkSelectedContractBinding_MaterializesDurableForkRunBinding(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000510, 0).UTC()
	seedActivationReadySourceRun(t, ctx, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}

	selection := runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeSelectedContracts,
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		RootAdmission:     rooted.Admission(),
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
		loaded.ContractSelection != selection {
		t.Fatalf("loaded selected binding = %#v", loaded)
	}

}

func TestRunForkSelectedContractBinding_MaterializesDurableBundleHashBinding(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000515, 0).UTC()
	seedActivationReadySourceRun(t, ctx, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)

	targetArtifact := storeTestSourceArtifact("fork-selected-contract-binding")
	targetHash := targetArtifact.BundleHash()
	if targetHash == authorActivityTestBundleHash {
		t.Fatal("explicit target must differ from the source bundle")
	}
	targetRoot, _ := sourceartifactfixture.Rooted(t, "fixture", targetArtifact)
	if _, err := pg.EnsureRootedSource(ctx, targetRoot); err != nil {
		t.Fatal(err)
	}
	selection := runfork.RunForkContractSelection{
		Mode:       runfork.RunForkContractSelectionModeBundleHash,
		BundleHash: targetHash,
	}
	targetSource, err := runtimecorrelation.NewSourceArtifactFact(targetHash)
	if err != nil {
		t.Fatalf("NewSourceArtifactFact: %v", err)
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:        sourceRunID,
		At:                 eventID,
		SourceArtifactFact: targetSource,
		RootAdmission:      targetRoot.Admission(),
		ContractSelection:  &selection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	loaded, err := pg.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatalf("RequireRunForkSelectedContractBinding: %v", err)
	}
	if loaded.ContractSelection.Mode != runfork.RunForkContractSelectionModeBundleHash ||
		loaded.ContractSelection.BundleHash != targetHash {
		t.Fatalf("loaded bundle_hash binding = %#v", loaded)
	}
	var forkBundleHash string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkBundleHash); err != nil {
		t.Fatalf("load fork bundle identity: %v", err)
	}
	if forkBundleHash != targetHash {
		t.Fatalf("fork bundle identity = %s, want %s", forkBundleHash, targetHash)
	}
}

func TestRunForkSelectedContractBinding_FailsClosedOnMissingDuplicateAndInvalidSelection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)

	if _, err := pg.RequireRunForkSelectedContractBinding(ctx, uuid.NewString()); err == nil || !strings.Contains(err.Error(), "selected contract binding") {
		t.Fatalf("RequireRunForkSelectedContractBinding error = %v, want missing binding failure", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000520, 0).UTC()
	seedActivationReadySourceRun(t, ctx, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	invalidSelection := runfork.RunForkContractSelection{
		Mode:       runfork.RunForkContractSelectionModeSelectedContracts,
		BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		RootAdmission:     rooted.Admission(),
		ContractSelection: &invalidSelection,
	}); err == nil || !strings.Contains(err.Error(), "cannot carry bundle_hash") {
		t.Fatalf("MaterializeRunFork invalid selection error = %v, want selected_contracts/hash conflict", err)
	}

	validSelection := runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeSelectedContracts,
	}
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                eventID,
		RootAdmission:     rooted.Admission(),
		ContractSelection: &validSelection,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (
			fork_run_id, source_run_id, fork_point_kind, fork_revision, fork_event_id,
			mode, bundle_hash, target_bundle_hash, source_root_directory
		)
		VALUES (
			$1::uuid, $2::uuid, 'event', 1, $3::uuid,
			'selected_contracts', NULL, $4, $5
		)
	`, materialized.ForkRunID, sourceRunID, eventID, rooted.Admission().BundleHash(), rooted.Admission().RootDirectory())
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || string(pqErr.Code) != "23505" {
		t.Fatalf("duplicate selected contract binding error = %v, want unique failure", err)
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
	route := testEntitylessNodeDeliveryRoute("in-progress-node")
	sourceEvent := eventtest.TargetRouted(semanticEventRecordFixture(eventID, sourceRunID, "fork.pending", eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`), semanticEventRecordFixtureEnvelope(entityID, ""), at), route.Target.Route())
	native := prepareFixedForkHistoryFixture(t, pg, "unsupported-delivery", sourceRunID, entityID, sourceEvent)
	ctx = native.ctx
	initialRevision := captureRunForkTestRevision(t, db, sourceRunID)
	if err := runSelectedFixtureMutation(ctx, pg, "fixed construction and in-progress delivery cut", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, pg, attempt, sourceEvent, []events.DeliveryRoute{route}); err != nil {
			return err
		}
		if _, err := commitFixedForkHistoryFixture(ctx, attempt, native); err != nil {
			return err
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = deliveryFixtureAuthorityForRun(ctx, tx, deliveryadapter.DialectPostgres, sourceRunID)
			return err
		}); err != nil {
			return err
		}
		claim, err := postgresDeliveryAdapter.ClaimExactResult(ctx, attempt, authority, sourceEvent, route, runtimedelivery.DefaultLeaseTTL)
		if err != nil {
			return err
		}
		if _, acquired := claim.Acquired(); !acquired {
			return fmt.Errorf("fixed delivery cut did not acquire original contender: %s", claim.Disposition)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if revision := captureRunForkTestRevision(t, db, sourceRunID); revision != initialRevision+1 {
		t.Fatalf("construction, event and in-progress delivery split the original cut: before=%d after=%d", initialRevision, revision)
	}
	rooted, err := pg.LoadRunRootedSource(ctx, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}

	blocked, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID, RootAdmission: rooted.Admission()})
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
	if _, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID, RootAdmission: rooted.Admission()}); err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork original frontier after delete error = %v, want immutable non-agent delivery blocker", err)
	}
	first, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: clearEventID, RootAdmission: rooted.Admission()})
	if err != nil {
		t.Fatalf("MaterializeRunFork first: %v", err)
	}
	repeated, err := pg.RequireMaterializedRunFork(ctx, first.ForkRunID, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: clearEventID, RootAdmission: rooted.Admission()})
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
	if _, err := pg.RequireMaterializedRunFork(ctx, first.ForkRunID, runfork.RunForkMaterializeRequest{
		SourceRunID:   sourceRunID,
		At:            clearEventID,
		RootAdmission: rooted.Admission(),
	}); err == nil || !strings.Contains(err.Error(), "conflicts with persisted lifecycle state") {
		t.Fatalf("MaterializeRunFork conflicting replay error = %v", err)
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
				item.Status = "in_progress"
				item.SourceExecution = runtimedelivery.SourceExecutionAtCut{InProgress: true, Started: true, SessionBound: true}
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

type execContextDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func seedActivationReadySourceRun(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	selected := newPostgresStoreWithBackend(mustPostgresBackend(db))
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash})
	commitPreparedWorkflowAggregateFixture(t, ctx, selected, sourceRunID, activationReadySourceInstance(entityID, at), at)
	seedActivationReadySourceState(t, ctx, db, sourceRunID, entityID, eventID, at)
}

func activationReadySourceInstance(entityID string, at time.Time) runtimepipeline.WorkflowInstance {
	return runtimepipeline.WorkflowInstance{
		InstanceID: "1", StorageRef: "flow-a/1", EntityID: entityID, EntityType: "fork_entity",
		WorkflowName: "flow-a", WorkflowVersion: "1", Mode: "static", StageDefined: true,
		Name: "Activation Entity", CurrentState: "ready", Fields: map[string]any{"name": "Activation Entity"},
		CreatedAt: at, EnteredStageAt: at,
	}
}

func seedActivationReadySourceState(t *testing.T, ctx context.Context, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, sourceRunID, "fork.ready", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'lifecycle_state', '', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'activation-test', 'seed', $4),
			($1::uuid, $2::uuid, 'authored_field', 'name', 'null'::jsonb, '"Activation Entity"'::jsonb, $3::uuid, 'platform', 'activation-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
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
