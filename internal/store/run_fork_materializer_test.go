package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
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
	if result.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", result.ReplayResumeAdmission.Owner, RunForkReplayResumeAdmissionOwner)
	}
	if !result.ReplayResumeAdmission.StateOnlyExecutionReady || result.ReplayResumeAdmission.HistoricalReplaySupported {
		t.Fatalf("taxonomy flags = state_only:%v historical_supported:%v, want true/false",
			result.ReplayResumeAdmission.StateOnlyExecutionReady,
			result.ReplayResumeAdmission.HistoricalReplaySupported,
		)
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
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, started_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'in-progress-node', 'in_progress', $4::uuid, $3, $3)
	`, sourceRunID, eventID, at, uuid.NewString()); err != nil {
		t.Fatalf("seed in-progress delivery: %v", err)
	}

	blocked, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), "delivery_history_unproven") {
		t.Fatalf("MaterializeRunFork error = %v, want delivery blocker", err)
	}
	if blocked.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner || !blocked.ReplayResumeAdmission.HistoricalReplayRequired {
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
	if !activated.HistoricalReplayBlocked {
		t.Fatal("HistoricalReplayBlocked = false, want true for activation-only boundary")
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
	_, db, _ := testutil.StartPostgres(t)
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
	activated, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
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
	if err := pg.UpsertPipelineReceipt(ctx, eventID, "processed", ""); err != nil {
		t.Fatalf("mark source event pipeline receipt: %v", err)
	}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("safe-agent", events.EventType("fork.ready"))
	replayed, err := eb.SweepUndispatched(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("SweepUndispatched replayed = %d, want 1", replayed)
	}
	select {
	case evt := <-ch:
		if evt.ID != forkEventID || evt.RunID != materialized.ForkRunID {
			t.Fatalf("delivered fork replay event = id:%s run:%s", evt.ID, evt.RunID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fork replay event delivery")
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
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, started_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'blocked-node', 'in_progress', $4::uuid, $3, $3)
	`, sourceRunID, eventID, at, uuid.NewString()); err != nil {
		t.Fatalf("seed in-progress delivery: %v", err)
	}
	blocked, err := pg.ActivateRunFork(ctx, RunForkActivateRequest{ForkRunID: materialized.ForkRunID})
	if err == nil || !strings.Contains(err.Error(), "delivery_history_unproven") {
		t.Fatalf("ActivateRunFork in-progress delivery error = %v, want delivery blocker", err)
	}
	if blocked.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner || !blocked.ReplayResumeAdmission.HistoricalReplayRequired {
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
	_, db, _ := testutil.StartPostgres(t)
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
			_, db, _ := testutil.StartPostgres(t)
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
					agent_id, role, model_tier, llm_backend, conversation_mode, created_at
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
	_, db, _ := testutil.StartPostgres(t)
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
