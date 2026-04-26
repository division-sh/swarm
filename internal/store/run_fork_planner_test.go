package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestRunForkPlanner_ResolvesEventAndTimestampForkPoints(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	firstEventID := "00000000-0000-0000-0000-000000000001"
	secondEventID := "00000000-0000-0000-0000-000000000002"
	at := time.Unix(1700000000, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'fork.first', 'global', '{}'::jsonb, 'test', 'platform', $4),
			($1::uuid, $3::uuid, 'fork.second', 'global', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, firstEventID, secondEventID, at); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	byEvent, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: firstEventID})
	if err != nil {
		t.Fatalf("PlanRunFork(event): %v", err)
	}
	if byEvent.ForkPoint.EventID != firstEventID {
		t.Fatalf("event fork point = %s, want %s", byEvent.ForkPoint.EventID, firstEventID)
	}
	if byEvent.EventCountAtFork != 1 {
		t.Fatalf("event count at event fork = %d, want 1", byEvent.EventCountAtFork)
	}

	byTimestamp, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: at.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatalf("PlanRunFork(timestamp): %v", err)
	}
	if byTimestamp.ForkPoint.EventID != secondEventID {
		t.Fatalf("timestamp fork point = %s, want same-timestamp max event %s", byTimestamp.ForkPoint.EventID, secondEventID)
	}
	if byTimestamp.EventCountAtFork != 2 {
		t.Fatalf("event count at timestamp fork = %d, want 2", byTimestamp.EventCountAtFork)
	}
}

func TestRunForkPlanner_ReconstructsEntityStateAtForkPointFromMutations(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	firstEventID := uuid.NewString()
	secondEventID := uuid.NewString()
	at := time.Unix(1700000100, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'fork.before', $4::uuid, 'entity', '{}'::jsonb, 'test', 'platform', $5),
			($1::uuid, $3::uuid, 'fork.after', $4::uuid, 'entity', '{}'::jsonb, 'test', 'platform', $6)
	`, runID, firstEventID, secondEventID, entityID, at, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $5),
			($1::uuid, $2::uuid, 'title', 'null'::jsonb, '"before-title"'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $5),
			($1::uuid, $2::uuid, 'gates.ready', 'null'::jsonb, 'true'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $5),
			($1::uuid, $2::uuid, 'accumulator.score', 'null'::jsonb, '7'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $5),
			($1::uuid, $2::uuid, 'current_state', '"queued"'::jsonb, '"done"'::jsonb, $4::uuid, 'platform', 'planner-test', 'after', $6),
			($1::uuid, $2::uuid, 'title', '"before-title"'::jsonb, '"after-title"'::jsonb, $4::uuid, 'platform', 'planner-test', 'after', $6)
	`, runID, entityID, firstEventID, secondEventID, at, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: firstEventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ReconstructedEntityCount != 1 || len(plan.Entities) != 1 {
		t.Fatalf("reconstructed entities = %d/%d, want 1", plan.ReconstructedEntityCount, len(plan.Entities))
	}
	got := plan.Entities[0]
	if got.EntityID != entityID {
		t.Fatalf("entity_id = %s, want %s", got.EntityID, entityID)
	}
	if got.CurrentState != "queued" {
		t.Fatalf("current_state = %q, want queued", got.CurrentState)
	}
	if got.Fields["title"] != "before-title" {
		t.Fatalf("field title = %#v, want before-title", got.Fields["title"])
	}
	if got.Gates["ready"] != true {
		t.Fatalf("gate ready = %#v, want true", got.Gates["ready"])
	}
	if got.Accumulator["score"] != float64(7) {
		t.Fatalf("accumulator score = %#v, want 7", got.Accumulator["score"])
	}
}

func TestRunForkPlanner_ClassifiesPendingWorkAndNamedBlockers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700000200, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.work', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, active_session_id, started_at, delivered_at, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'agent', 'done-agent', 'delivered', 0, 'ok', NULL, NULL, $3, $3),
			($1::uuid, $2::uuid, 'agent', 'pending-agent', 'pending', 0, 'matched_agent_subscription', NULL, NULL, NULL, $3),
			($1::uuid, $2::uuid, 'agent', 'progress-agent', 'in_progress', 0, 'agent_processing', $4::uuid, $3, NULL, $3),
			($1::uuid, $2::uuid, 'agent', 'retry-agent', 'failed', 1, 'retryable_error', NULL, $3, $3, $3),
			($1::uuid, $2::uuid, 'agent', 'terminal-agent', 'failed', 2, 'max_retries', NULL, $3, $3, $3),
			($1::uuid, $2::uuid, 'agent', 'dead-agent', 'dead_letter', 3, 'dead_letter', NULL, $3, $3, $3),
			($1::uuid, $2::uuid, 'node', '__runtime_replay_scope__', 'delivered', 0, 'replay_scope_direct', NULL, NULL, $3, $3)
	`, runID, eventID, at, sessionID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'agent', 'done-agent', 'success', 'ok', '{}'::jsonb, $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	got := map[string]string{}
	for _, item := range plan.PendingWork {
		got[item.SubscriberID] = item.Classification
	}
	want := map[string]string{
		"done-agent":               RunForkPendingClassificationDeliveredCompleted,
		"pending-agent":            RunForkPendingClassificationPending,
		"progress-agent":           RunForkPendingClassificationInProgress,
		"retry-agent":              RunForkPendingClassificationFailedRetryable,
		"terminal-agent":           RunForkPendingClassificationFailedTerminal,
		"dead-agent":               RunForkPendingClassificationDeadLetter,
		"__runtime_replay_scope__": RunForkPendingClassificationCommittedReplay,
	}
	for subscriber, classification := range want {
		if got[subscriber] != classification {
			t.Fatalf("classification[%s] = %q, want %q; all=%#v", subscriber, got[subscriber], classification, got)
		}
	}
	if plan.ExecutionReady {
		t.Fatal("ExecutionReady = true, want false while recorder blockers remain")
	}
	blockers := map[string]bool{}
	for _, blocker := range plan.UnsupportedBlockers {
		blockers[blocker.Code] = true
	}
	for _, code := range []string{
		"delivery_history_unproven",
		"timer_history_unproven",
		"flow_route_history_unproven",
		"session_history_unproven",
		"active_turn_history_unproven",
	} {
		if !blockers[code] {
			t.Fatalf("missing blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
}

func TestRunForkPlanner_ScopesDeadLettersToMatchingDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000250, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.work', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, started_at, delivered_at, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'node', 'node-dead', 'failed', 1, 'retryable_error', $3, NULL, $3),
			($1::uuid, $2::uuid, 'node', 'node-ok', 'delivered', 0, 'ok', NULL, $3, $3),
			($1::uuid, $2::uuid, 'agent', 'agent-ok', 'delivered', 0, 'ok', NULL, $3, $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES
			($1::uuid, 'node', 'node-ok', 'success', 'ok', '{}'::jsonb, $2),
			($1::uuid, 'agent', 'agent-ok', 'success', 'ok', '{}'::jsonb, $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed receipts: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			original_event_id, original_event, original_payload, flow_instance,
			failure_type, error_message, retry_count, handler_node, created_at
		)
		VALUES
			($1::uuid, 'fork.work', '{}'::jsonb, 'runtime', 'handler_error', 'node failed', 1, 'node-dead', $2),
			($1::uuid, 'fork.work', '{}'::jsonb, 'runtime', 'retry_exhausted', 'different node failed', 3, 'node-other', $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed dead letters: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.PendingWorkCount != 3 || len(plan.PendingWork) != 3 {
		t.Fatalf("pending work count = %d/%d, want 3 without dead-letter row multiplication; items=%#v", plan.PendingWorkCount, len(plan.PendingWork), plan.PendingWork)
	}
	got := map[string]string{}
	for _, item := range plan.PendingWork {
		got[item.SubscriberID] = item.Classification
	}
	want := map[string]string{
		"node-dead": RunForkPendingClassificationDeadLetter,
		"node-ok":   RunForkPendingClassificationDeliveredCompleted,
		"agent-ok":  RunForkPendingClassificationDeliveredCompleted,
	}
	for subscriber, classification := range want {
		if got[subscriber] != classification {
			t.Fatalf("classification[%s] = %q, want %q; all=%#v", subscriber, got[subscriber], classification, got)
		}
	}
	if _, ok := got["node-other"]; ok {
		t.Fatalf("dead-letter-only handler became pending work row; all=%#v", got)
	}
}

func TestRunForkPlanner_DoesNotReportPostForkCompletionAsCompletedAtFork(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000260, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.work', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, started_at, delivered_at, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'agent', 'completed-after-fork', 'delivered', 0, 'ok', $3, $4, $3),
			($1::uuid, $2::uuid, 'agent', 'started-after-fork', 'delivered', 0, 'ok', $4, $5, $3)
	`, runID, eventID, at, at.Add(time.Minute), at.Add(2*time.Minute)); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	got := map[string]RunForkPendingWork{}
	for _, item := range plan.PendingWork {
		got[item.SubscriberID] = item
	}
	inProgress := got["completed-after-fork"]
	if inProgress.Classification != RunForkPendingClassificationInProgress {
		t.Fatalf("completed-after-fork classification = %q, want %q; item=%#v", inProgress.Classification, RunForkPendingClassificationInProgress, inProgress)
	}
	if inProgress.Status != "in_progress" {
		t.Fatalf("completed-after-fork status = %q, want in_progress", inProgress.Status)
	}
	if inProgress.DeliveredAt != nil {
		t.Fatalf("completed-after-fork delivered_at = %v, want nil because completion happened after fork", inProgress.DeliveredAt)
	}
	pending := got["started-after-fork"]
	if pending.Classification != RunForkPendingClassificationPending {
		t.Fatalf("started-after-fork classification = %q, want %q; item=%#v", pending.Classification, RunForkPendingClassificationPending, pending)
	}
	if pending.Status != "pending" {
		t.Fatalf("started-after-fork status = %q, want pending", pending.Status)
	}
	if pending.StartedAt != nil || pending.DeliveredAt != nil {
		t.Fatalf("started-after-fork timestamps = started %v delivered %v, want both nil at fork", pending.StartedAt, pending.DeliveredAt)
	}
}

func TestRunForkPlanner_FailsClosedWhenMutationLogUnavailable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DROP TABLE entity_mutations`); err != nil {
		t.Fatalf("drop entity_mutations: %v", err)
	}
	_, err := pg.PlanRunFork(ctx, RunForkPlanRequest{
		SourceRunID: uuid.NewString(),
		At:          uuid.NewString(),
	})
	if err == nil {
		t.Fatal("PlanRunFork error = nil, want fail-closed missing mutation log error")
	}
	if !strings.Contains(err.Error(), "entity_mutations") {
		t.Fatalf("PlanRunFork error = %v, want entity_mutations capability failure", err)
	}
}

func TestRunForkPlanner_FailsClosedWhenDeadLettersUnavailable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DROP TABLE dead_letters`); err != nil {
		t.Fatalf("drop dead_letters: %v", err)
	}
	_, err := pg.PlanRunFork(ctx, RunForkPlanRequest{
		SourceRunID: uuid.NewString(),
		At:          uuid.NewString(),
	})
	if err == nil {
		t.Fatal("PlanRunFork error = nil, want fail-closed missing dead_letters error")
	}
	if !strings.Contains(err.Error(), "dead_letters") {
		t.Fatalf("PlanRunFork error = %v, want dead_letters capability failure", err)
	}
}
