package store

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkPlanner_ResolvesEventAndTimestampForkPoints(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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

func TestRunForkPlanner_FailsClosedForOpenReplyContextAtForkPoint(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	requestEventID := uuid.NewString()
	at := time.Unix(1700000050, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2::uuid, 'provider.requested', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, requestEventID, at); err != nil {
		t.Fatalf("seed request event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id,
			request_output_pin, reply_input_pin, provider_flow_id, provider_input_pin,
			provider_output_pin, origin_route, request_correlation_id, state
		)
		VALUES (
			'reply-v1:test-open', $1::uuid, $2::uuid, 'requester',
			'provider_requested', 'provider_replied', 'provider', 'requested',
			'replied', '{"flow_instance":"requester"}'::jsonb, $2, 'open'
		)
	`, runID, requestEventID); err != nil {
		t.Fatalf("seed reply context: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: at.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("open reply context plan unexpectedly execution-ready: %#v", plan)
	}
	if !runForkTestHasPlanBlocker(plan, RunForkBlockerOpenReplyContextUnsupported) {
		t.Fatalf("open reply context blocker missing: %#v", plan.UnsupportedBlockers)
	}
}

func TestRunForkPlanner_ReconstructsEntityStateAtForkPointFromMutations(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
			run_id, event_id, event_name, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.work', 'review/inst-1', 'global', '{}'::jsonb, 'test', 'platform', $3)
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
		if item.FlowInstance != "review/inst-1" {
			t.Fatalf("pending item flow_instance = %q, want review/inst-1; item=%#v", item.FlowInstance, item)
		}
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
	if plan.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", plan.ReplayResumeAdmission.Owner, RunForkReplayResumeAdmissionOwner)
	}
	if plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatal("taxonomy StateOnlyExecutionReady = true, want false")
	}
	if !plan.ReplayResumeAdmission.ReplayResumeFactsPresent || plan.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("taxonomy replay flags = required:%v supported:%v, want required true/supported false", plan.ReplayResumeAdmission.ReplayResumeFactsPresent, plan.ReplayResumeAdmission.BoundedReplaySupported)
	}
	blockers := map[string]bool{}
	for _, blocker := range plan.UnsupportedBlockers {
		blockers[blocker.Code] = true
	}
	for _, code := range []string{
		"delivery_history_unproven",
		RunForkBlockerCommittedReplayScopeReplayUnsupported,
		"session_history_unproven",
		"active_turn_history_unproven",
	} {
		if !blockers[code] {
			t.Fatalf("missing blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	for _, code := range []string{"timer_history_unproven", "flow_route_history_unproven"} {
		if blockers[code] {
			t.Fatalf("unexpected unrelated blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	for _, fact := range []string{
		RunForkReplayResumeFactDeliveryCompletedHistory,
		RunForkReplayResumeFactDeliveryPendingHistory,
		RunForkReplayResumeFactDeliveryInProgressHistory,
		RunForkReplayResumeFactDeliveryFailedHistory,
		RunForkReplayResumeFactDeliveryDeadLetterHistory,
		RunForkReplayResumeFactCommittedReplayScope,
	} {
		if !runForkTestHasDisposition(plan.ReplayResumeAdmission, fact) {
			t.Fatalf("missing taxonomy disposition for %s; admission=%#v", fact, plan.ReplayResumeAdmission)
		}
	}
}

func TestRunForkPlanner_PendingUnstartedDeliveryIsDeliveryEventReplayReady(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
		VALUES ($1::uuid, $2::uuid, 'fork.safe_pending', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'matched_agent_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed safe pending delivery: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for safe pending delivery; blockers=%#v", plan.UnsupportedBlockers)
	}
	if plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatal("StateOnlyExecutionReady = true, want false because delivery/event replay is required")
	}
	if !plan.ReplayResumeAdmission.DeliveryEventReplayReady || !plan.ReplayResumeAdmission.ReplayResumeFactsPresent || !plan.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("replay flags = %#v, want delivery-event replay ready and supported", plan.ReplayResumeAdmission)
	}
	for _, disposition := range plan.ReplayResumeAdmission.Dispositions {
		if disposition.Fact == RunForkReplayResumeFactDeliveryPendingHistory && disposition.Disposition == RunForkReplayResumeDispositionForkReplay {
			return
		}
	}
	t.Fatalf("missing fork_replay disposition for pending delivery; admission=%#v", plan.ReplayResumeAdmission)
}

func TestRunForkPlanner_NodePendingDeliveryRemainsBlocked(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
		VALUES ($1::uuid, $2::uuid, 'fork.node_pending', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'node-handler', 'pending', 0, 'matched_node_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed node pending delivery: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		t.Fatalf("node pending plan became replay-ready: %#v", plan.ReplayResumeAdmission)
	}
	if !runForkTestHasBlocker(plan, RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("node pending blockers = %#v, want non-agent delivery replay blocker", plan.UnsupportedBlockers)
	}
	if !runForkTestHasDispositionBlocker(plan.ReplayResumeAdmission, RunForkReplayResumeFactDeliveryPendingHistory, RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("node pending admission = %#v, want non-agent delivery replay disposition", plan.ReplayResumeAdmission)
	}
}

func TestRunForkPlanner_NonAgentDeliveryStatesRemainNamedBlockers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        string
		retryCount    int
		reasonCode    string
		activeSession bool
		delivered     bool
		wantFact      string
		wantClass     string
	}{
		{
			name:       "pending node",
			status:     "pending",
			reasonCode: "matched_node_subscription",
			wantFact:   RunForkReplayResumeFactDeliveryPendingHistory,
			wantClass:  RunForkPendingClassificationPending,
		},
		{
			name:          "in progress node",
			status:        "in_progress",
			reasonCode:    "node_processing",
			activeSession: true,
			wantFact:      RunForkReplayResumeFactDeliveryInProgressHistory,
			wantClass:     RunForkPendingClassificationInProgress,
		},
		{
			name:       "failed node",
			status:     "failed",
			retryCount: 1,
			reasonCode: "retryable_node_error",
			delivered:  true,
			wantFact:   RunForkReplayResumeFactDeliveryFailedHistory,
			wantClass:  RunForkPendingClassificationFailedRetryable,
		},
		{
			name:       "dead letter node",
			status:     "dead_letter",
			retryCount: 3,
			reasonCode: "dead_letter",
			delivered:  true,
			wantFact:   RunForkReplayResumeFactDeliveryDeadLetterHistory,
			wantClass:  RunForkPendingClassificationDeadLetter,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			pg := &PostgresStore{DB: db}
			ctx := context.Background()

			runID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700000265, 0).UTC()
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
				VALUES ($1::uuid, $2::uuid, 'fork.non_agent', 'global', '{}'::jsonb, 'test', 'platform', $3)
			`, runID, eventID, at); err != nil {
				t.Fatalf("seed event: %v", err)
			}
			var activeSessionID any
			var startedAt any
			var deliveredAt any
			if tc.activeSession {
				activeSessionID = uuid.NewString()
				startedAt = at
			}
			if tc.delivered {
				deliveredAt = at
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, active_session_id, started_at, delivered_at, created_at
				)
				VALUES ($1::uuid, $2::uuid, 'node', 'node-handler', $3, $4, $5, $6::uuid, $7, $8, $9)
			`, runID, eventID, tc.status, tc.retryCount, tc.reasonCode, activeSessionID, startedAt, deliveredAt, at); err != nil {
				t.Fatalf("seed node delivery: %v", err)
			}

			plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
			if err != nil {
				t.Fatalf("PlanRunFork: %v", err)
			}
			if plan.ExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady {
				t.Fatalf("non-agent delivery became replay-ready: %#v", plan.ReplayResumeAdmission)
			}
			if !runForkTestHasBlocker(plan, RunForkBlockerNonAgentDeliveryReplayUnsupported) {
				t.Fatalf("blockers = %#v, want non-agent delivery replay blocker", plan.UnsupportedBlockers)
			}
			if !runForkTestHasDispositionBlocker(plan.ReplayResumeAdmission, tc.wantFact, RunForkBlockerNonAgentDeliveryReplayUnsupported) {
				t.Fatalf("admission = %#v, want %s/%s disposition", plan.ReplayResumeAdmission, tc.wantFact, RunForkBlockerNonAgentDeliveryReplayUnsupported)
			}
			if len(plan.PendingWork) != 1 {
				t.Fatalf("pending work = %#v, want one non-agent item", plan.PendingWork)
			}
			if plan.PendingWork[0].Classification != tc.wantClass {
				t.Fatalf("classification = %q, want %q; item=%#v", plan.PendingWork[0].Classification, tc.wantClass, plan.PendingWork[0])
			}
		})
	}
}

func TestRunForkPlanner_ReplayScopeMarkerRemainsCommittedReplayBlocker(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000266, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.replay_scope', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, 'delivered', 0, $5, $6, $6)
	`, runID, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, replayScopeReasonDirect, at); err != nil {
		t.Fatalf("seed replay-scope marker: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		t.Fatalf("replay-scope marker became replay-ready: %#v", plan.ReplayResumeAdmission)
	}
	if !runForkTestHasBlocker(plan, RunForkBlockerCommittedReplayScopeReplayUnsupported) {
		t.Fatalf("blockers = %#v, want committed replay-scope blocker", plan.UnsupportedBlockers)
	}
	if !runForkTestHasDispositionBlocker(plan.ReplayResumeAdmission, RunForkReplayResumeFactCommittedReplayScope, RunForkBlockerCommittedReplayScopeReplayUnsupported) {
		t.Fatalf("admission = %#v, want committed replay-scope blocker disposition", plan.ReplayResumeAdmission)
	}
	if len(plan.PendingWork) != 1 || plan.PendingWork[0].Classification != RunForkPendingClassificationCommittedReplay {
		t.Fatalf("pending work = %#v, want committed replay-scope marker classification", plan.PendingWork)
	}
}

func TestRunForkPlanner_SystemDeliveryRowsAreNotCanonicalEventDeliveries(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000267, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.system_delivery', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'system', 'system-handler', 'pending', 0, 'matched_system_subscription', $3)
	`, runID, eventID, at)
	if err == nil {
		t.Fatal("seed system delivery succeeded, want canonical event_deliveries subscriber_type check to reject system rows")
	}
	if !strings.Contains(err.Error(), "subscriber_type") {
		t.Fatalf("system delivery error = %v, want subscriber_type constraint proof", err)
	}
}

func TestRunForkPlanner_StateOnlyPlanExecutionReadyWithEmptyAndUnrelatedTimerRouteTables(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000210, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.state', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'planner-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default',
			'ready', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			run_id, timer_name, entity_id, flow_instance, fire_event, fire_at, owner_node, task_type, status, created_at
		)
		VALUES ($1::uuid, 'unrelated', $2::uuid, 'flow-other/1', 'timer.fire', $3, 'other-node', 'timer', 'active', $4)
	`, runID, uuid.NewString(), at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed unrelated timer: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at
		)
		VALUES ('other.event', 'node', 'other-node', 'flow-other/1', 'flow-other', true, 'active', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed unrelated route: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for state-only plan; blockers=%#v", plan.UnsupportedBlockers)
	}
	if plan.UnsupportedBlockerCount != 0 {
		t.Fatalf("UnsupportedBlockerCount = %d, want 0; blockers=%#v", plan.UnsupportedBlockerCount, plan.UnsupportedBlockers)
	}
	if plan.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", plan.ReplayResumeAdmission.Owner, RunForkReplayResumeAdmissionOwner)
	}
	if !plan.ReplayResumeAdmission.StateOnlyExecutionReady || plan.ReplayResumeAdmission.ReplayResumeFactsPresent || plan.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("taxonomy flags = state_only:%v historical_required:%v bounded_supported:%v, want true/false/false",
			plan.ReplayResumeAdmission.StateOnlyExecutionReady,
			plan.ReplayResumeAdmission.ReplayResumeFactsPresent,
			plan.ReplayResumeAdmission.BoundedReplaySupported,
		)
	}
	if !runForkTestHasDisposition(plan.ReplayResumeAdmission, RunForkReplayResumeFactEntityStateSnapshot) {
		t.Fatalf("missing entity-state taxonomy disposition; admission=%#v", plan.ReplayResumeAdmission)
	}
	if !runForkTestHasDisposition(plan.ReplayResumeAdmission, RunForkReplayResumeFactHistoricalReplayExecution) {
		t.Fatalf("missing split historical replay taxonomy disposition; admission=%#v", plan.ReplayResumeAdmission)
	}
	if plan.ReconstructedEntityCount != 1 {
		t.Fatalf("ReconstructedEntityCount = %d, want 1", plan.ReconstructedEntityCount)
	}
}

func TestRunForkPlanner_RelevantTimerAndRouteRemainBlockers(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000220, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.timer_route', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"waiting"'::jsonb, $3::uuid, 'platform', 'planner-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			run_id, timer_name, entity_id, flow_instance, fire_event, fire_at, owner_node, task_type, status, created_at
		)
		VALUES ($1::uuid, 'relevant', $2::uuid, 'flow-a/1', 'timer.fire', $3, 'node-a', 'timer', 'active', $4)
	`, runID, entityID, at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed relevant timer: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at
		)
		VALUES ('fork.timer_route', 'node', 'node-a', 'flow-a/2', 'flow-a', true, 'active', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed relevant route: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatal("ExecutionReady = true, want false for relevant timer/route facts")
	}
	for _, code := range []string{"timer_history_unproven", "flow_route_history_unproven"} {
		if !runForkTestHasBlocker(plan, code) {
			t.Fatalf("missing blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	for _, fact := range []string{RunForkReplayResumeFactTimerHistory, RunForkReplayResumeFactRouteHistory} {
		if !runForkTestHasDisposition(plan.ReplayResumeAdmission, fact) {
			t.Fatalf("missing taxonomy disposition for %s; admission=%#v", fact, plan.ReplayResumeAdmission)
		}
	}
}

func TestRunForkPlanner_ScopesDeadLettersToMatchingDelivery(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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
			failure, retry_count, handler_node, created_at
		)
		VALUES
			($1::uuid, 'fork.work', '{}'::jsonb, 'runtime', $2::jsonb, 1, 'node-dead', $4),
			($1::uuid, 'fork.work', '{}'::jsonb, 'runtime', $3::jsonb, 3, 'node-other', $4)
	`, eventID,
		mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "node_failed", nil)),
		mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "different_node_failed", nil)), at); err != nil {
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
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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

func TestRunForkPlanner_SuppressesPostForkTerminalMetadata(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000270, 0).UTC()
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
			($1::uuid, $2::uuid, 'agent', 'failed-after-fork', 'failed', 2, 'retry_exhausted', $3, $4, $3),
			($1::uuid, $2::uuid, 'agent', 'pending-then-failed-after-fork', 'failed', 2, 'retry_exhausted', $4, $5, $3)
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
	inProgress := got["failed-after-fork"]
	if inProgress.Classification != RunForkPendingClassificationInProgress {
		t.Fatalf("failed-after-fork classification = %q, want %q; item=%#v", inProgress.Classification, RunForkPendingClassificationInProgress, inProgress)
	}
	if inProgress.RetryCount != 0 || inProgress.ReasonCode != "" {
		t.Fatalf("failed-after-fork terminal metadata leaked: retry_count=%d reason_code=%q", inProgress.RetryCount, inProgress.ReasonCode)
	}
	pending := got["pending-then-failed-after-fork"]
	if pending.Classification != RunForkPendingClassificationPending {
		t.Fatalf("pending-then-failed-after-fork classification = %q, want %q; item=%#v", pending.Classification, RunForkPendingClassificationPending, pending)
	}
	if pending.RetryCount != 0 || pending.ReasonCode != "" {
		t.Fatalf("pending-then-failed-after-fork terminal metadata leaked: retry_count=%d reason_code=%q", pending.RetryCount, pending.ReasonCode)
	}
}

func TestRunForkPlanner_AccountsForReceiptOnlyPlatformAndNodeProcessing(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000280, 0).UTC()
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
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES
			($1::uuid, 'platform', 'pipeline', 'success', 'pipeline_processed', '{}'::jsonb, $2),
			($1::uuid, 'node', 'node-a', 'no_op', 'idempotent_no_op', '{}'::jsonb, $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed receipt-only processing facts: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, started_at, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-done', 'delivered', 0, 'ok', $3, $3, $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed delivered processing fact: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'agent', 'agent-done', 'success', 'ok', '{}'::jsonb, $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed delivered receipt: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.PendingWorkCount != 3 || len(plan.PendingWork) != 3 {
		t.Fatalf("pending work count = %d/%d, want 3 completed processing facts; items=%#v", plan.PendingWorkCount, len(plan.PendingWork), plan.PendingWork)
	}
	got := map[string]RunForkPendingWork{}
	for _, item := range plan.PendingWork {
		got[item.SubscriberType+"/"+item.SubscriberID] = item
	}
	for key, wantOutcome := range map[string]string{
		"platform/pipeline": "success",
		"node/node-a":       "no_op",
		"agent/agent-done":  "success",
	} {
		item, ok := got[key]
		if !ok {
			t.Fatalf("missing completed processing fact %s; all=%#v", key, got)
		}
		if key != "agent/agent-done" && item.DeliveryID != "" {
			t.Fatalf("%s delivery_id = %q, want empty for receipt-only row", key, item.DeliveryID)
		}
		if item.Classification != RunForkPendingClassificationDeliveredCompleted {
			t.Fatalf("%s classification = %q, want %q; item=%#v", key, item.Classification, RunForkPendingClassificationDeliveredCompleted, item)
		}
		if item.ReceiptOutcome != wantOutcome || item.ReceiptAt == nil {
			t.Fatalf("%s receipt = outcome %q at %v, want outcome %q with timestamp", key, item.ReceiptOutcome, item.ReceiptAt, wantOutcome)
		}
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for completed-only delivery/receipt facts; blockers=%#v", plan.UnsupportedBlockers)
	}
	if runForkTestHasBlocker(plan, "delivery_history_unproven") {
		t.Fatalf("completed-only delivery/receipt facts emitted delivery blocker: %#v", plan.UnsupportedBlockers)
	}
}

func TestRunForkPlanner_RunScopedActiveSessionAndTurnRemainBlockers(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700000290, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.session', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, role, model, llm_backend, conversation_mode, created_at
		)
		VALUES ('agent-a', 'worker', 'standard', 'mock', 'session', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'global', 'global', 'session', 'active', $3, $3)
	`, sessionID, runID, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed active session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, trigger_event_id, trigger_event_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'session', $4::uuid, 'fork.session', $5)
	`, turnID, runID, sessionID, eventID, at); err != nil {
		t.Fatalf("seed active turn: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatal("ExecutionReady = true, want false for source-run active session/turn facts")
	}
	for _, code := range []string{"session_history_unproven", "active_turn_history_unproven"} {
		if !runForkTestHasBlocker(plan, code) {
			t.Fatalf("missing blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	for _, fact := range []string{RunForkReplayResumeFactSessionHistory, RunForkReplayResumeFactActiveTurnHistory} {
		if !runForkTestHasDisposition(plan.ReplayResumeAdmission, fact) {
			t.Fatalf("missing disposition %q; admission=%#v", fact, plan.ReplayResumeAdmission)
		}
	}
}

func TestRunForkPlanner_ActiveConversationAuditRemainsPolicyBlocker(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	auditSessionID := uuid.NewString()
	at := time.Unix(1700000295, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.task_audit', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-task', 'global', 'global', 'task', '{}'::jsonb, 'active', $3, $3)
	`, auditSessionID, runID, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed active task audit: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatal("ExecutionReady = true, want false for active task conversation audit facts")
	}
	if !runForkTestHasBlocker(plan, RunForkBlockerConversationAuditUnproven) {
		t.Fatalf("missing conversation audit blocker; blockers=%#v", plan.UnsupportedBlockers)
	}
	if !runForkTestHasDisposition(plan.ReplayResumeAdmission, RunForkReplayResumeFactConversationAuditHistory) {
		t.Fatalf("missing conversation audit disposition; admission=%#v", plan.ReplayResumeAdmission)
	}
}

func TestRunForkPlanner_TerminatedSessionBeforeForkIsLineageOnly(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700000297, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.completed_session', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, role, model, llm_backend, conversation_mode, created_at
		)
		VALUES ('agent-a', 'worker', 'standard', 'mock', 'session', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, status, termination_reason, terminated_at, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'global', 'global', 'session', 'terminated', 'normal', $3, $4, $3)
	`, sessionID, runID, at.Add(-time.Second), at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed terminated session: %v", err)
	}
	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	for _, code := range []string{RunForkBlockerSessionHistoryUnproven, RunForkBlockerActiveTurnHistoryUnproven} {
		if runForkTestHasBlocker(plan, code) {
			t.Fatalf("completed lineage emitted blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want completed session lineage-only plan ready; blockers=%#v", plan.UnsupportedBlockers)
	}
}

func TestRunForkPlanner_TerminatedAuditStillBlocksWithoutAtForkTerminationProof(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	auditSessionID := uuid.NewString()
	at := time.Unix(1700000298, 0).UTC()
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
		VALUES ($1::uuid, $2::uuid, 'fork.terminated_audit', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-task', 'global', 'global', 'task', '{}'::jsonb, 'terminated', $3, $4)
	`, auditSessionID, runID, at.Add(-time.Second), at.Add(time.Second)); err != nil {
		t.Fatalf("seed terminated audit: %v", err)
	}

	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatal("ExecutionReady = true, want false because audit termination is not append-only proven at fork T")
	}
	if !runForkTestHasBlocker(plan, RunForkBlockerConversationAuditUnproven) {
		t.Fatalf("missing conversation audit blocker; blockers=%#v", plan.UnsupportedBlockers)
	}
	if !runForkTestHasDisposition(plan.ReplayResumeAdmission, RunForkReplayResumeFactConversationAuditHistory) {
		t.Fatalf("missing conversation audit disposition; admission=%#v", plan.ReplayResumeAdmission)
	}
}

func TestRunForkPlanner_FailsClosedWhenMutationLogUnavailable(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
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
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
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

func runForkTestHasBlocker(plan RunForkPlan, code string) bool {
	for _, blocker := range plan.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkTestHasDisposition(admission RunForkReplayResumeAdmission, fact string) bool {
	for _, disposition := range admission.Dispositions {
		if disposition.Fact == fact {
			return true
		}
	}
	return false
}

func runForkTestHasDispositionBlocker(admission RunForkReplayResumeAdmission, fact, blockerCode string) bool {
	for _, disposition := range admission.Dispositions {
		if disposition.Fact == fact && disposition.BlockerCode == blockerCode {
			return true
		}
	}
	return false
}
