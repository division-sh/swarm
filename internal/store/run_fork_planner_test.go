package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkPlanner_ResolvesCommitOrderedEventRevisionsAndRejectsTimestampSelectors(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, firstEventID, runID, "fork.first", events.EventProducerPlatform, "test", "", "", at)
	firstRevision := captureRunForkTestRevision(t, db, runID)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, secondEventID, runID, "fork.second", events.EventProducerPlatform, "test", "", "", at)
	secondRevision := captureRunForkTestRevision(t, db, runID)

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
	if byEvent.ForkPoint.Revision != firstRevision {
		t.Fatalf("first event revision = %d, want %d", byEvent.ForkPoint.Revision, firstRevision)
	}

	bySecondEvent, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: secondEventID})
	if err != nil {
		t.Fatalf("PlanRunFork(second event): %v", err)
	}
	if bySecondEvent.ForkPoint.EventID != secondEventID || bySecondEvent.ForkPoint.Revision != secondRevision {
		t.Fatalf("second fork point = %#v, want event %s revision %d", bySecondEvent.ForkPoint, secondEventID, secondRevision)
	}
	if bySecondEvent.EventCountAtFork != 2 {
		t.Fatalf("event count at second event = %d, want 2", bySecondEvent.EventCountAtFork)
	}

	if _, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: at.Format(time.RFC3339Nano)}); err == nil || !strings.Contains(err.Error(), "UUID") {
		t.Fatalf("timestamp selector error = %v, want UUID-only rejection", err)
	}
}

func TestRunForkPlanner_FailsClosedForOpenReplyContextAtForkPoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	requestEventID := uuid.NewString()
	at := time.Unix(1700000050, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, requestEventID, runID, "provider.requested", events.EventProducerPlatform, "test", "", "", at)
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

	captureRunForkTestRevision(t, db, runID)
	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: requestEventID})
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, firstEventID, runID, "fork.before", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"queued"'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $4),
			($1::uuid, $2::uuid, 'title', 'null'::jsonb, '"before-title"'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $4),
			($1::uuid, $2::uuid, 'gates.ready', 'null'::jsonb, 'true'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $4),
			($1::uuid, $2::uuid, 'accumulator.score', 'null'::jsonb, '7'::jsonb, $3::uuid, 'platform', 'planner-test', 'before', $4)
	`, runID, entityID, firstEventID, at); err != nil {
		t.Fatalf("seed first revision mutations: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, secondEventID, runID, "fork.after", events.EventProducerPlatform, "test", entityID, "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', '"queued"'::jsonb, '"done"'::jsonb, $3::uuid, 'platform', 'planner-test', 'after', $4),
			($1::uuid, $2::uuid, 'title', '"before-title"'::jsonb, '"after-title"'::jsonb, $3::uuid, 'platform', 'planner-test', 'after', $4)
	`, runID, entityID, secondEventID, at); err != nil {
		t.Fatalf("seed second revision mutations: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000200, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.work", events.EventProducerPlatform, "test", "", "review/inst-1", at)
	retryFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "retryable_error", nil)
	deadFailure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "dead_letter", nil)
	for _, fixture := range []struct {
		subscriber string
		state      runtimedelivery.State
		failure    *runtimefailures.Envelope
	}{
		{subscriber: "done-agent", state: runtimedelivery.StateDelivered},
		{subscriber: "pending-agent", state: runtimedelivery.StateQueued},
		{subscriber: "progress-agent", state: runtimedelivery.StateLaunching},
		{subscriber: "retry-agent", state: runtimedelivery.StateRetrying, failure: &retryFailure},
		{subscriber: "dead-agent", state: runtimedelivery.StateExhausted, failure: &deadFailure},
	} {
		seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: fixture.subscriber}, fixture.state, fixture.failure)
	}

	captureRunForkTestRevision(t, db, runID)
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
		"done-agent":     RunForkPendingClassificationDeliveredCompleted,
		"pending-agent":  RunForkPendingClassificationPending,
		"progress-agent": RunForkPendingClassificationInProgress,
		"retry-agent":    RunForkPendingClassificationFailedRetryable,
		"dead-agent":     RunForkPendingClassificationDeadLetter,
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
		RunForkBlockerFlowRouteHistoryUnproven,
	} {
		if !blockers[code] {
			t.Fatalf("missing blocker %q; blockers=%#v", code, plan.UnsupportedBlockers)
		}
	}
	for _, code := range []string{"timer_history_unproven"} {
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
	} {
		if !runForkTestHasDisposition(plan.ReplayResumeAdmission, fact) {
			t.Fatalf("missing taxonomy disposition for %s; admission=%#v", fact, plan.ReplayResumeAdmission)
		}
	}
}

func TestRunForkPlanner_PendingUnstartedDeliveryIsDeliveryEventReplayReady(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000250, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.safe_pending", events.EventProducerPlatform, "test", "", "", at)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "safe-agent"}, runtimedelivery.StateQueued, nil)

	captureRunForkTestRevision(t, db, runID)
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
	if plan.RouteHistory.State != RunForkRouteHistoryNotApplicable {
		t.Fatalf("route history = %#v, want %s", plan.RouteHistory, RunForkRouteHistoryNotApplicable)
	}
	for _, disposition := range plan.ReplayResumeAdmission.Dispositions {
		if disposition.Fact == RunForkReplayResumeFactDeliveryPendingHistory && disposition.Disposition == RunForkReplayResumeDispositionForkReplay {
			return
		}
	}
	t.Fatalf("missing fork_replay disposition for pending delivery; admission=%#v", plan.ReplayResumeAdmission)
}

func TestRunForkPlanner_NodePendingDeliveryRemainsBlocked(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000260, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.node_pending", events.EventProducerPlatform, "test", "", "", at)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-handler"}, runtimedelivery.StateQueued, nil)

	captureRunForkTestRevision(t, db, runID)
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
		name      string
		state     runtimedelivery.State
		failure   *runtimefailures.Envelope
		wantFact  string
		wantClass string
	}{
		{
			name:      "pending node",
			state:     runtimedelivery.StateQueued,
			wantFact:  RunForkReplayResumeFactDeliveryPendingHistory,
			wantClass: RunForkPendingClassificationPending,
		},
		{
			name:      "in progress node",
			state:     runtimedelivery.StateLaunching,
			wantFact:  RunForkReplayResumeFactDeliveryInProgressHistory,
			wantClass: RunForkPendingClassificationInProgress,
		},
		{
			name: "failed node", state: runtimedelivery.StateRetrying,
			failure: func() *runtimefailures.Envelope {
				failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "retryable_node_error", nil)
				return &failure
			}(),
			wantFact: RunForkReplayResumeFactDeliveryFailedHistory, wantClass: RunForkPendingClassificationFailedRetryable,
		},
		{
			name: "dead letter node", state: runtimedelivery.StateExhausted,
			failure: func() *runtimefailures.Envelope {
				failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "dead_letter", nil)
				return &failure
			}(),
			wantFact: RunForkReplayResumeFactDeliveryDeadLetterHistory, wantClass: RunForkPendingClassificationDeadLetter,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()

			runID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700000265, 0).UTC()
			if _, err := db.ExecContext(ctx, `
				INSERT INTO runs (run_id, status, started_at)
				VALUES ($1::uuid, 'running', $2)
			`, runID, at.Add(-time.Minute)); err != nil {
				t.Fatalf("seed run: %v", err)
			}
			event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.non_agent", events.EventProducerPlatform, "test", "", "", at)
			seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-handler"}, tc.state, tc.failure)

			captureRunForkTestRevision(t, db, runID)
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

func TestRunForkPlanner_CommittedReplayScopeDoesNotMasqueradeAsExecutableDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000266, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.replay_scope", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, 'direct', $3, $3)
	`, eventID, runID, at); err != nil {
		t.Fatalf("seed committed replay scope: %v", err)
	}

	captureRunForkTestRevision(t, db, runID)
	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !plan.ExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		t.Fatalf("separate replay scope changed executable-delivery admission: %#v", plan.ReplayResumeAdmission)
	}
	if len(plan.PendingWork) != 0 {
		t.Fatalf("pending work = %#v, want no synthetic executable delivery", plan.PendingWork)
	}
}

func TestRunForkPlanner_SystemDeliveryRowsAreNotCanonicalEventDeliveries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000267, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.system_delivery", events.EventProducerPlatform, "test", "", "", at)
	err := commitDeliveryObligationFixture(ctx, pg, event, events.DeliveryRoute{SubscriberType: "system", SubscriberID: "system-handler"})
	if err == nil {
		t.Fatal("seed system delivery succeeded, want canonical event_deliveries subscriber_type check to reject system rows")
	}
	if !strings.Contains(err.Error(), "unsupported subscriber type") {
		t.Fatalf("system delivery error = %v, want typed subscriber refusal", err)
	}
}

func TestRunForkPlanner_RouteRelevantStateRemainsBlockedDespiteUnrelatedCurrentRouteRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.state", events.EventProducerPlatform, "test", entityID, "flow-a/1", at)
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

	captureRunForkTestRevision(t, db, runID)
	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if plan.ExecutionReady {
		t.Fatalf("ExecutionReady = true, want route-history blocker; blockers=%#v", plan.UnsupportedBlockers)
	}
	if !runForkTestHasBlocker(plan, RunForkBlockerFlowRouteHistoryUnproven) {
		t.Fatalf("blockers=%#v, want %s", plan.UnsupportedBlockers, RunForkBlockerFlowRouteHistoryUnproven)
	}
	if plan.RouteHistory.State != RunForkRouteHistoryUnknownUnversioned {
		t.Fatalf("route history = %#v, want %s", plan.RouteHistory, RunForkRouteHistoryUnknownUnversioned)
	}
	baseline, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal baseline fixed-revision plan: %v", err)
	}
	for _, mutation := range []struct {
		label string
		query string
	}{
		{label: "update", query: `UPDATE routing_rules SET event_pattern='fork.state', flow_instance='flow-a/1', source_flow='flow-a', status='active'`},
		{label: "delete", query: `DELETE FROM routing_rules`},
		{label: "insert", query: `INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES ('fork.state', 'node', 'late-node', 'flow-a/9', 'flow-a', true, 'active', now())`},
	} {
		if _, err := db.ExecContext(ctx, mutation.query); err != nil {
			t.Fatalf("%s current routing rules: %v", mutation.label, err)
		}
		replanned, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID})
		if err != nil {
			t.Fatalf("PlanRunFork after current route %s: %v", mutation.label, err)
		}
		got, err := json.Marshal(replanned)
		if err != nil {
			t.Fatalf("marshal fixed-revision plan after route %s: %v", mutation.label, err)
		}
		if string(got) != string(baseline) {
			t.Fatalf("fixed-revision plan changed after current route %s\nbefore: %s\nafter:  %s", mutation.label, baseline, got)
		}
	}
	if plan.ReplayResumeAdmission.Owner != RunForkReplayResumeAdmissionOwner {
		t.Fatalf("taxonomy owner = %q, want %q", plan.ReplayResumeAdmission.Owner, RunForkReplayResumeAdmissionOwner)
	}
	if plan.ReplayResumeAdmission.StateOnlyExecutionReady || !plan.ReplayResumeAdmission.ReplayResumeFactsPresent || plan.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("taxonomy flags = state_only:%v historical_required:%v bounded_supported:%v, want false/true/false", plan.ReplayResumeAdmission.StateOnlyExecutionReady, plan.ReplayResumeAdmission.ReplayResumeFactsPresent, plan.ReplayResumeAdmission.BoundedReplaySupported)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.timer_route", events.EventProducerPlatform, "test", entityID, "flow-a/1", at)
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

	captureRunForkTestRevision(t, db, runID)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000250, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.work", events.EventProducerPlatform, "test", "", "", at)
	retryFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "retryable_error", nil)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-dead"}, runtimedelivery.StateRetrying, &retryFailure)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-ok"}, runtimedelivery.StateDelivered, nil)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-ok"}, runtimedelivery.StateDelivered, nil)
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

	captureRunForkTestRevision(t, db, runID)
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000260, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.work", events.EventProducerPlatform, "test", "", "", at)
	completedRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "completed-after-fork"}
	pendingRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "started-after-fork"}
	if err := commitDeliveryObligationFixture(ctx, pg, event, completedRoute); err != nil {
		t.Fatalf("seed selected-revision completed route: %v", err)
	}
	completedClaim, err := pg.ClaimAgentDelivery(ctx, event, completedRoute)
	if err != nil {
		t.Fatalf("claim selected-revision delivery: %v", err)
	}
	if err := commitDeliveryObligationFixture(ctx, pg, event, pendingRoute); err != nil {
		t.Fatalf("seed selected-revision pending route: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
	if _, err := pg.SettleSuccess(ctx, completedClaim.Claim, nil, 0); err != nil {
		t.Fatalf("complete claimed delivery after selected revision: %v", err)
	}
	pendingClaim, err := pg.ClaimAgentDelivery(ctx, event, pendingRoute)
	if err != nil {
		t.Fatalf("claim pending delivery after selected revision: %v", err)
	}
	if _, err := pg.SettleSuccess(ctx, pendingClaim.Claim, nil, 0); err != nil {
		t.Fatalf("complete pending delivery after selected revision: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000270, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.work", events.EventProducerPlatform, "test", "", "", at)
	failedRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "failed-after-fork"}
	pendingRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "pending-then-failed-after-fork"}
	if err := commitDeliveryObligationFixture(ctx, pg, event, failedRoute); err != nil {
		t.Fatalf("seed selected-revision failed route: %v", err)
	}
	failedClaim, err := pg.ClaimAgentDelivery(ctx, event, failedRoute)
	if err != nil {
		t.Fatalf("claim selected-revision delivery: %v", err)
	}
	if err := commitDeliveryObligationFixture(ctx, pg, event, pendingRoute); err != nil {
		t.Fatalf("seed selected-revision pending route: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
	failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "retry_after_fork", nil)
	settlement := runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: &failure, RetryBase: time.Hour}
	if _, err := pg.SettleFailure(ctx, failedClaim.Claim, settlement); err != nil {
		t.Fatalf("fail claimed delivery after selected revision: %v", err)
	}
	pendingClaim, err := pg.ClaimAgentDelivery(ctx, event, pendingRoute)
	if err != nil {
		t.Fatalf("claim pending delivery after selected revision: %v", err)
	}
	if _, err := pg.SettleFailure(ctx, pendingClaim.Claim, settlement); err != nil {
		t.Fatalf("fail pending delivery after selected revision: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
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

func TestRunForkPlanner_AccountsForPlatformReceiptAndExactDeliveryOutcomes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000280, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.work", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'platform', 'pipeline', 'success', 'pipeline_processed', '{}'::jsonb, $2)
	`, eventID, at); err != nil {
		t.Fatalf("seed platform processing fact: %v", err)
	}
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-a"}, runtimedelivery.StateDelivered, nil)
	seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-done"}, runtimedelivery.StateDelivered, nil)

	captureRunForkTestRevision(t, db, runID)
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
		"node/node-a":       "",
		"agent/agent-done":  "",
	} {
		item, ok := got[key]
		if !ok {
			t.Fatalf("missing completed processing fact %s; all=%#v", key, got)
		}
		if key == "platform/pipeline" && item.DeliveryID != "" {
			t.Fatalf("%s delivery_id = %q, want event-level receipt", key, item.DeliveryID)
		}
		if key != "platform/pipeline" && item.DeliveryID == "" {
			t.Fatalf("%s delivery_id is empty, want exact executable obligation", key)
		}
		if item.Classification != RunForkPendingClassificationDeliveredCompleted {
			t.Fatalf("%s classification = %q, want %q; item=%#v", key, item.Classification, RunForkPendingClassificationDeliveredCompleted, item)
		}
		if item.ReceiptOutcome != wantOutcome || (key == "platform/pipeline") != (item.ReceiptAt != nil) {
			t.Fatalf("%s event-level receipt = outcome %q at %v, want outcome %q", key, item.ReceiptOutcome, item.ReceiptAt, wantOutcome)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.session", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, created_at
		)
		VALUES ('agent-a', 'fork-planner', 'worker', 'regular', 'mock', TRUE, 'authored', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'fork-planner', TRUE, 'authored', 'active', $3, $3)
	`, sessionID, runID, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed active session: %v", err)
	}
	capabilitySurfaceID := seedManagedAgentTurnCapabilitySurface(t, pg, runID, "agent-a", sessionID, turnID, "session", "global")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, trigger_event_id, trigger_event_type, capability_surface_id, execution_mode, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'fork-planner', TRUE, 'authored', $4::uuid, 'fork.session', $5::uuid, 'live', $6)
	`, turnID, runID, sessionID, eventID, capabilitySurfaceID, at); err != nil {
		t.Fatalf("seed active turn: %v", err)
	}

	captureRunForkTestRevision(t, db, runID)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.task_audit", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-task', 'fork-planner', FALSE, 'authored', '{}'::jsonb, 'active', $3, $3)
	`, auditSessionID, runID, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed active task audit: %v", err)
	}

	captureRunForkTestRevision(t, db, runID)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.completed_session", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, created_at
		)
		VALUES ('agent-a', 'fork-planner', 'worker', 'regular', 'mock', TRUE, 'authored', $1)
	`, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, termination_reason, terminated_at, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'fork-planner', TRUE, 'authored', 'terminated', 'normal', $3, $4, $3)
	`, sessionID, runID, at.Add(-time.Second), at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed terminated session: %v", err)
	}
	captureRunForkTestRevision(t, db, runID)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "fork.terminated_audit", events.EventProducerPlatform, "test", "", "", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-task', 'fork-planner', FALSE, 'authored', '{}'::jsonb, 'terminated', $3, $4)
	`, auditSessionID, runID, at.Add(-time.Second), at.Add(time.Second)); err != nil {
		t.Fatalf("seed terminated audit: %v", err)
	}

	captureRunForkTestRevision(t, db, runID)
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
