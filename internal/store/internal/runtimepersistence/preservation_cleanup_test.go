package runtimepersistence

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestPreservationCleanupPublishesOneCompleteRunForkRevisionPostgres(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })

	ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
	now := time.Now().UTC().Add(time.Minute)
	agentIdentity := testAgentIdentity(t, "agent-a", "preservation")
	agentFields, err := agentIdentityFields(agentIdentity)
	if err != nil {
		t.Fatalf("agent identity fields: %v", err)
	}
	seedTestAgentRow(t, ctx, pg.backend.ConstructionHandle(), true, agentIdentity, "active")

	targets := []preservationcleanup.RunTarget{}
	byRun := map[string]preservationcleanup.RunTarget{}
	type seededRun struct {
		runID       string
		eventID     string
		untouchedID string
		sessionID   string
		generic     runtimegenericschedule.Activation
		workflow    workflowTimerDDLProofRow
		beforeHead  int64
		gateChanged bool
	}
	seeded := map[string]seededRun{}
	for _, source := range []runbundle.AvailabilitySource{
		runbundle.AvailabilitySourceEphemeral,
		runbundle.AvailabilitySourceDeleted,
	} {
		runID := uuid.NewString()
		sessionID := uuid.NewString()
		requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(pg.backend.ConstructionHandle())), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, BundleHash: testCanonicalBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
		gateChanged := !source.IsDeleted()
		if gateChanged {
			entityID := uuid.NewString()
			activation, err := gateruntime.New(runID, "preservation/review", entityID, "preservation", "awaiting_review", "preservation_review", testCanonicalBundleHash, testGateRoutes(t), "state:awaiting_review", now)
			if err != nil {
				t.Fatal(err)
			}
			card := newDecisionCardTestCard(t, runID, now)
			card.CardID = activation.CardID
			card.Anchor = newDecisionCardTestStageAnchor("preservation/review", "preservation", entityID, activation.Stage, activation.ActivationID)
			card.Snapshot.Decision, card.BundleHash = activation.DecisionID, activation.BundleHash
			card, err = decisioncard.New(card)
			if err != nil {
				t.Fatal(err)
			}
			if err := pg.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			seedDecisionCardGateEntity(t, pg.backend.ConstructionHandle(), true, runID, entityID, activation, now)
		}
		if _, err := pg.backend.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source, status
			)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', 'active')
		`, sessionID, runID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
			agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath); err != nil {
			t.Fatalf("seed session %s: %v", source, err)
		}
		eventID, activeClaim := seedPreservationClaimedDelivery(t, ctx, pg, runID, "startup."+source.String()+".active")
		if _, err := pg.BindAgentSession(ctx, activeClaim, sessionID); err != nil {
			t.Fatalf("bind active delivery %s: %v", source, err)
		}
		untouchedID, retryClaim := seedPreservationClaimedDelivery(t, ctx, pg, runID, "startup."+source.String()+".retry")
		if snapshot, err := pg.SettleFailure(ctx, retryClaim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     testRetryableFailure(),
			RetryBase:   time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		}); err != nil || snapshot.Status != runtimedelivery.StatusFailed {
			t.Fatalf("seed retryable delivery %s: snapshot=%#v err=%v", source, snapshot, err)
		}
		runCtx := runtimecorrelation.WithRunID(ctx, runID)
		admitted, err := pg.AdmitGenericSchedule(runCtx, testAgentGenericScheduleCommand(
			t, runID, "agent-a", "preservation", uuid.NewString(), "preservation-"+source.String(),
			runtimegenericschedule.AbsoluteDue(now.Add(time.Hour)),
		))
		if err != nil {
			t.Fatalf("seed generic schedule %s: %v", source, err)
		}
		workflow := newWorkflowTimerDDLProofRow(runID)
		if err := insertWorkflowTimerDDLProofRow(runCtx, pg.backend.ConstructionHandle(), pg, workflow); err != nil {
			t.Fatalf("seed workflow timer %s: %v", source, err)
		}
		publishCompleteRunForkRevisionBaseline(t, ctx, pg.backend.ConstructionHandle(), true, runID)
		if source.IsDeleted() {
			runlifecyclefixture.CorruptPostgresSource(
				t, ctx, pg.backend.ConstructionHandle(), runID, testCanonicalBundleHash, source.String(),
			)
		}
		cause, ok := preservationcleanup.CauseForBundleSource(source)
		if !ok {
			t.Fatalf("missing cause for source %s", source)
		}
		target := preservationcleanup.RunTarget{RunID: runID, BundleSource: source, BundleHash: testCanonicalBundleHash, ReasonCode: cause}
		targets = append(targets, target)
		byRun[runID] = target
		seeded[source.String()] = seededRun{
			runID: runID, eventID: eventID, untouchedID: untouchedID, sessionID: sessionID,
			generic: admitted.Activation, workflow: workflow, gateChanged: gateChanged,
		}
	}
	for source, item := range seeded {
		if err := pg.backend.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, item.runID).Scan(&item.beforeHead); err != nil {
			t.Fatalf("load pre-cleanup revision head %s: %v", source, err)
		}
		seeded[source] = item
	}

	result, err := pg.ApplyUnavailableBundleStartupPreservationCleanup(ctx, preservationcleanup.Request{
		OperationName: preservationcleanup.UnavailableBundleStartupOperationName,
		RequestedAt:   now,
		ControlledBy:  preservationcleanup.UnavailableBundleStartupControlledBy,
		Targets:       targets,
	})
	if err != nil {
		t.Fatalf("ApplyUnavailableBundleStartupPreservationCleanup: %v", err)
	}
	if len(result.Runs) != 2 || len(result.Deliveries) != 4 || len(result.Sessions) != 2 || len(result.Timers) != 4 || result.PipelineReceiptCount != 4 {
		t.Fatalf("cleanup result = runs:%d deliveries:%d sessions:%d timers:%d pipeline:%d, want 2/4/2/4/4", len(result.Runs), len(result.Deliveries), len(result.Sessions), len(result.Timers), result.PipelineReceiptCount)
	}

	for source, item := range seeded {
		target := byRun[item.runID]
		assertUnavailableBundlePreservationRun(t, ctx, pg, item.runID, source, target.ReasonCode)
		assertUnavailableBundlePreservationDelivery(t, ctx, pg, item.eventID, target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, "agent-a", target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, activeRunQuiescencePipelineSubscriberID, target.ReasonCode)
		assertUnavailableBundlePreservationDelivery(t, ctx, pg, item.untouchedID, target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.untouchedID, "agent-a", target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.untouchedID, activeRunQuiescencePipelineSubscriberID, target.ReasonCode)
		assertUnavailableBundlePreservationSession(t, ctx, pg, item.sessionID, target.ReasonCode)
		assertUnavailableBundlePreservationTimerResult(
			t, ctx, pg, result.Timers, runtimetimercancellation.FamilyGenericSchedule,
			item.generic.ID, item.runID, target.ReasonCode,
		)
		assertUnavailableBundlePreservationTimerResult(
			t, ctx, pg, result.Timers, runtimetimercancellation.FamilyWorkflowTimer,
			item.workflow.timerID, item.runID, target.ReasonCode,
		)
		assertPreservationCleanupRunForkRevision(t, ctx, pg, item)
	}
	var eventCount int
	if err := pg.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 4 {
		t.Fatalf("events count = %d, want 4 preserved rows", eventCount)
	}
}

func assertPreservationCleanupRunForkRevision(t *testing.T, ctx context.Context, pg *PostgresStore, item struct {
	runID       string
	eventID     string
	untouchedID string
	sessionID   string
	generic     runtimegenericschedule.Activation
	workflow    workflowTimerDDLProofRow
	beforeHead  int64
	gateChanged bool
}) {
	t.Helper()
	var terminalRevision int64
	if err := pg.backend.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, item.runID).Scan(&terminalRevision); err != nil {
		t.Fatalf("load preservation cleanup revision head %s: %v", item.runID, err)
	}
	if terminalRevision != item.beforeHead+1 {
		t.Fatalf("preservation cleanup revision head %s = %d, want %d", item.runID, terminalRevision, item.beforeHead+1)
	}
	rows, err := pg.backend.QueryContext(ctx, `
		SELECT DISTINCT family
		FROM run_fork_fact_revisions
		WHERE run_id=$1::uuid AND revision=$2
		ORDER BY family
	`, item.runID, terminalRevision)
	if err != nil {
		t.Fatalf("load preservation cleanup revision families %s: %v", item.runID, err)
	}
	defer rows.Close()
	var families []string
	for rows.Next() {
		var family string
		if err := rows.Scan(&family); err != nil {
			t.Fatalf("scan preservation cleanup revision family %s: %v", item.runID, err)
		}
		families = append(families, family)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read preservation cleanup revision families %s: %v", item.runID, err)
	}
	wantFamilies := []string{
		string(privaterunforkrevision.FamilyAgentSessions),
		string(privaterunforkrevision.FamilyDeadLetters),
		string(privaterunforkrevision.FamilyEventDeliveries),
		string(privaterunforkrevision.FamilyEventReceipts),
		string(privaterunforkrevision.FamilyTimers),
	}
	if item.gateChanged {
		wantFamilies = append(wantFamilies, string(privaterunforkrevision.FamilyEntityMutations))
		slices.Sort(wantFamilies)
	}
	if !slices.Equal(families, wantFamilies) {
		t.Fatalf("preservation cleanup revision families %s = %v, want %v", item.runID, families, wantFamilies)
	}
	validationTx, err := pg.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin preservation cleanup validation %s: %v", item.runID, err)
	}
	defer validationTx.Rollback()
	if err := privaterunforkrevision.ValidateCompletePostgres(ctx, validationTx, item.runID); err != nil {
		t.Fatalf("validate preservation cleanup revision %s: %v", item.runID, err)
	}
	if err := validationTx.Commit(); err != nil {
		t.Fatalf("commit preservation cleanup validation %s: %v", item.runID, err)
	}
	if _, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: item.runID, At: item.eventID}); err != nil {
		t.Fatalf("plan preservation cleanup historical event %s/%s: %v", item.runID, item.eventID, err)
	}
}

func assertUnavailableBundlePreservationRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, wantSource, wantReason string) {
	t.Helper()
	var status, source, controlStatus, controlReason, controlledBy string
	var failure []byte
	var endedAt sql.NullTime
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT
			r.status,
			r.bundle_source,
			r.failure,
			r.ended_at,
			rc.control_status,
			COALESCE(rc.reason, ''),
			COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&status, &source, &failure, &endedAt, &controlStatus, &controlReason, &controlledBy); err != nil {
		t.Fatalf("load run/control %s: %v", runID, err)
	}
	if status != "cancelled" || source != wantSource || len(failure) != 0 || !endedAt.Valid {
		t.Fatalf("run %s = status:%s source:%s failure:%s ended:%v, want cancelled/%s/no-failure/ended", runID, status, source, failure, endedAt.Valid, wantSource)
	}
	if controlStatus != "stopped" || controlReason != wantReason || controlledBy != preservationcleanup.UnavailableBundleStartupControlledBy {
		t.Fatalf("run control %s = %s/%s/%s, want stopped/%s/%s", runID, controlStatus, controlReason, controlledBy, wantReason, preservationcleanup.UnavailableBundleStartupControlledBy)
	}
}

func assertUnavailableBundlePreservationDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, wantReason string) {
	t.Helper()
	var status, reason string
	var activeSession sql.NullString
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), current_attempt_version::text
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&status, &reason, &activeSession); err != nil {
		t.Fatalf("load delivery %s: %v", eventID, err)
	}
	if status != "dead_letter" || reason != wantReason || activeSession.Valid {
		t.Fatalf("delivery %s = %s/%s active=%v, want dead_letter/%s/no active session", eventID, status, reason, activeSession.Valid, wantReason)
	}
}

func assertUnavailableBundlePreservationReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberID, wantReason string) {
	t.Helper()
	var outcome, reason string
	query := `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid AND d.subscriber_id = $2
		ORDER BY o.claim_version DESC
		LIMIT 1
	`
	wantOutcome := "terminalized"
	if subscriberID == activeRunQuiescencePipelineSubscriberID {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id = $2`
		wantOutcome = "dead_letter"
	}
	if err := pg.backend.QueryRowContext(ctx, query, eventID, subscriberID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load receipt %s/%s: %v", eventID, subscriberID, err)
	}
	if outcome != wantOutcome || reason != wantReason {
		t.Fatalf("receipt %s/%s = %s/%s, want %s/%s", eventID, subscriberID, outcome, reason, wantOutcome, wantReason)
	}
}

func assertUnavailableBundlePreservationSession(t *testing.T, ctx context.Context, pg *PostgresStore, sessionID, wantDetail string) {
	t.Helper()
	var status, reason, detail string
	var terminatedAt sql.NullTime
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT status, COALESCE(termination_reason, ''), COALESCE(termination_detail, ''), terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &reason, &detail, &terminatedAt); err != nil {
		t.Fatalf("load session %s: %v", sessionID, err)
	}
	if status != "terminated" || reason != preservationcleanup.SessionTerminationReasonOrphaned || detail != wantDetail || !terminatedAt.Valid {
		t.Fatalf("session %s = %s/%s/%s ended:%v, want terminated/orphaned/%s/ended", sessionID, status, reason, detail, terminatedAt.Valid, wantDetail)
	}
}

func assertUnavailableBundlePreservationTimerResult(
	t *testing.T,
	ctx context.Context,
	pg *PostgresStore,
	results []preservationcleanup.TimerResult,
	family runtimetimercancellation.Family,
	timerID, runID, reason string,
) {
	t.Helper()
	matched := false
	for _, result := range results {
		if result.Family == family && result.TimerID == timerID {
			matched = result.RunID == runID && result.PreviousStatus == "active" &&
				result.Status == "cancelled" && result.ReasonCode == reason && result.Changed
			break
		}
	}
	if !matched {
		t.Fatalf("preservation timer result missing exact %s/%s cancellation: %#v", family, timerID, results)
	}
	assertUnavailableBundlePreservationTimer(t, ctx, pg, timerID)
}

func assertUnavailableBundlePreservationTimer(t *testing.T, ctx context.Context, pg *PostgresStore, timerID string) {
	t.Helper()
	var status string
	if err := pg.backend.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id = $1::uuid`, timerID).Scan(&status); err != nil {
		t.Fatalf("load timer %s: %v", timerID, err)
	}
	if status != "cancelled" {
		t.Fatalf("timer %s status = %s, want cancelled", timerID, status)
	}
}

func seedPreservationClaimedDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, runID, eventName string) (string, runtimedelivery.Claim) {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(), events.EventType(eventName), "test", "", []byte(`{}`), 0,
		runID, events.EventEnvelope{}, time.Now().UTC(),
	)
	route := testAgentDeliveryRoute(t, "agent-a", "preservation")
	if err := commitSemanticEventFixtureWithRoutes(ctx, pg, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit preservation delivery %s: %v", eventName, err)
	}
	claimed, err := claimDeliveryFixture(ctx, pg, event, route)
	if err != nil {
		t.Fatalf("claim preservation delivery %s: %v", eventName, err)
	}
	return event.ID(), claimed.Claim
}
