package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkSourceFreezeIsTheOnlyForkedStatusWriter(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			now := time.Now().UTC()
			var db *sql.DB
			var mark func(context.Context, string, string, time.Time) error
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				pg := admitTestPostgresStore(t, db)
				if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, now); err != nil {
					t.Fatal(err)
				}
				mark = func(ctx context.Context, runID, status string, at time.Time) error {
					_, err := pg.MarkRunTerminal(ctx, runID, status, nil, at)
					return err
				}
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.DB
				if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
					t.Fatal(err)
				}
				mark = func(ctx context.Context, runID, status string, at time.Time) error {
					_, err := store.MarkRunTerminal(ctx, runID, status, nil, at)
					return err
				}
			}

			err := mark(ctx, runID, RunForkSourceFrozenStatus, now.Add(time.Minute))
			if err == nil || !strings.Contains(err.Error(), `unsupported terminal run status "forked"`) {
				t.Fatalf("generic forked transition error = %v", err)
			}
			var status string
			var continuedAs sql.NullString
			query := `SELECT status, continued_as_run_id FROM runs WHERE run_id = ?`
			if backend == "postgres" {
				query = `SELECT status, continued_as_run_id::text FROM runs WHERE run_id = $1::uuid`
			}
			if err := db.QueryRowContext(ctx, query, runID).Scan(&status, &continuedAs); err != nil {
				t.Fatal(err)
			}
			if status != "running" || continuedAs.Valid {
				t.Fatalf("generic writer mutated run to status=%q continued_as=%v", status, continuedAs)
			}
		})
	}
}

func TestRunForkSourceFreezeCommitsCoupledLifecycleDecisionAndActivityOutcome(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityBundleSourceContext()
	now := time.Now().UTC().Truncate(time.Microsecond)
	lineage := seedRunForkSourceFreezePair(t, db, "running", RunForkMaterializedStatus, now)

	stageCard := newDecisionCardTestCard(t, lineage.SourceRunID, now)
	if err := pg.CreateDecisionCard(ctx, stageCard); err != nil {
		t.Fatal(err)
	}
	draft, err := pg.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{
		CardID: stageCard.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	humanCard, humanContinuation := newHumanTaskDecisionCardTestFixture(t, lineage.SourceRunID, "source-freeze-human", now, 1, now.Add(24*time.Hour))
	if err := pg.CreateHumanTaskCard(ctx, humanCard, humanContinuation); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: humanCard.CardID, Verdict: "approve", ActorTokenID: "operator-a",
		ObservedContentHash: humanCard.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: uuid.NewString(), RevisionField: "revision_id", RevisionID: uuid.NewString(), Attempt: 1}
	effectCard, effectContinuation := newProposedEffectTestCard(t, lineage.SourceRunID, now, generation)
	if err := pg.CreateProposedEffectCard(ctx, effectCard, effectContinuation); err != nil {
		t.Fatal(err)
	}

	if err := commitRunForkSourceFreezeForTest(ctx, db, lineage, now.Add(2*time.Second), true); err != nil {
		t.Fatalf("source freeze: %v", err)
	}

	var sourceStatus, childStatus, continuedAs string
	var endedAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT status, ended_at, continued_as_run_id::text FROM runs WHERE run_id = $1::uuid`, lineage.SourceRunID).Scan(&sourceStatus, &endedAt, &continuedAs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, lineage.ForkRunID).Scan(&childStatus); err != nil {
		t.Fatal(err)
	}
	if sourceStatus != RunForkSourceFrozenStatus || childStatus != RunForkActivatedStatus || continuedAs != lineage.ForkRunID || endedAt.IsZero() {
		t.Fatalf("lifecycle outcome source=%s child=%s continued_as=%s ended_at=%v", sourceStatus, childStatus, continuedAs, endedAt)
	}

	for _, cardID := range []string{stageCard.CardID, humanCard.CardID, effectCard.CardID} {
		card, err := pg.GetDecisionCard(ctx, cardID)
		if err != nil || card.Status != decisioncard.StatusSuperseded || card.SupersededReason != "run_forked" {
			t.Fatalf("card %s after freeze = %#v, %v", cardID, card, err)
		}
	}
	humanAfter, err := pg.LoadHumanTaskContinuation(ctx, humanCard.CardID)
	if err != nil || humanAfter.State != decisioncard.HumanTaskContinuationSuperseded {
		t.Fatalf("human continuation after freeze = %#v, %v", humanAfter, err)
	}
	effectAfter, err := pg.LoadProposedEffectContinuation(ctx, effectCard.CardID)
	if err != nil || effectAfter.State != decisioncard.ProposedEffectSuperseded {
		t.Fatalf("effect continuation after freeze = %#v, %v", effectAfter, err)
	}
	var draftStatus, obligationStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM decision_card_input_drafts WHERE input_draft_id = $1`, draft.InputDraftID).Scan(&draftStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM decision_card_route_obligations WHERE card_id = $1::uuid`, humanCard.CardID).Scan(&obligationStatus); err != nil {
		t.Fatal(err)
	}
	if draftStatus != decisioncard.DraftStatusCancelled || obligationStatus != "superseded" {
		t.Fatalf("draft/obligation after freeze = %s/%s", draftStatus, obligationStatus)
	}
	var changeCount, activityCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_card_changes WHERE run_id = $1::uuid AND change_type = 'superseded'`, lineage.SourceRunID).Scan(&changeCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences WHERE (run_id = $1::uuid AND transition = 'forked') OR (run_id = $2::uuid AND transition = 'fork_started')`, lineage.SourceRunID, lineage.ForkRunID).Scan(&activityCount); err != nil {
		t.Fatal(err)
	}
	if changeCount != 3 || activityCount != 2 {
		t.Fatalf("change/activity counts = %d/%d, want 3/2", changeCount, activityCount)
	}
}

func TestRunForkSourceFreezeRollbackLeavesNoPartialOutcome(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityBundleSourceContext()
	now := time.Now().UTC().Truncate(time.Microsecond)
	lineage := seedRunForkSourceFreezePair(t, db, "running", "completed", now)
	card := newDecisionCardTestCard(t, lineage.SourceRunID, now)
	if err := pg.CreateDecisionCard(ctx, card); err != nil {
		t.Fatal(err)
	}

	err := commitRunForkSourceFreezeForTest(ctx, db, lineage, now.Add(time.Second), true)
	if err == nil || !strings.Contains(err.Error(), "fork_run_activation_not_applied") {
		t.Fatalf("source freeze injected failure = %v", err)
	}
	var sourceStatus, childStatus string
	var continuedAs sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, continued_as_run_id::text FROM runs WHERE run_id = $1::uuid`, lineage.SourceRunID).Scan(&sourceStatus, &continuedAs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, lineage.ForkRunID).Scan(&childStatus); err != nil {
		t.Fatal(err)
	}
	persisted, err := pg.GetDecisionCard(ctx, card.CardID)
	if err != nil {
		t.Fatal(err)
	}
	var activities int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id IN ($1::uuid, $2::uuid)`, lineage.SourceRunID, lineage.ForkRunID).Scan(&activities); err != nil {
		t.Fatal(err)
	}
	if sourceStatus != "running" || childStatus != "completed" || continuedAs.Valid || persisted.Status != decisioncard.StatusPending || activities != 1 {
		// The pending card's create occurrence is the only expected activity.
		t.Fatalf("rollback outcome source=%s child=%s continued_as=%v card=%s activities=%d", sourceStatus, childStatus, continuedAs, persisted.Status, activities)
	}
}

func TestRunForkSourceFreezeRequiresConfirmationBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityBundleSourceContext()
	now := time.Now().UTC().Truncate(time.Microsecond)
	lineage := seedRunForkSourceFreezePair(t, db, "running", RunForkMaterializedStatus, now)

	err := commitRunForkSourceFreezeForTest(ctx, db, lineage, now.Add(time.Second), false)
	if !errors.Is(err, ErrRunForkSourceFreezeConfirmationRequired) {
		t.Fatalf("missing confirmation error = %v", err)
	}
	assertRunForkSourceFreezeLifecycleUnchanged(t, db, lineage, "running", RunForkMaterializedStatus)
}

func TestRunForkSourceFreezeRejectsCompletedSourceWithoutConfirmationCeremony(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityBundleSourceContext()
	now := time.Now().UTC().Truncate(time.Microsecond)
	lineage := seedRunForkSourceFreezePair(t, db, "completed", RunForkMaterializedStatus, now)

	err := commitRunForkSourceFreezeForTest(ctx, db, lineage, now.Add(time.Second), false)
	if !errors.Is(err, storerunlifecycle.ErrRunNotActive) || errors.Is(err, ErrRunForkSourceFreezeConfirmationRequired) {
		t.Fatalf("completed source error = %v", err)
	}
	assertRunForkSourceFreezeLifecycleUnchanged(t, db, lineage, "completed", RunForkMaterializedStatus)
}

func TestRunForkSourceFreezeBlocksOnlyLiveExecutionAuthority(t *testing.T) {
	tests := []struct {
		name        string
		blockerName string
		seed        func(*testing.T, context.Context, *sql.DB, runForkActivationLineage, time.Time, bool)
	}{
		{name: "delivery", blockerName: "claimed_delivery", seed: seedRunForkFreezeDeliveryAuthority},
		{name: "session", blockerName: "leased_session", seed: seedRunForkFreezeSessionAuthority},
		{name: "activity", blockerName: "started_activity", seed: seedRunForkFreezeActivityAuthority},
		{name: "directive", blockerName: "directive_operation", seed: seedRunForkFreezeDirectiveAuthority},
		{name: "external_effect", blockerName: "managed_external_attempt", seed: seedRunForkFreezeExternalEffectAuthority},
	}
	for _, test := range tests {
		test := test
		for _, live := range []bool{true, false} {
			live := live
			label := "historical"
			if live {
				label = "live"
			}
			t.Run(test.name+"/"+label, func(t *testing.T) {
				_, db, _ := testutil.StartPostgres(t)
				ctx := testAuthorActivityBundleSourceContext()
				now := time.Now().UTC().Truncate(time.Microsecond)
				lineage := seedRunForkSourceFreezePair(t, db, "running", RunForkMaterializedStatus, now)
				test.seed(t, ctx, db, lineage, now, live)

				err := commitRunForkSourceFreezeForTest(ctx, db, lineage, now.Add(time.Second), true)
				if live {
					if !errors.Is(err, ErrRunForkSourceFreezeBusy) || !strings.Contains(err.Error(), test.blockerName) {
						t.Fatalf("live authority error = %v, want %s", err, test.blockerName)
					}
					assertRunForkSourceFreezeLifecycleUnchanged(t, db, lineage, "running", RunForkMaterializedStatus)
					return
				}
				if err != nil {
					t.Fatalf("historical authority blocked freeze: %v", err)
				}
				var sourceStatus, continuedAs string
				if err := db.QueryRowContext(ctx, `SELECT status, continued_as_run_id::text FROM runs WHERE run_id = $1::uuid`, lineage.SourceRunID).Scan(&sourceStatus, &continuedAs); err != nil {
					t.Fatal(err)
				}
				if sourceStatus != RunForkSourceFrozenStatus || continuedAs != lineage.ForkRunID {
					t.Fatalf("historical freeze outcome = %s/%s", sourceStatus, continuedAs)
				}
			})
		}
	}
}

func seedRunForkFreezeDeliveryAuthority(t *testing.T, ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, live bool) {
	t.Helper()
	eventID := uuid.NewString()
	seedPostgresRootEventRecordFixture(
		t, ctx, db, eventID, lineage.SourceRunID, events.EventType("freeze.delivery"),
		events.EventProducerPlatform, "test", "", "", now,
	)
	status := "pending"
	activeSession := any(nil)
	startedAt := any(nil)
	if live {
		status = "in_progress"
		activeSession = uuid.NewString()
		startedAt = now
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, started_at, created_at)
		VALUES ($1::uuid, $2::uuid, 'agent', 'freeze-agent', $3, $4::uuid, $5, $6)
	`, lineage.SourceRunID, eventID, status, activeSession, startedAt, now); err != nil {
		t.Fatal(err)
	}
}

func seedRunForkFreezeSessionAuthority(t *testing.T, ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, live bool) {
	t.Helper()
	agentID := "freeze-session-agent"
	sessionID := uuid.NewString()
	seedRunForkSessionProjection(t, db, lineage.SourceRunID, agentID, sessionID, "active", now)
	leaseHolder := any(nil)
	leaseExpiry := any(nil)
	if live {
		leaseHolder = "freeze-worker"
		leaseExpiry = now.Add(time.Minute)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agent_sessions SET lease_holder = $2, lease_expires_at = $3 WHERE session_id = $1::uuid`, sessionID, leaseHolder, leaseExpiry); err != nil {
		t.Fatal(err)
	}
}

func seedRunForkFreezeActivityAuthority(t *testing.T, ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, live bool) {
	t.Helper()
	status := "succeeded"
	resultEventID := any(uuid.NewString())
	resultEventType := any("activity.succeeded")
	resultPayload := any(`{"ok":true}`)
	completedAt := any(now)
	if live {
		status = "started"
		resultEventID, resultEventType, resultPayload, completedAt = nil, nil, nil, nil
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO activity_attempts (
			request_event_id, run_id, execution_mode, node_id, handler_event_key, activity_id, tool, effect_class,
			attempt, status, success_event, failure_event, result_event_id, result_event_type,
			result_payload, input_hash, loop_generation, started_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'live', 'freeze-node', 'freeze.handler', 'freeze-activity', 'test.tool',
			'non_idempotent_write', 1, $3, 'activity.succeeded', 'activity.failed', $4::uuid, $5,
			$6::jsonb, 'input-hash', '{}'::jsonb, $7, $8, $7
		)
	`, uuid.NewString(), lineage.SourceRunID, status, resultEventID, resultEventType, resultPayload, now, completedAt); err != nil {
		t.Fatal(err)
	}
}

func seedRunForkFreezeDirectiveAuthority(t *testing.T, ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, live bool) {
	t.Helper()
	eventID := uuid.NewString()
	if err := insertPostgresCanonicalEventRecordFixture(ctx, db, eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformAgentDirective, "operator", "", []byte(`{}`), 0,
		lineage.SourceRunID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, now,
	)); err != nil {
		t.Fatal(err)
	}
	state := "succeeded"
	response := any(`{"ok":true}`)
	completedAt := any(now)
	if live {
		state, response, completedAt = "executing", nil, nil
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_directive_operations (
			operation_id, method, actor_token_id, request_hash, agent_id, directive_text,
			resolved_run_id, run_id_resolution, source, directive_event_id, state,
			response, completed_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'agent.send_directive', 'operator', 'hash', 'freeze-agent', 'continue',
			$2::uuid, 'specified', 'v1_rpc', $3::uuid, $4, $5::jsonb, $6, $7, $7
		)
	`, uuid.NewString(), lineage.SourceRunID, eventID, state, response, completedAt, now); err != nil {
		t.Fatal(err)
	}
}

func seedRunForkFreezeExternalEffectAuthority(t *testing.T, ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, live bool) {
	t.Helper()
	agentID := "freeze-effect-agent"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, status, created_at)
		VALUES ($1, 'freeze/effect', 'worker', 'standard', 'mock', TRUE, 'authored', 'active', $2)
	`, agentID, now); err != nil {
		t.Fatal(err)
	}
	operationID := uuid.NewString()
	operationState := "settled"
	attemptState := "settled"
	leaseExpiry := now.Add(-time.Minute)
	completedAt := any(now)
	if live {
		operationState, attemptState, leaseExpiry, completedAt = "launched", "launched", now.Add(time.Minute), nil
	}
	turnID, sessionID := uuid.NewString(), uuid.NewString()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{RuntimeEpoch: 1, AgentID: agentID, Generation: 1},
		"freeze-worker",
		leaseExpiry,
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: lineage.SourceRunID,
		AgentID: agentID, SessionID: sessionID, Memory: agentmemory.PlatformDefault(), FlowInstance: "freeze/effect",
	}
	capabilitySurface := managedCompletionTestSurface(t, authority, "test")
	if err := (admitTestPostgresStore(t, db)).SaveManagedCapabilitySurface(ctx, capabilitySurface); err != nil {
		t.Fatalf("seed source-freeze external-effect capability surface: %v", err)
	}
	capabilityPlanFingerprint, err := capabilitySurface.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint source-freeze external-effect capability plan: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_operations (
			operation_id, effect_kind, effect_class, execution_mode, bundle_hash, authority_kind, authority_id,
			agent_id, runtime_epoch, generation, capability_plan_fingerprint, authority_evidence, lineage, request_fingerprint,
			state, created_at, updated_at, completed_at
		) VALUES (
			$1::uuid, 'tool', 'write_or_unknown', 'live', $2, 'normal_agent', $3,
			$3, 1, 1, $4, '{}'::jsonb, jsonb_build_object('run_id', $5::text), 'fingerprint',
			$6, $7, $7, $8
		)
	`, operationID, authorActivityTestBundleHash, agentID, capabilityPlanFingerprint, lineage.SourceRunID, operationState, now, completedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, execution_mode,
			runtime_epoch, generation, execution_owner, lease_expires_at, fence_generation,
			usage_target_kind, usage_target_id, capability_surface_id,
			state, evidence, authorized_at, launched_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 1, 'test', 'process', 'live', 1, 1, 'freeze-worker', $3, 1,
			'agent_turn', $4::uuid, $5::uuid,
			$6, '{}'::jsonb, $7::timestamptz, CASE WHEN $6 = 'launched' THEN $7::timestamptz ELSE NULL END, $8::timestamptz, $7::timestamptz
		)
	`, uuid.NewString(), operationID, leaseExpiry, turnID, capabilitySurface.ID, attemptState, now, completedAt); err != nil {
		t.Fatal(err)
	}
}

func seedRunForkSourceFreezePair(t *testing.T, db *sql.DB, sourceStatus, forkStatus string, now time.Time) runForkActivationLineage {
	t.Helper()
	lineage := runForkActivationLineage{
		SourceRunID: uuid.NewString(), ForkRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
		ForkEventName: "source.freeze.requested", ForkEventTime: now, ForkEventRevision: 1,
		SourceRunStatus: sourceStatus, ForkStatus: forkStatus,
		SourceBundleHash: authorActivityTestBundleHash, ForkBundleHash: authorActivityTestBundleHash,
	}
	endedAt := any(nil)
	if sourceStatus != "running" && sourceStatus != "paused" {
		endedAt = now
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at, ended_at)
		VALUES ($1::uuid, $2, $3, 'ephemeral', $4, $5)
	`, lineage.SourceRunID, sourceStatus, authorActivityTestBundleHash, now.Add(-time.Hour), endedAt); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runs (run_id, status, forked_from_run_id, forked_from_event_id, bundle_hash, bundle_source, started_at, ended_at)
		VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5, 'ephemeral', $6::timestamptz, CASE WHEN $2 IN ('running', 'paused') THEN NULL ELSE $6::timestamptz END)
	`, lineage.ForkRunID, forkStatus, lineage.SourceRunID, lineage.ForkEventID, authorActivityTestBundleHash, now); err != nil {
		t.Fatalf("seed fork run: %v", err)
	}
	return lineage
}

func commitRunForkSourceFreezeForTest(ctx context.Context, db *sql.DB, lineage runForkActivationLineage, now time.Time, confirmed bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	storyctx, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		return err
	}
	if err := applyRunForkSourceFreeze(storyctx, tx, lineage, now, confirmed); err != nil {
		return err
	}
	return commitRunForkAuthorActivityTransaction(storyctx, tx)
}

func assertRunForkSourceFreezeLifecycleUnchanged(t *testing.T, db *sql.DB, lineage runForkActivationLineage, sourceStatus, childStatus string) {
	t.Helper()
	var gotSource, gotChild string
	var continuedAs sql.NullString
	if err := db.QueryRow(`SELECT status, continued_as_run_id::text FROM runs WHERE run_id = $1::uuid`, lineage.SourceRunID).Scan(&gotSource, &continuedAs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM runs WHERE run_id = $1::uuid`, lineage.ForkRunID).Scan(&gotChild); err != nil {
		t.Fatal(err)
	}
	if gotSource != sourceStatus || gotChild != childStatus || continuedAs.Valid {
		t.Fatalf("lifecycle mutated source=%s child=%s continued_as=%v", gotSource, gotChild, continuedAs)
	}
}
