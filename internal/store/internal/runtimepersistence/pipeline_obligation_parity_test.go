package runtimepersistence

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"path/filepath"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type pipelineObligationParityStore interface {
	semanticEventFixtureStore
	diagnosticRuntimeLogFixtureStore
	PipelineObligations() runtimepipelineobligation.Store
}

func TestPipelineObligationSQLitePostgresParityMatrix(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected, ok := fixture.store.(pipelineObligationParityStore)
			if !ok {
				t.Fatalf("%T does not expose the canonical pipeline obligation owner", fixture.store)
			}
			ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), fixture.store)

			t.Run("age_independent_selection_and_global_presence", func(t *testing.T) {
				provePipelineAgeIndependentSelection(t, ctx, fixture, selected)
			})
			t.Run("exact_payload_recovery_claim", func(t *testing.T) {
				provePipelineExactPayloadRecovery(t, ctx, fixture, selected)
			})
			t.Run("claim_exclusion_release_and_stale_fencing", func(t *testing.T) {
				provePipelineClaimLifecycle(t, ctx, fixture, selected)
			})
			t.Run("missing_and_invalid_scope_fail_closed", func(t *testing.T) {
				provePipelineScopeFailure(t, ctx, fixture, selected)
			})
			t.Run("malformed_recovery_is_preclassified_before_dispatch", func(t *testing.T) {
				provePipelineMalformedRecoveryPreclassification(t, ctx, fixture, selected)
			})
			t.Run("typed_run_summary", func(t *testing.T) {
				provePipelineRunSummary(t, ctx, fixture, selected)
			})
			t.Run("decision_route_dispositions", func(t *testing.T) {
				provePipelineDecisionRouteDispositions(t, ctx, fixture, selected)
			})
			t.Run("receipt_family_separation", func(t *testing.T) {
				provePipelineReceiptFamilySeparation(t, ctx, fixture, selected)
			})
			t.Run("parent_terminalization_is_claim_fenced", func(t *testing.T) {
				provePipelineParentTerminalizationFence(t, ctx, fixture, selected)
			})
			t.Run("settlement_failure_rolls_back_and_retains_claim", func(t *testing.T) {
				provePipelineSettlementRollback(t, ctx, fixture, selected)
			})
			t.Run("committed_settlement_survives_cleanup_failure", func(t *testing.T) {
				provePipelineCommittedSettlementCleanupOutcome(t, ctx, fixture, selected)
			})
		})
	}
}

func provePipelineExactPayloadRecovery(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	payload := []byte("{\n  \"value\": 1.0, \"order\": [2, 1]\n}")
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("recovery.payload_bytes"), "runtime", "", payload,
		0, runID, "", events.EventEnvelope{}, time.Now().UTC().Add(-time.Minute),
	)
	wantAdmission, err := eventtest.PayloadAdmission(event, event.RoutingSource().Route().FlowID, string(event.Type()))
	if err != nil {
		t.Fatalf("build recovery payload admission: %v", err)
	}
	if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
		t.Fatalf("commit recovery event: %v", err)
	}
	work, err := selected.PipelineObligations().ClaimEvent(ctx, event.ID(), runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim recovery event: %v", err)
	}
	if got := work.Event.Payload(); !bytes.Equal(got, payload) {
		t.Fatalf("recovery payload = %q, want %q", got, payload)
	}
	gotAdmission, ok := work.Event.PayloadAdmission()
	if !ok || !gotAdmission.Binding().Equal(wantAdmission.Binding()) {
		t.Fatalf("recovery payload binding = %#v/%v, want exact persisted binding", gotAdmission.Binding(), ok)
	}
	if _, err := selected.PipelineObligations().Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed")); err != nil {
		t.Fatalf("settle recovery event: %v", err)
	}
	requireCompleteRunForkRevision(t, ctx, fixture, runID)
}

func provePipelineCommittedSettlementCleanupOutcome(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
	owner := selected.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim pipeline event: %v", err)
	}
	injectedErr := errors.New("injected pipeline settlement cleanup failure")
	switch store := selected.(type) {
	case *SQLiteRuntimeStore:
		store.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(func() error { return injectedErr })
		defer store.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(nil)
	case *PostgresStore:
		state, err := store.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(work.Claim)
		if err != nil {
			t.Fatalf("load postgres pipeline claim state: %v", err)
		}
		session := state.LeaseForTest().Session()
		session.SetEndTxErrorForTest(func() error { return injectedErr })
		defer func() {
			session.SetEndTxErrorForTest(nil)
		}()
	default:
		t.Fatalf("unsupported pipeline settlement fixture %T", selected)
	}

	outcome, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed"))
	if !errors.Is(err, injectedErr) {
		t.Fatalf("settlement error = %v, want cleanup failure", err)
	}
	if !outcome.Committed() || !outcome.DeliveryHandoffCommitted() {
		t.Fatalf("settlement outcome = committed:%v handoff:%v, want true/true",
			outcome.Committed(), outcome.DeliveryHandoffCommitted())
	}
	if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
		t.Fatalf("committed settlement remained replayable after cleanup failure: %v", err)
	}
}

func provePipelineAgeIndependentSelection(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	createdAt := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, createdAt)
	owner := selected.PipelineObligations()

	presence, err := owner.GlobalWorkPresence(ctx)
	if err != nil {
		t.Fatalf("GlobalWorkPresence: %v", err)
	}
	if !presence.ProcessingEligible || presence.DecisionRouteDue || presence.OldestEligibleEvent.IsZero() {
		t.Fatalf("global work presence = %#v, want old processing-eligible work only", presence)
	}
	if delta := presence.OldestEligibleEvent.Sub(createdAt); delta < -time.Second || delta > time.Second {
		t.Fatalf("oldest eligible event = %s, want %s", presence.OldestEligibleEvent, createdAt)
	}

	work, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.RunRecoveryQuery(runID))
	if err != nil || !ok {
		t.Fatalf("scan old event: ok=%v err=%v", ok, err)
	}
	if work.Event.ID() != eventID || work.Scope != runtimepipelineobligation.ScopeDirect || work.Acknowledged {
		t.Fatalf("claimed work = %#v, want exact old direct unacknowledged event %s", work, eventID)
	}
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed")); err != nil {
		t.Fatalf("Settle old event: %v", err)
	}
}

func provePipelineClaimLifecycle(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Hour))
	owner := selected.PipelineObligations()

	publication, err := owner.ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatalf("ClaimPublication: %v", err)
	}
	if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
		t.Fatalf("recovery during publication error = %v, want ErrBusy", err)
	}
	if _, err := owner.ClaimPublication(ctx, eventID); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
		t.Fatalf("second publication claim error = %v, want ErrBusy", err)
	}
	if err := owner.Release(ctx, publication); err != nil {
		t.Fatalf("release publication claim: %v", err)
	}

	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("ClaimEvent after release: %v", err)
	}
	if _, err := owner.MarkDecisionProcessed(ctx, work.Claim); !errors.Is(err, runtimepipelineobligation.ErrWrongClaim) {
		t.Fatalf("wrong-purpose decision settlement error = %v, want ErrWrongClaim", err)
	}
	if err := owner.Release(ctx, work.Claim); err != nil {
		t.Fatalf("release recovery claim: %v", err)
	}
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("stale")); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("released claim settlement error = %v, want ErrStaleClaim", err)
	}
	if err := owner.Release(ctx, work.Claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("duplicate release error = %v, want ErrStaleClaim", err)
	}

	reclaimed, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("reclaim recovery event: %v", err)
	}
	if _, err := owner.Settle(ctx, reclaimed.Claim, runtimepipelineobligation.Terminal("test_terminal", nil)); err != nil {
		t.Fatalf("settle reclaimed event: %v", err)
	}
	if _, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.RunRecoveryQuery(runID)); err != nil || ok {
		t.Fatalf("terminal event reclaimed: ok=%v err=%v", ok, err)
	}
	if _, _, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.ClaimQuery{Purpose: runtimepipelineobligation.PurposePublication}); err == nil {
		t.Fatal("scan accepted publication query")
	}
}

func provePipelineScopeFailure(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
	if err := updatePipelineScopeRaw(ctx, fixture, eventID, "guessed"); err == nil {
		t.Fatal("fresh schema accepted an invalid committed pipeline scope")
	}
	if err := deletePipelineScope(ctx, fixture, eventID); err != nil {
		t.Fatalf("delete committed scope corruption fixture: %v", err)
	}

	owner := selected.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("ClaimEvent missing scope: %v", err)
	}
	disposition, preclassified := work.PreDispatchDisposition()
	if !preclassified || disposition.Kind() != runtimepipelineobligation.DispositionQuarantined ||
		disposition.ReasonCode() != "committed_pipeline_scope_missing" {
		t.Fatalf("missing scope classification = %#v classified=%v", disposition, preclassified)
	}
	if _, err := owner.Settle(ctx, work.Claim, disposition); err != nil {
		t.Fatalf("settle missing scope classification: %v", err)
	}
	if _, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.RunRecoveryQuery(runID)); err != nil || ok {
		t.Fatalf("settled corrupt scope reclaimed: ok=%v err=%v", ok, err)
	}
	count, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 1 || outcome != "dead_letter" || reason != "committed_pipeline_scope_missing" {
		t.Fatalf("scope quarantine receipt = count:%d outcome:%q reason:%q", count, outcome, reason)
	}
}

func provePipelineMalformedRecoveryPreclassification(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := uuid.NewString()
	event := eventtest.RuntimeDiagnostic(
		eventID,
		events.EventType("platform.test_recovery"),
		"runtime",
		"",
		[]byte(`{"ok":true}`),
		0,
		runID,
		"",
		events.EventEnvelope{Scope: events.EventScopeGlobal},
		time.Now().UTC().Add(-time.Minute),
	)
	if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
		t.Fatalf("commit malformed recovery fixture: %v", err)
	}
	query := `UPDATE events SET run_id = NULL, run_owner_key = '' WHERE event_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `UPDATE events SET run_id = NULL, run_owner_key = '' WHERE event_id = $1::uuid`
	}
	if _, err := fixture.db.ExecContext(ctx, query, eventID); err != nil {
		t.Fatalf("plant malformed recovery fixture: %v", err)
	}

	owner := selected.PipelineObligations()
	work, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.GlobalRecoveryQuery())
	if err != nil || !ok || work.Event.ID() != eventID {
		t.Fatalf("claim malformed recovery: work=%s ok=%v err=%v", work.Event.ID(), ok, err)
	}
	disposition, classified := work.PreDispatchDisposition()
	if !classified || disposition.Kind() != runtimepipelineobligation.DispositionQuarantined ||
		disposition.ReasonCode() != "persisted_replay_run_identity_invalid" {
		t.Fatalf("malformed recovery disposition = %#v classified=%v", disposition, classified)
	}
	if _, err := owner.Settle(ctx, work.Claim, disposition); err != nil {
		t.Fatalf("settle malformed recovery: %v", err)
	}
	count, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 1 || outcome != "dead_letter" || reason != "persisted_replay_run_identity_invalid" {
		t.Fatalf("malformed recovery receipt = count:%d outcome:%q reason:%q", count, outcome, reason)
	}
}

func provePipelineRunSummary(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	base := time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
	owner := selected.PipelineObligations()

	_ = commitPipelineParityEvent(t, ctx, selected, runID, base)
	successID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(time.Second))
	settlePipelineParityEvent(t, ctx, owner, successID, runtimepipelineobligation.Acknowledged("processed"))
	terminalID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(2*time.Second))
	settlePipelineParityEvent(t, ctx, owner, terminalID, runtimepipelineobligation.Terminal("terminal", nil))
	deadLetterID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(3*time.Second))
	settlePipelineParityEvent(t, ctx, owner, deadLetterID, runtimepipelineobligation.DeadLetter("dead_letter", nil))
	deferredID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(4*time.Second))
	insertProducerIdentityDecisionObligation(t, fixture, ctx, deferredID, runID, base.Add(4*time.Second))
	deferred, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.DecisionRouteQuery())
	if err != nil || !ok || deferred.Event.ID() != deferredID {
		t.Fatalf("claim deferred decision route: work=%s ok=%v err=%v", deferred.Event.ID(), ok, err)
	}
	if _, err := owner.Settle(ctx, deferred.Claim, runtimepipelineobligation.Deferred("retry", time.Now().UTC().Add(time.Hour), nil)); err != nil {
		t.Fatalf("defer decision route: %v", err)
	}
	diagnostic := eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{"message":"summary evidence"}`),
		0, runID, "", events.EventEnvelope{}, base.Add(5*time.Second),
	)
	if err := commitDiagnosticRuntimeLogFixture(ctx, selected, diagnostic); err != nil {
		t.Fatalf("commit diagnostic event: %v", err)
	}

	summary, err := owner.SummarizeRun(ctx, runID)
	if err != nil {
		t.Fatalf("SummarizeRun: %v", err)
	}
	if summary.Replayable != 1 || summary.Acknowledged != 1 || summary.TerminalNonSuccess != 2 ||
		summary.Deferred != 1 || summary.DiagnosticExcluded != 1 {
		t.Fatalf("active run summary = %#v", summary)
	}
	if !summary.BlocksCompletion() {
		t.Fatalf("summary %#v should block completion", summary)
	}
	if !summary.HasOpenWork() {
		t.Fatalf("summary %#v should report open work", summary)
	}

	terminalOnlyRunID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, terminalOnlyRunID)
	terminalOnlyID := commitPipelineParityEvent(t, ctx, selected, terminalOnlyRunID, base.Add(6*time.Second))
	settlePipelineParityEvent(t, ctx, owner, terminalOnlyID, runtimepipelineobligation.DeadLetter("terminal_only", nil))
	terminalOnly, err := owner.SummarizeRun(ctx, terminalOnlyRunID)
	if err != nil {
		t.Fatalf("SummarizeRun terminal-only: %v", err)
	}
	if terminalOnly.TerminalNonSuccess != 1 || terminalOnly.HasOpenWork() || !terminalOnly.BlocksCompletion() {
		t.Fatalf("terminal-only summary = %#v", terminalOnly)
	}

}

func provePipelineDecisionRouteDispositions(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	owner := selected.PipelineObligations()
	at := time.Now().UTC().Add(-time.Minute)

	processedID := commitPipelineParityEvent(t, ctx, selected, runID, at)
	insertProducerIdentityDecisionObligation(t, fixture, ctx, processedID, runID, at)
	processed, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.DecisionRouteQuery())
	if err != nil || !ok || processed.Event.ID() != processedID {
		t.Fatalf("claim processed decision route: work=%s ok=%v err=%v", processed.Event.ID(), ok, err)
	}
	if _, err := owner.MarkDecisionProcessed(ctx, processed.Claim); err != nil {
		t.Fatalf("MarkDecisionProcessed: %v", err)
	}
	requireCompleteRunForkRevision(t, ctx, fixture, runID)
	pendingSummary, err := owner.SummarizeRun(ctx, runID)
	if err != nil || pendingSummary.Acknowledged != 1 || pendingSummary.Deferred != 1 ||
		pendingSummary.ProcessedDeferred != 1 || pendingSummary.BlocksCompletion() || !pendingSummary.HasOpenWork() {
		t.Fatalf("processed-before-convergence summary = %#v err=%v", pendingSummary, err)
	}
	if _, err := owner.Settle(ctx, processed.Claim, runtimepipelineobligation.Acknowledged("decision_route_converged")); err != nil {
		t.Fatalf("complete processed decision route: %v", err)
	}
	requireCompleteRunForkRevision(t, ctx, fixture, runID)
	if status := readDecisionRouteStatus(t, ctx, fixture, processedID); status != "completed" {
		t.Fatalf("processed decision route status = %q, want completed", status)
	}

	quarantineID := commitPipelineParityEvent(t, ctx, selected, runID, at.Add(time.Second))
	insertProducerIdentityDecisionObligation(t, fixture, ctx, quarantineID, runID, at.Add(time.Second))
	quarantined, ok, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.DecisionRouteQuery())
	if err != nil || !ok || quarantined.Event.ID() != quarantineID {
		t.Fatalf("claim quarantine decision route: work=%s ok=%v err=%v", quarantined.Event.ID(), ok, err)
	}
	if _, err := owner.Settle(ctx, quarantined.Claim, runtimepipelineobligation.Quarantined("invalid_decision", nil)); err != nil {
		t.Fatalf("quarantine decision route: %v", err)
	}
	requireCompleteRunForkRevision(t, ctx, fixture, runID)
	if status := readDecisionRouteStatus(t, ctx, fixture, quarantineID); status != "quarantined" {
		t.Fatalf("quarantined decision route status = %q", status)
	}
}

func provePipelineReceiptFamilySeparation(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
	insertWorkflowTransitionReceipt(t, ctx, fixture, eventID)

	work, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("workflow-transition receipt hid platform obligation: %v", err)
	}
	if _, err := selected.PipelineObligations().Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed")); err != nil {
		t.Fatalf("settle exact platform obligation: %v", err)
	}
	count, outcome, _ := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 1 || outcome != "success" {
		t.Fatalf("exact platform receipt = count:%d outcome:%q", count, outcome)
	}
}

func provePipelineParentTerminalizationFence(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
	owner := selected.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim parent terminalization target: %v", err)
	}

	if _, err := terminalizePipelineRunForTest(ctx, selected, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), time.Now().UTC()); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
		t.Fatalf("terminalization during claim error = %v, want ErrBusy", err)
	}
	if err := owner.Release(ctx, work.Claim); err != nil {
		t.Fatalf("release active claim: %v", err)
	}

	count, err := terminalizePipelineRunForTest(ctx, selected, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), time.Now().UTC())
	if err != nil || count != 1 {
		t.Fatalf("TerminalizeRun count=%d err=%v", count, err)
	}
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("late_success")); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("late success error = %v, want ErrStaleClaim", err)
	}
	_, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if outcome != "dead_letter" || reason != "run_stopped" {
		t.Fatalf("parent terminal receipt outcome=%q reason=%q", outcome, reason)
	}
}

func terminalizePipelineRunForTest(
	ctx context.Context,
	selected pipelineObligationParityStore,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	var count int
	err := runSelectedFixtureMutation(ctx, selected, "pipeline terminalization fixture", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		switch store := selected.(type) {
		case *PostgresStore:
			count, err = store.TerminalizeRunTx(txctx, attempt, runID, disposition, at)
		case *SQLiteRuntimeStore:
			count, err = store.TerminalizeRunTx(txctx, attempt, runID, disposition, at)
		default:
			err = fmt.Errorf("unexpected pipeline store %T", selected)
		}
		return err
	})
	return count, err
}

func provePipelineSettlementRollback(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := uuid.NewString()
	event := eventtest.PersistedProjection(
		eventID, events.EventType("test.event"), "runtime", "", []byte(`{"ok":true}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC().Add(-time.Minute),
	)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient("pipeline-handoff-rollback")}}); err != nil {
		t.Fatalf("commit pipeline handoff rollback event: %v", err)
	}
	if total, handoff := readDeliveryContinuationHandoffCounts(t, ctx, fixture, eventID); total != 1 || handoff != 0 {
		t.Fatalf("initial delivery continuation facts = total:%d handoff:%d, want total:1 handoff:0", total, handoff)
	}
	owner := selected.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim rollback target: %v", err)
	}
	removeFault := installPipelineReceiptInsertFault(t, ctx, fixture)
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed")); err == nil {
		removeFault()
		t.Fatal("forced receipt failure was ignored")
	}
	count, _, _ := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 0 {
		removeFault()
		t.Fatalf("failed settlement committed %d receipts", count)
	}
	if _, handoff := readDeliveryContinuationHandoffCounts(t, ctx, fixture, eventID); handoff != 0 {
		removeFault()
		t.Fatalf("failed settlement committed %d delivery continuation handoffs", handoff)
	}
	removeFault()
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed")); err != nil {
		t.Fatalf("claim was not retained after rollback: %v", err)
	}
	if _, handoff := readDeliveryContinuationHandoffCounts(t, ctx, fixture, eventID); handoff != 1 {
		t.Fatalf("successful settlement committed %d delivery continuation handoffs, want 1", handoff)
	}
}

func isPipelinePersistenceOwnerFile(relativePath string) bool {
	return filepath.ToSlash(filepath.Dir(relativePath)) == "internal/store/internal/backend/pipelinepersistence"
}

func TestPipelinePersistenceSQLGuardIsOwnerBounded(t *testing.T) {
	for _, scenario := range []struct {
		path string
		want bool
	}{
		{"internal/store/internal/backend/pipelinepersistence/owner_operations.go", true},
		{"internal/store/internal/backend/pipelinepersistence/standing_service.go", true},
		{"internal/store/internal/backend/pipelinepersistence/test_issue2564_evidence.go", true},
		{"internal/store/internal/runtimepersistence/legacy.go", false},
		{"internal/runtime/pipeline/worker.go", false},
		{"internal/store/internal/backend/pipelinepersistence_extra/owner.go", false},
		{"internal/store/internal/backend/pipelinepersistence/child/owner.go", false},
	} {
		t.Run(scenario.path, func(t *testing.T) {
			if got := isPipelinePersistenceOwnerFile(scenario.path); got != scenario.want {
				t.Fatalf("pipeline SQL ownership for %s = %v, want %v", scenario.path, got, scenario.want)
			}
		})
	}
}

func commitPipelineParityEvent(t *testing.T, ctx context.Context, selected pipelineObligationParityStore, runID string, at time.Time) string {
	t.Helper()
	eventID := uuid.NewString()
	event := eventtest.PersistedProjection(
		eventID, events.EventType("test.event"), "runtime", "", []byte(`{"ok":true}`),
		0, runID, "", events.EventEnvelope{}, at.UTC(),
	)
	if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
		t.Fatalf("commit pipeline parity event: %v", err)
	}
	return eventID
}

func settlePipelineParityEvent(
	t *testing.T,
	ctx context.Context,
	owner runtimepipelineobligation.Store,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
) {
	t.Helper()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim event %s: %v", eventID, err)
	}
	if _, err := owner.Settle(ctx, work.Claim, disposition); err != nil {
		t.Fatalf("settle event %s: %v", eventID, err)
	}
}

func updatePipelineScopeRaw(ctx context.Context, fixture authorActivityReceiptFixture, eventID, scope string) error {
	query := `UPDATE committed_replay_scopes SET scope = ? WHERE event_id = ?`
	args := []any{scope, eventID}
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `UPDATE committed_replay_scopes SET scope = $1 WHERE event_id = $2::uuid`
	}
	_, err := fixture.db.ExecContext(ctx, query, args...)
	return err
}

func deletePipelineScope(ctx context.Context, fixture authorActivityReceiptFixture, eventID string) error {
	query := `DELETE FROM committed_replay_scopes WHERE event_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `DELETE FROM committed_replay_scopes WHERE event_id = $1::uuid`
	}
	_, err := fixture.db.ExecContext(ctx, query, eventID)
	return err
}

func readExactPipelineReceipt(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) (int, string, string) {
	t.Helper()
	query := `
		SELECT COUNT(*), COALESCE(MAX(outcome), ''), COALESCE(MAX(reason_code), '')
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `
			SELECT COUNT(*), COALESCE(MAX(outcome), ''), COALESCE(MAX(reason_code), '')
			FROM event_receipts
			WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var count int
	var outcome, reason string
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&count, &outcome, &reason); err != nil {
		t.Fatalf("read exact platform pipeline receipt: %v", err)
	}
	return count, outcome, reason
}

func readDeliveryContinuationHandoffCounts(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	eventID string,
) (int, int) {
	t.Helper()
	query := `SELECT COUNT(*), COUNT(continuation_handoff_at) FROM event_deliveries WHERE event_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `SELECT COUNT(*), COUNT(continuation_handoff_at) FROM event_deliveries WHERE event_id = $1::uuid`
	}
	var total, handoff int
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&total, &handoff); err != nil {
		t.Fatalf("read delivery continuation handoff: %v", err)
	}
	return total, handoff
}

func readDecisionRouteStatus(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) string {
	t.Helper()
	query := `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	var status string
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&status); err != nil {
		t.Fatalf("read decision-route status: %v", err)
	}
	return status
}

func insertWorkflowTransitionReceipt(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) {
	t.Helper()
	receiptID := uuid.NewString()
	now := time.Now().UTC()
	query := `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT ?, e.event_id, 'platform', 'pipeline:workflow-node', e.entity_id, e.flow_instance,
		       'success', 'transition_processed', 'null', '{}', ?
		FROM events e WHERE e.event_id = ?`
	args := []any{receiptID, now, eventID}
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, failure, side_effects, processed_at
			)
			SELECT $1::uuid, e.event_id, 'platform', 'pipeline:workflow-node', e.entity_id, e.flow_instance,
			       'success', 'transition_processed', 'null'::jsonb, '{}'::jsonb, $2
			FROM events e WHERE e.event_id = $3::uuid`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert workflow-transition receipt: %v", err)
	}
}

func installPipelineReceiptInsertFault(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture) func() {
	t.Helper()
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		if _, err := fixture.db.ExecContext(ctx, `
			CREATE OR REPLACE FUNCTION fail_pipeline_obligation_receipt_insert() RETURNS trigger AS $$
			BEGIN
				IF NEW.subscriber_type = 'platform' AND NEW.subscriber_id = 'pipeline' THEN
					RAISE EXCEPTION 'forced pipeline receipt failure';
				END IF;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create postgres pipeline receipt fault function: %v", err)
		}
		if _, err := fixture.db.ExecContext(ctx, `
			CREATE TRIGGER fail_pipeline_obligation_receipt_insert
			BEFORE INSERT ON event_receipts
			FOR EACH ROW EXECUTE FUNCTION fail_pipeline_obligation_receipt_insert()`); err != nil {
			t.Fatalf("create postgres pipeline receipt fault trigger: %v", err)
		}
		return func() {
			if _, err := fixture.db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS fail_pipeline_obligation_receipt_insert ON event_receipts`); err != nil {
				t.Fatalf("drop postgres pipeline receipt fault trigger: %v", err)
			}
			if _, err := fixture.db.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS fail_pipeline_obligation_receipt_insert()`); err != nil {
				t.Fatalf("drop postgres pipeline receipt fault function: %v", err)
			}
		}
	}
	if _, err := fixture.db.ExecContext(ctx, `
		CREATE TRIGGER fail_pipeline_obligation_receipt_insert
		BEFORE INSERT ON event_receipts
		WHEN NEW.subscriber_type = 'platform' AND NEW.subscriber_id = 'pipeline'
		BEGIN
			SELECT RAISE(ABORT, 'forced pipeline receipt failure');
		END`); err != nil {
		t.Fatalf("create sqlite pipeline receipt fault trigger: %v", err)
	}
	return func() {
		if _, err := fixture.db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS fail_pipeline_obligation_receipt_insert`); err != nil {
			t.Fatalf("drop sqlite pipeline receipt fault trigger: %v", err)
		}
	}
}
