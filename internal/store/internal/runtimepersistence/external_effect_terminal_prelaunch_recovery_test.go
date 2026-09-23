package runtimepersistence

import (
	"context"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func forEachTerminalEffectBackend(t *testing.T, run func(*testing.T, neutralEffectParityFixture)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		run(t, newNeutralEffectParityFixture(t, store, store.backend.ConstructionHandle(), true))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		run(t, newNeutralEffectParityFixture(t, admitTestPostgresStore(t, db), db, false))
	})
}

func terminalizeNeutralEffectRun(t *testing.T, fixture neutralEffectParityFixture) {
	t.Helper()
	selected, ok := fixture.store.(interface {
		ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	})
	if !ok {
		t.Fatalf("selected store %T lacks run quiescence", fixture.store)
	}
	runID := fixture.authority.Normal.Identity.RunID
	quiesced, err := selected.ApplyActiveRunQuiescence(testAuthorActivityContext(), runtimerunquiescence.Request{
		OperationName: "effect_terminal_prelaunch_recovery", RequestedAt: time.Now().UTC(),
		RunIDs: []string{runID}, ReasonCode: runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy: "external-effect-recovery-test", DeliveryNote: "terminal prelaunch recovery proof",
	})
	if err != nil || len(quiesced.Runs) != 1 || !quiesced.Runs[0].Changed {
		t.Fatalf("terminalize effect run: result=%+v err=%v", quiesced, err)
	}
}

func TestExternalEffectTerminalRunRecoversOnlyPrelaunchNonProvider(t *testing.T) {
	forEachTerminalEffectBackend(t, func(t *testing.T, fixture neutralEffectParityFixture) {
		attempts := beginGenericRecoveryPostureMatrix(t, fixture, executionmode.Live)
		terminalizeNeutralEffectRun(t, fixture)
		before := make([]externalEffectRecoverySnapshot, len(attempts))
		for i, attempt := range attempts {
			before[i] = readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, attempt)
		}
		summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC().Add(time.Minute)))
		if err != nil || summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 0 {
			t.Fatalf("terminal generic recovery = %#v err=%v, want 1/0", summary, err)
		}
		for i, attempt := range attempts {
			after := readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, attempt)
			if after.CompletionDueAt != before[i].CompletionDueAt || after.CompletionRevision != before[i].CompletionRevision {
				t.Fatalf("terminal run completion candidate changed for %s: before=%#v after=%#v", attempt.Initial, before[i], after)
			}
			if attempt.Initial == runtimeeffects.StateAuthorized {
				if after.AttemptState != string(runtimeeffects.StateTerminalFailure) || after.OperationState != string(runtimeeffects.StateTerminalFailure) || after.StoryRows != before[i].StoryRows+1 {
					t.Fatalf("terminal prelaunch recovery = %#v, want terminal failure story", after)
				}
			} else if after != before[i] {
				t.Fatalf("terminal %s attempt was broadened into recovery: before=%#v after=%#v", attempt.Initial, before[i], after)
			}
		}
		summary, err = fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC().Add(2*time.Minute)))
		if err != nil || summary != (runtimeeffects.RecoverySummary{}) {
			t.Fatalf("repeat terminal recovery = %#v err=%v, want no-op", summary, err)
		}
	})
}

func TestExternalEffectTerminalRunDirectSettlementDoesNotRequestCompletion(t *testing.T) {
	forEachTerminalEffectBackend(t, func(t *testing.T, fixture neutralEffectParityFixture) {
		handle := beginNeutralRecoveryAttempt(t, fixture, "authored_http_tool", "terminal-direct-settlement", false, false)
		attempt := handle.Attempt()
		terminalizeNeutralEffectRun(t, fixture)
		posture := recoveryPostureAttempt{AttemptID: attempt.AttemptID, RunID: fixture.authority.Normal.Identity.RunID, Initial: runtimeeffects.StateAuthorized}
		before := readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, posture)
		failureErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "effect_test_terminal_failure", "external-effects", "settle_attempt", nil)
		failure, ok := runtimefailures.EnvelopeFromError(failureErr)
		if !ok {
			t.Fatal("construct settlement failure")
		}
		// Recovery settlement has no live execution authority after run quiescence.
		err := fixture.store.SettleExternalAttempt(testAuthorActivityContext(), runtimeeffects.Settlement{
			OperationID: attempt.OperationID, AttemptID: attempt.AttemptID,
			State: runtimeeffects.StateTerminalFailure, Failure: &failure, Now: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("settle authorized attempt after terminal run: %v", err)
		}
		after := readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, posture)
		if after.AttemptState != string(runtimeeffects.StateTerminalFailure) || after.OperationState != string(runtimeeffects.StateTerminalFailure) {
			t.Fatalf("terminal direct settlement = %#v, want terminal failure", after)
		}
		if after.CompletionDueAt != before.CompletionDueAt || after.CompletionRevision != before.CompletionRevision {
			t.Fatalf("terminal direct settlement requested completion: before=%#v after=%#v", before, after)
		}
	})
}

func TestExternalEffectTerminalPrelaunchSelectorParksStillCurrentStandingOwner(t *testing.T) {
	forEachTerminalEffectBackend(t, func(t *testing.T, fixture neutralEffectParityFixture) {
		selected, ok := fixture.store.(workflowTestSelectedStore)
		if !ok {
			t.Fatalf("selected store %T lacks standing persistence", fixture.store)
		}
		requireStoreTestPersistedBundle(t, fixture.db, runLifecycleCandidateParityBundleHash)
		workflow := newPostgresWorkflowTestCoordinator(t, fixture.db, selected)
		if fixture.sqlite {
			workflow = newSQLiteWorkflowTestCoordinator(t, fixture.db, selected)
		}
		flowPath := "effect-recovery/still-current/" + uuid.NewString()
		candidate := runtimepipeline.StandingServiceCandidate{
			ServiceID: runtimeflowidentity.StandingServiceID(flowPath), FlowPath: flowPath,
			InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
			Source: mustStoreTestSourceArtifactFact(runLifecycleCandidateParityBundleHash),
		}
		standing, err := workflow.ReconcileStandingService(testAuthorActivityRuntimeContext(), candidate)
		if err != nil {
			t.Fatalf("create standing recovery owner: %v", err)
		}
		fixture = bindNeutralEffectFixtureToRun(t, fixture, standing.RunID)
		handle := beginNeutralRecoveryAttempt(t, fixture, "authored_http_tool", "still-current", false, false)
		setStandingRecoveryOwnerDesiredState(t, fixture.db, fixture.sqlite, standing.ServiceID, "suspended", "suspended")
		attempt := recoveryPostureAttempt{AttemptID: handle.Attempt().AttemptID, RunID: standing.RunID, Initial: runtimeeffects.StateAuthorized}
		before := readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, attempt)
		summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC().Add(time.Minute)))
		if err != nil || summary != (runtimeeffects.RecoverySummary{}) {
			t.Fatalf("still-current owner recovery = %#v err=%v, want no-op", summary, err)
		}
		if after := readExternalEffectRecoverySnapshot(t, fixture.db, fixture.sqlite, attempt); after != before {
			t.Fatalf("still-current standing owner was mutated: before=%#v after=%#v", before, after)
		}
	})
}
