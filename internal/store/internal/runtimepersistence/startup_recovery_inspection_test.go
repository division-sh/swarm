package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

type startupRecoveryInspector interface {
	InspectStartupRecovery(context.Context, time.Time, bool) (effects.StartupRecoveryCounts, error)
	InspectSnapshot(context.Context, func(context.Context) error) error
}

func TestStartupRecoveryInspectionMatchesSettlementBothStores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	if err := testplanning.BuildGoProduct(t.Context(), pipeline.WorkflowRepoRoot(), os.Getenv("SWARM_TEST_BUILD_CACHE"), binary, os.Getenv("SWARM_TEST_PROOF_PROFILE"), "build", "./cmd/swarm"); err != nil {
		t.Fatal(err)
	}
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		reader := fixture.store.(startupRecoveryInspector)
		now := time.Now().UTC()
		zero, err := reader.InspectStartupRecovery(context.Background(), now, true)
		if err != nil || zero != (effects.StartupRecoveryCounts{}) {
			t.Fatalf("empty recovery selection: %+v %v", zero, err)
		}
		resume, _ := beginReadOnlyProviderCallForTest(t, fixture)
		setCompletionAttemptLease(t, fixture, resume.Attempt().AttemptID, now.Add(-time.Minute))
		dead := newCompletionSettlementFixture(t, fixture.store, fixture.db, fixture.sqlite)
		deadCall := beginObservedCompletionForSettlementTest(t, dead.context, "anthropic_api", "unresumable")
		setCompletionAttemptLease(t, dead, deadCall.Attempt().AttemptID, now.Add(-time.Minute))
		exhausted := newCompletionSettlementFixture(t, fixture.store, fixture.db, fixture.sqlite)
		failed, err := exhausted.store.(deliverylifecycle.Store).SettleFailure(exhausted.original, exhausted.origin, deliverylifecycle.Settlement{
			Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Nanosecond,
			RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(max(0, time.Until(failed.NextEligibleAt)) + time.Millisecond)
		event := loadCompletionOriginEvent(t, exhausted, failed.EventID)
		claimed, err := claimDeliveryFixture(exhausted.original, exhausted.store, event, events.DeliveryRoute{
			Recipient: events.MustAgentDeliveryRecipient(exhausted.agentID), AgentIdentity: exhausted.authority.Normal.Identity,
		})
		if err != nil {
			t.Fatal(err)
		}
		exhausted.origin = claimed.Claim
		exhaustedCall, _ := beginReadOnlyProviderCallForTest(t, exhausted)
		setCompletionAttemptLease(t, exhausted, exhaustedCall.Attempt().AttemptID, now.Add(-time.Minute))
		neutral := newNeutralEffectParityFixture(t, fixture.store, fixture.db, fixture.sqlite)
		httpCall := beginNeutralRecoveryAttempt(t, neutral, "authored_http_tool", "inspection", true, false)
		for _, attemptID := range []string{resume.Attempt().AttemptID, deadCall.Attempt().AttemptID, exhaustedCall.Attempt().AttemptID, httpCall.Attempt().AttemptID} {
			setCompletionAttemptLease(t, fixture, attemptID, now.Add(time.Minute))
			if !completionAttemptLease(t, fixture, attemptID).After(now) {
				t.Fatalf("inspection fixture attempt %s already expired", attemptID)
			}
		}
		want := effects.StartupRecoveryCounts{ProviderTurnsToResume: 1, ProviderTurnsToDeadLetter: 2, HTTPToolCallsUnknown: 1}
		before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
		requireStartupRecoveryCLI(t, fixture, binary)
		// A native recovery row lock must not block an observational read.
		if !fixture.sqlite {
			locked, err := fixture.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer locked.Rollback()
			var state string
			if err := locked.QueryRow(`SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=$1 FOR UPDATE`, resume.Attempt().AttemptID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			bounded, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			got, err := reader.InspectStartupRecovery(bounded, now, true)
			cancel()
			if err != nil || got != want {
				t.Fatalf("inspection waited for recovery row lock: %+v %v", got, err)
			}
			if err := locked.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		var expired context.Context
		if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			expired = ctx
			got, err := reader.InspectStartupRecovery(ctx, now, true)
			if err != nil || got != want {
				t.Fatalf("seeded recovery selection: %+v %v, want %+v", got, err, want)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := reader.InspectStartupRecovery(canceled, now, true); !errors.Is(err, context.Canceled) {
				t.Fatalf("inspection lost cancellation: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.InspectStartupRecovery(expired, now, true); err == nil {
			t.Fatal("inspection retained an expired snapshot")
		}
		historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
		for _, owned := range []struct {
			fixture   completionSettlementFixture
			attemptID string
		}{{fixture, resume.Attempt().AttemptID}, {dead, deadCall.Attempt().AttemptID}, {exhausted, exhaustedCall.Attempt().AttemptID}} {
			root, present, err := correlation.RootAdmissionFromContext(owned.fixture.original)
			if err != nil || !present {
				t.Fatalf("takeover source admission: present=%v err=%v", present, err)
			}
			fact, present := correlation.SourceArtifactFactFromContext(owned.fixture.original)
			if !present {
				t.Fatal("takeover source fact is absent")
			}
			takeover, request := providerDrainStartupTakeover(t, owned.fixture)
			if !completionAttemptLease(t, owned.fixture, owned.attemptID).After(request.Now) {
				t.Fatalf("successor recovery lease expired before takeover for %s", owned.attemptID)
			}
			ctx := correlation.WithRootAdmission(correlation.WithSourceArtifactFact(testAuthorActivityContext(), fact), root)
			result, err := takeover.CommitAgentLifecycleTransition(ctx, request)
			if err != nil || result.ProviderDrainCount != 1 {
				t.Fatalf("unexpired successor recovery: result=%+v err=%v", result, err)
			}
		}
		summary, err := fixture.store.ReconcileExternalEffectAttempts(fixture.original, liveExternalEffectRecoveryRequest(now))
		if err != nil || summary.OutcomeUncertain != 1 || summary.PrelaunchTerminal != 0 {
			t.Fatalf("same-row native recovery: %+v %v", summary, err)
		}
		requireOriginDeliveryOutcome(t, fixture, "failed", "effect_recovery_outcome_unconfirmed")
		requireOriginDeliveryOutcome(t, dead, "dead_letter", "effect_recovery_outcome_unconfirmed")
		requireOriginDeliveryOutcome(t, exhausted, "dead_letter", "retry_exhausted")
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, httpCall.Attempt().AttemptID, effects.StateOutcomeUncertain)
		zero, err = reader.InspectStartupRecovery(context.Background(), now, true)
		if err != nil || zero != (effects.StartupRecoveryCounts{}) {
			t.Fatalf("settled rows still counted: %+v %v", zero, err)
		}
	})
}

func requireStartupRecoveryCLI(t *testing.T, fixture completionSettlementFixture, binary string) {
	t.Helper()
	backend, document := "sqlite", ""
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "TMPDIR=" + os.Getenv("TMPDIR"), "NO_COLOR=1"}
	if fixture.sqlite {
		var sequence int
		var name, path string
		if err := fixture.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil || name != "main" {
			t.Fatalf("read exact fixture database: name=%q err=%v", name, err)
		}
		document = fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %q\n", path)
	} else {
		backend = "postgres"
		parameters, err := testpostgres.ParseConnectionParameters(os.Getenv(testpostgres.SourceEnv))
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.db.QueryRow(`SELECT current_database()`).Scan(&parameters.Database); err != nil {
			t.Fatal(err)
		}
		document = fmt.Sprintf("store:\n  backend: postgres\ndatabase:\n  host: %q\n  port: %d\n  name: %q\n  user: %q\n  password_env: INSPECTION_TEST_PASSWORD\n  sslmode: %q\n", parameters.Host, parameters.Port, parameters.Database, parameters.User, parameters.SSLMode)
		env = append(env, "INSPECTION_TEST_PASSWORD="+parameters.Password)
	}
	config := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(config, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	process, err := agentfixture.ProcessCapability(t, fixture.original, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "store", "status", "--config", config, "--store", backend)
	command.Dir, command.Env = pipeline.WorkflowRepoRoot(), env
	output, err := command.CombinedOutput()
	want := fmt.Sprintf("Project owner: selected-store lease is held; last admitted serve %s (generation %d); interrupted: 0 provider turns to resume, 0 provider turns to dead-letter, 0 HTTP tool calls outcome unknown\n", stamp.RuntimeInstanceID, stamp.AuthorityGeneration)
	if err != nil || string(output) != want {
		t.Fatalf("public held-store count read: %v\ngot=%s\nwant=%s", err, output, want)
	}
	if err := process.ProveCurrent(fixture.original); err != nil {
		t.Fatalf("inspection released native possession: %v", err)
	}
}

func TestStartupRecoveryInspectionRejectsCanceledCurrentOriginBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := effects.WithTurnTimeout(providerDrainContext(t, fixture, "current-cancellation"), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "current-cancellation")
		clock, _ := handle.LogicalTurnClock()
		intent, err := fixture.store.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
		if err != nil || !intent.Requested {
			t.Fatalf("request timeout: %+v %v", intent, err)
		}
		now := time.Now().UTC()
		setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, now.Add(-time.Minute))
		before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
		_, inspectionErr := fixture.store.(startupRecoveryInspector).InspectStartupRecovery(context.Background(), now, true)
		_, recoveryErr := fixture.store.ReconcileExternalEffectAttempts(fixture.original, liveExternalEffectRecoveryRequest(now))
		for _, err := range []error{inspectionErr, recoveryErr} {
			if err == nil || !strings.Contains(err.Error(), "authored cancellation requires canceled-origin settlement") {
				t.Fatalf("current canceled origin did not retain recovery's refusal: %v", err)
			}
		}
		historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
	})
}
