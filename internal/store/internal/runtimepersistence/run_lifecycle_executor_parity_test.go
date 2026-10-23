package runtimepersistence

import (
	"context"

	"errors"
	"runtime"

	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"

	"github.com/google/uuid"
)

func TestPostgresPipelineCompletionHandoffSurvivesPostCommitCleanupError(t *testing.T) {
	for _, operation := range []string{"mark_decision_processed", "settle"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			fixture := openPostgresAuthorActivityReceiptFixture(t)
			selected, ok := fixture.store.(*PostgresStore)
			if !ok {
				t.Fatalf("fixture store = %T, want *PostgresStore", fixture.store)
			}
			ctx := storeTestOriginalProcessContext(t, testAuthorActivityContext(), selected)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
			owner := selected.PipelineObligations()

			var claim runtimepipelineobligation.Claim
			var settlementOutcome runtimepipelineobligation.SettlementOutcome
			switch operation {
			case "mark_decision_processed":
				insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, time.Now().UTC().Add(-time.Minute))
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
				if err != nil {
					t.Fatalf("claim decision route: %v", err)
				}
				claim = work.Claim
			case "settle":
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("claim recovery work: %v", err)
				}
				claim = work.Claim
			default:
				t.Fatalf("unsupported operation %q", operation)
			}

			state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
			if err != nil {
				t.Fatalf("load pipeline claim state: %v", err)
			}
			injectedErr := errors.New("injected post-commit session cleanup failure")
			session := state.LeaseForTest().Session()
			session.SetEndTxErrorForTest(func() error { return injectedErr })

			switch operation {
			case "mark_decision_processed":
				settlementOutcome, err = owner.MarkDecisionProcessed(ctx, claim)
			case "settle":
				settlementOutcome, err = owner.Settle(ctx, claim, runtimepipelineobligation.Acknowledged("processed"))
			}
			session.SetEndTxErrorForTest(nil)
			if !errors.Is(err, injectedErr) {
				t.Fatalf("%s error = %v, want injected cleanup failure", operation, err)
			}
			if !settlementOutcome.Committed() || !settlementOutcome.DeliveryHandoffCommitted() {
				t.Fatalf("settlement outcome after cleanup failure = committed:%v handoff:%v, want true/true",
					settlementOutcome.Committed(), settlementOutcome.DeliveryHandoffCommitted())
			}

			if operation == "settle" {
				if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
					t.Fatalf("settled claim remained eligible after cleanup failure: %v", err)
				}
			} else {
				count, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
				if count != 1 || outcome != "success" || reason != "decision_route_processed" {
					t.Fatalf(
						"decision route receipt = count:%d outcome:%q reason:%q",
						count, outcome, reason,
					)
				}
				if state.LeaseForTest().Current() {
					t.Fatal("unsafe post-commit cleanup left the claim session reusable")
				}
				if err := owner.Release(ctx, claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
					t.Fatalf("disposed decision claim must be stale: %v", err)
				}
			}

		})
	}
}

func newRunLifecycleExecutorOccurrence(
	t *testing.T,
	process *worklifetime.Process,
) *worklifetime.RuntimeOccurrence {
	return newRunLifecycleExecutorOccurrenceForBundle(t, process, runLifecycleCandidateParityBundleHash)
}

func newRunLifecycleExecutorOccurrenceForBundle(
	t *testing.T,
	process *worklifetime.Process,
	bundleHash string,
) *worklifetime.RuntimeOccurrence {
	t.Helper()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        bundleHash,
	})
	if err != nil {
		t.Fatalf("create candidate runtime occurrence: %v", err)
	}
	return occurrence
}

func awaitRunLifecycleState(
	t *testing.T,
	store runLifecycleCandidateParityStore,
	runID string,
	want runtimerunlifecycle.State,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := store.LoadRunLifecycleSnapshot(context.Background(), runID)
		if err == nil && snapshot.Status == string(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run state = %#v/%v, want %s", snapshot, err, want)
		}
		runtime.Gosched()
	}
}

func retireRunLifecycleExecutorOccurrence(t *testing.T, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := occurrence.RetireAndWait(ctx); err != nil {
		t.Fatalf("retire candidate runtime occurrence: %v", err)
	}
}

func retireRunLifecycleProcess(t *testing.T, process *worklifetime.Process) {
	t.Helper()
	process.Retire()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := process.Join(ctx); err != nil {
		t.Fatalf("join candidate process owner: %v", err)
	}
}
