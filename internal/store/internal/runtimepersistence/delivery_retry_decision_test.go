package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type retryDecisionFixture struct {
	t       *testing.T
	ctx     context.Context
	store   authorActivityReceiptStore
	event   events.Event
	route   events.DeliveryRoute
	backend string
}

func newRetryDecisionFixture(t *testing.T, backend string, store authorActivityReceiptStore) retryDecisionFixture {
	t.Helper()
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	requireRunningRunForTest(t, ctx, store, runID, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC))
	event := eventtest.PersistedProjection(uuid.NewString(), "retry.decision", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	route := testEntitylessNodeDeliveryRoute("retry-decision")
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	return retryDecisionFixture{t: t, ctx: ctx, store: store, event: event, route: route, backend: backend}
}

func (f retryDecisionFixture) claim() deliverylifecycle.ClaimedObligation {
	f.t.Helper()
	claimed, err := claimDeliveryFixture(f.ctx, f.store, f.event, f.route)
	if err != nil {
		f.t.Fatal(err)
	}
	return claimed
}

func (f retryDecisionFixture) outcomes(deliveryID string) []deliverylifecycle.Outcome {
	f.t.Helper()
	outcomes, err := f.store.Outcomes(f.ctx, deliveryID)
	if err != nil {
		f.t.Fatal(err)
	}
	return outcomes
}

func (f retryDecisionFixture) historyAt(eventID, deliveryID string) runfork.RunForkPendingWork {
	f.t.Helper()
	planner := f.store.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	plan, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.event.RunID(), At: eventID})
	if err != nil {
		f.t.Fatalf("canonical whole-history read at %s: %v", eventID, err)
	}
	for _, item := range plan.PendingWork {
		if item.DeliveryID == deliveryID {
			return item
		}
	}
	f.t.Fatalf("delivery %s absent at cut %s", deliveryID, eventID)
	return runfork.RunForkPendingWork{}
}

func (f retryDecisionFixture) cut(deliveryID string) (string, runfork.RunForkPendingWork) {
	f.t.Helper()
	marker := eventtest.PersistedProjection(uuid.NewString(), "retry.cut", "gateway", "", []byte(`{}`), 0, f.event.RunID(), "", events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
		f.t.Fatal(err)
	}
	return marker.ID(), f.historyAt(marker.ID(), deliveryID)
}

func (f retryDecisionFixture) retry(claim deliverylifecycle.Claim, base time.Duration) deliverylifecycle.Snapshot {
	f.t.Helper()
	snapshot, err := f.store.SettleFailure(f.ctx, claim, deliverylifecycle.Settlement{
		Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: base,
		RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
	})
	if err != nil || snapshot.Status != deliverylifecycle.StatusFailed {
		f.t.Fatalf("commit retry decision: snapshot=%+v err=%v", snapshot, err)
	}
	return snapshot
}

func (f retryDecisionFixture) expire(claim deliverylifecycle.Claim) {
	f.t.Helper()
	var expiresAt time.Time
	err := runDeliveryFixtureAttempt(f.ctx, f.store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		adapter := sqliteDeliveryAdapter
		if f.backend == "postgres" {
			adapter = postgresDeliveryAdapter
		}
		updated, err := adapter.RenewClaim(ctx, attempt, claim, time.Millisecond)
		expiresAt = updated.ClaimExpiresAt
		return err
	})
	if err != nil {
		f.t.Fatal(err)
	}
	waitRetryDecisionCoordinate(f.t, f.ctx, expiresAt)
}

func waitRetryDecisionCoordinate(t *testing.T, ctx context.Context, due time.Time) {
	t.Helper()
	timer := time.NewTimer(time.Until(due))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatalf("wait for committed coordinate: %v", ctx.Err())
	case <-timer.C:
	}
}

func TestRetryDecisionSurvivesClaimExpiryAndReclaimBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newRetryDecisionFixture(t, backend.name, backend.open(t).store)
			first := f.claim()
			retry := f.retry(first.Claim, 13*time.Millisecond)
			original := f.outcomes(first.Claim.DeliveryID())
			if len(original) != 1 || original[0].RetryEligibleAt == nil || !original[0].RetryEligibleAt.Equal(retry.NextEligibleAt) || original[0].RetryEligibleAt.Sub(original[0].SettledAt) != 13*time.Millisecond {
				t.Fatalf("retry due not retained exactly: outcome=%+v physical=%+v", original, retry)
			}
			retryCut, retryFact := f.cut(first.Claim.DeliveryID())
			decision := deliverylifecycle.CommittedRetryDecision{ClaimVersion: first.Claim.Version(), SettledAt: original[0].SettledAt, EligibleAt: *original[0].RetryEligibleAt}
			if retryFact.RetryDecision == nil || *retryFact.RetryDecision != decision || retryFact.Status != "failed" || retryFact.ClaimVersion != 0 {
				t.Fatalf("retry projection did not retain the exact committed decision: %+v", retryFact)
			}
			waitRetryDecisionCoordinate(t, f.ctx, retry.NextEligibleAt)
			second := f.claim()
			if !second.Snapshot.NextEligibleAt.IsZero() {
				t.Fatal("active claim exposes a physical retry due")
			}
			_, active := f.cut(first.Claim.DeliveryID())
			if active.RetryDecision == nil || *active.RetryDecision != decision || active.Status != "in_progress" || active.SourceExecution != (deliverylifecycle.SourceExecutionAtCut{InProgress: true, Started: true}) || active.ClaimVersion != 0 {
				t.Fatalf("open attempt replaced committed retry evidence: %+v", active)
			}
			f.expire(second.Claim)
			third := f.claim()
			if third.Claim.Version() != second.Claim.Version()+1 || !reflect.DeepEqual(f.outcomes(first.Claim.DeliveryID()), original) {
				t.Fatal("open/expired attempts changed the committed retry decision")
			}
			_, reclaimed := f.cut(first.Claim.DeliveryID())
			if reclaimed.RetryDecision == nil || *reclaimed.RetryDecision != decision || reclaimed.SourceExecution != active.SourceExecution {
				t.Fatalf("lease-expired generation replaced the latest settled decision: %+v", reclaimed)
			}
			nextRetry := f.retry(third.Claim, 17*time.Millisecond)
			two := f.outcomes(first.Claim.DeliveryID())
			if len(two) != 2 || !reflect.DeepEqual(two[0], original[0]) || two[1].ClaimVersion != third.Claim.Version() || two[1].RetryEligibleAt == nil || two[1].RetryEligibleAt.Sub(two[1].SettledAt) != 34*time.Millisecond {
				t.Fatalf("subsequent retry decision replaced earlier evidence: %+v", two)
			}
			_, laterRetry := f.cut(first.Claim.DeliveryID())
			if laterRetry.RetryDecision == nil || laterRetry.RetryDecision.ClaimVersion != third.Claim.Version() || !laterRetry.RetryDecision.EligibleAt.Equal(*two[1].RetryEligibleAt) {
				t.Fatalf("later committed retry not selected: %+v", laterRetry)
			}
			waitRetryDecisionCoordinate(t, f.ctx, nextRetry.NextEligibleAt)
			last := f.claim()
			selection, err := handlerselection.NoMatch(handlerselection.ContextRules)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.SettleSuccess(f.ctx, last.Claim, nil, 0, selection); err != nil {
				t.Fatal(err)
			}
			settled := f.outcomes(first.Claim.DeliveryID())
			if len(settled) != 3 || !reflect.DeepEqual(settled[:2], two) || settled[2].Outcome != "delivered" || settled[2].RetryEligibleAt != nil {
				t.Fatalf("terminal settlement changed prior retry evidence: %+v", settled)
			}
			_, terminal := f.cut(first.Claim.DeliveryID())
			if terminal.Status != "delivered" || terminal.RetryDecision != nil || terminal.ClaimVersion != last.Claim.Version() {
				t.Fatalf("terminal settlement resurrected an earlier retry: %+v", terminal)
			}
			if older := f.historyAt(retryCut, first.Claim.DeliveryID()); !reflect.DeepEqual(older, retryFact) {
				t.Fatalf("later source changes altered the old retry cut: before=%+v after=%+v", retryFact, older)
			}
		})
	}
}

func TestRetryDecisionAndRevisionRollbackTogetherBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newRetryDecisionFixture(t, backend.name, backend.open(t).store)
			claimed := f.claim()
			before, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, f.event.RunID(), f.event.ID())
			if err != nil {
				t.Fatal(err)
			}
			rolledBack := errors.New("rollback after retry writes")
			err = runDeliveryFixtureAttempt(f.ctx, f.store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				adapter := sqliteDeliveryAdapter
				if backend.name == "postgres" {
					adapter = postgresDeliveryAdapter
				}
				_, err := adapter.SettleFailure(ctx, attempt, claimed.Claim, deliverylifecycle.Settlement{
					Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: 19 * time.Millisecond,
					RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
				})
				if err != nil {
					return err
				}
				return rolledBack
			})
			if !errors.Is(err, rolledBack) {
				t.Fatalf("did not reach post-write rollback: %v", err)
			}
			after, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, f.event.RunID(), f.event.ID())
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("rolled-back retry changed history: before=%+v after=%+v err=%v", before, after, err)
			}
			current, err := f.store.Snapshot(f.ctx, claimed.Claim.DeliveryID())
			if err != nil || !reflect.DeepEqual(current, claimed.Snapshot) || len(f.outcomes(claimed.Claim.DeliveryID())) != 0 {
				t.Fatalf("rolled-back retry leaked delivery/attempt state: %+v err=%v", current, err)
			}
			f.retry(claimed.Claim, 19*time.Millisecond)
			committed, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, f.event.RunID(), f.event.ID())
			if err != nil {
				t.Fatal(err)
			}
			committedOutcomes := f.outcomes(claimed.Claim.DeliveryID())
			if _, err := f.store.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Second, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); !errors.Is(err, deliverylifecycle.ErrConflict) {
				t.Fatalf("settled claim reused: %v", err)
			}
			refused, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, f.event.RunID(), f.event.ID())
			if err != nil || !reflect.DeepEqual(refused, committed) || !reflect.DeepEqual(f.outcomes(claimed.Claim.DeliveryID()), committedOutcomes) {
				t.Fatalf("refused settlement changed committed history: before=%+v after=%+v err=%v", committed, refused, err)
			}
		})
	}
}

func TestRetryDecisionQueuedCancellationPreservesOlderCutBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newRetryDecisionFixture(t, backend.name, backend.open(t).store)
			nodeRoute := f.route
			f.route = testAgentDeliveryRoute(t, f.event.RunID(), "retry-agent", "fixture/retry-agent")
			if err := commitDeliveryObligationFixture(f.ctx, f.store, f.event, f.route); err != nil {
				t.Fatal(err)
			}
			claimed := f.claim()
			retry := f.retry(claimed.Claim, time.Hour)
			outcomes := f.outcomes(claimed.Claim.DeliveryID())
			retryCut, retryFact := f.cut(claimed.Claim.DeliveryID())
			var canceled deliverylifecycle.Snapshot
			err := runDeliveryFixtureAttempt(f.ctx, f.store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				var err error
				switch selected := f.store.(type) {
				case *SQLiteRuntimeStore:
					canceled, err = selected.deliverySQLiteOwner.CancelQueuedAgentTx(ctx, attempt, retry)
				case *PostgresStore:
					canceled, err = selected.deliveryPostgresOwner.CancelQueuedAgentTx(ctx, attempt, retry)
				default:
					t.Fatalf("unsupported native store %T", f.store)
				}
				return err
			})
			if err != nil || canceled.Status != deliverylifecycle.StatusCanceled || canceled.ReasonCode != string(deliverylifecycle.CancellationTerminate) || !canceled.NextEligibleAt.IsZero() || canceled.ClaimVersion != retry.ClaimVersion || canceled.Failure != nil {
				t.Fatalf("queued cancellation: snapshot=%+v err=%v", canceled, err)
			}
			if got := f.outcomes(claimed.Claim.DeliveryID()); !reflect.DeepEqual(got, outcomes) {
				t.Fatalf("queued cancellation manufactured a settled attempt: before=%+v after=%+v", outcomes, got)
			}
			_, terminal := f.cut(claimed.Claim.DeliveryID())
			if terminal.Status != "canceled" || terminal.ReasonCode != "terminate" || terminal.RetryDecision != nil || terminal.ClaimVersion != retry.ClaimVersion {
				t.Fatalf("queued cancellation lost its exact delivery generation or retained a current retry: status=%s reason=%s decision=%+v version=%d want=%d", terminal.Status, terminal.ReasonCode, terminal.RetryDecision, terminal.ClaimVersion, retry.ClaimVersion)
			}
			if old := f.historyAt(retryCut, claimed.Claim.DeliveryID()); !reflect.DeepEqual(old, retryFact) {
				t.Fatalf("cancellation rewrote the older retry cut: before=%+v after=%+v", retryFact, old)
			}
			if node := loadDeliverySnapshotFixture(t, f.ctx, f.store, f.event.ID(), nodeRoute); node.Status != deliverylifecycle.StatusPending || node.ClaimVersion != 0 {
				t.Fatalf("agent cancellation changed unrelated node: %+v", node)
			}
		})
	}
}

func TestRetryDecisionParentTerminalizationSupersedesRetryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newRetryDecisionFixture(t, backend.name, backend.open(t).store)
			first := f.claim()
			retry := f.retry(first.Claim, time.Microsecond)
			outcomes := f.outcomes(first.Claim.DeliveryID())
			retryCut, retryFact := f.cut(first.Claim.DeliveryID())
			waitRetryDecisionCoordinate(t, f.ctx, retry.NextEligibleAt)
			interrupted := f.claim()
			rows, err := f.store.TerminalizeRun(f.ctx, f.event.RunID(), "retry_parent_terminalized")
			if err != nil || len(rows) != 1 || rows[0].Previous.ClaimVersion != interrupted.Claim.Version() || rows[0].Current.ClaimVersion != interrupted.Claim.Version()+1 {
				t.Fatalf("terminalize active retry: rows=%+v err=%v", rows, err)
			}
			settled := f.outcomes(first.Claim.DeliveryID())
			if len(settled) != 2 || !reflect.DeepEqual(settled[0], outcomes[0]) || settled[1].ClaimVersion != interrupted.Claim.Version()+1 || settled[1].Outcome != "terminalized" || settled[1].RetryEligibleAt != nil || settled[1].Failure == nil {
				t.Fatalf("parent-interrupted attempt replaced terminal settlement: %+v", settled)
			}
			_, terminal := f.cut(first.Claim.DeliveryID())
			if terminal.Status != "dead_letter" || terminal.RetryDecision != nil || terminal.ClaimVersion != settled[1].ClaimVersion || terminal.ReasonCode != "retry_parent_terminalized" {
				t.Fatalf("terminalization resurrected retry evidence: %+v", terminal)
			}
			if old := f.historyAt(retryCut, first.Claim.DeliveryID()); !reflect.DeepEqual(old, retryFact) {
				t.Fatalf("parent terminalization rewrote the older retry cut: before=%+v after=%+v", retryFact, old)
			}
			if _, err := f.store.SettleSuccess(f.ctx, interrupted.Claim, nil, 0, handlerselection.NotApplicable()); !errors.Is(err, deliverylifecycle.ErrConflict) {
				t.Fatalf("interrupted claim settled after parent terminalization: %v", err)
			}
			if got := f.outcomes(first.Claim.DeliveryID()); !reflect.DeepEqual(got, settled) {
				t.Fatalf("stale interrupted settlement changed outcomes: before=%+v after=%+v", settled, got)
			}
		})
	}
}

func TestRetryDecisionPreservesSuppliedPrecisionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t).store
			for _, precision := range []struct{ base, stored time.Duration }{
				{time.Nanosecond, time.Microsecond},
				{499 * time.Nanosecond, time.Microsecond},
				{500 * time.Nanosecond, time.Microsecond},
				{999 * time.Nanosecond, time.Microsecond},
				{time.Microsecond, time.Microsecond},
				{1001 * time.Nanosecond, 2 * time.Microsecond},
				{1500 * time.Nanosecond, 2 * time.Microsecond},
				{2 * time.Microsecond, 2 * time.Microsecond},
				{13 * time.Microsecond, 13 * time.Microsecond},
				{23 * time.Millisecond, 23 * time.Millisecond},
			} {
				t.Run(precision.base.String(), func(t *testing.T) {
					f := newRetryDecisionFixture(t, backend.name, store)
					claim := f.claim()
					snapshot, err := f.store.SettleFailure(f.ctx, claim.Claim, deliverylifecycle.Settlement{
						Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: precision.base,
						RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
					})
					if err != nil {
						current, readErr := f.store.Snapshot(f.ctx, claim.Claim.DeliveryID())
						if readErr != nil || !reflect.DeepEqual(current, claim.Snapshot) || len(f.outcomes(claim.Claim.DeliveryID())) != 0 {
							t.Fatalf("failed precision settlement leaked mutation: current=%+v readErr=%v settlementErr=%v", current, readErr, err)
						}
						t.Fatalf("positive retry base %s did not settle (no delivery/attempt mutation): %v", precision.base, err)
					}
					outcomes := f.outcomes(claim.Claim.DeliveryID())
					if len(outcomes) != 1 || outcomes[0].RetryEligibleAt == nil || !outcomes[0].RetryEligibleAt.Equal(snapshot.NextEligibleAt) || outcomes[0].RetryEligibleAt.Sub(outcomes[0].SettledAt) != precision.stored {
						t.Fatalf("supplied retry precision changed: base=%s storedDelay=%s outcome=%+v physical=%+v", precision.base, precision.stored, outcomes, snapshot)
					}
					_, cut := f.cut(claim.Claim.DeliveryID())
					if cut.RetryDecision == nil || !cut.RetryDecision.EligibleAt.Equal(snapshot.NextEligibleAt) || cut.RetryDecision.EligibleAt.Sub(cut.RetryDecision.SettledAt) != precision.stored {
						t.Fatalf("cut lost original retry precision: base=%s storedDelay=%s cut=%+v", precision.base, precision.stored, cut)
					}
				})
			}
		})
	}
}
