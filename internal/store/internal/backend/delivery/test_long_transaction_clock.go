package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type LongTransactionClockFixture struct {
	Event                  events.Event
	ClaimRoute             events.DeliveryRoute
	ClaimSnapshot          runtimedelivery.Snapshot
	RetryClaim, LeaseClaim runtimedelivery.ClaimedObligation
	Failure                *failures.Envelope
}

func (s *DeliveryPostgresOwner) CheckLongTransactionClockForTest(ctx context.Context, fixture LongTransactionClockFixture) error {
	adapter, err := NewAdapter(DialectPostgres)
	if err != nil {
		return err
	}
	return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, checkLongTransactionClockTxForTest(txctx, attempt, adapter, fixture)
	}).Err()
}

func checkLongTransactionClockTxForTest(txctx context.Context, attempt *mutationprotocol.Attempt, adapter *Adapter, fixture LongTransactionClockFixture) error {
	longEvent, longClaimRoute, longClaimSnapshot := fixture.Event, fixture.ClaimRoute, fixture.ClaimSnapshot
	longRetryClaim, longLeaseClaim := fixture.RetryClaim, fixture.LeaseClaim

	var transactionStartedAt time.Time
	if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(txctx, `SELECT CURRENT_TIMESTAMP`).Scan(&transactionStartedAt); err != nil {
			return fmt.Errorf("read PostgreSQL transaction timestamp: %w", err)
		}
		_, err := tx.ExecContext(txctx, `UPDATE event_delivery_attempts SET lease_expires_at=$1
						 WHERE delivery_id=$2::uuid AND claim_version=$3 AND open_marker=TRUE`,
			transactionStartedAt.Add(500*time.Millisecond),
			longLeaseClaim.Claim.DeliveryID(),
			longLeaseClaim.Claim.Version(),
		)
		if err != nil {
			return fmt.Errorf("age long-transaction lease: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	time.Sleep(1100 * time.Millisecond)
	longClaimResult, err := adapter.ClaimExactResult(
		txctx,
		attempt,
		longClaimSnapshot.Authority,
		longEvent,
		longClaimRoute,
		runtimedelivery.DefaultLeaseTTL,
	)
	if err != nil {
		return fmt.Errorf("claim in long PostgreSQL transaction: %w", err)
	}
	longClaimed, acquired := longClaimResult.Acquired()
	if !acquired {
		return fmt.Errorf("long-transaction claim = %#v, want acquired", longClaimResult)
	}
	longRetry, err := adapter.SettleFailure(txctx, attempt, longRetryClaim.Claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureRetry,
		Failure:     fixture.Failure,
		RetryBase:   10 * time.Second, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
	})
	if err != nil {
		return fmt.Errorf("settle retry in long PostgreSQL transaction: %w", err)
	}
	var leaseObservation runtimedelivery.ContinuationObservation
	var longPage runtimedelivery.ContinuationPage
	if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		leaseObservation, err = adapter.ObserveContinuation(txctx, tx, longClaimSnapshot.Authority, longLeaseClaim.Claim.DeliveryID())
		if err != nil {
			return fmt.Errorf("observe aged lease in long PostgreSQL transaction: %w", err)
		}
		longPage, err = adapter.ScanContinuations(txctx, tx, longClaimSnapshot.Authority, runtimedelivery.ContinuationCursor{}, 10)
		if err != nil {
			return fmt.Errorf("scan long PostgreSQL transaction continuations: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if leaseObservation.Disposition != runtimedelivery.ClaimReclaimable {
		return fmt.Errorf("aged lease observation = %s, want reclaimable after transaction-start time", leaseObservation.Disposition)
	}
	longDispositions := map[string]runtimedelivery.ClaimDisposition{}
	longWakes := map[string]time.Duration{}
	for _, item := range longPage.Items {
		longDispositions[item.DeliveryID] = item.Disposition
		if after, ok := item.Wake.After(); ok {
			longWakes[item.DeliveryID] = after
		}
	}
	if longDispositions[longClaimed.Snapshot.DeliveryID] != runtimedelivery.ClaimBusy ||
		longWakes[longClaimed.Snapshot.DeliveryID] <= 0 {
		return fmt.Errorf("fresh long-transaction claim scan = %s/%s, want busy with opaque wake", longDispositions[longClaimed.Snapshot.DeliveryID], longWakes[longClaimed.Snapshot.DeliveryID])
	}
	if longDispositions[longRetry.DeliveryID] != runtimedelivery.ClaimDeferred ||
		longWakes[longRetry.DeliveryID] <= 0 {
		return fmt.Errorf("long-transaction retry scan = %s/%s, want deferred with opaque wake", longDispositions[longRetry.DeliveryID], longWakes[longRetry.DeliveryID])
	}
	if longDispositions[longLeaseClaim.Claim.DeliveryID()] != runtimedelivery.ClaimReclaimable {
		return fmt.Errorf("long-transaction expired lease scan = %s, want reclaimable", longDispositions[longLeaseClaim.Claim.DeliveryID()])
	}
	for label, observed := range map[string]time.Time{
		"claim": longClaimed.Snapshot.UpdatedAt,
		"retry": longRetry.UpdatedAt,
	} {
		if elapsed := observed.Sub(transactionStartedAt); elapsed < 900*time.Millisecond {
			return fmt.Errorf("%s database clock advanced %s across long transaction; want clock_timestamp semantics", label, elapsed)
		}
		if observed.Nanosecond()%1000 != 0 || observed.Location() != time.UTC {
			return fmt.Errorf("%s database clock = %s (%s), want UTC microseconds", label, observed, observed.Location())
		}
	}
	if delay := longRetry.NextEligibleAt.Sub(longRetry.UpdatedAt); delay != 10*time.Second {
		return fmt.Errorf("long-transaction retry delay = %s, want exact 10s from current database time", delay)
	}
	return nil
}
