package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

var (
	postgresDeliveryAdapter = mustDeliveryAdapter(DialectPostgres)
	sqliteDeliveryAdapter   = mustDeliveryAdapter(DialectSQLite)
)

func mustDeliveryAdapter(dialect Dialect) *Adapter {
	adapter, err := NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func withDeliverySQL[T any](ctx context.Context, attempt *mutationprotocol.Attempt, write func(context.Context, *sql.Tx) (T, error)) (T, error) {
	var value T
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		value, err = write(ctx, tx)
		return err
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return value, err
}

func (s *DeliveryPostgresOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return postgresDeliveryAdapter.CommitInitial(ctx, attempt, eventID, runID, routes, authority)
}

func (s *DeliverySQLiteOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return sqliteDeliveryAdapter.CommitInitial(ctx, attempt, eventID, runID, routes, authority)
}

func (s *DeliveryPostgresOwner) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	_, err := s.ActivateDeliveryAuthorityOutcome(ctx, authority)
	return err
}

func (s *DeliveryPostgresOwner) ActivateDeliveryAuthorityOutcome(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ActivationCommit{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := postgresDeliveryAdapter.ActivateNormalAuthority(txctx, attempt, authority)
		return struct{}{}, err
	})
	return runtimedelivery.ActivationCommit{Acknowledged: result.Acknowledged()}, result.Err()
}

func (s *DeliverySQLiteOwner) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	_, err := s.ActivateDeliveryAuthorityOutcome(ctx, authority)
	return err
}

func (s *DeliverySQLiteOwner) ActivateDeliveryAuthorityOutcome(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ActivationCommit{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite activate delivery authority", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := sqliteDeliveryAdapter.ActivateNormalAuthority(txctx, attempt, authority)
		return struct{}{}, err
	})
	return runtimedelivery.ActivationCommit{Acknowledged: result.Acknowledged()}, result.Err()
}

func (s *DeliveryPostgresOwner) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.SourceArtifactFact,
) (runtimedelivery.RecoveryInventory, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RecoveryInventory{}, err
	}
	return postgresDeliveryAdapter.InspectRecovery(ctx, s.backend, source)
}

func (s *DeliverySQLiteOwner) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.SourceArtifactFact,
) (runtimedelivery.RecoveryInventory, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RecoveryInventory{}, err
	}
	return sqliteDeliveryAdapter.InspectRecovery(ctx, s.backend, source)
}

func (s *DeliveryPostgresOwner) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.ClaimResult, error) {
		var claimed runtimedelivery.ClaimResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			snapshot, err := postgresDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
			if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
				return err
			}
			if err == nil {
				if err := runstate.RequirePostgresActiveTx(txctx, tx, snapshot.RunID); err != nil {
					return err
				}
			}
			claimed, err = s.receiverAdapter.ClaimExactResult(txctx, attempt, authority, event, route, runtimedelivery.DefaultLeaseTTL)
			return err
		})
		return claimed, err
	})
	claimed, acknowledged := result.Value()
	claimed.Acknowledged = acknowledged
	return claimed, result.Err()
}

func (s *DeliverySQLiteOwner) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite claim delivery", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.ClaimResult, error) {
		var claimed runtimedelivery.ClaimResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			snapshot, err := sqliteDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
			if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
				return err
			}
			if err == nil {
				if err := runstate.RequireSQLiteActiveTx(txctx, tx, snapshot.RunID); err != nil {
					return err
				}
			}
			claimed, err = s.receiverAdapter.ClaimExactResult(txctx, attempt, authority, event, route, runtimedelivery.DefaultLeaseTTL)
			return err
		})
		return claimed, err
	})
	claimed, acknowledged := result.Value()
	claimed.Acknowledged = acknowledged
	return claimed, result.Err()
}

func (s *DeliveryPostgresOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	var page runtimedelivery.ContinuationPage
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		page, err = s.receiverAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
		if err != nil {
			return err
		}
		for i := range page.Items {
			if page.Items[i].Disposition == runtimedelivery.ClaimAbsent || page.Items[i].Disposition == runtimedelivery.ClaimInvariantInvalid {
				continue
			}
			record, found, err := eventrecordpostgres.Load(txctx, tx, page.Items[i].Snapshot.EventID)
			if err != nil {
				return err
			}
			if !found {
				page.Items[i].Disposition = runtimedelivery.ClaimInvariantInvalid
				page.Items[i].Invariant = fmt.Errorf("delivery event %s is absent", page.Items[i].Snapshot.EventID)
				continue
			}
			admitted, err := record.Decode()
			if err != nil {
				page.Items[i].Disposition = runtimedelivery.ClaimInvariantInvalid
				page.Items[i].Invariant = err
				continue
			}
			page.Items[i].Event = admitted.Event()
		}
		return nil
	})
	return page, err
}

func (s *DeliverySQLiteOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	var page runtimedelivery.ContinuationPage
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		page, err = s.receiverAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
		if err != nil {
			return err
		}
		for i := range page.Items {
			if page.Items[i].Disposition == runtimedelivery.ClaimAbsent || page.Items[i].Disposition == runtimedelivery.ClaimInvariantInvalid {
				continue
			}
			record, found, err := eventrecordsqlite.Load(txctx, tx, page.Items[i].Snapshot.EventID)
			if err != nil {
				return err
			}
			if !found {
				page.Items[i].Disposition = runtimedelivery.ClaimInvariantInvalid
				page.Items[i].Invariant = fmt.Errorf("delivery event %s is absent", page.Items[i].Snapshot.EventID)
				continue
			}
			admitted, err := record.Decode()
			if err != nil {
				page.Items[i].Disposition = runtimedelivery.ClaimInvariantInvalid
				page.Items[i].Invariant = err
				continue
			}
			page.Items[i].Event = admitted.Event()
		}
		return nil
	})
	return page, err
}

func (s *DeliveryPostgresOwner) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationObservation{}, err
	}
	var observation runtimedelivery.ContinuationObservation
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		observation, err = s.receiverAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func (s *DeliverySQLiteOwner) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationObservation{}, err
	}
	var observation runtimedelivery.ContinuationObservation
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		observation, err = s.receiverAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func (s *DeliveryPostgresOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	return postgresDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return s.renewClaimTx(txctx, attempt, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *DeliverySQLiteOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	return sqliteDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return s.renewClaimTx(txctx, attempt, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *DeliveryPostgresOwner) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.ClaimCommit, error) {
	return postgresDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.BindAgentSession(txctx, attempt, claim, sessionID)
	})
}

func (s *DeliveryPostgresOwner) ValidateProviderOriginTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim) error {
	return postgresDeliveryAdapter.ValidateCurrentClaim(ctx, tx, claim)
}

func (s *DeliverySQLiteOwner) ValidateProviderOriginTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim) error {
	return sqliteDeliveryAdapter.ValidateCurrentClaim(ctx, tx, claim)
}

func (s *DeliveryPostgresOwner) RenewProviderOriginTx(ctx context.Context, attempt *mutationprotocol.Attempt, claim runtimedelivery.Claim, lease time.Duration) error {
	_, err := s.renewClaimTx(ctx, attempt, claim, lease)
	return err
}

func (s *DeliverySQLiteOwner) RenewProviderOriginTx(ctx context.Context, attempt *mutationprotocol.Attempt, claim runtimedelivery.Claim, lease time.Duration) error {
	_, err := s.renewClaimTx(ctx, attempt, claim, lease)
	return err
}

func (s *DeliveryPostgresOwner) renewClaimTx(ctx context.Context, attempt *mutationprotocol.Attempt, claim runtimedelivery.Claim, lease time.Duration) (runtimedelivery.Snapshot, error) {
	snapshot, err := postgresDeliveryAdapter.RenewClaim(ctx, attempt, claim, lease)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *DeliverySQLiteOwner) renewClaimTx(ctx context.Context, attempt *mutationprotocol.Attempt, claim runtimedelivery.Claim, lease time.Duration) (runtimedelivery.Snapshot, error) {
	snapshot, err := sqliteDeliveryAdapter.RenewClaim(ctx, attempt, claim, lease)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *DeliveryPostgresOwner) SettleProviderOriginSuccessTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := postgresDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection())
		if err != nil {
			return err
		}
		return settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx)
	})
}

func (s *DeliverySQLiteOwner) SettleProviderOriginSuccessTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := sqliteDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection())
		if err != nil {
			return err
		}
		return settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx)
	})
}

// SettleWorkflowNodeSuccessTx terminally settles the exact inbound node claim
// inside the selected workflow-engine mutation transaction.
func (s *DeliveryPostgresOwner) SettleWorkflowNodeSuccessTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
	selection runtimedelivery.HandlerRuleSelectionFact,
) (runtimedelivery.Snapshot, error) {
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		snapshot, err := postgresDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if err := settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

// SettleWorkflowNodeSuccessTx terminally settles the exact inbound node claim
// inside the selected workflow-engine mutation transaction.
func (s *DeliverySQLiteOwner) SettleWorkflowNodeSuccessTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
	selection runtimedelivery.HandlerRuleSelectionFact,
) (runtimedelivery.Snapshot, error) {
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		snapshot, err := sqliteDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if err := settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *DeliveryPostgresOwner) SettleProviderOriginFailureTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := postgresDeliveryAdapter.SettleFailure(ctx, attempt, claim, settlement)
		if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
			return err
		}
		if err := settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx); err != nil {
			return err
		}
		record, found, err := eventrecordpostgres.Load(ctx, tx, snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(snapshot.EventID)
			}
			return err
		}
		diagnostic, err := deliveryDeadLetterRecord(record, snapshot)
		if err != nil {
			return err
		}
		return s.RecordDeadLetterTx(ctx, attempt, diagnostic, true)
	})
}

func (s *DeliverySQLiteOwner) SettleProviderOriginFailureTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := sqliteDeliveryAdapter.SettleFailure(ctx, attempt, claim, settlement)
		if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
			return err
		}
		if err := settleReceiverDependentsTx(ctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx); err != nil {
			return err
		}
		record, found, err := eventrecordsqlite.Load(ctx, tx, snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(snapshot.EventID)
			}
			return err
		}
		diagnostic, err := deliveryDeadLetterRecord(record, snapshot)
		if err != nil {
			return err
		}
		return s.RecordDeadLetterTx(ctx, attempt, diagnostic, true)
	})
}

func (s *DeliveryPostgresOwner) SettleProviderOriginRecoveryFailureTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		alreadyTerminal, err := postgresDeliveryAdapter.prepareProviderOriginRecovery(ctx, tx, attempt, claim)
		if err != nil || alreadyTerminal {
			return err
		}
		return s.SettleProviderOriginFailureTx(ctx, attempt, claim, settlement)
	})
}

func (s *DeliverySQLiteOwner) SettleProviderOriginRecoveryFailureTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		alreadyTerminal, err := sqliteDeliveryAdapter.prepareProviderOriginRecovery(ctx, tx, attempt, claim)
		if err != nil || alreadyTerminal {
			return err
		}
		return s.SettleProviderOriginFailureTx(ctx, attempt, claim, settlement)
	})
}

func (s *DeliverySQLiteOwner) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.ClaimCommit, error) {
	return sqliteDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.BindAgentSession(txctx, attempt, claim, sessionID)
	})
}

func (s *DeliveryPostgresOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	commit, err := postgresDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := postgresDeliveryAdapter.SettleSuccess(txctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
	return commit.Snapshot, err
}

func (s *DeliverySQLiteOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	commit, err := sqliteDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := sqliteDeliveryAdapter.SettleSuccess(txctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
	return commit.Snapshot, err
}

func (s *DeliveryPostgresOwner) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	commit, err := postgresDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := postgresDeliveryAdapter.SettleFailure(txctx, attempt, claim, settlement)
		if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
			return snapshot, err
		}
		record, found, err := eventrecordpostgres.Load(txctx, tx, snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(snapshot.EventID)
			}
			return runtimedelivery.Snapshot{}, err
		}
		diagnostic, err := deliveryDeadLetterRecord(record, snapshot)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if err := s.RecordDeadLetterTx(txctx, attempt, diagnostic, true); err != nil {
			return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
	return commit.Snapshot, err
}

func (s *DeliverySQLiteOwner) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	commit, err := sqliteDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := sqliteDeliveryAdapter.SettleFailure(txctx, attempt, claim, settlement)
		if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
			return snapshot, err
		}
		record, found, err := eventrecordsqlite.Load(txctx, tx, snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(snapshot.EventID)
			}
			return runtimedelivery.Snapshot{}, err
		}
		diagnostic, err := deliveryDeadLetterRecord(record, snapshot)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if err := s.RecordDeadLetterTx(txctx, attempt, diagnostic, true); err != nil {
			return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
	return commit.Snapshot, err
}

func deliveryDeadLetterRecord(record eventrecord.Record, snapshot runtimedelivery.Snapshot) (runtimedeadletters.Record, error) {
	failure := snapshot.Failure
	if failure == nil {
		return runtimedeadletters.Record{}, fmt.Errorf("terminal delivery %s has no failure envelope", snapshot.DeliveryID)
	}
	if snapshot.SettledAt.IsZero() {
		return runtimedeadletters.Record{}, fmt.Errorf("terminal delivery %s has no settlement timestamp", snapshot.DeliveryID)
	}
	flowInstance := strings.Trim(strings.TrimSpace(record.FlowInstance), "/")
	if flowInstance == "" {
		flowInstance = strings.Trim(strings.TrimSpace(snapshot.Route.Normalized().Target.Route().FlowInstance), "/")
	}
	if flowInstance == "" {
		// An event and delivery route with no flow coordinate is the declared
		// runtime-root diagnostic scope; storage never supplies this fact.
		flowInstance = "runtime"
	}
	return runtimedeadletters.Record{
		OriginalEventID: record.EventID,
		DeliveryID:      snapshot.DeliveryID,
		ClaimVersion:    snapshot.ClaimVersion,
		OriginalEvent:   record.EventName,
		OriginalPayload: append([]byte(nil), record.Payload...),
		EntityID:        record.EntityID,
		FlowInstance:    flowInstance,
		Failure:         *failure,
		RetryCount:      snapshot.RetryCount,
		ChainDepth:      record.ChainDepth,
		HandlerNode:     snapshot.SubscriberID,
		Timestamp:       snapshot.SettledAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func postgresDeliveryMutation(s *DeliveryPostgresOwner, ctx context.Context, candidates *runhandoff.CandidateCoordinator, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error)) (runtimedelivery.ClaimCommit, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ClaimCommit{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		var snapshot runtimedelivery.Snapshot
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			snapshot, err = operation(txctx, tx, attempt)
			if err != nil || strings.TrimSpace(snapshot.RunID) == "" {
				return err
			}
			return settleReceiverDependentsTx(txctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx)
		})
		return snapshot, err
	})
	snapshot, acknowledged := result.Value()
	return runtimedelivery.ClaimCommit{Snapshot: snapshot, Acknowledged: acknowledged}, result.Err()
}

func sqliteDeliveryMutation(s *DeliverySQLiteOwner, ctx context.Context, candidates *runhandoff.CandidateCoordinator, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error)) (runtimedelivery.ClaimCommit, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ClaimCommit{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite delivery mutation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		var snapshot runtimedelivery.Snapshot
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			snapshot, err = operation(txctx, tx, attempt)
			if err != nil || strings.TrimSpace(snapshot.RunID) == "" {
				return err
			}
			return settleReceiverDependentsTx(txctx, tx, attempt, s.receiverAdapter, snapshot, s.RecordDeadLetterTx)
		})
		return snapshot, err
	})
	snapshot, acknowledged := result.Value()
	return runtimedelivery.ClaimCommit{Snapshot: snapshot, Acknowledged: acknowledged}, result.Err()
}

func settleReceiverDependentsTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, adapter *Adapter, materializer runtimedelivery.Snapshot, recordDiagnostic func(context.Context, *mutationprotocol.Attempt, runtimedeadletters.Record, bool) error) error {
	terminalizations, err := adapter.TerminalizeMaterializationDependents(ctx, attempt, materializer)
	if err != nil || len(terminalizations) == 0 {
		return err
	}
	var record eventrecord.Record
	var found bool
	if adapter.dialect == DialectSQLite {
		record, found, err = eventrecordsqlite.Load(ctx, tx, materializer.EventID)
	} else {
		record, found, err = eventrecordpostgres.Load(ctx, tx, materializer.EventID)
	}
	if err != nil {
		return err
	}
	if !found {
		return eventrecord.Missing(materializer.EventID)
	}
	for _, terminalization := range terminalizations {
		diagnostic, err := deliveryDeadLetterRecord(record, terminalization.Current)
		if err != nil {
			return err
		}
		if err := recordDiagnostic(ctx, attempt, diagnostic, true); err != nil {
			return err
		}
	}
	return nil
}

func (s *DeliveryPostgresOwner) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return postgresDeliveryAdapter.Snapshot(ctx, s.backend, deliveryID)
}

func (s *DeliverySQLiteOwner) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return sqliteDeliveryAdapter.Snapshot(ctx, s.backend, deliveryID)
}

func (s *DeliveryPostgresOwner) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return postgresDeliveryAdapter.Outcomes(ctx, s.backend, deliveryID)
}

func (s *DeliverySQLiteOwner) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return sqliteDeliveryAdapter.Outcomes(ctx, s.backend, deliveryID)
}

func (s *DeliveryPostgresOwner) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return postgresDeliveryAdapter.ProveHandoff(ctx, s.backend, eventID, route)
}

func (s *DeliverySQLiteOwner) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return sqliteDeliveryAdapter.ProveHandoff(ctx, s.backend, eventID, route)
}

func (s *DeliveryPostgresOwner) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return postgresDeliveryAdapter.SummarizeRun(ctx, s.backend, runID)
}

func (s *DeliverySQLiteOwner) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return sqliteDeliveryAdapter.SummarizeRun(ctx, s.backend, runID)
}

func (s *DeliveryPostgresOwner) TerminalizeRun(ctx context.Context, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) ([]runtimedelivery.Terminalization, error) {
		return s.TerminalizeRunDeliveriesTx(txctx, attempt, runID, reason)
	})
	out, _ := result.Value()
	return out, result.Err()
}

func (s *DeliverySQLiteOwner) TerminalizeRun(ctx context.Context, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite terminalize deliveries", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) ([]runtimedelivery.Terminalization, error) {
		return s.TerminalizeRunDeliveriesTx(txctx, attempt, runID, reason)
	})
	out, _ := result.Value()
	return out, result.Err()
}

func declareAuthorityDeliveryRuns(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority, attempt *mutationprotocol.Attempt) error {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT CAST(run_id AS TEXT) FROM event_deliveries WHERE run_id IS NOT NULL AND execution_authority_kind=$1 AND authority_bundle_hash=$2 AND execution_authority_id=$3 AND execution_authority_generation=$4`, string(authority.Kind()), authority.SourceArtifact().BundleHash(), authority.ExecutionID(), authority.Generation())
	if err != nil {
		return fmt.Errorf("resolve delivery authority affected runs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return fmt.Errorf("scan delivery authority affected run: %w", err)
		}
		if err := attempt.AddWholeFamily(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *DeliveryPostgresOwner) DeliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *DeliveryPostgresOwner) DeliverySnapshotsForEventTx(ctx context.Context, tx *sql.Tx, eventID string) ([]runtimedelivery.Snapshot, error) {
	if s == nil || tx == nil {
		return nil, errors.New("delivery PostgreSQL transaction owner is required")
	}
	return postgresDeliveryAdapter.SnapshotsForEvent(ctx, tx, eventID)
}

func (s *DeliverySQLiteOwner) DeliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return s.receiverAdapter.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *DeliverySQLiteOwner) DeliverySnapshotsForEventTx(ctx context.Context, tx *sql.Tx, eventID string) ([]runtimedelivery.Snapshot, error) {
	if s == nil || tx == nil {
		return nil, errors.New("delivery SQLite transaction owner is required")
	}
	return sqliteDeliveryAdapter.SnapshotsForEvent(ctx, tx, eventID)
}

func (s *DeliveryPostgresOwner) SummarizeRunTx(ctx context.Context, tx *sql.Tx, runID string) (runtimedelivery.RunSummary, error) {
	if s == nil || tx == nil {
		return runtimedelivery.RunSummary{}, errors.New("delivery PostgreSQL transaction owner is required")
	}
	return postgresDeliveryAdapter.SummarizeRun(ctx, tx, runID)
}

func (s *DeliverySQLiteOwner) SummarizeRunTx(ctx context.Context, tx *sql.Tx, runID string) (runtimedelivery.RunSummary, error) {
	if s == nil || tx == nil {
		return runtimedelivery.RunSummary{}, errors.New("delivery SQLite transaction owner is required")
	}
	return sqliteDeliveryAdapter.SummarizeRun(ctx, tx, runID)
}

func (s *DeliveryPostgresOwner) TerminalizeRunDeliveriesTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) ([]runtimedelivery.Terminalization, error) {
		terminalizations, err := postgresDeliveryAdapter.TerminalizeRun(ctx, attempt, runID, reason)
		if err != nil {
			return nil, err
		}
		for _, terminalization := range terminalizations {
			record, found, err := eventrecordpostgres.Load(ctx, tx, terminalization.Current.EventID)
			if err != nil || !found {
				if err == nil {
					err = eventrecord.Missing(terminalization.Current.EventID)
				}
				return nil, err
			}
			diagnostic, err := deliveryDeadLetterRecord(record, terminalization.Current)
			if err != nil {
				return nil, err
			}
			if err := s.RecordDeadLetterTx(ctx, attempt, diagnostic, false); err != nil {
				return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
			}
		}
		return terminalizations, nil
	})
}

func (s *DeliverySQLiteOwner) TerminalizeRunDeliveriesTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) ([]runtimedelivery.Terminalization, error) {
		terminalizations, err := sqliteDeliveryAdapter.TerminalizeRun(ctx, attempt, runID, reason)
		if err != nil {
			return nil, err
		}
		for _, terminalization := range terminalizations {
			record, found, err := eventrecordsqlite.Load(ctx, tx, terminalization.Current.EventID)
			if err != nil || !found {
				if err == nil {
					err = eventrecord.Missing(terminalization.Current.EventID)
				}
				return nil, err
			}
			diagnostic, err := deliveryDeadLetterRecord(record, terminalization.Current)
			if err != nil {
				return nil, err
			}
			if err := s.RecordDeadLetterTx(ctx, attempt, diagnostic, false); err != nil {
				return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
			}
		}
		return terminalizations, nil
	})
}

func (s *DeliveryPostgresOwner) ActiveRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

func (s *DeliverySQLiteOwner) ActiveRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

var _ runtimedelivery.Store = (*DeliveryPostgresOwner)(nil)
var _ runtimedelivery.Store = (*DeliverySQLiteOwner)(nil)
