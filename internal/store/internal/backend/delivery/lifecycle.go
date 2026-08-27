package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
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

func (s *DeliveryPostgresOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	proofs, err := postgresDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes, authority)
	if err != nil {
		return nil, err
	}
	if len(proofs) > 0 {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
			return nil, err
		}
	}
	return proofs, nil
}

func (s *DeliverySQLiteOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	proofs, err := sqliteDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes, authority)
	if err != nil {
		return nil, err
	}
	if len(proofs) > 0 {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
			return nil, err
		}
	}
	return proofs, nil
}

func (s *DeliveryPostgresOwner) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	effects := privaterunforkrevision.NewEffects()
	return s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		if err := postgresDeliveryAdapter.ActivateNormalAuthority(txctx, tx, authority); err != nil {
			return err
		}
		return declareAuthorityDeliveryRuns(txctx, tx, authority, effects)
	})
}

func (s *DeliverySQLiteOwner) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	effects := privaterunforkrevision.NewEffects()
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite activate delivery authority", effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		if err := sqliteDeliveryAdapter.ActivateNormalAuthority(txctx, tx, authority); err != nil {
			return err
		}
		return declareAuthorityDeliveryRuns(txctx, tx, authority, effects)
	})
}

func (s *DeliveryPostgresOwner) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RecoveryInventory{}, err
	}
	return postgresDeliveryAdapter.InspectRecovery(ctx, s.backend, source)
}

func (s *DeliverySQLiteOwner) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
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
	var result runtimedelivery.ClaimResult
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		snapshot, err := postgresDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
			return err
		}
		if err == nil {
			if err := runstate.RequirePostgresActiveTx(txctx, tx, snapshot.RunID); err != nil {
				return err
			}
		}
		result, err = postgresDeliveryAdapter.ClaimExactResult(txctx, tx, story, authority, event, route, runtimedelivery.DefaultLeaseTTL)
		if err != nil || strings.TrimSpace(result.Snapshot.RunID) == "" {
			return err
		}
		return effects.Add(result.Snapshot.RunID, privaterunforkrevision.FamilyEventDeliveries)
	})
	return result, err
}

func (s *DeliverySQLiteOwner) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	var result runtimedelivery.ClaimResult
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, "sqlite claim delivery", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		snapshot, err := sqliteDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
			return err
		}
		if err == nil {
			if err := runstate.RequireSQLiteActiveTx(txctx, tx, snapshot.RunID); err != nil {
				return err
			}
		}
		result, err = sqliteDeliveryAdapter.ClaimExactResult(txctx, tx, story, authority, event, route, runtimedelivery.DefaultLeaseTTL)
		if err != nil || strings.TrimSpace(result.Snapshot.RunID) == "" {
			return err
		}
		return effects.Add(result.Snapshot.RunID, privaterunforkrevision.FamilyEventDeliveries)
	})
	return result, err
}

func (s *DeliveryPostgresOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	var page runtimedelivery.ContinuationPage
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		page, err = postgresDeliveryAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
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
		page, err = sqliteDeliveryAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
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
		observation, err = postgresDeliveryAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
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
		observation, err = sqliteDeliveryAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func (s *DeliveryPostgresOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return s.renewClaimTx(txctx, tx, effects, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *DeliverySQLiteOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return s.renewClaimTx(txctx, tx, effects, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *DeliveryPostgresOwner) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation, _ *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *DeliveryPostgresOwner) ValidateProviderOriginTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim) error {
	return postgresDeliveryAdapter.ValidateCurrentClaim(ctx, tx, claim)
}

func (s *DeliverySQLiteOwner) ValidateProviderOriginTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim) error {
	return sqliteDeliveryAdapter.ValidateCurrentClaim(ctx, tx, claim)
}

func (s *DeliveryPostgresOwner) RenewProviderOriginTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, claim runtimedelivery.Claim, lease time.Duration) error {
	_, err := s.renewClaimTx(ctx, tx, effects, claim, lease)
	return err
}

func (s *DeliverySQLiteOwner) RenewProviderOriginTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, claim runtimedelivery.Claim, lease time.Duration) error {
	_, err := s.renewClaimTx(ctx, tx, effects, claim, lease)
	return err
}

func (s *DeliveryPostgresOwner) renewClaimTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, claim runtimedelivery.Claim, lease time.Duration) (runtimedelivery.Snapshot, error) {
	snapshot, err := postgresDeliveryAdapter.RenewClaim(ctx, tx, claim, lease)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *DeliverySQLiteOwner) renewClaimTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, claim runtimedelivery.Claim, lease time.Duration) (runtimedelivery.Snapshot, error) {
	snapshot, err := sqliteDeliveryAdapter.RenewClaim(ctx, tx, claim, lease)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *DeliveryPostgresOwner) SettleProviderOriginSuccessTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
) error {
	if _, err := postgresDeliveryAdapter.SettleSuccess(ctx, tx, story, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		return err
	}
	return effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries)
}

func (s *DeliverySQLiteOwner) SettleProviderOriginSuccessTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
) error {
	if _, err := sqliteDeliveryAdapter.SettleSuccess(ctx, tx, story, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		return err
	}
	return effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries)
}

// SettleWorkflowNodeSuccessTx terminally settles the exact inbound node claim
// inside the selected workflow-engine mutation transaction.
func (s *DeliveryPostgresOwner) SettleWorkflowNodeSuccessTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
	selection runtimedelivery.HandlerRuleSelectionFact,
) (runtimedelivery.Snapshot, error) {
	snapshot, err := postgresDeliveryAdapter.SettleSuccess(ctx, tx, story, claim, sideEffects, duration, selection)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

// SettleWorkflowNodeSuccessTx terminally settles the exact inbound node claim
// inside the selected workflow-engine mutation transaction.
func (s *DeliverySQLiteOwner) SettleWorkflowNodeSuccessTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
	selection runtimedelivery.HandlerRuleSelectionFact,
) (runtimedelivery.Snapshot, error) {
	snapshot, err := sqliteDeliveryAdapter.SettleSuccess(ctx, tx, story, claim, sideEffects, duration, selection)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *DeliveryPostgresOwner) SettleProviderOriginFailureTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	snapshot, err := postgresDeliveryAdapter.SettleFailure(ctx, tx, story, claim, settlement)
	if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
		return err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
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
	return s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, true)
}

func (s *DeliverySQLiteOwner) SettleProviderOriginFailureTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	snapshot, err := sqliteDeliveryAdapter.SettleFailure(ctx, tx, story, claim, settlement)
	if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
		return err
	}
	if err := effects.Add(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries); err != nil {
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
	return s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, true)
}

func (s *DeliveryPostgresOwner) SettleProviderOriginRecoveryFailureTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	alreadyTerminal, err := postgresDeliveryAdapter.prepareProviderOriginRecovery(ctx, tx, claim)
	if err != nil || alreadyTerminal {
		return err
	}
	return s.SettleProviderOriginFailureTx(ctx, tx, story, effects, claim, settlement)
}

func (s *DeliverySQLiteOwner) SettleProviderOriginRecoveryFailureTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	claim runtimedelivery.Claim,
	settlement runtimedelivery.Settlement,
) error {
	alreadyTerminal, err := sqliteDeliveryAdapter.prepareProviderOriginRecovery(ctx, tx, claim)
	if err != nil || alreadyTerminal {
		return err
	}
	return s.SettleProviderOriginFailureTx(ctx, tx, story, effects, claim, settlement)
}

func (s *DeliverySQLiteOwner) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation, _ *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *DeliveryPostgresOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	return runhandoff.WithCandidateHandoffResult(ctx, func(handoff *runhandoff.CandidateHandoff) (runtimedelivery.Snapshot, error) {
		return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, _ *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
			if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			snapshot, err := postgresDeliveryAdapter.SettleSuccess(txctx, tx, story, claim, sideEffects, duration, selection)
			if err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			if _, err := s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, claim.RunID(), nil, handoff); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			return snapshot, nil
		})
	})
}

func (s *DeliverySQLiteOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	return runhandoff.WithCandidateHandoffResult(ctx, func(handoff *runhandoff.CandidateHandoff) (runtimedelivery.Snapshot, error) {
		return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, _ *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
			if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			snapshot, err := sqliteDeliveryAdapter.SettleSuccess(txctx, tx, story, claim, sideEffects, duration, selection)
			if err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			if _, err := s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, claim.RunID(), nil, handoff); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			return snapshot, nil
		})
	})
}

func (s *DeliveryPostgresOwner) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return runhandoff.WithCandidateHandoffResult(ctx, func(handoff *runhandoff.CandidateHandoff) (runtimedelivery.Snapshot, error) {
		return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
			if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			snapshot, err := postgresDeliveryAdapter.SettleFailure(txctx, tx, story, claim, settlement)
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
			if err := s.RecordDeadLetterTx(txctx, tx, story, effects, diagnostic, true); err != nil {
				return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
			}
			if _, err := s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, claim.RunID(), nil, handoff); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			return snapshot, nil
		})
	})
}

func (s *DeliverySQLiteOwner) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return runhandoff.WithCandidateHandoffResult(ctx, func(handoff *runhandoff.CandidateHandoff) (runtimedelivery.Snapshot, error) {
		return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error) {
			if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			snapshot, err := sqliteDeliveryAdapter.SettleFailure(txctx, tx, story, claim, settlement)
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
			if err := s.RecordDeadLetterTx(txctx, tx, story, effects, diagnostic, true); err != nil {
				return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
			}
			if _, err := s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, claim.RunID(), nil, handoff); err != nil {
				return runtimedelivery.Snapshot{}, err
			}
			return snapshot, nil
		})
	})
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

func postgresDeliveryMutation(s *DeliveryPostgresOwner, ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error)) (runtimedelivery.Snapshot, error) {
	var snapshot runtimedelivery.Snapshot
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		snapshot, err = operation(txctx, tx, story, effects)
		if err != nil || strings.TrimSpace(snapshot.RunID) == "" {
			return err
		}
		return effects.Add(snapshot.RunID, privaterunforkrevision.FamilyEventDeliveries)
	})
	return snapshot, err
}

func sqliteDeliveryMutation(s *DeliverySQLiteOwner, ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *privaterunforkrevision.Effects) (runtimedelivery.Snapshot, error)) (runtimedelivery.Snapshot, error) {
	var snapshot runtimedelivery.Snapshot
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, "sqlite delivery mutation", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		snapshot, err = operation(txctx, tx, story, effects)
		if err != nil || strings.TrimSpace(snapshot.RunID) == "" {
			return err
		}
		return effects.Add(snapshot.RunID, privaterunforkrevision.FamilyEventDeliveries)
	})
	return snapshot, err
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
	var out []runtimedelivery.Terminalization
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		out, err = s.TerminalizeRunDeliveriesTx(txctx, tx, story, effects, runID, reason)
		return err
	})
	return out, err
}

func (s *DeliverySQLiteOwner) TerminalizeRun(ctx context.Context, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []runtimedelivery.Terminalization
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, "sqlite terminalize deliveries", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		out, err = s.TerminalizeRunDeliveriesTx(txctx, tx, story, effects, runID, reason)
		return err
	})
	return out, err
}

func declareAuthorityDeliveryRuns(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority, effects *privaterunforkrevision.Effects) error {
	bundleHash, bundleSource := authority.BundleSource().StorageValues()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT CAST(run_id AS TEXT) FROM event_deliveries WHERE run_id IS NOT NULL AND execution_authority_kind=$1 AND authority_bundle_hash=$2 AND authority_bundle_source=$3 AND execution_authority_id=$4 AND execution_authority_generation=$5`, string(authority.Kind()), bundleHash, bundleSource, authority.ExecutionID(), authority.Generation())
	if err != nil {
		return fmt.Errorf("resolve delivery authority affected runs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return fmt.Errorf("scan delivery authority affected run: %w", err)
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
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
	return sqliteDeliveryAdapter.SnapshotsForEvent(ctx, s.backend, eventID)
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

func (s *DeliveryPostgresOwner) TerminalizeRunDeliveriesTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	terminalizations, err := postgresDeliveryAdapter.TerminalizeRun(ctx, tx, story, runID, reason)
	if err != nil {
		return nil, err
	}
	if len(terminalizations) > 0 {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
			return nil, err
		}
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
		if err := s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, false); err != nil {
			return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
		}
	}
	return terminalizations, nil
}

func (s *DeliverySQLiteOwner) TerminalizeRunDeliveriesTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	terminalizations, err := sqliteDeliveryAdapter.TerminalizeRun(ctx, tx, story, runID, reason)
	if err != nil {
		return nil, err
	}
	if len(terminalizations) > 0 {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries); err != nil {
			return nil, err
		}
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
		if err := s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, false); err != nil {
			return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
		}
	}
	return terminalizations, nil
}

func (s *DeliveryPostgresOwner) ActiveRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

func (s *DeliverySQLiteOwner) ActiveRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

var _ runtimedelivery.Store = (*DeliveryPostgresOwner)(nil)
var _ runtimedelivery.Store = (*DeliverySQLiteOwner)(nil)
