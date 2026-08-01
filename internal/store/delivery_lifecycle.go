package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/delivery"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
)

var (
	postgresDeliveryAdapter = mustDeliveryAdapter(deliveryadapter.DialectPostgres)
	sqliteDeliveryAdapter   = mustDeliveryAdapter(deliveryadapter.DialectSQLite)
)

func mustDeliveryAdapter(dialect deliveryadapter.Dialect) *deliveryadapter.Adapter {
	adapter, err := deliveryadapter.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func (s *PostgresStore) commitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return postgresDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes, authority)
}

func (s *SQLiteRuntimeStore) commitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return sqliteDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes, authority)
}

func (s *PostgresStore) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return postgresDeliveryAdapter.ActivateNormalAuthority(txctx, tx, authority)
	})
}

func (s *SQLiteRuntimeStore) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return sqliteDeliveryAdapter.ActivateNormalAuthority(txctx, tx, authority)
	})
}

func (s *PostgresStore) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RecoveryInventory{}, err
	}
	return postgresDeliveryAdapter.InspectRecovery(ctx, s.backend.db, source)
}

func (s *SQLiteRuntimeStore) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RecoveryInventory{}, err
	}
	return sqliteDeliveryAdapter.InspectRecovery(ctx, s.backend.db, source)
}

func (s *PostgresStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	var result runtimedelivery.ClaimResult
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := postgresDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
			return err
		}
		if err == nil {
			if err := requirePostgresRunActive(txctx, tx, snapshot.RunID); err != nil {
				return err
			}
		}
		result, err = postgresDeliveryAdapter.ClaimExactResult(txctx, tx, authority, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return result, err
}

func (s *SQLiteRuntimeStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	var result runtimedelivery.ClaimResult
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := sqliteDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
			return err
		}
		if err == nil {
			if err := requireSQLiteRunActive(txctx, tx, snapshot.RunID); err != nil {
				return err
			}
		}
		result, err = sqliteDeliveryAdapter.ClaimExactResult(txctx, tx, authority, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return result, err
}

func (s *PostgresStore) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	var page runtimedelivery.ContinuationPage
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
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

func (s *SQLiteRuntimeStore) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	var page runtimedelivery.ContinuationPage
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
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

func (s *PostgresStore) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	var observation runtimedelivery.ContinuationObservation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		observation, err = postgresDeliveryAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func (s *SQLiteRuntimeStore) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	var observation runtimedelivery.ContinuationObservation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		observation, err = sqliteDeliveryAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func (s *PostgresStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requirePostgresRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.RenewClaim(txctx, tx, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *SQLiteRuntimeStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requireSQLiteRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.RenewClaim(txctx, tx, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *PostgresStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requirePostgresRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *SQLiteRuntimeStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requireSQLiteRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *PostgresStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requirePostgresRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := postgresDeliveryAdapter.SettleSuccess(txctx, tx, claim, sideEffects, duration)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := s.requestCompletionCandidateTx(txctx, tx, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *SQLiteRuntimeStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requireSQLiteRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := sqliteDeliveryAdapter.SettleSuccess(txctx, tx, claim, sideEffects, duration)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := s.requestCompletionCandidateTx(txctx, tx, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *PostgresStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requirePostgresRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := postgresDeliveryAdapter.SettleFailure(txctx, tx, claim, settlement)
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
		if err := s.RecordDeadLetterTx(txctx, tx, diagnostic); err != nil {
			return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
		}
		if _, err := s.requestCompletionCandidateTx(txctx, tx, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *SQLiteRuntimeStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := requireSQLiteRunActive(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := sqliteDeliveryAdapter.SettleFailure(txctx, tx, claim, settlement)
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
		if err := s.RecordDeadLetterTx(txctx, tx, diagnostic); err != nil {
			return runtimedelivery.Snapshot{}, fmt.Errorf("commit terminal delivery diagnostic: %w", err)
		}
		if _, err := s.requestCompletionCandidateTx(txctx, tx, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
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
	return runtimedeadletters.Record{
		OriginalEventID: record.EventID,
		DeliveryID:      snapshot.DeliveryID,
		ClaimVersion:    snapshot.ClaimVersion,
		OriginalEvent:   record.EventName,
		OriginalPayload: append([]byte(nil), record.Payload...),
		EntityID:        record.EntityID,
		FlowInstance:    record.FlowInstance,
		Failure:         *failure,
		RetryCount:      snapshot.RetryCount,
		ChainDepth:      record.ChainDepth,
		HandlerNode:     snapshot.SubscriberID,
		Timestamp:       snapshot.SettledAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func postgresDeliveryMutation(s *PostgresStore, ctx context.Context, operation func(context.Context, *sql.Tx) (runtimedelivery.Snapshot, error)) (runtimedelivery.Snapshot, error) {
	var snapshot runtimedelivery.Snapshot
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = operation(txctx, tx)
		return err
	})
	return snapshot, err
}

func sqliteDeliveryMutation(s *SQLiteRuntimeStore, ctx context.Context, operation func(context.Context, *sql.Tx) (runtimedelivery.Snapshot, error)) (runtimedelivery.Snapshot, error) {
	var snapshot runtimedelivery.Snapshot
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = operation(txctx, tx)
		return err
	})
	return snapshot, err
}

func (s *PostgresStore) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return postgresDeliveryAdapter.Snapshot(ctx, eventReadQueryerFromContext(ctx, s.backend.db), deliveryID)
}

func (s *SQLiteRuntimeStore) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return sqliteDeliveryAdapter.Snapshot(ctx, eventReadQueryerFromContext(ctx, s.backend.db), deliveryID)
}

func (s *PostgresStore) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return postgresDeliveryAdapter.Outcomes(ctx, eventReadQueryerFromContext(ctx, s.backend.db), deliveryID)
}

func (s *SQLiteRuntimeStore) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return sqliteDeliveryAdapter.Outcomes(ctx, eventReadQueryerFromContext(ctx, s.backend.db), deliveryID)
}

func (s *PostgresStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return postgresDeliveryAdapter.ProveHandoff(ctx, eventReadQueryerFromContext(ctx, s.backend.db), eventID, route)
}

func (s *SQLiteRuntimeStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return sqliteDeliveryAdapter.ProveHandoff(ctx, eventReadQueryerFromContext(ctx, s.backend.db), eventID, route)
}

func (s *PostgresStore) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return postgresDeliveryAdapter.SummarizeRun(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID)
}

func (s *SQLiteRuntimeStore) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return sqliteDeliveryAdapter.SummarizeRun(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID)
}

func (s *PostgresStore) TerminalizeRun(ctx context.Context, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return s.terminalizeRunDeliveriesTx(ctx, tx, runID, reason)
	}
	var out []runtimedelivery.Terminalization
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = s.terminalizeRunDeliveriesTx(txctx, tx, runID, reason)
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) TerminalizeRun(ctx context.Context, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return s.terminalizeRunDeliveriesTx(ctx, tx, runID, reason)
	}
	var out []runtimedelivery.Terminalization
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = s.terminalizeRunDeliveriesTx(txctx, tx, runID, reason)
		return err
	})
	return out, err
}

func (s *PostgresStore) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.SnapshotsForEvent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), eventID)
}

func (s *SQLiteRuntimeStore) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.SnapshotsForEvent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), eventID)
}

func (s *PostgresStore) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return postgresDeliveryAdapter.RunDiagnosticCounts(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID)
}

func (s *SQLiteRuntimeStore) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return sqliteDeliveryAdapter.RunDiagnosticCounts(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID)
}

func (s *PostgresStore) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.RunDiagnosticFailures(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID, limit)
}

func (s *SQLiteRuntimeStore) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.RunDiagnosticFailures(ctx, eventReadQueryerFromContext(ctx, s.backend.db), runID, limit)
}

func (s *PostgresStore) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return postgresDeliveryAdapter.RunTraceReferencePage(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *SQLiteRuntimeStore) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return sqliteDeliveryAdapter.RunTraceReferencePage(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *PostgresStore) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return postgresDeliveryAdapter.LifecycleSnapshotPageForAgent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *SQLiteRuntimeStore) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return sqliteDeliveryAdapter.LifecycleSnapshotPageForAgent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *PostgresStore) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return postgresDeliveryAdapter.DiagnosticSnapshotPageForAgent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *SQLiteRuntimeStore) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return sqliteDeliveryAdapter.DiagnosticSnapshotPageForAgent(ctx, eventReadQueryerFromContext(ctx, s.backend.db), query)
}

func (s *PostgresStore) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return postgresDeliveryAdapter.DiagnosticCountsForAgentSince(ctx, eventReadQueryerFromContext(ctx, s.backend.db), identity, since)
}

func (s *SQLiteRuntimeStore) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return sqliteDeliveryAdapter.DiagnosticCountsForAgentSince(ctx, eventReadQueryerFromContext(ctx, s.backend.db), identity, since)
}

func (s *PostgresStore) terminalizeRunDeliveriesTx(ctx context.Context, tx *sql.Tx, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	terminalizations, err := postgresDeliveryAdapter.TerminalizeRun(ctx, tx, runID, reason)
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
		if err := s.recordTerminalizedDeliveryDeadLetterTx(ctx, tx, diagnostic); err != nil {
			return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
		}
	}
	return terminalizations, nil
}

func (s *SQLiteRuntimeStore) terminalizeRunDeliveriesTx(ctx context.Context, tx *sql.Tx, runID, reason string) ([]runtimedelivery.Terminalization, error) {
	terminalizations, err := sqliteDeliveryAdapter.TerminalizeRun(ctx, tx, runID, reason)
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
		if err := s.recordTerminalizedDeliveryDeadLetterTx(ctx, tx, diagnostic); err != nil {
			return nil, fmt.Errorf("commit terminalized delivery diagnostic: %w", err)
		}
	}
	return terminalizations, nil
}

func (s *PostgresStore) activeRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

func (s *SQLiteRuntimeStore) activeRunDeliverySnapshotsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.ActiveRunSnapshots(ctx, tx, runID)
}

var _ runtimedelivery.Store = (*PostgresStore)(nil)
var _ runtimedelivery.Store = (*SQLiteRuntimeStore)(nil)
