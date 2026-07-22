package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

var (
	postgresDeliveryAdapter = mustDeliveryAdapter(runtimedelivery.DialectPostgres)
	sqliteDeliveryAdapter   = mustDeliveryAdapter(runtimedelivery.DialectSQLite)
)

func mustDeliveryAdapter(dialect runtimedelivery.Dialect) *runtimedelivery.Adapter {
	adapter, err := runtimedelivery.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func (s *PostgresStore) commitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	_, err := postgresDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes)
	return err
}

func (s *SQLiteRuntimeStore) commitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	_, err := sqliteDeliveryAdapter.CommitInitial(ctx, tx, eventID, runID, routes)
	return err
}

func (s *PostgresStore) ClaimAgentDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := requireDeliveryRouteClass(route, runtimedelivery.SubscriberAgent); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var claimed runtimedelivery.ClaimedObligation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := postgresDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, snapshot.RunID, storerunlifecycle.DialectPostgres); err != nil {
			return err
		}
		claimed, err = postgresDeliveryAdapter.ClaimExact(txctx, tx, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func (s *SQLiteRuntimeStore) ClaimAgentDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := requireDeliveryRouteClass(route, runtimedelivery.SubscriberAgent); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var claimed runtimedelivery.ClaimedObligation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := sqliteDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, snapshot.RunID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		claimed, err = sqliteDeliveryAdapter.ClaimExact(txctx, tx, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func (s *PostgresStore) ClaimNodeDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := requireDeliveryRouteClass(route, runtimedelivery.SubscriberNode); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var claimed runtimedelivery.ClaimedObligation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := postgresDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, snapshot.RunID, storerunlifecycle.DialectPostgres); err != nil {
			return err
		}
		claimed, err = postgresDeliveryAdapter.ClaimExact(txctx, tx, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func (s *SQLiteRuntimeStore) ClaimNodeDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := requireDeliveryRouteClass(route, runtimedelivery.SubscriberNode); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var claimed runtimedelivery.ClaimedObligation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		snapshot, err := sqliteDeliveryAdapter.SnapshotExact(txctx, tx, event, route)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, snapshot.RunID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		claimed, err = sqliteDeliveryAdapter.ClaimExact(txctx, tx, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func requireDeliveryRouteClass(route events.DeliveryRoute, want runtimedelivery.SubscriberClass) error {
	class, err := runtimedelivery.ParseSubscriberClass(route.SubscriberType)
	if err != nil {
		return err
	}
	if class != want {
		return fmt.Errorf("delivery route class %q does not match operation class %q", class, want)
	}
	return nil
}

func (s *PostgresStore) ClaimAgentBacklog(ctx context.Context, agentID string, limit int) ([]runtimedelivery.AgentExecution, error) {
	claimed := []runtimedelivery.AgentExecution{}
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := postgresDeliveryAdapter.AgentClaimCandidates(txctx, tx, agentID, limit)
		if err != nil {
			return err
		}
		if err := requireActiveDeliveryCandidateRuns(txctx, tx, candidates, storerunlifecycle.DialectPostgres); err != nil {
			return err
		}
		claims, err := postgresDeliveryAdapter.ClaimCandidates(txctx, tx, candidates, runtimedelivery.DefaultLeaseTTL)
		if err != nil {
			return err
		}
		claimed, err = hydratePostgresAgentExecutions(txctx, tx, claims)
		return err
	})
	return claimed, err
}

func (s *SQLiteRuntimeStore) ClaimAgentBacklog(ctx context.Context, agentID string, limit int) ([]runtimedelivery.AgentExecution, error) {
	claimed := []runtimedelivery.AgentExecution{}
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := sqliteDeliveryAdapter.AgentClaimCandidates(txctx, tx, agentID, limit)
		if err != nil {
			return err
		}
		if err := requireActiveDeliveryCandidateRuns(txctx, tx, candidates, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		claims, err := sqliteDeliveryAdapter.ClaimCandidates(txctx, tx, candidates, runtimedelivery.DefaultLeaseTTL)
		if err != nil {
			return err
		}
		claimed, err = hydrateSQLiteAgentExecutions(txctx, tx, claims)
		return err
	})
	return claimed, err
}

func (s *PostgresStore) ClaimNodeBacklog(ctx context.Context, nodeID string, limit int) ([]runtimedelivery.NodeExecution, error) {
	claimed := []runtimedelivery.NodeExecution{}
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := postgresDeliveryAdapter.NodeClaimCandidates(txctx, tx, nodeID, limit)
		if err != nil {
			return err
		}
		if err := requireActiveDeliveryCandidateRuns(txctx, tx, candidates, storerunlifecycle.DialectPostgres); err != nil {
			return err
		}
		claims, err := postgresDeliveryAdapter.ClaimCandidates(txctx, tx, candidates, runtimedelivery.DefaultLeaseTTL)
		if err != nil {
			return err
		}
		claimed, err = hydratePostgresAgentExecutions(txctx, tx, claims)
		return err
	})
	return claimed, err
}

func (s *SQLiteRuntimeStore) ClaimNodeBacklog(ctx context.Context, nodeID string, limit int) ([]runtimedelivery.NodeExecution, error) {
	claimed := []runtimedelivery.NodeExecution{}
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := sqliteDeliveryAdapter.NodeClaimCandidates(txctx, tx, nodeID, limit)
		if err != nil {
			return err
		}
		if err := requireActiveDeliveryCandidateRuns(txctx, tx, candidates, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		claims, err := sqliteDeliveryAdapter.ClaimCandidates(txctx, tx, candidates, runtimedelivery.DefaultLeaseTTL)
		if err != nil {
			return err
		}
		claimed, err = hydrateSQLiteAgentExecutions(txctx, tx, claims)
		return err
	})
	return claimed, err
}

func requireActiveDeliveryCandidateRuns(ctx context.Context, tx *sql.Tx, candidates []runtimedelivery.ClaimCandidate, dialect storerunlifecycle.Dialect) error {
	runSet := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		runSet[candidate.RunID()] = struct{}{}
	}
	runIDs := make([]string, 0, len(runSet))
	for runID := range runSet {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if err := storerunlifecycle.RequireActive(ctx, tx, runID, dialect); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectPostgres); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.RenewClaim(txctx, tx, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *SQLiteRuntimeStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectSQLite); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.RenewClaim(txctx, tx, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func hydratePostgresAgentExecutions(ctx context.Context, tx *sql.Tx, claims []runtimedelivery.ClaimedObligation) ([]runtimedelivery.AgentExecution, error) {
	out := make([]runtimedelivery.AgentExecution, 0, len(claims))
	for _, claimed := range claims {
		record, found, err := eventrecordpostgres.Load(ctx, tx, claimed.Snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(claimed.Snapshot.EventID)
			}
			return nil, err
		}
		execution, err := deliveryExecutionFromRecord(record, claimed)
		if err != nil {
			return nil, err
		}
		out = append(out, execution)
	}
	return out, nil
}

func hydrateSQLiteAgentExecutions(ctx context.Context, tx *sql.Tx, claims []runtimedelivery.ClaimedObligation) ([]runtimedelivery.AgentExecution, error) {
	out := make([]runtimedelivery.AgentExecution, 0, len(claims))
	for _, claimed := range claims {
		record, found, err := eventrecordsqlite.Load(ctx, tx, claimed.Snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(claimed.Snapshot.EventID)
			}
			return nil, err
		}
		execution, err := deliveryExecutionFromRecord(record, claimed)
		if err != nil {
			return nil, err
		}
		out = append(out, execution)
	}
	return out, nil
}

func deliveryExecutionFromRecord(record eventrecord.Record, claimed runtimedelivery.ClaimedObligation) (runtimedelivery.AgentExecution, error) {
	admitted, err := record.Decode()
	if err != nil {
		return runtimedelivery.AgentExecution{}, err
	}
	delivery, err := events.NewDeliveryEvent(admitted.Event(), claimed.Snapshot.Route)
	if err != nil {
		return runtimedelivery.AgentExecution{}, fmt.Errorf("hydrate claimed delivery event: %w", err)
	}
	return runtimedelivery.AgentExecution{Event: delivery.Event(), Snapshot: claimed.Snapshot, Claim: claimed.Claim}, nil
}

func (s *PostgresStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectPostgres); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *SQLiteRuntimeStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectSQLite); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.BindAgentSession(txctx, tx, claim, sessionID)
	})
}

func (s *PostgresStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectPostgres); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return postgresDeliveryAdapter.SettleSuccess(txctx, tx, claim, sideEffects, duration)
	})
}

func (s *SQLiteRuntimeStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectSQLite); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return sqliteDeliveryAdapter.SettleSuccess(txctx, tx, claim, sideEffects, duration)
	})
}

func (s *PostgresStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return postgresDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectPostgres); err != nil {
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
		return snapshot, nil
	})
}

func (s *SQLiteRuntimeStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return sqliteDeliveryMutation(s, ctx, func(txctx context.Context, tx *sql.Tx) (runtimedelivery.Snapshot, error) {
		if err := storerunlifecycle.RequireActive(txctx, tx, claim.RunID(), storerunlifecycle.DialectSQLite); err != nil {
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
	return postgresDeliveryAdapter.Snapshot(ctx, eventReadQueryerFromContext(ctx, s.DB), deliveryID)
}

func (s *SQLiteRuntimeStore) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return sqliteDeliveryAdapter.Snapshot(ctx, eventReadQueryerFromContext(ctx, s.DB), deliveryID)
}

func (s *PostgresStore) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return postgresDeliveryAdapter.Outcomes(ctx, eventReadQueryerFromContext(ctx, s.DB), deliveryID)
}

func (s *SQLiteRuntimeStore) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return sqliteDeliveryAdapter.Outcomes(ctx, eventReadQueryerFromContext(ctx, s.DB), deliveryID)
}

func (s *PostgresStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return postgresDeliveryAdapter.ProveHandoff(ctx, eventReadQueryerFromContext(ctx, s.DB), eventID, route)
}

func (s *SQLiteRuntimeStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.DurableHandoffProof{}, err
	}
	return sqliteDeliveryAdapter.ProveHandoff(ctx, eventReadQueryerFromContext(ctx, s.DB), eventID, route)
}

func (s *PostgresStore) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return postgresDeliveryAdapter.SummarizeRun(ctx, eventReadQueryerFromContext(ctx, s.DB), runID)
}

func (s *SQLiteRuntimeStore) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return sqliteDeliveryAdapter.SummarizeRun(ctx, eventReadQueryerFromContext(ctx, s.DB), runID)
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
	return postgresDeliveryAdapter.SnapshotsForEvent(ctx, eventReadQueryerFromContext(ctx, s.DB), eventID)
}

func (s *SQLiteRuntimeStore) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.SnapshotsForEvent(ctx, eventReadQueryerFromContext(ctx, s.DB), eventID)
}

func (s *PostgresStore) deliverySnapshotsForRun(ctx context.Context, runID string) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.SnapshotsForRun(ctx, eventReadQueryerFromContext(ctx, s.DB), runID)
}

func (s *SQLiteRuntimeStore) deliverySnapshotsForRun(ctx context.Context, runID string) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.SnapshotsForRun(ctx, eventReadQueryerFromContext(ctx, s.DB), runID)
}

func (s *PostgresStore) deliverySnapshotsForAgent(ctx context.Context, agentID string, since time.Time) ([]runtimedelivery.Snapshot, error) {
	return postgresDeliveryAdapter.SnapshotsForAgent(ctx, eventReadQueryerFromContext(ctx, s.DB), agentID, since)
}

func (s *SQLiteRuntimeStore) deliverySnapshotsForAgent(ctx context.Context, agentID string, since time.Time) ([]runtimedelivery.Snapshot, error) {
	return sqliteDeliveryAdapter.SnapshotsForAgent(ctx, eventReadQueryerFromContext(ctx, s.DB), agentID, since)
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
