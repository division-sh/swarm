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
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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
	if err := authority.Validate(); err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	if _, err := route.Identity(); err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.ClaimResult, error) {
		var claimed runtimedelivery.ClaimResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			transactiontest.Mark(txctx, transactiontest.DeliveryClaim)
			runID, err := postgresDeliveryAdapter.RunIDExact(txctx, tx, event, route)
			if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
				return err
			}
			if err == nil {
				if err := runstate.RequirePostgresActiveTx(txctx, tx, runID); err != nil {
					return err
				}
			}
			claimed, err = s.receiverAdapter.ClaimExactResultWithRenewal(txctx, attempt, authority, event, route, runtimedelivery.DefaultLeaseTTL)
			return err
		})
		return claimed, err
	})
	claimed, acknowledged := result.Value()
	claimed.Acknowledged = acknowledged
	claimed.Renewal.Acknowledged = acknowledged && claimed.Disposition == runtimedelivery.ClaimAcquired
	return claimed, result.Err()
}

func (s *DeliverySQLiteOwner) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if route.Normalized().Recipient.Empty() {
		return runtimedelivery.ClaimResult{}, fmt.Errorf("delivery recipient is required")
	}
	if err := authority.Validate(); err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	if _, err := route.Identity(); err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite claim delivery", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimedelivery.ClaimResult, error) {
		var claimed runtimedelivery.ClaimResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			transactiontest.Mark(txctx, transactiontest.DeliveryClaim)
			runID, err := sqliteDeliveryAdapter.RunIDExact(txctx, tx, event, route)
			if err != nil && !errors.Is(err, runtimedelivery.ErrNotFound) && !errors.Is(err, runtimedelivery.ErrConflict) {
				return err
			}
			if err == nil {
				if err := runstate.RequireSQLiteActiveTx(txctx, tx, runID); err != nil {
					return err
				}
			}
			claimed, err = s.receiverAdapter.ClaimExactResultWithRenewal(txctx, attempt, authority, event, route, runtimedelivery.DefaultLeaseTTL)
			return err
		})
		return claimed, err
	})
	claimed, acknowledged := result.Value()
	claimed.Acknowledged = acknowledged
	claimed.Renewal.Acknowledged = acknowledged && claimed.Disposition == runtimedelivery.ClaimAcquired
	return claimed, result.Err()
}

func (s *DeliveryPostgresOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	var page runtimedelivery.ContinuationPage
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationScan)
		var err error
		page, err = s.receiverAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
		if err != nil {
			return err
		}
		return hydrateContinuationEvents(txctx, tx, &page,
			func(ctx context.Context, tx *sql.Tx, ids []string) ([]eventrecord.Record, error) {
				return eventrecordpostgres.LoadMany(ctx, tx, ids)
			},
			func(ctx context.Context, tx *sql.Tx, id string) (eventrecord.Record, bool, error) {
				return eventrecordpostgres.Load(ctx, tx, id)
			},
		)
	})
	return page, err
}

func (s *DeliverySQLiteOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	var page runtimedelivery.ContinuationPage
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationScan)
		var err error
		page, err = s.receiverAdapter.ScanContinuations(txctx, tx, authority, cursor, limit)
		if err != nil {
			return err
		}
		return hydrateContinuationEvents(txctx, tx, &page,
			func(ctx context.Context, tx *sql.Tx, ids []string) ([]eventrecord.Record, error) {
				return eventrecordsqlite.LoadMany(ctx, tx, ids)
			},
			func(ctx context.Context, tx *sql.Tx, id string) (eventrecord.Record, bool, error) {
				return eventrecordsqlite.Load(ctx, tx, id)
			},
		)
	})
	return page, err
}

func hydrateContinuationEvents(
	ctx context.Context,
	tx *sql.Tx,
	page *runtimedelivery.ContinuationPage,
	loadMany func(context.Context, *sql.Tx, []string) ([]eventrecord.Record, error),
	loadOne func(context.Context, *sql.Tx, string) (eventrecord.Record, bool, error),
) error {
	ids := make([]string, 0, len(page.Items))
	seen := make(map[string]struct{}, len(page.Items))
	for _, item := range page.Items {
		if item.Disposition == runtimedelivery.ClaimAbsent || item.Disposition == runtimedelivery.ClaimInvariantInvalid {
			continue
		}
		id := item.Snapshot.EventID
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	records, err := loadMany(ctx, tx, ids)
	if errors.Is(err, eventrecord.ErrMissing) {
		return hydrateContinuationEventsIndividually(ctx, tx, page, loadOne)
	}
	if err != nil {
		return err
	}
	byID := make(map[string]eventrecord.Record, len(records))
	for _, record := range records {
		byID[record.EventID] = record
	}
	for i := range page.Items {
		item := &page.Items[i]
		if item.Disposition == runtimedelivery.ClaimAbsent || item.Disposition == runtimedelivery.ClaimInvariantInvalid {
			continue
		}
		admitted, err := byID[item.Snapshot.EventID].Decode()
		if err != nil {
			item.Disposition = runtimedelivery.ClaimInvariantInvalid
			item.Invariant = err
			continue
		}
		item.Event = admitted.Event()
	}
	return nil
}

func hydrateContinuationEventsIndividually(
	ctx context.Context,
	tx *sql.Tx,
	page *runtimedelivery.ContinuationPage,
	loadOne func(context.Context, *sql.Tx, string) (eventrecord.Record, bool, error),
) error {
	for i := range page.Items {
		item := &page.Items[i]
		if item.Disposition == runtimedelivery.ClaimAbsent || item.Disposition == runtimedelivery.ClaimInvariantInvalid {
			continue
		}
		record, found, err := loadOne(ctx, tx, item.Snapshot.EventID)
		if err != nil {
			return err
		}
		if !found {
			item.Disposition = runtimedelivery.ClaimInvariantInvalid
			item.Invariant = fmt.Errorf("delivery event %s is absent", item.Snapshot.EventID)
			continue
		}
		admitted, err := record.Decode()
		if err != nil {
			item.Disposition = runtimedelivery.ClaimInvariantInvalid
			item.Invariant = err
			continue
		}
		item.Event = admitted.Event()
	}
	return nil
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
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationObserve)
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
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationObserve)
		var err error
		observation, err = s.receiverAdapter.ObserveContinuation(txctx, tx, authority, deliveryID)
		return err
	})
	return observation, err
}

func observeDeliveryContinuationsTx(ctx context.Context, tx *sql.Tx, adapter *Adapter, authority runtimedelivery.ExecutionAuthority, deliveryIDs []string) ([]runtimedelivery.ContinuationObservation, error) {
	observations := make([]runtimedelivery.ContinuationObservation, 0, len(deliveryIDs))
	for _, deliveryID := range deliveryIDs {
		observation, err := adapter.ObserveContinuation(ctx, tx, authority, deliveryID)
		if err != nil {
			return nil, err
		}
		if observation.DeliveryID != deliveryID {
			return nil, fmt.Errorf("delivery continuation observation %s returned identity %s", deliveryID, observation.DeliveryID)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func validateContinuationObservationBatch(deliveryIDs []string) error {
	if len(deliveryIDs) == 0 || len(deliveryIDs) > runtimedelivery.MaxContinuationObservationBatch {
		return fmt.Errorf("delivery continuation observation batch must contain 1..%d identities", runtimedelivery.MaxContinuationObservationBatch)
	}
	seen := make(map[string]struct{}, len(deliveryIDs))
	for _, deliveryID := range deliveryIDs {
		if deliveryID == "" {
			return errors.New("delivery continuation observation identity is empty")
		}
		if _, exists := seen[deliveryID]; exists {
			return fmt.Errorf("duplicate delivery continuation observation identity %s", deliveryID)
		}
		seen[deliveryID] = struct{}{}
	}
	return nil
}

func (s *DeliveryPostgresOwner) ObserveDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, deliveryIDs []string) ([]runtimedelivery.ContinuationObservation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if err := validateContinuationObservationBatch(deliveryIDs); err != nil {
		return nil, err
	}
	var observations []runtimedelivery.ContinuationObservation
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationObserve)
		var err error
		observations, err = observeDeliveryContinuationsTx(txctx, tx, s.receiverAdapter, authority, deliveryIDs)
		return err
	})
	return observations, err
}

func (s *DeliverySQLiteOwner) ObserveDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, deliveryIDs []string) ([]runtimedelivery.ContinuationObservation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if err := validateContinuationObservationBatch(deliveryIDs); err != nil {
		return nil, err
	}
	var observations []runtimedelivery.ContinuationObservation
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.DeliveryContinuationObserve)
		var err error
		observations, err = observeDeliveryContinuationsTx(txctx, tx, s.receiverAdapter, authority, deliveryIDs)
		return err
	})
	return observations, err
}

func (s *DeliveryPostgresOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	return postgresDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		transactiontest.Mark(txctx, transactiontest.DeliveryRenew)
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return s.renewClaimTx(txctx, attempt, claim, runtimedelivery.DefaultLeaseTTL)
	})
}

func (s *DeliverySQLiteOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	return sqliteDeliveryMutation(s, ctx, nil, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		transactiontest.Mark(txctx, transactiontest.DeliveryRenew)
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
	_, err := postgresDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection())
	return err
}

func (s *DeliverySQLiteOwner) SettleProviderOriginSuccessTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	claim runtimedelivery.Claim,
	sideEffects []string,
	duration time.Duration,
) error {
	_, err := sqliteDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, runtimedelivery.NotApplicableHandlerRuleSelection())
	return err
}

// RequireWorkflowAcceptedEventTx binds a preserved-state timer reaction to its
// exact inbound delivery; claim renewal and settlement still occur together.
func (s *DeliveryPostgresOwner) RequireWorkflowAcceptedEventTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim, cause workflowlifecycle.Effect) error {
	return requireWorkflowAcceptedEventTx(ctx, tx, true, claim, cause)
}

func (s *DeliverySQLiteOwner) RequireWorkflowAcceptedEventTx(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim, cause workflowlifecycle.Effect) error {
	return requireWorkflowAcceptedEventTx(ctx, tx, false, claim, cause)
}

func requireWorkflowAcceptedEventTx(ctx context.Context, tx *sql.Tx, postgres bool, claim runtimedelivery.Claim, cause workflowlifecycle.Effect) error {
	query := `SELECT e.event_name, e.created_at FROM events e JOIN event_deliveries d ON d.event_id=e.event_id AND d.run_id=e.run_id
		WHERE e.run_id=? AND e.event_id=? AND d.delivery_id=?`
	if postgres {
		query = `SELECT e.event_name, e.created_at FROM events e JOIN event_deliveries d ON d.event_id=e.event_id AND d.run_id=e.run_id
			WHERE e.run_id=$1::uuid AND e.event_id=$2::uuid AND d.delivery_id=$3::uuid`
	}
	var eventType string
	var occurredAt any
	if err := tx.QueryRowContext(ctx, query, claim.RunID(), cause.EventID(), claim.DeliveryID()).Scan(&eventType, &occurredAt); err != nil {
		return fmt.Errorf("read preserved workflow accepted-event delivery: %w", err)
	}
	at, found, err := parseNullableTime(occurredAt)
	if err != nil {
		return fmt.Errorf("decode preserved workflow accepted-event occurrence: %w", err)
	}
	if !found || eventType != cause.EventType() || !at.Equal(cause.OccurredAt()) {
		return fmt.Errorf("preserved workflow accepted-event identity or canonical occurrence disagrees with delivery")
	}
	return nil
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
		if _, err := s.renewClaimTx(ctx, attempt, claim, runtimedelivery.DefaultLeaseTTL); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := postgresDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
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
		if _, err := s.renewClaimTx(ctx, attempt, claim, runtimedelivery.DefaultLeaseTTL); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := sqliteDeliveryAdapter.SettleSuccess(ctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
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

// SettleWorkflowNodeSuccess owns mutation-free node success: renewal, outcome,
// exact claim settlement and completion evidence share one transaction.
func (s *DeliveryPostgresOwner) SettleWorkflowNodeSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.ClaimCommit, error) {
	if claim.SubscriberClass() != runtimedelivery.SubscriberNode {
		return runtimedelivery.ClaimCommit{}, fmt.Errorf("workflow node success requires a node claim")
	}
	return postgresDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
		if err := runstate.RequirePostgresActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := s.SettleWorkflowNodeSuccessTx(txctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *DeliverySQLiteOwner) SettleWorkflowNodeSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.ClaimCommit, error) {
	if claim.SubscriberClass() != runtimedelivery.SubscriberNode {
		return runtimedelivery.ClaimCommit{}, fmt.Errorf("workflow node success requires a node claim")
	}
	return sqliteDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
		if err := runstate.RequireSQLiteActiveTx(txctx, tx, claim.RunID()); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		snapshot, err := s.SettleWorkflowNodeSuccessTx(txctx, attempt, claim, sideEffects, duration, selection)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		if _, err := attempt.RequestCompletion(txctx, s.candidateRequests, claim.RunID(), nil); err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		return snapshot, nil
	})
}

func (s *DeliveryPostgresOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	commit, err := postgresDeliveryMutation(s, ctx, s.candidates, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) (runtimedelivery.Snapshot, error) {
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
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
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
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
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
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
		transactiontest.Mark(txctx, transactiontest.DeliverySettle)
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
			return err
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
			return err
		})
		return snapshot, err
	})
	snapshot, acknowledged := result.Value()
	return runtimedelivery.ClaimCommit{Snapshot: snapshot, Acknowledged: acknowledged}, result.Err()
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
