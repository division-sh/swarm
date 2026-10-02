package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func (a *Adapter) materializationEvent(ctx context.Context, q queryer, eventID string) (events.Event, error) {
	var absent events.Event
	var admitted events.AdmittedEvent
	var found bool
	var err error
	if a.dialect == DialectSQLite {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, q, eventID)
	} else {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, q, eventID)
	}
	if err != nil {
		return absent, err
	}
	if !found {
		return absent, fmt.Errorf("receiver materialization publication %s is missing", eventID)
	}
	return admitted.Event(), nil
}

func (a *Adapter) materializationReady(ctx context.Context, q queryer, record deliveryRecord, claimTx *sql.Tx) (bool, error) {
	if !record.Route.Recipient.IsAgent() || !record.Route.Target.MaterializingEntity() {
		return true, nil
	}
	if !record.Route.Initialization.FlowLifecycle() {
		return false, fmt.Errorf("receiver claim requires a canonical flow construction receipt, not node completion")
	}
	event, err := a.materializationEvent(ctx, q, record.EventID)
	if err != nil {
		return false, err
	}
	if err := record.Route.Initialization.ValidateEvent(event); err != nil {
		return false, err
	}
	if err := record.Route.Initialization.ValidateRoute(record.Route); err != nil {
		return false, err
	}
	return a.materializedReceiverExecutionReady(ctx, q, record, claimTx)
}

func (a *Adapter) materializedReceiverExecutionReady(ctx context.Context, q queryer, record deliveryRecord, claimTx *sql.Tx) (bool, error) {
	tx, ok := q.(*sql.Tx)
	if !ok || a.receiverExecution == nil {
		return false, fmt.Errorf("receiver execution admission requires its transactional owner")
	}
	materialized, err := a.receiverMaterialized(ctx, tx, record.Route)
	if err != nil {
		return false, err
	}
	if !materialized {
		return false, fmt.Errorf("receiver construction receipt has no exact persisted target")
	}
	return a.receiverExecution.ReceiverExecutionReadyTx(ctx, tx, record.Route, record.Authority, claimTx != nil)
}

func (a *Adapter) receiverMaterialized(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute) (bool, error) {
	if a.receiverTarget == nil {
		return false, fmt.Errorf("receiver materialization requires canonical target persistence")
	}
	return a.receiverTarget.ReceiverMaterializedTx(ctx, tx, route)
}

func (a *Adapter) continuationWithMaterialization(ctx context.Context, q queryer, record deliveryRecord, now time.Time) (deliverylifecycle.ClaimDisposition, deliverylifecycle.ContinuationWake, error) {
	disposition := continuationDisposition(record, now)
	wake := continuationWake(record, disposition, now)
	if disposition != deliverylifecycle.ClaimTerminal && disposition != deliverylifecycle.ClaimInvariantInvalid {
		parked, err := a.normalDispatchParked(ctx, q, record)
		if err != nil {
			return deliverylifecycle.ClaimInvariantInvalid, deliverylifecycle.ContinuationWake{}, err
		}
		if parked {
			return deliverylifecycle.ClaimParked, deliverylifecycle.ContinuationWake{}, nil
		}
	}
	if !record.Route.Recipient.IsAgent() || !record.Route.Target.MaterializingEntity() || disposition == deliverylifecycle.ClaimTerminal || disposition == deliverylifecycle.ClaimInvariantInvalid {
		return disposition, wake, nil
	}
	ready, err := a.materializationReady(ctx, q, record, nil)
	if err != nil {
		return deliverylifecycle.ClaimInvariantInvalid, deliverylifecycle.ContinuationWake{}, err
	}
	if !ready {
		// Materialization/readiness commits signal the existing continuation owner;
		// absence is not a timed retry and must never acquire an agent claim.
		return deliverylifecycle.ClaimDeferred, deliverylifecycle.ContinuationWake{}, nil
	}
	return disposition, wake, nil
}

// Selected deliveries have already passed the exact selected-execution fence;
// their materialized paused lifecycle is not an ordinary operator pause.
func (a *Adapter) normalDispatchParked(ctx context.Context, q queryer, record deliveryRecord) (bool, error) {
	if record.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
		return false, nil
	}
	return runstate.DispatchParked(ctx, q, a.dialect == DialectPostgres, record.RunID)
}
