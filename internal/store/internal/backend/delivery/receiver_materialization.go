package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

func (a *Adapter) materializationEvent(ctx context.Context, q queryer, eventID string) (events.Event, error) {
	var absent events.Event
	var record eventrecord.Record
	var found bool
	var err error
	if a.dialect == DialectSQLite {
		record, found, err = eventrecordsqlite.Load(ctx, q, eventID)
	} else {
		record, found, err = eventrecordpostgres.Load(ctx, q, eventID)
	}
	if err != nil {
		return absent, err
	}
	if !found {
		return absent, fmt.Errorf("receiver materialization publication %s is missing", eventID)
	}
	admitted, err := record.Decode()
	if err != nil {
		return absent, err
	}
	return admitted.Event(), nil
}

func (a *Adapter) publicationRecords(ctx context.Context, q queryer, eventID string) ([]deliveryRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 ORDER BY delivery_id`, eventID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	records := make([]deliveryRecord, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, q, id, false)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (a *Adapter) materializationReady(ctx context.Context, q queryer, record deliveryRecord, claimTx *sql.Tx) (bool, error) {
	plan := record.Route.Materialization
	if !record.Route.Recipient.IsAgent() || !record.Route.Target.MaterializingEntity() {
		return true, nil
	}
	event, err := a.materializationEvent(ctx, q, record.EventID)
	if err != nil {
		return false, err
	}
	records, err := a.publicationRecords(ctx, q, record.EventID)
	if err != nil {
		return false, err
	}
	routes := make([]events.DeliveryRoute, len(records))
	var materializer deliveryRecord
	for i, candidate := range records {
		routes[i] = candidate.Route
		if candidate.RouteIdentity == plan.Materializer() {
			materializer = candidate
		}
	}
	if err := events.ValidateReceiverMaterializations(event, routes); err != nil {
		return false, err
	}
	if plan.Empty() {
		if !record.Route.Initialization.FlowLifecycle() {
			return false, fmt.Errorf("materializing agent has no admitted lifecycle supplier or node dependency")
		}
		return a.materializedReceiverExecutionReady(ctx, q, record, claimTx)
	}
	if err := validateMaterializerAuthority(materializer.Snapshot, record.Snapshot); err != nil {
		return false, err
	}
	if materializer.Status == deliverylifecycle.StatusDeadLetter {
		return false, fmt.Errorf("receiver dependency survived terminal materializer settlement")
	}
	if materializer.Status != deliverylifecycle.StatusDelivered {
		return false, nil
	}
	return a.materializedReceiverExecutionReady(ctx, q, record, claimTx)
}

func validateMaterializerAuthority(materializer, dependent deliverylifecycle.Snapshot) error {
	if materializer.RunID != dependent.RunID {
		return fmt.Errorf("receiver materializer run contradicts dependent delivery")
	}
	if materializer.Authority.Equal(dependent.Authority) {
		return nil
	}
	// Normal startup rebinds unfinished deliveries, not completed history. A
	// committed materialization remains valid; the dependent's own current grant
	// is still independently admitted and fenced before it can acquire a claim.
	if materializer.Status == deliverylifecycle.StatusDelivered &&
		materializer.Authority.Kind() == deliverylifecycle.ExecutionAuthorityNormalRuntime &&
		dependent.Authority.Kind() == deliverylifecycle.ExecutionAuthorityNormalRuntime &&
		materializer.Authority.SourceArtifact() == dependent.Authority.SourceArtifact() {
		return nil
	}
	return fmt.Errorf("receiver materializer execution authority contradicts dependent delivery")
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
		return false, fmt.Errorf("settled receiver materializer has no exact persisted target")
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

// TerminalizeMaterializationDependents is part of the materializer's terminal
// transaction. It never runs an agent or fabricates a successful materialization.
func (a *Adapter) TerminalizeMaterializationDependents(ctx context.Context, tx *sql.Tx, story authoractivity.Mutation, materializer deliverylifecycle.Snapshot) ([]deliverylifecycle.Terminalization, error) {
	if !materializer.Route.Recipient.IsNode() || (materializer.Status != deliverylifecycle.StatusDeadLetter && materializer.Status != deliverylifecycle.StatusDelivered) {
		return nil, nil
	}
	records, err := a.publicationRecords(ctx, tx, materializer.EventID)
	if err != nil {
		return nil, err
	}
	var ids []string
	var routes []events.DeliveryRoute
	reason := "receiver_materialization_terminal"
	for _, record := range records {
		routes = append(routes, record.Route)
		plan := record.Route.Materialization
		if plan.Empty() || plan.Materializer() != materializer.RouteIdentity {
			continue
		}
		if err := validateMaterializerAuthority(materializer, record.Snapshot); err != nil {
			return nil, err
		}
		if materializer.Status == deliverylifecycle.StatusDelivered {
			materialized, err := a.receiverMaterialized(ctx, tx, record.Route)
			if err != nil {
				return nil, err
			}
			if materialized {
				continue
			}
			reason = "receiver_materialization_missing"
		}
		if record.Status == deliverylifecycle.StatusDelivered || record.Status == deliverylifecycle.StatusInProgress {
			return nil, fmt.Errorf("dependent agent executed before materialization succeeded")
		}
		ids = append(ids, record.DeliveryID)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	event, err := a.materializationEvent(ctx, tx, materializer.EventID)
	if err != nil {
		return nil, err
	}
	if err := events.ValidateReceiverMaterializations(event, routes); err != nil {
		return nil, err
	}
	failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict,
		reason, "delivery_lifecycle", "receiver_materialization",
		map[string]any{"materializer_delivery_id": materializer.DeliveryID, "materializer_reason_code": materializer.ReasonCode}))
	if !ok {
		return nil, fmt.Errorf("construct receiver materialization failure")
	}
	return a.terminalizeDeliveries(ctx, tx, story, ids, reason, failure)
}
