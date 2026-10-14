package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	. "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func CountSuccessfulDirectivePipelineReceiptsStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline' AND outcome = 'success'`, eventID).Scan(&count)
	return count, err
}

func ReadDirectivePipelineFailureStorageTx(ctx context.Context, tx *sql.Tx, key string) ([]byte, error) {
	var failure []byte
	err := tx.QueryRowContext(ctx, `SELECT er.failure FROM event_receipts er JOIN agent_directive_operations op ON op.directive_event_id = er.event_id WHERE op.method = 'agent.send_directive' AND op.idempotency_key = $1 AND er.subscriber_type = 'platform' AND er.subscriber_id = 'pipeline'`, key).Scan(&failure)
	return failure, err
}

func CountUnfinishedMockAgentDeliveriesStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type = 'agent' AND (subscriber_id LIKE 'phrase-bot%' OR subscriber_id = 'memory-bot') AND status <> 'delivered'`).Scan(&count)
	return count, err
}

type MockAgentDeliveryDiagnosticStorage struct {
	SubscriberID, Status string
	ReasonCode, Failure  sql.NullString
}

type MockAgentDeliveryDiagnosticsStorage struct {
	Rows      []MockAgentDeliveryDiagnosticStorage
	ScanError error
}

func ReadMockAgentDeliveryDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) (MockAgentDeliveryDiagnosticsStorage, error) {
	var out MockAgentDeliveryDiagnosticsStorage
	rows, err := tx.QueryContext(ctx, `SELECT subscriber_id, status, reason_code, CAST(failure AS TEXT) FROM event_deliveries WHERE subscriber_type = 'agent' ORDER BY created_at`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row MockAgentDeliveryDiagnosticStorage
		if out.ScanError = rows.Scan(&row.SubscriberID, &row.Status, &row.ReasonCode, &row.Failure); out.ScanError != nil {
			break
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

func ReadMockConnectorDeliveryDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT subscriber_id, status, COALESCE(reason_code, ''), CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE subscriber_id LIKE '%telegram%' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var subscriber, status, reason, failure string
		if err := rows.Scan(&subscriber, &status, &reason, &failure); err != nil {
			out = append(out, "scan_error="+err.Error())
			break
		}
		out = append(out, fmt.Sprintf("%s/%s/%s=%s", subscriber, status, reason, failure))
	}
	return out, nil
}

func CountUnfinishedMemoryAgentDeliveriesStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE (subscriber_id LIKE 'phrase-bot%' OR subscriber_id = 'memory-bot') AND status <> 'delivered'`).Scan(&count)
	return count, err
}

type ServedDeliveryRouteEvidenceStorage struct {
	ID, EventID, Identity, Kind, Recipient, Target, Context, Projection, Claim, Initialization string
}

type ServedTemplateCompletionDiagnosticStorage struct {
	Columns                                 []string
	Rows                                    [][]sql.NullString
	ColumnsError, ScanError, IterationError error
}

func scanServedPhysicalDiagnosticRows(rows *sql.Rows) (ServedTemplateCompletionDiagnosticStorage, error) {
	var out ServedTemplateCompletionDiagnosticStorage
	defer rows.Close()
	out.Columns, out.ColumnsError = rows.Columns()
	if out.ColumnsError != nil {
		return out, nil
	}
	for rows.Next() {
		values := make([]sql.NullString, len(out.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			out.ScanError = err
			return out, nil
		}
		out.Rows = append(out.Rows, values)
	}
	out.IterationError = rows.Err()
	return out, nil
}
func ReadTemplateCompletionDeliveryDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,d.event_id,d.subscriber_id,d.status,COALESCE(d.reason_code,''),CAST(d.delivery_target_route AS TEXT) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY d.created_at,d.delivery_id`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}

func ReadServedDeliveryRouteEvidenceStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]ServedDeliveryRouteEvidenceStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id,event_id,route_identity,subscriber_type,subscriber_id,CAST(delivery_target_route AS TEXT),CAST(delivery_context AS TEXT),CAST(delivery_payload_projection AS TEXT),CAST(connect_execution_claim AS TEXT),CAST(receiver_materialization_plan AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY delivery_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServedDeliveryRouteEvidenceStorage
	for rows.Next() {
		var row ServedDeliveryRouteEvidenceStorage
		if err := rows.Scan(&row.ID, &row.EventID, &row.Identity, &row.Kind, &row.Recipient, &row.Target, &row.Context, &row.Projection, &row.Claim, &row.Initialization); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type DeliveryRecoveryIdentityStorage struct{ EventID, Route, Status string }

func ReadDeliveryRecoveryIdentityStorageTx(ctx context.Context, tx *sql.Tx, deliveryID string) (DeliveryRecoveryIdentityStorage, error) {
	var out DeliveryRecoveryIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT event_id, CAST(delivery_target_route AS TEXT), status FROM event_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&out.EventID, &out.Route, &out.Status)
	return out, err
}

type DeliveryRecoveryAttemptStorage struct {
	Version         int64
	Outcome, Reason string
}

func ReadDeliveryRecoveryAttemptsStorageTx(ctx context.Context, tx *sql.Tx, deliveryID string) ([]DeliveryRecoveryAttemptStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT claim_version, COALESCE(outcome,''), COALESCE(reason_code,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeliveryRecoveryAttemptStorage
	for rows.Next() {
		var row DeliveryRecoveryAttemptStorage
		if err := rows.Scan(&row.Version, &row.Outcome, &row.Reason); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountSourceRunDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`, runID, sourceEventID).Scan(&count)
	return count, err
}

type OrderedFanOutOutcomeStorage struct {
	Ordinal  int
	ID, Kind string
}

func ReadOrderedRunFanOutOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]OrderedFanOutOutcomeStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,CAST(event_id AS TEXT),outcome_kind FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrderedFanOutOutcomeStorage
	for rows.Next() {
		var row OrderedFanOutOutcomeStorage
		if err := rows.Scan(&row.Ordinal, &row.ID, &row.Kind); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type CommittedFanOutOrdinalStorage struct {
	ID      string
	Ordinal int
}

func ReadCommittedRunFanOutOrdinalsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]CommittedFanOutOrdinalStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT),ordinal FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CommittedFanOutOrdinalStorage
	for rows.Next() {
		var row CommittedFanOutOrdinalStorage
		if err := rows.Scan(&row.ID, &row.Ordinal); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type StandingDeliveryDiagnostic struct{ EventID, Subscriber, Status string }

func ReadStandingDeliveryDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]StandingDeliveryDiagnostic, error) {
	query := `SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`
	if postgres {
		query = `SELECT event_id::text, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StandingDeliveryDiagnostic
	for rows.Next() {
		var row StandingDeliveryDiagnostic
		if rows.Scan(&row.EventID, &row.Subscriber, &row.Status) == nil {
			out = append(out, row)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadGateEventFailureTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var failure string
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1`, eventID).Scan(&failure)
	return failure, err
}

func ReadGateSubscriberFailureTx(ctx context.Context, tx *sql.Tx, eventID, subscriberID string) (string, error) {
	var failure string
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2`, eventID, subscriberID).Scan(&failure)
	return failure, err
}

func ReadGateNodeFailureTx(ctx context.Context, tx *sql.Tx, eventID, nodeID string) (string, error) {
	var failure string
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, eventID, nodeID).Scan(&failure)
	return failure, err
}

type WorkflowTimerNodeFailureStorage struct {
	Status  string
	Failure []byte
}

type ExactJoinNodeDeliveryStorage struct{ Status, Failure string }

func ReadExactJoinNodeDeliveryStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, subscriberKey string) (ExactJoinNodeDeliveryStorage, error) {
	query := `SELECT status, CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	if postgres {
		query = `SELECT status, COALESCE(failure, '{}'::jsonb)::text FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	var out ExactJoinNodeDeliveryStorage
	if err := tx.QueryRowContext(ctx, query, eventID, subscriberKey).Scan(&out.Status, &out.Failure); err != nil {
		return ExactJoinNodeDeliveryStorage{}, err
	}
	return out, nil
}

func ReadWorkflowTimerNodeFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, nodeID string) (WorkflowTimerNodeFailureStorage, error) {
	query := `SELECT status, COALESCE(failure, '{}') FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	if postgres {
		query = `SELECT status, COALESCE(failure, '{}'::jsonb) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	var out WorkflowTimerNodeFailureStorage
	if err := tx.QueryRowContext(ctx, query, eventID, nodeID).Scan(&out.Status, &out.Failure); err != nil {
		return WorkflowTimerNodeFailureStorage{}, err
	}
	return out, nil
}

type WorkflowTimerDeliveryTraceStorage struct {
	Lines     []string
	ScanError string
}

func ReadWorkflowTimerDeliveryTraceTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) (out WorkflowTimerDeliveryTraceStorage, err error) {
	query := `SELECT e.event_name, COALESCE(d.subscriber_id, ''), COALESCE(d.status, '') FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id WHERE e.run_id = ? ORDER BY e.created_at`
	if postgres {
		query = `SELECT e.event_name, COALESCE(d.subscriber_id, ''), COALESCE(d.status, '') FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id WHERE e.run_id = $1::uuid ORDER BY e.created_at`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = WorkflowTimerDeliveryTraceStorage{}
		}
	}()
	for rows.Next() {
		var eventName, subscriberID, status string
		if err := rows.Scan(&eventName, &subscriberID, &status); err != nil {
			out.ScanError = err.Error()
			return out, nil
		}
		out.Lines = append(out.Lines, eventName+" subscriber="+subscriberID+" status="+status)
	}
	return out, rows.Err()
}

func ReadGateSubscriberFailureOrEmptyObjectTx(ctx context.Context, tx *sql.Tx, eventID, subscriberID string) (string, error) {
	var failure string
	err := tx.QueryRowContext(ctx, `SELECT CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2`, eventID, subscriberID).Scan(&failure)
	return failure, err
}

func ReadGateDeadLetterFailureTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var failure string
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM dead_letters WHERE original_event_id=$1`, eventID).Scan(&failure)
	return failure, err
}

type PreparedTargetConflictStorage struct {
	Count                                       int
	SubscriberID, RouteIdentity, TargetEncoding string
}

type PostgresSystemNodeDeliveryStorage struct {
	Status, Reason string
	SettledAt      sql.NullTime
}

type PostgresSystemNodeOutcomeStorage struct{ Outcome, Reason string }

type PostgresRunStatusSettlementStorage struct{ EventCount, ActiveDeliveries int }
type PostgresRunStatusFailureDiagnosticStorage struct{ Status, Failure string }

func ReadPostgresRunStatusSettlementStorageTx(ctx context.Context, tx *sql.Tx, runID string) (PostgresRunStatusSettlementStorage, error) {
	var out PostgresRunStatusSettlementStorage
	err := tx.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name <> 'platform.runtime_log'),
			(SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress'))
	`, runID).Scan(&out.EventCount, &out.ActiveDeliveries)
	if err != nil {
		return PostgresRunStatusSettlementStorage{}, err
	}
	return out, nil
}

func CountRunDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountExactRunEventDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&count)
	return count, err
}

func CountRunDeliveryAttemptStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`, runID).Scan(&count)
	return count, err
}

func CountRunEventLinkedDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)`, runID).Scan(&count)
	return count, err
}

func CountRunNonterminalDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')`, runID).Scan(&count)
	return count, err
}

func CountPostgresRunStatusActiveDeliveriesTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&count)
	return count, err
}

func ReadPostgresRunStatusFailureDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (out []PostgresRunStatusFailureDiagnosticStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT status, COALESCE(CAST(failure AS TEXT), '') FROM event_deliveries WHERE run_id=$1::uuid`, runID)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row PostgresRunStatusFailureDiagnosticStorage
		if err := rows.Scan(&row.Status, &row.Failure); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func ReadPostgresSystemNodeDeliveryStorageTx(ctx context.Context, tx *sql.Tx, eventID, nodeID string) (PostgresSystemNodeDeliveryStorage, error) {
	var out PostgresSystemNodeDeliveryStorage
	err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), settled_at
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&out.Status, &out.Reason, &out.SettledAt)
	if err != nil {
		return PostgresSystemNodeDeliveryStorage{}, err
	}
	return out, nil
}

func ReadPostgresSystemNodeOutcomeStorageTx(ctx context.Context, tx *sql.Tx, eventID, nodeID string) (PostgresSystemNodeOutcomeStorage, error) {
	var out PostgresSystemNodeOutcomeStorage
	err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(o.outcome, ''), COALESCE(o.reason_code, '')
		FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = $2
	`, eventID, nodeID).Scan(&out.Outcome, &out.Reason)
	if err != nil {
		return PostgresSystemNodeOutcomeStorage{}, err
	}
	return out, nil
}

type TemplateDeliveryDiagnosticStorage struct{ SubscriberType, SubscriberID, Status, Reason, Failure, Target string }

type BusAncestorDeliveryDiagnostic struct{ Name, SubscriberID, Status, Target string }

type BusRoutingDeliveryDiagnostic struct {
	Name, FlowInstance, SubscriberID, Status, Target string
	HasSubscriber                                    bool
}

func CountBusValidationDeadLetterNodeTx(ctx context.Context, tx *sql.Tx, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d
	JOIN events e ON e.event_id = d.event_id
	WHERE e.event_name = 'validate.requested' AND d.subscriber_type = 'node'
	AND d.subscriber_id = $1 AND d.status = 'dead_letter'`, node).Scan(&count)
	return count, err
}

func ReadBusRoutingDeliveryDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]BusRoutingDeliveryDiagnostic, error) {
	query := `SELECT e.event_name, COALESCE(e.flow_instance, ''), d.subscriber_id,
	COALESCE(d.status, ''), COALESCE(CAST(d.delivery_target_route AS TEXT), '')
	FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id
	ORDER BY e.created_at, e.event_id, d.subscriber_id`
	if postgres {
		query = `SELECT e.event_name, COALESCE(e.flow_instance, ''), d.subscriber_id,
		COALESCE(d.status, ''), COALESCE(d.delivery_target_route::text, '')
		FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		ORDER BY e.created_at, e.event_id, d.subscriber_id`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BusRoutingDeliveryDiagnostic
	for rows.Next() {
		var row BusRoutingDeliveryDiagnostic
		var subscriber sql.NullString
		if err := rows.Scan(&row.Name, &row.FlowInstance, &subscriber, &row.Status, &row.Target); err != nil {
			return nil, err
		}
		row.SubscriberID, row.HasSubscriber = subscriber.String, subscriber.Valid
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadBusAncestorDeliveryDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]BusAncestorDeliveryDiagnostic, error) {
	query := `SELECT e.event_name, d.subscriber_id, COALESCE(d.status, ''), COALESCE(CAST(d.delivery_target_route AS TEXT), '')
	FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
	ORDER BY e.created_at, d.subscriber_id, CAST(d.delivery_target_route AS TEXT)`
	if postgres {
		query = `SELECT e.event_name, d.subscriber_id, COALESCE(d.status, ''), COALESCE(d.delivery_target_route::text, '')
		FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
		ORDER BY e.created_at, d.subscriber_id, d.delivery_target_route::text`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BusAncestorDeliveryDiagnostic
	for rows.Next() {
		var row BusAncestorDeliveryDiagnostic
		if err := rows.Scan(&row.Name, &row.SubscriberID, &row.Status, &row.Target); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func CountConnectorRunNodeDeliveriesTx(ctx context.Context, tx *sql.Tx, run, node string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, run, node).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadConnectorLatestNodeDeliveryDiagnosticTx(ctx context.Context, tx *sql.Tx, postgres bool, run, node string) (string, error) {
	query := `SELECT COALESCE(status,'') || ':' || COALESCE(reason_code,'') || ':' || COALESCE(json_extract(failure,'$.detail.code'),'') FROM event_deliveries WHERE run_id=$1 AND subscriber_type='node' AND subscriber_id=$2 ORDER BY created_at DESC LIMIT 1`
	if postgres {
		query = `SELECT COALESCE(status,'') || ':' || COALESCE(reason_code,'') || ':' || COALESCE(failure->'detail'->>'code','') FROM event_deliveries WHERE run_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2 ORDER BY created_at DESC LIMIT 1`
	}
	var out string
	if err := tx.QueryRowContext(ctx, query, run, node).Scan(&out); err != nil {
		return "", err
	}
	return out, nil
}

func CountConnectorEntityDeadLettersTx(ctx context.Context, tx *sql.Tx, entity string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE entity_id=$1`, entity).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadConnectorLatestEntityDeadLetterDiagnosticTx(ctx context.Context, tx *sql.Tx, postgres bool, entity string) (string, error) {
	query := `SELECT COALESCE(json_extract(failure,'$.class'),'') || ':' || COALESCE(json_extract(failure,'$.detail.code'),'') FROM dead_letters WHERE entity_id=$1 ORDER BY created_at DESC LIMIT 1`
	if postgres {
		query = `SELECT COALESCE(failure->>'class','') || ':' || COALESCE(failure->'detail'->>'code','') FROM dead_letters WHERE entity_id=$1::uuid ORDER BY created_at DESC LIMIT 1`
	}
	var out string
	if err := tx.QueryRowContext(ctx, query, entity).Scan(&out); err != nil {
		return "", err
	}
	return out, nil
}

func ReadTemplateEventDeliveryDiagnosticsTx(ctx context.Context, tx *sql.Tx, event string) (out []TemplateDeliveryDiagnosticStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT subscriber_type,subscriber_id,status,COALESCE(reason_code,''),COALESCE(CAST(failure AS TEXT),''),COALESCE(CAST(delivery_target_route AS TEXT),'') FROM event_deliveries WHERE event_id=$1 ORDER BY subscriber_type,subscriber_id`, event)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row TemplateDeliveryDiagnosticStorage
		if err := rows.Scan(&row.SubscriberType, &row.SubscriberID, &row.Status, &row.Reason, &row.Failure, &row.Target); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountTemplateNodeDeliveryTx(ctx context.Context, tx *sql.Tx, event, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event, node).Scan(&count)
	return count, err
}

func CountExactRunNodeEventDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID, nodeID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries
		WHERE run_id=$1 AND event_id=$2 AND subscriber_type='node' AND subscriber_id=$3`, runID, eventID, nodeID).Scan(&count)
	return count, err
}

func CountCurrentEventDeliveryOutcomeStorageTx(ctx context.Context, tx *sql.Tx, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d
		JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version
		WHERE a.closure_kind='settled' AND d.event_id=$1 AND d.subscriber_type=$2
		AND d.subscriber_id=$3 AND a.outcome=$4`, eventID, subscriberType, subscriberID, outcome).Scan(&count)
	return count, err
}

func ReadGateNodeDeliveryStatusTx(ctx context.Context, tx *sql.Tx, event, node string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE event_id = $1 AND subscriber_type = 'node' AND subscriber_id = $2`, event, node).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

type A2EarlyRefusalStorage struct {
	Status, Failure, Outcome string
	SettledAttempts          int
}

type CanonicalAccumulatorClaimStorage struct {
	DeliveryID, Status string
	Attempts, Settled  int
}

type CanonicalAccumulatorRefusalStorage struct {
	Status                  string
	Attempts, Settled, Open int
}

type ReceiverAppearanceSettlementStorage struct {
	Status   string
	Outcomes int
}

func ReadCanonicalAccumulatorClaimStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (CanonicalAccumulatorClaimStorage, error) {
	var out CanonicalAccumulatorClaimStorage
	err := tx.QueryRowContext(ctx, `SELECT d.delivery_id, d.status,
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.closure_kind='settled' AND a.open_marker=FALSE)
 FROM event_deliveries d WHERE d.event_id=$1 AND d.subscriber_type='node'`, eventID).Scan(&out.DeliveryID, &out.Status, &out.Attempts, &out.Settled)
	return out, err
}

func ReadCanonicalAccumulatorRefusalStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (CanonicalAccumulatorRefusalStorage, error) {
	var out CanonicalAccumulatorRefusalStorage
	err := tx.QueryRowContext(ctx, `SELECT d.status,
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.closure_kind='settled' AND a.open_marker=FALSE),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.open_marker=TRUE)
 FROM event_deliveries d WHERE d.event_id=$1 AND d.subscriber_type='node'`, eventID).Scan(&out.Status, &out.Attempts, &out.Settled, &out.Open)
	return out, err
}

func ReadReceiverAppearanceSettlementStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (ReceiverAppearanceSettlementStorage, error) {
	var out ReceiverAppearanceSettlementStorage
	err := tx.QueryRowContext(ctx, `SELECT d.status, (SELECT count(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o WHERE o.delivery_id=d.delivery_id) FROM event_deliveries d WHERE d.event_id=$1`, eventID).Scan(&out.Status, &out.Outcomes)
	return out, err
}

func ReadA2EarlyRefusalTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, nodeID string) (A2EarlyRefusalStorage, error) {
	query := `SELECT d.status, CAST(COALESCE(d.failure, '{}') AS TEXT), COUNT(a.delivery_id), COALESCE(MAX(a.outcome), '')
		FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.closure_kind='settled'
		WHERE d.event_id=? AND d.subscriber_type='node' AND d.subscriber_id=? GROUP BY d.status, d.failure`
	if postgres {
		query = `SELECT d.status, COALESCE(d.failure, '{}'::jsonb)::text, COUNT(a.delivery_id), COALESCE(MAX(a.outcome), '')
			FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.closure_kind='settled'
			WHERE d.event_id=$1::uuid AND d.subscriber_type='node' AND d.subscriber_id=$2 GROUP BY d.status, d.failure`
	}
	var out A2EarlyRefusalStorage
	if err := tx.QueryRowContext(ctx, query, eventID, nodeID).Scan(&out.Status, &out.Failure, &out.SettledAttempts, &out.Outcome); err != nil {
		return A2EarlyRefusalStorage{}, err
	}
	return out, nil
}

func ReadGateNodeDeliveryIdentityTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, nodeID string) (string, error) {
	query := `SELECT delivery_id FROM event_deliveries WHERE event_id=? AND subscriber_type='node' AND subscriber_id=?`
	if postgres {
		query = `SELECT delivery_id FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2`
	}
	var id string
	if err := tx.QueryRowContext(ctx, query, eventID, nodeID).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

func CountGateNodeSettledAttemptsTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, nodeID string) (int, error) {
	query := `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON a.delivery_id=d.delivery_id
					WHERE d.event_id=? AND d.subscriber_type='node' AND d.subscriber_id=? AND a.closure_kind='settled'`
	if postgres {
		query = `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON a.delivery_id=d.delivery_id
						WHERE d.event_id=$1::uuid AND d.subscriber_type='node' AND d.subscriber_id=$2 AND a.closure_kind='settled'`
	}
	var count int
	err := tx.QueryRowContext(ctx, query, eventID, nodeID).Scan(&count)
	return count, err
}

func CountGateEventNodeDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, eventID).Scan(&count)
	return count, err
}

func ReadGateEventNodeDeliveryIdentityTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, eventID).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

func CountGateEventDeliveredRowsTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND status='delivered'`, eventID).Scan(&count)
	return count, err
}

func CountGateEventSubscriberDeliveriesTx(ctx context.Context, tx *sql.Tx, event, subscriber string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1 AND subscriber_id = $2`, event, subscriber).Scan(&count)
	return count, err
}

func ReadGateLatestDeadLetterFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, event string) (string, error) {
	query := `SELECT COALESCE(CAST(failure AS TEXT), '') FROM dead_letters WHERE original_event_id = $1 ORDER BY created_at DESC LIMIT 1`
	if postgres {
		query = `SELECT COALESCE(failure::text, '') FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at DESC LIMIT 1`
	}
	var out string
	err := tx.QueryRowContext(ctx, query, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountTemplateNodeDeliveryChronologyTx(ctx context.Context, tx *sql.Tx, event, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2 AND settled_at IS NOT NULL AND created_at<settled_at`, event, node).Scan(&count)
	return count, err
}

func CountTemplateNodeDeliveredAttemptTx(ctx context.Context, tx *sql.Tx, event, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN event_delivery_attempts o ON o.delivery_id=d.delivery_id AND o.claim_version=d.claim_version WHERE o.closure_kind='settled' AND d.event_id=$1 AND d.subscriber_type='node' AND d.subscriber_id=$2 AND d.status='delivered' AND d.settled_at IS NOT NULL AND o.outcome='delivered'`, event, node).Scan(&count)
	return count, err
}

func CountTemplateAgentDeliveryTx(ctx context.Context, tx *sql.Tx, event, agent string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='agent' AND subscriber_id=$2`, event, agent).Scan(&count)
	return count, err
}

func CountTemplatePlaceholderDeliveryTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_id='ceo-{product_id}'`, event).Scan(&count)
	return count, err
}

func CountTemplateEventDeliveriesTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1`, event).Scan(&count)
	return count, err
}

func CountTemplateCarrierDeliveriesTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_id IN ('workflow-runtime','raw-source-listener')`, event).Scan(&count)
	return count, err
}

func CountPostgresTemplateNodeTargetTx(ctx context.Context, tx *sql.Tx, event, target, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$3 AND delivery_target_route @> $2::jsonb`, event, target, node).Scan(&count)
	return count, err
}

func CountPostgresTemplateDeliveredNodeTargetTx(ctx context.Context, tx *sql.Tx, event, node, target string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2 AND status='delivered' AND delivery_target_route @> $3::jsonb`, event, node, target).Scan(&count)
	return count, err
}

func CountPostgresArtifactCallbackSettledTargetTx(ctx context.Context, tx *sql.Tx, event, target, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $3 AND status = 'delivered' AND settled_at IS NOT NULL AND delivery_target_route @> $2::jsonb`, event, target, node).Scan(&count)
	return count, err
}

func ReadPostgresArtifactCallbackTargetDiagnosticsTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object(
		'subscriber_type', subscriber_type, 'subscriber_id', subscriber_id,
		'status', status, 'reason_code', reason_code, 'delivery_target_route', delivery_target_route
	))::text, '[]') FROM event_deliveries WHERE event_id = $1::uuid`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadPostgresArtifactCallbackDeliveriesTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(d))::text, '[]') FROM event_deliveries d WHERE event_id = $1::uuid`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

// Receipt storage remains with its owner. The selected coordinator supplies its
// detached aggregate within the same read transaction as the delivery snapshot.
func ReadPostgresArtifactCallbackSettlementDiagnosticsTx(ctx context.Context, tx *sql.Tx, event, receipts string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT jsonb_build_object(
		'deliveries', COALESCE(jsonb_agg(jsonb_build_object(
			'status', d.status, 'target', d.delivery_target_route, 'claim_version', d.claim_version,
			'outcomes', (SELECT COALESCE(jsonb_agg(to_jsonb(o)), '[]'::jsonb) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o WHERE o.delivery_id = d.delivery_id)
		)), '[]'::jsonb), 'receipts', $2::jsonb
	)::text FROM event_deliveries d WHERE d.event_id = $1::uuid`, event, receipts).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

type FanInDiagnosticDeliveryStorage struct {
	EventID, SubscriberID, Status, Reason, Failure string
}

type FanOutSemanticOutcomeStorage struct {
	Ordinal          int
	Kind, EventID    string
	EventIDPresent   bool
	Payload, Failure []byte
}

type FanOutBlockedSourceStorage struct {
	Outcomes, Leased int
	Failure          string
}

type FanOutSourceProgressStorage struct {
	Cursor  int
	Status  string
	Present bool
}

type FanOutTriggerDeliveryStorage struct {
	Status  string
	Failure []byte
}

func CountNestedContinuationHandoffsTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND continuation_handoff_at IS NOT NULL`, event).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadRepeatedFanOutSnapshotTx(ctx context.Context, tx *sql.Tx, run string) ([8]int64, error) {
	var out [8]int64
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1),
		(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COALESCE(SUM(revision),0) FROM entity_state WHERE run_id=$1),
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1))`, run).Scan(&out[0], &out[1], &out[2], &out[3], &out[4], &out[5], &out[6], &out[7])
	if err != nil {
		return [8]int64{}, err
	}
	return out, nil
}

func ReadFanOutSourceProgressStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (FanOutSourceProgressStorage, error) {
	var out FanOutSourceProgressStorage
	err := tx.QueryRowContext(ctx, `SELECT i.cursor,i.status FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id WHERE i.run_id=$1 AND d.event_id=$2`, runID, eventID).Scan(&out.Cursor, &out.Status)
	if err == sql.ErrNoRows {
		return FanOutSourceProgressStorage{}, nil
	}
	if err != nil {
		return FanOutSourceProgressStorage{}, err
	}
	out.Present = true
	return out, nil
}

func ReadFanOutSourceOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (out []FanOutSemanticOutcomeStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT o.ordinal,o.outcome_kind,e.event_id,o.failure FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id LEFT JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 AND d.event_id=$2 ORDER BY o.ordinal`, runID, eventID)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row FanOutSemanticOutcomeStorage
		var id sql.NullString
		if err := rows.Scan(&row.Ordinal, &row.Kind, &id, &row.Failure); err != nil {
			return nil, err
		}
		row.EventID, row.EventIDPresent = id.String, id.Valid
		row.Failure = append([]byte(nil), row.Failure...)
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountFanOutSourceOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id WHERE o.run_id=$1 AND d.event_id=$2`, runID, eventID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadFanOutBlockedSourceStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (FanOutBlockedSourceStorage, error) {
	var out FanOutBlockedSourceStorage
	err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id),CASE WHEN i.claim_owner IS NULL THEN 0 ELSE 1 END,i.blocked_reason FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id WHERE i.run_id=$1 AND d.event_id=$2`, runID, eventID).Scan(&out.Outcomes, &out.Leased, &out.Failure)
	if err != nil {
		return FanOutBlockedSourceStorage{}, err
	}
	return out, nil
}

func ReadFanOutTriggerDeliveryDiagnosticStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (FanOutTriggerDeliveryStorage, error) {
	var out FanOutTriggerDeliveryStorage
	err := tx.QueryRowContext(ctx, `SELECT status,failure FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&out.Status, &out.Failure)
	if err != nil {
		return FanOutTriggerDeliveryStorage{}, err
	}
	out.Failure = append([]byte(nil), out.Failure...)
	return out, nil
}

func CountFanOutDeliveredTriggerStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2 AND status='delivered'`, runID, eventID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadFanInDiagnosticDeliveriesTx(ctx context.Context, tx *sql.Tx) (out []FanInDiagnosticDeliveryStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT), subscriber_id, status, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), '') FROM event_deliveries ORDER BY created_at, delivery_id`)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row FanInDiagnosticDeliveryStorage
		if err := rows.Scan(&row.EventID, &row.SubscriberID, &row.Status, &row.Reason, &row.Failure); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func ReadPublicationDeliveryTargetEncodingsTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT COALESCE(delivery_target_route,'{}') FROM event_deliveries WHERE event_id=$1 ORDER BY subscriber_type,subscriber_id,delivery_id`
	if postgres {
		query = `SELECT COALESCE(delivery_target_route::text,'{}') FROM event_deliveries WHERE event_id=$1::uuid ORDER BY subscriber_type,subscriber_id,delivery_id`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Physical cardinality witnesses deliberately include every delivery status.
// They grant no eligibility or claim authority and do not decode route data.
func CountSelectedForkDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountSelectedForkDescendantDeliveryStorageTx(ctx context.Context, tx *sql.Tx, sourceRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id IN (SELECT run_id FROM runs WHERE forked_from_run_id=$1)`, sourceRunID).Scan(&count)
	return count, err
}

func ReadPreparedTargetConflictTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	if tx == nil {
		return PreparedTargetConflictStorage{}, fmt.Errorf("prepared target conflict requires its selected read transaction")
	}
	query := `SELECT COUNT(*), MIN(subscriber_id), MIN(route_identity), MIN(delivery_target_route) FROM event_deliveries WHERE event_id=? AND route_identity=?`
	if postgres {
		query = `SELECT COUNT(*), MIN(subscriber_id), MIN(route_identity), MIN(delivery_target_route::text) FROM event_deliveries WHERE event_id=$1::uuid AND route_identity=$2`
	}
	var out PreparedTargetConflictStorage
	if err := tx.QueryRowContext(ctx, query, eventID, routeIdentity).Scan(&out.Count, &out.SubscriberID, &out.RouteIdentity, &out.TargetEncoding); err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	return out, nil
}

type ReceiverDeliveryStorageRow struct {
	DeliveryID, EventID, Status, Target string
}

type StandaloneAgentDeliveryStorage struct{ Status, RunStatus string }

func ReadExactAgentDeliveryStatusTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, agentID string) (string, error) {
	query := `SELECT status FROM event_deliveries WHERE event_id=? AND subscriber_type='agent' AND subscriber_id=?`
	if postgres {
		query = `SELECT status FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='agent' AND subscriber_id=$2`
	}
	var status string
	if err := tx.QueryRowContext(ctx, query, eventID, agentID).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func ReadStopPendingDeliveryCountTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	// The historical name denotes unfinished work, including claimed and retrying rows.
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress','failed')`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadStandaloneAgentDeliveryStorageTx(ctx context.Context, tx *sql.Tx, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	if tx == nil {
		return StandaloneAgentDeliveryStorage{}, fmt.Errorf("standalone delivery evidence requires its selected read transaction")
	}
	var out StandaloneAgentDeliveryStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(d.status,''), COALESCE(r.status,'')
FROM event_deliveries d INNER JOIN runs r ON r.run_id=d.run_id
WHERE d.event_id=$1 AND d.subscriber_type='agent' AND d.subscriber_id=$2`, eventID, agentID).Scan(&out.Status, &out.RunStatus)
	if err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	return out, nil
}

type CausalDeliveryStatusCount struct {
	Class, Subscriber, Status, Reason string
	Count                             int
}

func ReadSubscriberEventNamesCreatedSince(ctx context.Context, q queryer, since time.Time, subscriberID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT e.event_name FROM event_deliveries d
JOIN events e ON e.event_id=d.event_id
WHERE d.created_at >= $1 AND d.subscriber_id=$2
ORDER BY d.created_at ASC,e.event_id ASC`, since, subscriberID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

type CausalDeliveryFrontier struct {
	Observed, Unsettled, DeadLetters int
}

const causalDeliveryFrontierQuery = `WITH RECURSIVE run_events AS MATERIALIZED (
	SELECT event_id,source_event_id FROM events WHERE run_id=$1 AND event_name<>'platform.runtime_log'
), descendants(event_id) AS (
	SELECT event_id FROM run_events WHERE event_id=$2
	UNION
	SELECT e.event_id FROM run_events e JOIN descendants p ON e.source_event_id=p.event_id
), delivery_counts AS (
	SELECT p.event_id, COUNT(d.delivery_id) AS total,
		COALESCE(SUM(CASE WHEN d.status<>'delivered' THEN 1 ELSE 0 END),0) AS unsettled
	FROM descendants p LEFT JOIN event_deliveries d ON d.event_id=p.event_id
	GROUP BY p.event_id
)
SELECT (SELECT COUNT(*) FROM descendants),
	(SELECT COUNT(*) FROM delivery_counts WHERE total<>1 OR unsettled<>0),
	(SELECT COUNT(*) FROM dead_letters d JOIN descendants p ON d.original_event_id=p.event_id)`

func ReadCausalDeliveryFrontier(ctx context.Context, q queryer, runID, eventID string) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	if err := q.QueryRowContext(ctx, causalDeliveryFrontierQuery, runID, eventID).Scan(&out.Observed, &out.Unsettled, &out.DeadLetters); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

type CausalDeliveryFrontierProbe uint8

const (
	FrontierComplete CausalDeliveryFrontierProbe = iota + 1
	FrontierMissing
	FrontierPending
	FrontierDuplicateDelivery
	FrontierDeadLetterDelivery
)

func ProbeCausalDeliveryFrontier(ctx context.Context, q queryer, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	var deliveries string
	switch probe {
	case FrontierComplete:
		deliveries = "('r','root','delivered'),('c','child','delivered'),('g','grandchild','delivered')"
	case FrontierMissing:
		deliveries = "('r','root','delivered')"
	case FrontierPending:
		deliveries = "('r','root','delivered'),('c','child','pending'),('g','grandchild','delivered')"
	case FrontierDuplicateDelivery:
		deliveries = "('r','root','delivered'),('c','child','delivered'),('c2','child','delivered'),('g','grandchild','delivered')"
	case FrontierDeadLetterDelivery:
		deliveries = "('r','root','delivered'),('c','child','dead_letter'),('g','grandchild','delivered')"
	default:
		return CausalDeliveryFrontier{}, fmt.Errorf("unsupported causal frontier probe %d", probe)
	}
	const prefix = `WITH RECURSIVE events(event_id,run_id,event_name,source_event_id) AS (VALUES
('root','run','root',''),('child','run','child','root'),('grandchild','run','grandchild','child'),
('log','run','platform.runtime_log','root'),('log-child','run','hidden','log'),
('foreign','other','child','root')),
dead_letters(original_event_id) AS (VALUES ('child'),('child'),('foreign'),('log')),
event_deliveries(delivery_id,event_id,status) AS (VALUES `
	query := prefix + deliveries + "), " + strings.TrimPrefix(causalDeliveryFrontierQuery, "WITH RECURSIVE ")
	var out CausalDeliveryFrontier
	if err := q.QueryRowContext(ctx, query, "run", "root").Scan(&out.Observed, &out.Unsettled, &out.DeadLetters); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func ReadCausalDeliveryStatusCounts(ctx context.Context, q queryer, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	rows, err := q.QueryContext(ctx, `WITH RECURSIVE descendants(event_id) AS (
SELECT event_id FROM events WHERE run_id=$1 AND event_id=$2
UNION
SELECT child.event_id FROM events child JOIN descendants parent ON child.source_event_id=parent.event_id
WHERE child.run_id=$1)
SELECT d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,''),COUNT(*)
FROM event_deliveries d JOIN descendants event ON event.event_id=d.event_id
WHERE d.run_id=$1
GROUP BY d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,'')
ORDER BY d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,'')`, runID, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CausalDeliveryStatusCount
	for rows.Next() {
		var row CausalDeliveryStatusCount
		if err := rows.Scan(&row.Class, &row.Subscriber, &row.Status, &row.Reason, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadNonLogRunDeliveryClaimTotals(ctx context.Context, q queryer, runID string) (deliveries, claims int64, err error) {
	err = q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(d.claim_version),0)
FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
WHERE e.run_id=$1 AND e.event_name<>'platform.runtime_log'`, runID).Scan(&deliveries, &claims)
	if err != nil {
		return 0, 0, err
	}
	return deliveries, claims, nil
}

type DeliveryAttemptDiagnosticRow struct {
	Version                           int
	Closure, Outcome, Reason, Failure string
}

type EventDeliveryDiagnosticRow struct {
	SubscriberType, SubscriberID, Status, Reason, FlowInstance, EntityID string
}

func ReadNodeDeliveryTargetEncodingTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, nodeID string) (string, error) {
	query := `SELECT delivery_target_route FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	if postgres {
		query = `SELECT delivery_target_route::text FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	var raw string
	if err := tx.QueryRowContext(ctx, query, eventID, nodeID).Scan(&raw); err != nil {
		return "", err
	}
	return raw, nil
}

func ReadNodeDeadLetterDiagnosticLinesTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT COALESCE(handler_node, ''), COALESCE(json_extract(failure, '$.class'), ''), COALESCE(failure, '')
FROM dead_letters WHERE original_event_id = ? ORDER BY created_at ASC`
	if postgres {
		query = `SELECT COALESCE(handler_node, ''), COALESCE(failure->>'class', ''), COALESCE(failure::text, '')
FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at ASC`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var node, failure, message string
		if err := rows.Scan(&node, &failure, &message); err != nil {
			return nil, err
		}
		out = append(out, node+" failure="+failure+" detail="+message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadNodeDeliveryDiagnosticLinesTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route, '')
FROM event_deliveries WHERE event_id = ? ORDER BY subscriber_id, delivery_target_route`
	if postgres {
		query = `SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route::text, '')
FROM event_deliveries WHERE event_id = $1::uuid ORDER BY subscriber_id, delivery_target_route::text`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var subscriber, status, reason, target string
		if err := rows.Scan(&subscriber, &status, &reason, &target); err != nil {
			return nil, err
		}
		out = append(out, subscriber+" status="+status+" reason="+reason+" target="+target)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadEventDeliveryDiagnosticRows(ctx context.Context, q queryer, eventID string, postgres bool) ([]EventDeliveryDiagnosticRow, error) {
	query := `
		SELECT COALESCE(subscriber_type, ''),
		       COALESCE(subscriber_id, ''),
		       COALESCE(status, ''),
		       COALESCE(reason_code, ''),
		       COALESCE(delivery_target_route->'route'->>'flow_instance', ''),
		       COALESCE(delivery_target_route->'route'->>'entity_id', '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		ORDER BY created_at ASC, delivery_id ASC`
	if !postgres {
		query = `
			SELECT COALESCE(subscriber_type, ''),
			       COALESCE(subscriber_id, ''),
			       COALESCE(status, ''),
			       COALESCE(reason_code, ''),
			       COALESCE(json_extract(delivery_target_route, '$.route.flow_instance'), ''),
			       COALESCE(json_extract(delivery_target_route, '$.route.entity_id'), '')
			FROM event_deliveries
			WHERE event_id = ?
			ORDER BY created_at ASC, delivery_id ASC`
	}
	rows, err := q.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventDeliveryDiagnosticRow
	for rows.Next() {
		var row EventDeliveryDiagnosticRow
		if err := rows.Scan(&row.SubscriberType, &row.SubscriberID, &row.Status, &row.Reason, &row.FlowInstance, &row.EntityID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeliveryAttemptDiagnosticRows(ctx context.Context, q queryer, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT claim_version,closure_kind,COALESCE(outcome,''),COALESCE(reason_code,''),COALESCE(CAST(failure AS TEXT),'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeliveryAttemptDiagnosticRow
	for rows.Next() {
		var row DeliveryAttemptDiagnosticRow
		if err := rows.Scan(&row.Version, &row.Closure, &row.Outcome, &row.Reason, &row.Failure); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

type PipelineHandoffDeliveryFacts struct {
	EventID        string
	Active         bool
	MissingHandoff bool
}

// Retain one detached fact per delivery, including rows sharing an event. The
// pipeline owner supplies its receipt facts in the same selected read snapshot.
func ReadPipelineHandoffDeliveryFacts(ctx context.Context, q queryer, runID string) ([]PipelineHandoffDeliveryFacts, error) {
	rows, err := q.QueryContext(ctx, `SELECT event_id, status IN ('pending','in_progress'), continuation_handoff_at IS NULL
		FROM event_deliveries WHERE run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PipelineHandoffDeliveryFacts
	for rows.Next() {
		var row PipelineHandoffDeliveryFacts
		if err := rows.Scan(&row.EventID, &row.Active, &row.MissingHandoff); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

// This physical witness is deliberately independent of eligibility and public
// target/status projection. It retains every historical delivery in the run.
func ReadReceiverDeliveryStorage(ctx context.Context, q queryer, runID string) ([]ReceiverDeliveryStorageRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT delivery_id,event_id,status,CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiverDeliveryStorageRow
	for rows.Next() {
		var row ReceiverDeliveryStorageRow
		if err := rows.Scan(&row.DeliveryID, &row.EventID, &row.Status, &row.Target); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadLatestNamedAgentDeliveryStatus(ctx context.Context, q queryer, runID, agentID, instance string) (string, error) {
	var status string
	err := q.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id=$2 AND agent_flow_instance_path=$3 ORDER BY created_at DESC,delivery_id DESC LIMIT 1`, runID, agentID, instance).Scan(&status)
	return status, err
}

func ReadVersionOneDeliveredSettlementCount(ctx context.Context, q queryer, deliveryID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts
		WHERE closure_kind='settled' AND delivery_id=$1 AND claim_version=1 AND outcome='delivered'`, deliveryID).Scan(&count)
	return count, err
}

// Preserve the historical conjunction, exact lookup values and optional
// subscriber-type semantics. This is evidence, never a work selector.
func ReadServedDeliveryStatusCount(ctx context.Context, q queryer, postgres bool, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	where, args := []string{}, []any{}
	add := func(column, value string) {
		predicate := column + " = ?"
		if postgres {
			predicate = fmt.Sprintf("%s = $%d", column, len(args)+1)
			if column == "event_id" {
				predicate += "::uuid"
			}
		}
		where, args = append(where, predicate), append(args, value)
	}
	add("event_id", eventID)
	if strings.TrimSpace(subscriberType) != "" {
		add("subscriber_type", subscriberType)
	}
	add("subscriber_id", subscriberID)
	for _, status := range statuses {
		if strings.TrimSpace(status) != "" {
			add("status", status)
		}
	}
	var count int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_deliveries WHERE "+strings.Join(where, " AND "), args...).Scan(&count)
	return count, err
}

type ManagedDeliveryFailureStorage struct {
	DeliveryID, EventID string
	Failure             json.RawMessage
}

type ManagedDeliveryStorage struct {
	AgentDeliveries, Delivered int
	Failures                   []ManagedDeliveryFailureStorage
}

func ReadManagedDeliveryStorage(ctx context.Context, q queryer, runID, agentID string) (ManagedDeliveryStorage, error) {
	var out ManagedDeliveryStorage
	where, args := `d.run_id=$1 AND d.subscriber_type='agent'`, []any{runID}
	if agentID != "" {
		where += ` AND d.subscriber_id=$2`
		args = append(args, agentID)
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN d.status='delivered' THEN 1 ELSE 0 END),0) FROM event_deliveries d WHERE `+where, args...).Scan(&out.AgentDeliveries, &out.Delivered); err != nil {
		return ManagedDeliveryStorage{}, err
	}
	rows, err := q.QueryContext(ctx, `SELECT d.delivery_id,d.event_id,a.failure FROM event_deliveries d JOIN event_delivery_attempts a
		ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version AND a.closure_kind='settled'
		WHERE `+where+` AND d.status='dead_letter' ORDER BY d.created_at,d.delivery_id`, args...)
	if err != nil {
		return ManagedDeliveryStorage{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var row ManagedDeliveryFailureStorage
		var failure []byte
		if err := rows.Scan(&row.DeliveryID, &row.EventID, &failure); err != nil {
			return ManagedDeliveryStorage{}, err
		}
		row.Failure = failure
		out.Failures = append(out.Failures, row)
	}
	if err := rows.Err(); err != nil {
		return ManagedDeliveryStorage{}, err
	}
	if err := rows.Close(); err != nil {
		return ManagedDeliveryStorage{}, err
	}
	return out, nil
}

func ReadWorkspaceInvocationStorageCounts(ctx context.Context, q queryer) (agent, delivered, emitted int, err error) {
	err = q.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type='agent'),
		(SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type='agent' AND status='delivered'),
		(SELECT COUNT(*) FROM events WHERE event_name='work.completed')`).Scan(&agent, &delivered, &emitted)
	return
}

type WorkspaceDeliveryPhase struct {
	DeliveryID, EventID, RunID, SubscriberType, SubscriberID, Status                           string
	ClaimVersion                                                                               int64
	CreatedAt, UpdatedAt                                                                       string
	SettledAt, HandoffAt                                                                       *string
	AttemptStartedAt, AttemptExpiresAt, AttemptClosureKind, AttemptOutcome, AttemptCompletedAt *string
	AttemptOpenMarker                                                                          *bool
}

// Closed diagnostic of the private invocation, never an eligibility selector.
// Payload, route/capability bodies and claim tokens cannot enter the result.
func ReadWorkspaceInvocationDeliveryPhases(ctx context.Context, q queryer) ([]WorkspaceDeliveryPhase, error) {
	rows, err := q.QueryContext(ctx, `SELECT CAST(d.delivery_id AS TEXT),CAST(d.event_id AS TEXT),CAST(d.run_id AS TEXT),
		d.subscriber_type,d.subscriber_id,d.status,d.claim_version,CAST(d.created_at AS TEXT),CAST(d.updated_at AS TEXT),
		CAST(d.settled_at AS TEXT),CAST(d.continuation_handoff_at AS TEXT),CAST(a.started_at AS TEXT),
		CAST(a.lease_expires_at AS TEXT),a.closure_kind,a.outcome,CAST(a.completed_at AS TEXT),a.open_marker
		FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version
		ORDER BY d.created_at,d.delivery_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WorkspaceDeliveryPhase{}
	for rows.Next() {
		var row WorkspaceDeliveryPhase
		if err := rows.Scan(&row.DeliveryID, &row.EventID, &row.RunID, &row.SubscriberType, &row.SubscriberID, &row.Status, &row.ClaimVersion,
			&row.CreatedAt, &row.UpdatedAt, &row.SettledAt, &row.HandoffAt, &row.AttemptStartedAt, &row.AttemptExpiresAt,
			&row.AttemptClosureKind, &row.AttemptOutcome, &row.AttemptCompletedAt, &row.AttemptOpenMarker); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSemanticEventSettledAttemptCount(ctx context.Context, q queryer, eventID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a
		JOIN event_deliveries d ON d.delivery_id=a.delivery_id
		WHERE d.event_id=$1 AND a.closure_kind='settled'`, eventID).Scan(&count)
	return count, err
}

func ReadAgentSettledAttemptCountTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, agentID string) (int, error) {
	query := `SELECT COUNT(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = ? AND d.subscriber_type = 'agent' AND d.subscriber_id = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = $1::uuid AND d.subscriber_type = 'agent' AND d.subscriber_id = $2`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, eventID, agentID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadSemanticEventDeliveryStorage(ctx context.Context, q queryer, eventID string) (map[string][18]string, map[string]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT delivery_id, status, route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			CAST(delivery_target_route AS TEXT), CAST(delivery_context AS TEXT),
			CAST(delivery_payload_projection AS TEXT), CAST(connect_execution_claim AS TEXT),
			CAST(receiver_materialization_plan AS TEXT), execution_authority_kind,
			authority_bundle_hash, execution_authority_id, CAST(execution_authority_generation AS TEXT)
		FROM event_deliveries WHERE event_id=$1`, eventID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	projections, statuses := map[string][18]string{}, map[string]string{}
	for rows.Next() {
		var deliveryID, status string
		var projection [18]string
		args := []any{&deliveryID, &status}
		for index := range projection {
			args = append(args, &projection[index])
		}
		if err := rows.Scan(args...); err != nil {
			return nil, nil, err
		}
		projections[deliveryID], statuses[deliveryID] = projection, status
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	return projections, statuses, nil
}

type SourceDeliveryStorageTable struct {
	Columns []string
	Rows    [][]any
}

// The table set and predicates are fixed. No table/query selector or live SQL
// rows cross this boundary; the shared snapshot owner still normalizes values.
func ReadSourceDeliveryStorageTables(ctx context.Context, q queryer, runID string) (map[string]SourceDeliveryStorageTable, error) {
	queries := map[string]string{
		"event_deliveries":                       `SELECT * FROM event_deliveries WHERE run_id=$1`,
		"event_delivery_attempts":                `SELECT * FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
		"event_delivery_handler_rule_selections": `SELECT * FROM event_delivery_handler_rule_selections WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
	}
	out := make(map[string]SourceDeliveryStorageTable, len(queries))
	for table, query := range queries {
		evidence, err := readSourceDeliveryStorageTable(ctx, q, query, runID)
		if err != nil {
			return nil, err
		}
		out[table] = evidence
	}
	return out, nil
}

func ReadMailboxCompletionDeliveryEffectsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	const query = `SELECT d.* FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 ORDER BY d.delivery_id`
	table, err := readSourceDeliveryStorageTable(ctx, tx, query, runID)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, values := range table.Rows {
		raw, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		out = append(out, query+":"+string(raw))
	}
	return out, nil
}

func CountRunDeadLetterDeliveriesTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID).Scan(&count)
	return count, err
}

func readSourceDeliveryStorageTable(ctx context.Context, q queryer, query, runID string) (SourceDeliveryStorageTable, error) {
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return SourceDeliveryStorageTable{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return SourceDeliveryStorageTable{}, err
	}
	out := SourceDeliveryStorageTable{Columns: append([]string(nil), columns...), Rows: [][]any{}}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return SourceDeliveryStorageTable{}, err
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = slices.Clone(raw)
			}
		}
		out.Rows = append(out.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return SourceDeliveryStorageTable{}, err
	}
	if err := rows.Close(); err != nil {
		return SourceDeliveryStorageTable{}, err
	}
	return out, nil
}

func ReadRunDeliveryDebugSection(ctx context.Context, q queryer, postgres bool, scope, runID string) ([]string, string, error) {
	query := runDeliveryDebugQuery(postgres, scope)
	if query == "" {
		return nil, "", fmt.Errorf("unsupported delivery diagnostic section")
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, " columns", err
	}
	out := []string{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return nil, " scan", err
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return nil, " rows", err
	}
	if err := rows.Close(); err != nil {
		return nil, "", err
	}
	return out, "", nil
}

func runDeliveryDebugQuery(postgres bool, scope string) string {
	if postgres {
		switch scope {
		case "event_deliveries":
			return `SELECT delivery_id::text, event_id::text, subscriber_type, subscriber_id, status, claim_version, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id=$1::uuid ORDER BY created_at,event_id LIMIT 8`
		case "settled_delivery_attempts":
			return `SELECT o.delivery_id::text, o.claim_version, o.outcome, COALESCE(o.reason_code, '') FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id=o.delivery_id WHERE d.run_id=$1::uuid ORDER BY o.settled_at,o.delivery_id LIMIT 8`
		case "delivery_agents":
			return `SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source, d.agent_route_presence, d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path, COALESCE(a.status, ''), COALESCE(a.lifecycle_phase, '') FROM event_deliveries d LEFT JOIN agents a ON a.agent_id=d.subscriber_id AND a.agent_name_owner=d.agent_name_owner AND a.agent_name_source=d.agent_name_source AND a.agent_route_presence=d.agent_route_presence AND a.flow_scope_key=d.agent_flow_scope_key AND a.flow_instance_id=d.agent_flow_instance_id AND a.flow_instance=d.agent_flow_instance_path WHERE d.run_id=$1::uuid ORDER BY d.created_at LIMIT 8`
		}
	} else {
		switch scope {
		case "event_deliveries":
			return `SELECT delivery_id, event_id, subscriber_type, subscriber_id, status, claim_version, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id=? ORDER BY created_at,event_id LIMIT 8`
		case "settled_delivery_attempts":
			return `SELECT o.delivery_id, o.claim_version, o.outcome, COALESCE(o.reason_code, '') FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id=o.delivery_id WHERE d.run_id=? ORDER BY o.settled_at,o.delivery_id LIMIT 8`
		case "delivery_agents":
			return `SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source, d.agent_route_presence, d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path, COALESCE(a.status, ''), COALESCE(a.lifecycle_phase, '') FROM event_deliveries d LEFT JOIN agents a ON a.agent_id=d.subscriber_id AND a.agent_name_owner=d.agent_name_owner AND a.agent_name_source=d.agent_name_source AND a.agent_route_presence=d.agent_route_presence AND a.flow_scope_key=d.agent_flow_scope_key AND a.flow_instance_id=d.agent_flow_instance_id AND a.flow_instance=d.agent_flow_instance_path WHERE d.run_id=? ORDER BY d.created_at LIMIT 8`
		}
	}
	return ""
}

const (
	postgresAgentPendingEligibility = `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= ` + postgresDatabaseNowExpression + `)
		OR d.status = 'in_progress'
	)`
	sqliteAgentPendingEligibility = `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND julianday(substr(d.next_eligible_at, 1, 23)) <= julianday(` + sqliteDatabaseNowExpression + `))
		OR d.status = 'in_progress'
	)`
)

func postgresAgentPendingEligibilityAt(asOf string) string {
	return `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= ` + asOf + `)
		OR d.status = 'in_progress'
	)`
}

func sqliteAgentPendingEligibilityAt(asOf string) string {
	return `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND julianday(substr(d.next_eligible_at, 1, 23)) <= julianday(` + asOf + `))
		OR d.status = 'in_progress'
	)`
}

// PendingRunEventIDs applies the active-run, pending-delivery, replay
// exclusion, ordering, de-duplication, and limit shape before event hydration.
func (a *Adapter) PendingRunEventIDs(ctx context.Context, q queryer, page PendingRunEventQuery) ([]string, error) {
	page.RunID = strings.TrimSpace(page.RunID)
	if _, err := uuid.Parse(page.RunID); err != nil {
		return nil, fmt.Errorf("pending run event ids run id: %w", err)
	}
	if page.Limit <= 0 {
		return nil, fmt.Errorf("pending run event ids limit must be positive")
	}
	if page.Since.IsZero() {
		page.Since = time.Unix(0, 0).UTC()
	}
	excluded := normalizedNonEmptyStrings(page.ExcludedEventNames)
	var (
		query string
		args  []any
	)
	activeStates := runtimerunlifecycle.ActiveStates()
	if a.dialect == DialectPostgres {
		args = []any{page.RunID, string(activeStates[0]), string(activeStates[1]), page.Since.UTC()}
		where := []string{
			"d.run_id = $1::uuid",
			"run.status IN ($2, $3)",
			"d.status = 'pending'",
			"d.created_at >= $4::timestamptz",
		}
		if len(excluded) > 0 {
			placeholders := make([]string, 0, len(excluded))
			for _, eventName := range excluded {
				args = append(args, eventName)
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
			}
			where = append(where, "e.event_name NOT IN ("+strings.Join(placeholders, ",")+")")
		}
		args = append(args, page.Limit)
		query = fmt.Sprintf(`
			SELECT e.event_id::text
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			JOIN runs run ON run.run_id = d.run_id
			WHERE %s
			GROUP BY e.event_id
			ORDER BY MIN(d.created_at), e.event_id
			LIMIT $%d`, strings.Join(where, " AND "), len(args))
	} else {
		args = []any{page.RunID, string(activeStates[0]), string(activeStates[1]), page.Since.UTC()}
		where := []string{
			"d.run_id = ?",
			"run.status IN (?, ?)",
			"d.status = 'pending'",
			"d.created_at >= ?",
		}
		if len(excluded) > 0 {
			placeholders := make([]string, 0, len(excluded))
			for _, eventName := range excluded {
				args = append(args, eventName)
				placeholders = append(placeholders, "?")
			}
			where = append(where, "e.event_name NOT IN ("+strings.Join(placeholders, ",")+")")
		}
		args = append(args, page.Limit)
		query = fmt.Sprintf(`
			SELECT e.event_id
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			JOIN runs run ON run.run_id = d.run_id
			WHERE %s
			GROUP BY e.event_id
			ORDER BY MIN(d.created_at), e.event_id
			LIMIT ?`, strings.Join(where, " AND "))
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select pending run event ids: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, page.Limit)
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("scan pending run event id: %w", err)
		}
		eventID = strings.TrimSpace(eventID)
		if _, err := uuid.Parse(eventID); err != nil {
			return nil, fmt.Errorf("%w: pending run event id is invalid", ErrConflict)
		}
		out = append(out, eventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending run event ids: %w", err)
	}
	return out, nil
}

// AgentPendingAggregates computes pending-obligation count and oldest event
// time for all requested agents without hydrating lifecycle or event records.
func (a *Adapter) AgentPendingAggregates(ctx context.Context, q queryer, identities []agentidentity.Identity, since, asOf time.Time) ([]AgentPendingAggregate, error) {
	identities, err := normalizeAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return []AgentPendingAggregate{}, nil
	}
	if since.IsZero() {
		since = time.Unix(0, 0).UTC()
	}
	if asOf.IsZero() {
		return nil, fmt.Errorf("agent pending aggregates as_of is required")
	}
	predicate, args, err := agentIdentityPredicate(a.dialect, "d", identities, 1)
	if err != nil {
		return nil, err
	}
	var query string
	activeStates := runtimerunlifecycle.ActiveStates()
	if a.dialect == DialectPostgres {
		args = append(args, since.UTC())
		sinceIndex := len(args)
		args = append(args, string(activeStates[0]), string(activeStates[1]))
		args = append(args, asOf.UTC())
		asOfIndex := len(args)
		query = fmt.Sprintf(`
			SELECT d.run_id::text, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			       d.agent_route_presence, d.agent_flow_scope_key,
			       d.agent_flow_instance_id, d.agent_flow_instance_path,
			       COUNT(*), MIN(e.created_at)
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE d.subscriber_type = 'agent'
			  AND (%s)
			  AND e.created_at >= $%d::timestamptz
			  AND (e.run_id IS NULL OR r.status IN ($%d, $%d))
			  AND %s
			GROUP BY d.run_id, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			         d.agent_route_presence, d.agent_flow_scope_key,
			         d.agent_flow_instance_id, d.agent_flow_instance_path
			ORDER BY d.subscriber_id, d.agent_flow_instance_path`,
			predicate, sinceIndex, sinceIndex+1, sinceIndex+2, postgresAgentPendingEligibilityAt(fmt.Sprintf("$%d::timestamptz", asOfIndex)))
	} else {
		args = append(args, since.UTC())
		args = append(args, string(activeStates[0]), string(activeStates[1]))
		args = append(args, sqliteTraceSQLTime(asOf.UTC()))
		query = fmt.Sprintf(`
			SELECT d.run_id, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			       d.agent_route_presence, d.agent_flow_scope_key,
			       d.agent_flow_instance_id, d.agent_flow_instance_path,
			       COUNT(*), MIN(e.created_at)
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE d.subscriber_type = 'agent'
			  AND (%s)
			  AND e.created_at >= ?
			  AND (e.run_id IS NULL OR r.status IN (?, ?))
			  AND %s
			GROUP BY d.run_id, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			         d.agent_route_presence, d.agent_flow_scope_key,
			         d.agent_flow_instance_id, d.agent_flow_instance_path
			ORDER BY d.subscriber_id, d.agent_flow_instance_path`,
			predicate, sqliteAgentPendingEligibilityAt("?"))
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select agent pending aggregates: %w", err)
	}
	defer rows.Close()
	out := make([]AgentPendingAggregate, 0, len(identities))
	for rows.Next() {
		var (
			item                                                 AgentPendingAggregate
			runID, agentID, nameOwner, nameSource, routePresence string
			flowScopeKey, flowInstanceID, flowInstancePath       string
			oldestRaw                                            any
		)
		if err := rows.Scan(
			&runID, &agentID, &nameOwner, &nameSource, &routePresence,
			&flowScopeKey, &flowInstanceID, &flowInstancePath,
			&item.Count, &oldestRaw,
		); err != nil {
			return nil, fmt.Errorf("scan agent pending aggregate: %w", err)
		}
		item.AgentIdentity, err = agentidentity.FromStorageFields(agentidentity.StorageFields{
			RunID:   runID,
			AgentID: agentID, NameOwner: nameOwner, NameSource: nameSource,
			RoutePresence: routePresence, FlowScopeKey: flowScopeKey,
			FlowInstanceID: flowInstanceID, FlowInstancePath: flowInstancePath,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: agent pending aggregate identity: %v", ErrConflict, err)
		}
		oldest, ok, err := parseNullableTime(oldestRaw)
		if err != nil {
			return nil, err
		}
		if item.Count <= 0 || !ok {
			return nil, fmt.Errorf("%w: agent pending aggregate violates structural policy", ErrConflict)
		}
		item.OldestEventAt = oldest
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent pending aggregates: %w", err)
	}
	return out, nil
}

// AgentPendingReferencePage selects limit+1 exact obligation identities, trims
// the lookahead row, and only then hydrates canonical lifecycle snapshots.
func (a *Adapter) AgentPendingReferencePage(ctx context.Context, q queryer, page AgentPendingPageQuery, asOf time.Time) (AgentPendingReferencePage, error) {
	page.AgentIdentity = page.AgentIdentity.Normalize()
	if err := page.AgentIdentity.Validate(); err != nil {
		return AgentPendingReferencePage{}, fmt.Errorf("agent pending page identity: %w", err)
	}
	if page.Limit <= 0 {
		return AgentPendingReferencePage{}, fmt.Errorf("agent pending page limit must be positive")
	}
	if page.Since.IsZero() {
		page.Since = time.Unix(0, 0).UTC()
	}
	if asOf.IsZero() {
		return AgentPendingReferencePage{}, fmt.Errorf("agent pending page as_of is required")
	}
	if page.After != nil {
		page.After.EventID = strings.TrimSpace(page.After.EventID)
		page.After.DeliveryID = strings.TrimSpace(page.After.DeliveryID)
		if page.After.EventCreatedAt.IsZero() || page.After.EventID == "" || page.After.DeliveryID == "" {
			return AgentPendingReferencePage{}, fmt.Errorf("agent pending page cursor requires event time, event id, and delivery id")
		}
		if _, err := uuid.Parse(page.After.EventID); err != nil {
			return AgentPendingReferencePage{}, fmt.Errorf("agent pending page cursor event id: %w", err)
		}
		if _, err := uuid.Parse(page.After.DeliveryID); err != nil {
			return AgentPendingReferencePage{}, fmt.Errorf("agent pending page cursor delivery id: %w", err)
		}
	}
	predicate, args, err := agentIdentityPredicate(a.dialect, "d", []agentidentity.Identity{page.AgentIdentity}, 1)
	if err != nil {
		return AgentPendingReferencePage{}, err
	}
	var query string
	activeStates := runtimerunlifecycle.ActiveStates()
	if a.dialect == DialectPostgres {
		args = append(args, page.Since.UTC())
		sinceIndex := len(args)
		args = append(args, string(activeStates[0]), string(activeStates[1]))
		args = append(args, asOf.UTC())
		asOfIndex := len(args)
		where := []string{
			"d.subscriber_type = 'agent'",
			"(" + predicate + ")",
			fmt.Sprintf("e.created_at >= $%d::timestamptz", sinceIndex),
			fmt.Sprintf("(e.run_id IS NULL OR r.status IN ($%d, $%d))", sinceIndex+1, sinceIndex+2),
			postgresAgentPendingEligibilityAt(fmt.Sprintf("$%d::timestamptz", asOfIndex)),
		}
		if page.After != nil {
			args = append(args, page.After.EventCreatedAt.UTC(), page.After.EventID, page.After.DeliveryID)
			where = append(where, fmt.Sprintf(
				"(e.created_at, e.event_id::text, d.delivery_id::text) > ($%d::timestamptz, $%d, $%d)",
				len(args)-2, len(args)-1, len(args),
			))
		}
		args = append(args, page.Limit+1)
		query = fmt.Sprintf(`
			SELECT d.delivery_id::text, e.event_id::text, e.created_at
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE %s
			ORDER BY e.created_at, e.event_id, d.delivery_id
			LIMIT $%d`, strings.Join(where, " AND "), len(args))
	} else {
		args = append(args, page.Since.UTC())
		args = append(args, string(activeStates[0]), string(activeStates[1]))
		args = append(args, sqliteTraceSQLTime(asOf.UTC()))
		where := []string{
			"d.subscriber_type = 'agent'",
			"(" + predicate + ")",
			"e.created_at >= ?",
			"(e.run_id IS NULL OR r.status IN (?, ?))",
			sqliteAgentPendingEligibilityAt("?"),
		}
		if page.After != nil {
			eventAt := sqliteTraceSQLTime(page.After.EventCreatedAt)
			where = append(where, `(
				`+sqliteTraceTimeExpression("e.created_at")+` > julianday(?)
				OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id > ?)
				OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id = ? AND d.delivery_id > ?)
			)`)
			args = append(args,
				eventAt,
				eventAt, page.After.EventID,
				eventAt, page.After.EventID, page.After.DeliveryID,
			)
		}
		args = append(args, page.Limit+1)
		query = fmt.Sprintf(`
			SELECT d.delivery_id, e.event_id, e.created_at
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE %s
			ORDER BY %s, e.event_id, d.delivery_id
			LIMIT ?`, strings.Join(where, " AND "), sqliteTraceTimeExpression("e.created_at"))
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return AgentPendingReferencePage{}, fmt.Errorf("select agent pending page: %w", err)
	}
	type rawReference struct {
		deliveryID string
		eventID    string
		eventAt    time.Time
	}
	raw := make([]rawReference, 0, page.Limit+1)
	for rows.Next() {
		var (
			item  rawReference
			atRaw any
		)
		if err := rows.Scan(&item.deliveryID, &item.eventID, &atRaw); err != nil {
			_ = rows.Close()
			return AgentPendingReferencePage{}, fmt.Errorf("scan agent pending page: %w", err)
		}
		item.deliveryID = strings.TrimSpace(item.deliveryID)
		item.eventID = strings.TrimSpace(item.eventID)
		at, ok, err := parseNullableTime(atRaw)
		if err != nil || !ok {
			_ = rows.Close()
			return AgentPendingReferencePage{}, fmt.Errorf("%w: agent pending page event time is invalid", ErrConflict)
		}
		item.eventAt = at
		raw = append(raw, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return AgentPendingReferencePage{}, fmt.Errorf("read agent pending page: %w", err)
	}
	if err := rows.Close(); err != nil {
		return AgentPendingReferencePage{}, fmt.Errorf("close agent pending page: %w", err)
	}
	result := AgentPendingReferencePage{HasMore: len(raw) > page.Limit}
	if result.HasMore {
		raw = raw[:page.Limit]
	}
	result.References = make([]AgentPendingReference, 0, len(raw))
	for _, reference := range raw {
		record, err := a.loadByID(ctx, q, reference.deliveryID, false)
		if err != nil {
			return AgentPendingReferencePage{}, err
		}
		snapshot := snapshotAt(record, asOf.UTC())
		if snapshot.DeliveryID != reference.deliveryID || snapshot.EventID != reference.eventID ||
			snapshot.SubscriberClass != SubscriberAgent || snapshot.Route.AgentIdentity != page.AgentIdentity ||
			!agentPendingSnapshotEligible(snapshot, asOf.UTC()) {
			return AgentPendingReferencePage{}, fmt.Errorf("%w: agent pending page reference changed during hydration", ErrConflict)
		}
		result.References = append(result.References, AgentPendingReference{
			Snapshot:       snapshot,
			EventCreatedAt: reference.eventAt,
		})
	}
	return result, nil
}

// CurrentAgentSnapshots selects at most one row-ranked current lifecycle
// candidate for each requested agent before canonical hydration.
func (a *Adapter) CurrentAgentSnapshots(ctx context.Context, q queryer, identities []agentidentity.Identity, asOf time.Time) ([]Snapshot, error) {
	identities, err := normalizeAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return []Snapshot{}, nil
	}
	if asOf.IsZero() {
		return nil, fmt.Errorf("current agent snapshots as_of is required")
	}
	predicate, args, err := agentIdentityPredicate(a.dialect, "d", identities, 1)
	if err != nil {
		return nil, err
	}
	var query string
	if a.dialect == DialectPostgres {
		query = fmt.Sprintf(`
			WITH ranked AS (
				SELECT d.delivery_id,
					ROW_NUMBER() OVER (
						PARTITION BY d.run_id, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
						             d.agent_route_presence, d.agent_flow_scope_key,
						             d.agent_flow_instance_id, d.agent_flow_instance_path
						ORDER BY
							CASE WHEN d.status IN ('pending', 'in_progress', 'failed') THEN 1 ELSE 0 END DESC,
							CASE
								WHEN d.status = 'failed' THEN 4
								WHEN d.status = 'in_progress' AND COALESCE(a.active_session_id::text, '') = '' THEN 3
								WHEN d.status = 'in_progress' THEN 2
								WHEN d.status = 'pending' THEN 1
								ELSE 0
							END DESC,
							COALESCE(d.settled_at, d.created_at) DESC,
							d.delivery_id DESC
					) AS row_number
				FROM event_deliveries d
				LEFT JOIN event_delivery_attempts a
					ON a.delivery_id = d.delivery_id
				   AND a.claim_version = d.current_attempt_version
				   AND a.open_marker = TRUE
				WHERE d.subscriber_type = 'agent'
				  AND (%s)
				  AND d.status IN ('pending', 'in_progress', 'failed', 'dead_letter')
			)
			SELECT delivery_id::text
			FROM ranked
			WHERE row_number = 1
			ORDER BY delivery_id`, predicate)
	} else {
		query = fmt.Sprintf(`
			WITH ranked AS (
				SELECT d.delivery_id,
					ROW_NUMBER() OVER (
						PARTITION BY d.run_id, d.subscriber_id, d.agent_name_owner, d.agent_name_source,
						             d.agent_route_presence, d.agent_flow_scope_key,
						             d.agent_flow_instance_id, d.agent_flow_instance_path
						ORDER BY
							CASE WHEN d.status IN ('pending', 'in_progress', 'failed') THEN 1 ELSE 0 END DESC,
							CASE
								WHEN d.status = 'failed' THEN 4
								WHEN d.status = 'in_progress' AND COALESCE(a.active_session_id, '') = '' THEN 3
								WHEN d.status = 'in_progress' THEN 2
								WHEN d.status = 'pending' THEN 1
								ELSE 0
							END DESC,
							COALESCE(d.settled_at, d.created_at) DESC,
							d.delivery_id DESC
					) AS row_number
				FROM event_deliveries d
				LEFT JOIN event_delivery_attempts a
					ON a.delivery_id = d.delivery_id
				   AND a.claim_version = d.current_attempt_version
				   AND a.open_marker = 1
				WHERE d.subscriber_type = 'agent'
				  AND (%s)
				  AND d.status IN ('pending', 'in_progress', 'failed', 'dead_letter')
			)
			SELECT delivery_id
			FROM ranked
			WHERE row_number = 1
			ORDER BY delivery_id`, predicate)
	}
	snapshots, err := a.snapshotsByIDQueryAt(ctx, q, asOf.UTC(), query, args...)
	if err != nil {
		return nil, err
	}
	agents := make(map[agentidentity.Identity]struct{}, len(identities))
	for _, identity := range identities {
		agents[identity] = struct{}{}
	}
	for _, snapshot := range snapshots {
		_, requested := agents[snapshot.Route.AgentIdentity]
		if !requested || snapshot.SubscriberClass != SubscriberAgent || snapshot.Status == StatusDelivered {
			return nil, fmt.Errorf("%w: current agent lifecycle reference changed during hydration", ErrConflict)
		}
	}
	return snapshots, nil
}

func normalizeAgentIdentities(values []agentidentity.Identity) ([]agentidentity.Identity, error) {
	seen := make(map[agentidentity.Identity]struct{}, len(values))
	out := make([]agentidentity.Identity, 0, len(values))
	for _, value := range values {
		value = value.Normalize()
		if err := value.Validate(); err != nil {
			return nil, fmt.Errorf("agent delivery identity: %w", err)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func agentIdentityPredicate(dialect Dialect, alias string, identities []agentidentity.Identity, firstArg int) (string, []any, error) {
	identities, err := normalizeAgentIdentities(identities)
	if err != nil {
		return "", nil, err
	}
	if len(identities) == 0 {
		return "", nil, fmt.Errorf("agent identity predicate requires at least one identity")
	}
	columns := []string{
		"run_id", "subscriber_id", "agent_name_owner", "agent_name_source", "agent_route_presence",
		"agent_flow_scope_key", "agent_flow_instance_id", "agent_flow_instance_path",
	}
	args := make([]any, 0, len(identities)*len(columns))
	groups := make([]string, 0, len(identities))
	for _, identity := range identities {
		fields, err := identity.StorageFields()
		if err != nil {
			return "", nil, err
		}
		values := []any{
			fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		}
		terms := make([]string, 0, len(columns))
		for idx, column := range columns {
			placeholder := "?"
			if dialect == DialectPostgres {
				placeholder = fmt.Sprintf("$%d", firstArg+len(args))
			}
			terms = append(terms, alias+"."+column+" = "+placeholder)
			args = append(args, values[idx])
		}
		groups = append(groups, "("+strings.Join(terms, " AND ")+")")
	}
	return strings.Join(groups, " OR "), args, nil
}

func (a *Adapter) NonterminalSnapshotsForRun(ctx context.Context, q queryer, runID string) ([]Snapshot, error) {
	return a.runSnapshotsByProjection(ctx, q, runID, runSnapshotProjectionNonterminal)
}

func (a *Adapter) ActiveCouplingSnapshotsForRun(ctx context.Context, q queryer, runID string) ([]Snapshot, error) {
	return a.runSnapshotsByProjection(ctx, q, runID, runSnapshotProjectionActiveCoupling)
}

func (a *Adapter) AgentSnapshotsForRun(ctx context.Context, q queryer, runID string) ([]Snapshot, error) {
	return a.runSnapshotsByProjection(ctx, q, runID, runSnapshotProjectionAgent)
}

func (a *Adapter) RunHasDeliveryObligations(ctx context.Context, q queryer, runID string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return false, fmt.Errorf("delivery run existence run id: %w", err)
	}
	query := `SELECT EXISTS (SELECT 1 FROM event_deliveries WHERE run_id = $1::uuid)`
	if a.dialect == DialectSQLite {
		query = `SELECT EXISTS (SELECT 1 FROM event_deliveries WHERE run_id = ?)`
	}
	var exists bool
	if err := q.QueryRowContext(ctx, query, runID).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect delivery run existence: %w", err)
	}
	return exists, nil
}

type runSnapshotProjection uint8

const (
	runSnapshotProjectionNonterminal runSnapshotProjection = iota + 1
	runSnapshotProjectionActiveCoupling
	runSnapshotProjectionAgent
)

func (a *Adapter) runSnapshotsByProjection(ctx context.Context, q queryer, runID string, projection runSnapshotProjection) ([]Snapshot, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("delivery run projection run id: %w", err)
	}
	var predicate, join string
	switch projection {
	case runSnapshotProjectionNonterminal:
		predicate = "d.status IN ('pending', 'in_progress', 'failed')"
	case runSnapshotProjectionActiveCoupling:
		predicate = `(
			d.status = 'in_progress'
			OR COALESCE(a.active_session_id::text, '') <> ''
			OR (d.started_at IS NOT NULL AND d.status NOT IN ('delivered', 'dead_letter', 'canceled'))
		)`
		join = `LEFT JOIN event_delivery_attempts a
			ON a.delivery_id = d.delivery_id
		   AND a.claim_version = d.current_attempt_version
		   AND a.open_marker = TRUE`
		if a.dialect == DialectSQLite {
			predicate = `(
				d.status = 'in_progress'
				OR COALESCE(a.active_session_id, '') <> ''
				OR (d.started_at IS NOT NULL AND d.status NOT IN ('delivered', 'dead_letter', 'canceled'))
			)`
			join = `LEFT JOIN event_delivery_attempts a
				ON a.delivery_id = d.delivery_id
			   AND a.claim_version = d.current_attempt_version
			   AND a.open_marker = 1`
		}
	case runSnapshotProjectionAgent:
		predicate = "d.subscriber_type = 'agent'"
	default:
		return nil, fmt.Errorf("delivery run projection kind %d is invalid", projection)
	}
	id := "d.delivery_id::text"
	argument := "$1::uuid"
	if a.dialect == DialectSQLite {
		id = "d.delivery_id"
		argument = "?"
	}
	query := fmt.Sprintf(`
		SELECT %s
		FROM event_deliveries d
		%s
		WHERE d.run_id = %s AND %s
		ORDER BY d.created_at, d.delivery_id`, id, join, argument, predicate)
	snapshots, err := a.snapshotsByIDQuery(ctx, q, query, runID)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		if snapshot.RunID != runID || !runSnapshotMatchesProjection(snapshot, projection) {
			return nil, fmt.Errorf("%w: delivery run projection changed during hydration", ErrConflict)
		}
	}
	return snapshots, nil
}

func runSnapshotMatchesProjection(snapshot Snapshot, projection runSnapshotProjection) bool {
	switch projection {
	case runSnapshotProjectionNonterminal:
		return snapshot.Status == StatusPending || snapshot.Status == StatusInProgress || snapshot.Status == StatusFailed
	case runSnapshotProjectionActiveCoupling:
		return snapshot.Status == StatusInProgress || snapshot.ActiveSessionID != "" ||
			(!snapshot.StartedAt.IsZero() && !snapshot.Terminal())
	case runSnapshotProjectionAgent:
		return snapshot.SubscriberClass == SubscriberAgent
	default:
		return false
	}
}

func agentPendingSnapshotEligible(snapshot Snapshot, now time.Time) bool {
	switch snapshot.Status {
	case StatusPending, StatusInProgress:
		return true
	case StatusFailed:
		return snapshot.RetryCount <= snapshot.MaxRetries && !snapshot.NextEligibleAt.After(now)
	default:
		return false
	}
}

func normalizedNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// UnfinishedRunDeliveryCountTx counts receiver work that cannot satisfy a
// run's durable completion gate, including routes awaiting pipeline handoff.
func UnfinishedRunDeliveryCountTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	if tx == nil || runID == "" {
		return 0, fmt.Errorf("run delivery completion requires transaction and run")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries
		WHERE run_id=$1 AND (status<>'delivered' OR continuation_handoff_at IS NULL)`, runID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unfinished run deliveries: %w", err)
	}
	return count, nil
}

func FixtureAgentEventCardinalityTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	if tx == nil {
		return 0, fmt.Errorf("agent event cardinality requires a selected read transaction")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='agent'`, eventID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// FixtureDeliveryCardinalityTx is a fixed physical witness, not an executable
// work selector. The caller's selected read transaction owns its snapshot.
func FixtureDeliveryCardinalityTx(ctx context.Context, tx *sql.Tx) (int, error) {
	if tx == nil {
		return 0, fmt.Errorf("delivery cardinality evidence requires a read transaction")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries`).Scan(&count); err != nil {
		return 0, fmt.Errorf("read delivery cardinality evidence: %w", err)
	}
	return count, nil
}

// FixtureNotifyAllChildrenDiagnostic is detached physical evidence, not a
// lifecycle selector. The original selected read transaction owns its snapshot.
type FixtureNotifyAllChildrenDiagnostic struct {
	Columns []string
	Rows    [][]any
}

func FixtureNotifyAllChildrenDiagnosticTx(ctx context.Context, tx *sql.Tx) (section FixtureNotifyAllChildrenDiagnostic, err error) {
	if tx == nil {
		return section, fmt.Errorf("delivery diagnostic evidence requires a read transaction")
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), ''), COALESCE(CAST(delivery_target_route AS TEXT), '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`)
	if err != nil {
		return section, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			section = FixtureNotifyAllChildrenDiagnostic{}
		}
	}()
	section.Columns, err = rows.Columns()
	if err != nil {
		return section, err
	}
	for rows.Next() {
		values := make([]any, len(section.Columns))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return section, err
		}
		for index, value := range values {
			if raw, ok := value.([]byte); ok {
				values[index] = string(raw)
			}
		}
		section.Rows = append(section.Rows, values)
	}
	return section, rows.Err()
}

// These observations retain physical columns, not live eligibility or normalized
// lifecycle projections. Their caller owns the original selected read snapshot.
type FixtureHandlerSelectionStorage struct {
	DeliveryID   string
	Context      string
	Disposition  string
	FlowPath     string
	Family       string
	SemanticPath string
	DisplayLabel string
}

func FixtureHandlerSelectionStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (evidence []FixtureHandlerSelectionStorage, err error) {
	if tx == nil {
		return nil, fmt.Errorf("handler selection evidence requires a read transaction")
	}
	rows, err := tx.QueryContext(ctx, `SELECT s.delivery_id, s.selection_context, s.disposition,
		COALESCE(s.flow_path, ''), COALESCE(s.declaration_family, ''),
		COALESCE(s.semantic_path, ''), s.display_label
		FROM event_delivery_handler_rule_selections s
		JOIN event_deliveries d ON d.delivery_id=s.delivery_id
		WHERE d.event_id=$1 AND d.subscriber_type='node' ORDER BY s.delivery_id`, eventID)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			evidence = nil
		}
	}()
	for rows.Next() {
		var row FixtureHandlerSelectionStorage
		if err := rows.Scan(&row.DeliveryID, &row.Context, &row.Disposition, &row.FlowPath, &row.Family, &row.SemanticPath, &row.DisplayLabel); err != nil {
			return nil, err
		}
		evidence = append(evidence, row)
	}
	return evidence, rows.Err()
}

type FixtureFanOutTriggeredIntentStorage struct {
	TriggeringDeliveryID, SemanticDigest string
	Capsule                              json.RawMessage
	Source                               fanoutobligation.SourceRef
	Cursor, Cardinality                  int
	Status                               string
}

func FixtureFanOutTriggeredIntentStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID, nodeKey string, ref runtimecontracts.FanOutElementRef) (evidence []FixtureFanOutTriggeredIntentStorage, err error) {
	if tx == nil {
		return nil, fmt.Errorf("fan-out trigger evidence requires a read transaction")
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.triggering_delivery_id,i.semantic_digest,i.capsule,
		i.source_kind,COALESCE(CAST(i.source_event_id AS TEXT),''),COALESCE(CAST(i.source_run_id AS TEXT),''),COALESCE(CAST(i.source_entity_id AS TEXT),''),
		i.source_field,COALESCE(CAST(i.source_mutation_id AS TEXT),''),i.cardinality,i.cursor,i.status
		FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id
		WHERE i.run_id=$1 AND d.event_id=$2 AND d.subscriber_type='node' AND d.subscriber_id=$3
		AND i.flow_path=$4 AND i.declaration_family=$5 AND i.semantic_path=$6`,
		runID, eventID, nodeKey, ref.FlowPath, ref.Family, ref.SemanticPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			evidence = nil
		}
	}()
	for rows.Next() {
		var row FixtureFanOutTriggeredIntentStorage
		var capsule []byte
		if err := rows.Scan(&row.TriggeringDeliveryID, &row.SemanticDigest, &capsule,
			&row.Source.Kind, &row.Source.EventID, &row.Source.RunID, &row.Source.EntityID,
			&row.Source.Field, &row.Source.MutationID, &row.Cardinality, &row.Cursor, &row.Status); err != nil {
			return nil, err
		}
		row.Capsule = append(json.RawMessage(nil), capsule...)
		evidence = append(evidence, row)
	}
	return evidence, rows.Err()
}

type FixtureH1BumpHistory struct {
	Entity string
	Event  string
	Before string
	After  string
}

func FixtureH1BumpHistoryTx(ctx context.Context, tx *sql.Tx, runID string) ([]FixtureH1BumpHistory, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.entity_id,m.caused_by_event,CAST(m.old_value AS TEXT),CAST(m.new_value AS TEXT) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event JOIN event_deliveries d ON d.event_id=e.event_id AND d.run_id=m.run_id AND d.subscriber_type='node' WHERE m.run_id=$1 AND m.domain='authored_field' AND m.path='count' AND e.event_name='hub.bump' AND d.status='delivered'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FixtureH1BumpHistory
	for rows.Next() {
		var row FixtureH1BumpHistory
		if err := rows.Scan(&row.Entity, &row.Event, &row.Before, &row.After); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func observeWriterRun(ctx context.Context, tx *sql.Tx, runID string, out *WriterRunDeliveryEvidence) error {
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'),
		(SELECT COALESCE(SUM(retry_count),0) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.claim_version<>1 OR (a.closure_kind='settled' AND a.outcome<>'delivered'))),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND a.closure_kind='settled' AND a.outcome='delivered')`, runID).Scan(&out.Total, &out.DeliveredAgents, &out.DeadLetters, &out.Retries, &out.BadClaims, &out.SettledDelivered)
}

func observeDeliveryEventEvidence(ctx context.Context, tx *sql.Tx, eventID string) (DeliveryEventEvidence, error) {
	var out DeliveryEventEvidence
	if _, err := uuid.Parse(eventID); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id,run_id,event_id,subscriber_type,subscriber_id,status,retry_count,claim_version,continuation_handoff_at,
		(SELECT COUNT(*) FROM event_delivery_handler_rule_selections s WHERE s.delivery_id=d.delivery_id)
		FROM event_deliveries d WHERE event_id=$1 ORDER BY delivery_id`, eventID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var row DeliveryRowEvidence
		var handed any
		if err := rows.Scan(&row.DeliveryID, &row.RunID, &row.EventID, &row.SubscriberType, &row.SubscriberID, &row.Status, &row.RetryCount, &row.ClaimVersion, &handed, &row.HandlerSelections); err != nil {
			return DeliveryEventEvidence{}, errors.Join(err, rows.Close())
		}
		row.HandoffPresent = handed != nil
		row.HandoffAt, _, err = parseNullableTime(handed)
		if err != nil {
			return DeliveryEventEvidence{}, errors.Join(err, rows.Close())
		}
		out.Deliveries = append(out.Deliveries, row)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	for i := range out.Deliveries {
		row := &out.Deliveries[i]
		attempts, err := tx.QueryContext(ctx, `SELECT claim_version,COALESCE(closure_kind,''),COALESCE(outcome,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`, row.DeliveryID)
		if err != nil {
			return DeliveryEventEvidence{}, err
		}
		for attempts.Next() {
			var attempt DeliveryAttemptEvidence
			if err := attempts.Scan(&attempt.ClaimVersion, &attempt.ClosureKind, &attempt.Outcome); err != nil {
				return DeliveryEventEvidence{}, errors.Join(err, attempts.Close())
			}
			row.Attempts = append(row.Attempts, attempt)
		}
		err = errors.Join(attempts.Err(), attempts.Close())
		if err != nil {
			return DeliveryEventEvidence{}, err
		}
	}
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, eventID).Scan(&out.DeadLetters)
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	return out, nil
}

func observeH1DeliveryAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1DeliveryAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node' AND e.event_name='hub.bump' AND d.status='delivered'`, runID).Scan(&out.DeliveredBumps); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'`, runID).Scan(&out.DeliveredAgents); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status<>'delivered'`, runID).Scan(&out.Undelivered); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(retry_count),0) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&out.Retries); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1`, runID).Scan(&out.DeadLetters); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.claim_version<>1 OR a.closure_kind<>'settled' OR a.outcome<>'delivered')`, runID).Scan(&out.BadAttempts); err != nil {
		return err
	}
	return nil
}

func observeH2DeliveryAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2DeliveryAccountingEvidence) error {
	var err error
	out.DeadLetters, err = CountRunDeadLetterDeliveriesTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.outcome='retry_scheduled' OR a.outcome='dead_letter')`, runID).Scan(&out.Retried); err != nil {
		return err
	}
	return nil
}

func observeH2PendingAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2PendingAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id IS NOT NULL AND status IN ('pending','in_progress')`).Scan(&out.Pending); err != nil {
		return err
	}
	return nil
}

func observeH1NodeFailuresForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1NodeFailuresEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,e.event_id,e.event_name,d.status,d.retry_count,COALESCE(d.reason_code,''),COALESCE(CAST(d.failure AS TEXT),'null')
		FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (d.retry_count<>0 OR d.status='dead_letter' OR d.failure IS NOT NULL)
		ORDER BY d.created_at,d.delivery_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1NodeFailuresEvidence
	for rows.Next() {
		var row H1NodeFailuresEvidence
		if err := rows.Scan(&row.Delivery, &row.Event, &row.Name, &row.Status, &row.Retries, &row.Reason, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1AttemptFailuresForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1AttemptFailuresEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,e.event_id,e.event_name,a.claim_version,a.closure_kind,COALESCE(a.outcome,''),COALESCE(a.reason_code,''),COALESCE(CAST(a.failure AS TEXT),'null')
		FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (a.outcome IN ('retry_scheduled','dead_letter') OR a.failure IS NOT NULL)
		ORDER BY d.delivery_id,a.claim_version`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1AttemptFailuresEvidence
	for rows.Next() {
		var row H1AttemptFailuresEvidence
		if err := rows.Scan(&row.Delivery, &row.Event, &row.Name, &row.Version, &row.Closure, &row.Outcome, &row.Reason, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1DeadLettersForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1DeadLettersEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT l.dead_letter_id,e.event_id,e.event_name,COALESCE(CAST(l.delivery_id AS TEXT),''),COALESCE(l.claim_version,0),l.retry_count,COALESCE(l.handler_node,''),CAST(l.failure AS TEXT)
		FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1 ORDER BY l.created_at,l.dead_letter_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1DeadLettersEvidence
	for rows.Next() {
		var row H1DeadLettersEvidence
		if err := rows.Scan(&row.ID, &row.Event, &row.Name, &row.Delivery, &row.Version, &row.Retries, &row.Node, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2ResponseQueueForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2ResponseQueueEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.event_name,d.status,COUNT(*) FROM events e LEFT JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 GROUP BY e.event_name,d.status`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2ResponseQueueEvidence
	for rows.Next() {
		var row H2ResponseQueueEvidence
		var status sql.NullString
		if err := rows.Scan(&row.Name, &status, &row.Count); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Status = WorkloadNullableText{String: status.String, Valid: status.Valid}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2NodeDeliveriesForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2NodeDeliveriesEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.event_id,d.status,d.retry_count,e.event_name,e.payload FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2NodeDeliveriesEvidence
	for rows.Next() {
		var row H2NodeDeliveriesEvidence
		if err := rows.Scan(&row.Event, &row.Status, &row.Retries, &row.Name, &row.Payload); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func CountStageCompletionNodeDeliveriesTx(ctx context.Context, tx *sql.Tx, runID, eventName, nodeID, rootNodeID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
WHERE e.run_id=$1 AND e.event_name=$2 AND d.subscriber_type='node' AND d.subscriber_id IN ($3,$4) AND d.status='delivered'`, runID, eventName, nodeID, rootNodeID).Scan(&count)
	return count, err
}

type FanOutDrainDeliveryStorage struct {
	DeliveryID, EventID, EventName, SubscriberType, SubscriberID            WorkloadNullableText
	Status, ClaimVersion, RetryCount, NextEligibleAt, ContinuationHandoffAt WorkloadNullableText
	UpdatedAt, AttemptClosure, LeaseExpiresAt, AttemptOutcome               WorkloadNullableText
}

func ReadFanOutDrainDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]FanOutDrainDeliveryStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(d.delivery_id AS TEXT),
CAST(d.event_id AS TEXT), e.event_name, d.subscriber_type, d.subscriber_id,
d.status, CAST(d.claim_version AS TEXT), CAST(d.retry_count AS TEXT),
CAST(d.next_eligible_at AS TEXT), CAST(d.continuation_handoff_at AS TEXT),
CAST(d.updated_at AS TEXT), a.closure_kind, CAST(a.lease_expires_at AS TEXT),
a.outcome FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id
AND a.claim_version=d.current_attempt_version
WHERE d.run_id=$1 AND d.status NOT IN ('delivered','dead_letter')
ORDER BY d.created_at,d.delivery_id LIMIT 33`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FanOutDrainDeliveryStorage
	for rows.Next() {
		var values [14]sql.NullString
		var targets [14]any
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets[:]...); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		var detached [14]WorkloadNullableText
		for i, value := range values {
			detached[i] = WorkloadNullableText{String: value.String, Valid: value.Valid}
		}
		out = append(out, FanOutDrainDeliveryStorage{
			DeliveryID: detached[0], EventID: detached[1], EventName: detached[2], SubscriberType: detached[3], SubscriberID: detached[4],
			Status: detached[5], ClaimVersion: detached[6], RetryCount: detached[7], NextEligibleAt: detached[8], ContinuationHandoffAt: detached[9],
			UpdatedAt: detached[10], AttemptClosure: detached[11], LeaseExpiresAt: detached[12], AttemptOutcome: detached[13],
		})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type PostgresAgentOneDeliverySessionStorage struct {
	Status          string
	ActiveSessionID string
}

func ReadPostgresAgentOneDeliverySessionStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (PostgresAgentOneDeliverySessionStorage, error) {
	var out PostgresAgentOneDeliverySessionStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(d.status, ''), COALESCE(a.active_session_id::text, '')
FROM event_deliveries d JOIN event_delivery_attempts a
ON a.delivery_id = d.delivery_id AND a.claim_version = d.current_attempt_version AND a.open_marker = TRUE
WHERE d.event_id = $1::uuid AND d.subscriber_type = 'agent' AND d.subscriber_id = 'agent-1'`, eventID).Scan(&out.Status, &out.ActiveSessionID)
	if err != nil {
		return PostgresAgentOneDeliverySessionStorage{}, err
	}
	return out, nil
}

type DeploymentDeliveryStorage struct {
	DeliveryID, EventID, SubscriberType, SubscriberID, Status string
	RetryCount, ClaimVersion                                  int64
	NextEligible, Handoff, Reason, Failure                    WorkloadNullableText
}
type DeploymentAttemptStorage struct {
	DeliveryID, Closure      string
	ClaimVersion             int64
	Outcome, Reason, Failure WorkloadNullableText
}
type DeploymentDeliveryStatusStorage struct {
	Status string
	Count  int
}

func CountDeploymentPendingDeliveriesStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, runID).Scan(&out)
	return out, err
}

func CountDeploymentFailedDeliveriesStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('failed','dead_letter')`, runID).Scan(&out)
	return out, err
}

func CountDeploymentStampedHandoffsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND continuation_handoff_at IS NOT NULL`, runID).Scan(&out)
	return out, err
}

func CountRootReadyDeliveredStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.status='delivered'`, runID).Scan(&out)
	return out, err
}

func CountRootReadyStampedHandoffsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.continuation_handoff_at IS NOT NULL`, runID).Scan(&out)
	return out, err
}

func ReadDeploymentDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentDeliveryStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(delivery_id AS TEXT),CAST(event_id AS TEXT),subscriber_type,subscriber_id,status,retry_count,claim_version,CAST(next_eligible_at AS TEXT),CAST(continuation_handoff_at AS TEXT),CAST(reason_code AS TEXT),CAST(failure AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY created_at,delivery_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentDeliveryStorage
	for rows.Next() {
		var row DeploymentDeliveryStorage
		var nextEligible, handoff, reason, failure sql.NullString
		if err := rows.Scan(&row.DeliveryID, &row.EventID, &row.SubscriberType, &row.SubscriberID, &row.Status, &row.RetryCount, &row.ClaimVersion, &nextEligible, &handoff, &reason, &failure); err != nil {
			return nil, err
		}
		row.NextEligible, row.Handoff, row.Reason, row.Failure = WorkloadNullableText(nextEligible), WorkloadNullableText(handoff), WorkloadNullableText(reason), WorkloadNullableText(failure)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeploymentAttemptStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentAttemptStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(a.delivery_id AS TEXT),a.claim_version,a.closure_kind,CAST(a.outcome AS TEXT),CAST(a.reason_code AS TEXT),CAST(a.failure AS TEXT) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 ORDER BY a.delivery_id,a.claim_version`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentAttemptStorage
	for rows.Next() {
		var row DeploymentAttemptStorage
		var outcome, reason, failure sql.NullString
		if err := rows.Scan(&row.DeliveryID, &row.ClaimVersion, &row.Closure, &outcome, &reason, &failure); err != nil {
			return nil, err
		}
		row.Outcome, row.Reason, row.Failure = WorkloadNullableText(outcome), WorkloadNullableText(reason), WorkloadNullableText(failure)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeploymentDeliveryStatusStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentDeliveryStatusStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT status,COUNT(*) FROM event_deliveries WHERE run_id=$1 GROUP BY status ORDER BY status`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentDeliveryStatusStorage
	for rows.Next() {
		var row DeploymentDeliveryStatusStorage
		if err := rows.Scan(&row.Status, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type ClosedReceiverReadinessStorage struct{ Actors, Events, Deliveries int }

func CountPortfolioDeliveredDeploymentRowsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered' AND d.status='delivered'`, runID).Scan(&count)
	return count, err
}

func CountPortfolioAllDeploymentRowsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered'`, runID).Scan(&count)
	return count, err
}

func ReadClosedReceiverReadinessStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, bridgeNodeID string) (ClosedReceiverReadinessStorage, error) {
	query := `
		SELECT
			(SELECT COUNT(DISTINCT o.agent_id)
			 FROM runtime_external_effect_operations o
			 JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
			 WHERE o.agent_id IN ('upstream-agent','downstream-agent') AND a.state = 'settled'),
			(SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name IN ('task.completed','task.finalized')),
			(SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
			 WHERE e.run_id = ? AND d.status = 'delivered'
			   AND ((d.subscriber_type = 'agent' AND d.subscriber_id IN ('upstream-agent','downstream-agent'))
			     OR (d.subscriber_type = 'node' AND d.subscriber_id = ?)))
	`
	args := []any{runID, runID, bridgeNodeID}
	if postgres {
		query = `
			SELECT
				(SELECT COUNT(DISTINCT o.agent_id)
				 FROM runtime_external_effect_operations o
				 JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
				 WHERE o.agent_id IN ('upstream-agent','downstream-agent') AND a.state = 'settled'),
				(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name IN ('task.completed','task.finalized')),
				(SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
				 WHERE e.run_id = $1::uuid AND d.status = 'delivered'
				   AND ((d.subscriber_type = 'agent' AND d.subscriber_id IN ('upstream-agent','downstream-agent'))
				     OR (d.subscriber_type = 'node' AND d.subscriber_id = $2)))
		`
		args = []any{runID, bridgeNodeID}
	}
	var out ClosedReceiverReadinessStorage
	err := tx.QueryRowContext(ctx, query, args...).Scan(&out.Actors, &out.Events, &out.Deliveries)
	return out, err
}

type ClosedReceiverDeliveryStorage struct {
	EventName, FlowInstance, EntityID, TargetRoute        string
	SubscriberType, SubscriberID, Status, Reason, Failure WorkloadNullableText
}

func ReadClosedReceiverDeliveryStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]ClosedReceiverDeliveryStorage, error) {
	query := `
		SELECT e.event_name, COALESCE(e.flow_instance, ''), COALESCE(CAST(e.entity_id AS TEXT), ''),
		       CAST(e.target_route AS TEXT), d.subscriber_type, d.subscriber_id, d.status,
		       COALESCE(d.reason_code, ''), COALESCE(CAST(d.failure AS TEXT), '')
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		WHERE e.run_id = ?
		ORDER BY e.created_at, d.subscriber_type, d.subscriber_id`
	if postgres {
		query = strings.Replace(query, "e.run_id = ?", "e.run_id = $1::uuid", 1)
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClosedReceiverDeliveryStorage
	for rows.Next() {
		var row ClosedReceiverDeliveryStorage
		var subscriberType, subscriberID, status, reason, failure sql.NullString
		if err := rows.Scan(&row.EventName, &row.FlowInstance, &row.EntityID, &row.TargetRoute, &subscriberType, &subscriberID, &status, &reason, &failure); err != nil {
			return nil, err
		}
		row.SubscriberType, row.SubscriberID, row.Status = WorkloadNullableText(subscriberType), WorkloadNullableText(subscriberID), WorkloadNullableText(status)
		row.Reason, row.Failure = WorkloadNullableText(reason), WorkloadNullableText(failure)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type ClosedReceiverAgentRouteStorage struct{ AgentID, Owner, Source, Presence, Scope, InstanceID, InstancePath string }

func ReadClosedReceiverAgentRouteStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]ClosedReceiverAgentRouteStorage, error) {
	query := `
		SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source,
		       d.agent_route_presence, d.agent_flow_scope_key,
		       d.agent_flow_instance_id, d.agent_flow_instance_path
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.run_id = ? AND d.subscriber_type = 'agent'
		ORDER BY d.subscriber_id`
	if postgres {
		query = `
			SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			       d.agent_route_presence, d.agent_flow_scope_key,
			       d.agent_flow_instance_id, d.agent_flow_instance_path
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE e.run_id = $1::uuid AND d.subscriber_type = 'agent'
			ORDER BY d.subscriber_id`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClosedReceiverAgentRouteStorage
	for rows.Next() {
		var row ClosedReceiverAgentRouteStorage
		if err := rows.Scan(&row.AgentID, &row.Owner, &row.Source, &row.Presence, &row.Scope, &row.InstanceID, &row.InstancePath); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Fixed channel budget failure evidence; no executable-delivery interpretation.
func scanChannelBudgetDiagnosticRows(rows *sql.Rows) ([]map[string]string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]string
	for rows.Next() {
		values, targets := make([]sql.NullString, len(columns)), make([]any, len(columns))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for index, column := range columns {
			if values[index].Valid {
				row[column] = values[index].String
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
func ReadChannelBudgetDeliveriesDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) ([]map[string]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT subscriber_type,subscriber_id,status,reason_code,retry_count FROM event_deliveries ORDER BY created_at DESC LIMIT 20")
	if err != nil {
		return nil, err
	}
	return scanChannelBudgetDiagnosticRows(rows)
}
