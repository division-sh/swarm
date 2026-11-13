package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
)

type LatestPipelineReceiptStorage = pipelinepersistence.LatestPipelineReceiptStorage

type NodeDeliveryDiagnosticStorage struct {
	Rows, DeadLetters, Receipts []string
}

func ReadServedRunDeliverySummaryForTest(ctx context.Context, selected any, runID string) (runtimedelivery.RunSummary, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadServedRunDeliverySummaryForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadServedRunDeliverySummaryForTest(ctx, runID)
	default:
		return runtimedelivery.RunSummary{}, fmt.Errorf("run delivery summary requires the original native owner")
	}
}

func ReadNodeDeliveryTargetEncodingForTest(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	if nodeID == "" || strings.TrimSpace(nodeID) != nodeID {
		return "", fmt.Errorf("node target observation requires its exact nonempty recipient")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadNodeDeliveryTargetEncodingForTest(ctx, eventID, nodeID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadNodeDeliveryTargetEncodingForTest(ctx, eventID, nodeID)
	default:
		return "", fmt.Errorf("node target observation requires the original native owner")
	}
}

func ReadNodeDeliveryDiagnosticStorageForTest(ctx context.Context, selected any, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out NodeDeliveryDiagnosticStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Rows, err = delivery.ReadNodeDeliveryDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("delivery dump: %w", err)
		}
		out.DeadLetters, err = delivery.ReadNodeDeadLetterDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("dead letter dump: %w", err)
		}
		out.Receipts, err = pipelinepersistence.ReadNodeReceiptDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("receipt dump: %w", err)
		}
		return nil
	})
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	return out, nil
}

type CausalDeliveryStatusCount = delivery.CausalDeliveryStatusCount

type CausalDeliveryFrontier = delivery.CausalDeliveryFrontier
type CausalDeliveryFrontierProbe = delivery.CausalDeliveryFrontierProbe

const (
	FrontierComplete           = delivery.FrontierComplete
	FrontierMissing            = delivery.FrontierMissing
	FrontierPending            = delivery.FrontierPending
	FrontierDuplicateDelivery  = delivery.FrontierDuplicateDelivery
	FrontierDeadLetterDelivery = delivery.FrontierDeadLetterDelivery
)

func ReadCausalDeliveryFrontierForTest(ctx context.Context, selected any, runID, eventID string) (CausalDeliveryFrontier, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadCausalDeliveryFrontierForTest(ctx, runID, eventID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadCausalDeliveryFrontierForTest(ctx, runID, eventID)
	default:
		return CausalDeliveryFrontier{}, fmt.Errorf("causal delivery frontier requires the original native owner")
	}
}

func ProbeCausalDeliveryFrontierForTest(ctx context.Context, selected any, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ProbeCausalDeliveryFrontierForTest(ctx, probe)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ProbeCausalDeliveryFrontierForTest(ctx, probe)
	default:
		return CausalDeliveryFrontier{}, fmt.Errorf("causal delivery frontier requires the original native owner")
	}
}

func CountClosedSourceFanOutIssuanceForTest(ctx context.Context, selected any, runID, eventID string, cardinality int) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CountClosedSourceFanOutIssuanceForTest(ctx, runID, eventID, cardinality)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CountClosedSourceFanOutIssuanceForTest(ctx, runID, eventID, cardinality)
	default:
		return 0, fmt.Errorf("closed-source fan-out issuance requires the original native owner")
	}
}

type DeliveryAttemptDiagnosticRow = delivery.DeliveryAttemptDiagnosticRow

type EventDeliveryDiagnosticRow = delivery.EventDeliveryDiagnosticRow

func ReadEventDeliveryDiagnosticRowsForTest(ctx context.Context, selected any, eventID string) ([]EventDeliveryDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadEventDeliveryDiagnosticRowsForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadEventDeliveryDiagnosticRowsForTest(ctx, eventID)
	default:
		return nil, fmt.Errorf("delivery diagnostics require the original native owner")
	}
}

func ReadSubscriberEventNamesCreatedSinceForTest(ctx context.Context, selected any, since time.Time, subscriberID string) ([]string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadSubscriberEventNamesCreatedSinceForTest(ctx, since, subscriberID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadSubscriberEventNamesCreatedSinceForTest(ctx, since, subscriberID)
	default:
		return nil, fmt.Errorf("subscriber event observation requires the original native owner")
	}
}

func ReadCausalDeliveryStatusCountsForTest(ctx context.Context, selected any, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadCausalDeliveryStatusCountsForTest(ctx, runID, eventID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadCausalDeliveryStatusCountsForTest(ctx, runID, eventID)
	default:
		return nil, fmt.Errorf("causal delivery counts require the original native owner")
	}
}

func ReadNonLogRunDeliveryClaimTotalsForTest(ctx context.Context, selected any, runID string) (deliveries, claims int64, err error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadNonLogRunDeliveryClaimTotalsForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadNonLogRunDeliveryClaimTotalsForTest(ctx, runID)
	default:
		return 0, 0, fmt.Errorf("delivery claim totals require the original native owner")
	}
}

func ReadDeliveryAttemptDiagnosticRowsForTest(ctx context.Context, selected any, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadDeliveryAttemptDiagnosticRowsForTest(ctx, deliveryID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadDeliveryAttemptDiagnosticRowsForTest(ctx, deliveryID)
	default:
		return nil, fmt.Errorf("delivery attempt diagnostics require the original native owner")
	}
}

type EventPipelineReceiptObservationRow = pipelinepersistence.EventPipelineReceiptObservationRow

type SourceFanOutIntentDiagnosticRow = pipelinepersistence.SourceFanOutIntentDiagnosticRow

func ReadSourceFanOutIntentDiagnosticRowsForTest(ctx context.Context, selected any, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadSourceFanOutIntentDiagnosticRowsForTest(ctx, runID, eventID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadSourceFanOutIntentDiagnosticRowsForTest(ctx, runID, eventID)
	default:
		return nil, fmt.Errorf("source fan-out intent observation requires the original native owner")
	}
}

func ReadEarliestEventPipelineReceiptRowsForTest(ctx context.Context, selected any) ([]EventPipelineReceiptObservationRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadEarliestEventPipelineReceiptRowsForTest(ctx)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadEarliestEventPipelineReceiptRowsForTest(ctx)
	default:
		return nil, fmt.Errorf("pipeline receipt observation requires the original native owner")
	}
}

func ReadLatestPlatformPipelineReceiptStorageForTest(ctx context.Context, selected any, eventID string) (LatestPipelineReceiptStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadLatestPlatformPipelineReceiptStorageForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadLatestPlatformPipelineReceiptStorageForTest(ctx, eventID)
	default:
		return LatestPipelineReceiptStorage{}, fmt.Errorf("platform pipeline receipt requires the original native owner")
	}
}

func CountDeadLettersForOriginalEventForTest(ctx context.Context, selected any, originalEventID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.CountDeadLettersForOriginalEventForTest(ctx, originalEventID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.CountDeadLettersForOriginalEventForTest(ctx, originalEventID)
	default:
		return 0, fmt.Errorf("dead-letter count requires the original native owner")
	}
}

type DeadLetterObservationRow = delivery.DeadLetterObservationRow

type TargetFailureDeadLetterStorage = delivery.TargetFailureDeadLetterStorage

func ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, selected any, eventID string) (TargetFailureDeadLetterStorage, error) {
	if err := validateSelectedForkStorageIdentity(eventID); err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadTargetFailureDeadLetterStorageForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadTargetFailureDeadLetterStorageForTest(ctx, eventID)
	default:
		return TargetFailureDeadLetterStorage{}, fmt.Errorf("target failure dead letter requires the original native owner")
	}
}

type ChainDepthDeadLetterStorage = delivery.ChainDepthDeadLetterStorage

func ReadChainDepthDeadLetterStorageForTest(ctx context.Context, selected any, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadChainDepthDeadLetterStorageForTest(ctx, runID, entityID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadChainDepthDeadLetterStorageForTest(ctx, runID, entityID)
	default:
		return ChainDepthDeadLetterStorage{}, fmt.Errorf("chain-depth dead letter requires the original native owner")
	}
}

func CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, selected any, since time.Time, entityID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.CountDeadLetterEntityRelationsSinceForTest(ctx, since, entityID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.CountDeadLetterEntityRelationsSinceForTest(ctx, since, entityID)
	default:
		return 0, fmt.Errorf("dead-letter relation count requires the original native owner")
	}
}

func ReadDeadLetterObservationRowsForTest(ctx context.Context, selected any) ([]DeadLetterObservationRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadDeadLetterObservationRowsForTest(ctx)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadDeadLetterObservationRowsForTest(ctx)
	default:
		return nil, fmt.Errorf("dead-letter observation requires the original native owner")
	}
}

// Transferred from B70ab4d333 /888cb4958 under6010346696. The three OR arms
// are one exact handoff predicate, not a delivery-active substitute.
func ReadServedIncompletePipelineHandoffCountForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadServedIncompletePipelineHandoffCountForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadServedIncompletePipelineHandoffCountForTest(ctx, runID)
	default:
		return 0, fmt.Errorf("pipeline handoff observation requires the original native owner")
	}
}

func ReadServedDeliveryStatusCountForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return 0, err
	}
	var count int
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.ReadServedDeliveryStatusCount(ctx, tx, string(dialect) == "postgres", eventID, subscriberType, subscriberID, statuses...)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type SelectedDeploymentForkDiagnosticCell struct {
	Text    string
	Present bool
}

type SelectedDeploymentForkDiagnosticSection struct {
	Name    string
	Columns []string
	Rows    [][]SelectedDeploymentForkDiagnosticCell
	Failure string
}

type SelectedDeploymentResourceWaitStorage struct {
	Intents, Cursor, Outcomes, Events, Deliveries, ReceiverInstances int
	Status, IntentBundle, Origin                                     string
	SourceKind, FeedID, Blocked, Claimant, Retry                     SelectedDeploymentForkDiagnosticCell
}

type SelectedDeploymentResourceWakeStorage struct {
	Events, Grants        int
	RunStatus, BundleHash string
}

func ReadSelectedDeploymentResourceEventForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return "", err
	}
	var eventID string
	found := false
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='root.ready' ORDER BY event_id LIMIT 1`, runID).Scan(&eventID)
		if err == sql.ErrNoRows {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", sql.ErrNoRows
	}
	return eventID, nil
}

func ReadSelectedDeploymentResourceWaitForTest(ctx context.Context, selected any, runID string) (SelectedDeploymentResourceWaitStorage, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	var out SelectedDeploymentResourceWaitStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Intents, &out.Cursor); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&out.Outcomes); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&out.Events); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&out.Deliveries); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, runID).Scan(&out.ReceiverInstances); err != nil {
			return err
		}
		var sourceKind, feedID, blocked, claimant, retry sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash,origin_kind,source_kind,deployment_feed_id,blocked_reason,claim_owner,retry_ready_at FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Status, &out.IntentBundle, &out.Origin, &sourceKind, &feedID, &blocked, &claimant, &retry); err != nil {
			return err
		}
		out.SourceKind = SelectedDeploymentForkDiagnosticCell{Text: sourceKind.String, Present: sourceKind.Valid}
		out.FeedID = SelectedDeploymentForkDiagnosticCell{Text: feedID.String, Present: feedID.Valid}
		out.Blocked = SelectedDeploymentForkDiagnosticCell{Text: blocked.String, Present: blocked.Valid}
		out.Claimant = SelectedDeploymentForkDiagnosticCell{Text: claimant.String, Present: claimant.Valid}
		out.Retry = SelectedDeploymentForkDiagnosticCell{Text: retry.String, Present: retry.Valid}
		return nil
	})
	if err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	return out, nil
}

func ReadSelectedDeploymentResourceWakeForTest(ctx context.Context, selected any, runID string) (SelectedDeploymentResourceWakeStorage, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	var out SelectedDeploymentResourceWakeStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, runID).Scan(&out.Events); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&out.RunStatus, &out.BundleHash); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants WHERE bundle_hash=$1 AND state='admitted' AND selected_binding_id IS NULL`, out.BundleHash).Scan(&out.Grants)
	})
	if err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	return out, nil
}

func ReadSelectedDeploymentForkDiagnosticsForTest(ctx context.Context, selected any, runID string) ([]SelectedDeploymentForkDiagnosticSection, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var sections []SelectedDeploymentForkDiagnosticSection
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, query := range []struct{ name, sql string }{
			{"run_control", `SELECT r.status,c.control_status FROM runs r JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`},
			{"feeds", `SELECT CAST(deployment_feed_id AS TEXT),status,cursor,cardinality,CAST(claim_owner AS TEXT),claim_generation,CAST(lease_expires_at AS TEXT),CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(blocked_reason AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY deployment_feed_id`},
			{"executions", `SELECT CAST(execution_id AS TEXT),generation,state,CAST(lease_expires_at AS TEXT),CAST(failure AS TEXT) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation`},
			{"outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`},
			{"events", `SELECT COUNT(*) FROM events WHERE run_id=$1`},
			{"pipeline_receipts", `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'`},
			{"deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`},
			{"pending_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`},
			{"failed_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('failed','dead_letter')`},
			{"stamped_handoffs", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND continuation_handoff_at IS NOT NULL`},
			{"receiver_instances", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`},
			{"receipts", `SELECT CAST(r.event_id AS TEXT),r.subscriber_type,r.subscriber_id,r.outcome,CAST(r.reason_code AS TEXT),CAST(r.failure AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 ORDER BY e.insertion_sequence,r.subscriber_type,r.subscriber_id`},
			{"delivery_rows", `SELECT CAST(delivery_id AS TEXT),CAST(event_id AS TEXT),subscriber_type,subscriber_id,status,retry_count,claim_version,CAST(next_eligible_at AS TEXT),CAST(continuation_handoff_at AS TEXT),CAST(reason_code AS TEXT),CAST(failure AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY created_at,delivery_id`},
			{"attempts", `SELECT CAST(a.delivery_id AS TEXT),a.claim_version,a.closure_kind,CAST(a.outcome AS TEXT),CAST(a.reason_code AS TEXT),CAST(a.failure AS TEXT) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 ORDER BY a.delivery_id,a.claim_version`},
			{"runtime_logs", `SELECT CAST(event_id AS TEXT),CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='platform.runtime_log' ORDER BY insertion_sequence DESC LIMIT 20`},
		} {
			rows, err := tx.QueryContext(ctx, query.sql, runID)
			if err != nil {
				return fmt.Errorf("read selected fork %s: %w", query.name, err)
			}
			columns, err := rows.Columns()
			if err != nil {
				return errors.Join(err, rows.Close())
			}
			section := SelectedDeploymentForkDiagnosticSection{Name: query.name, Columns: columns}
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				scan := make([]any, len(values))
				for i := range values {
					scan[i] = &values[i]
				}
				if err := rows.Scan(scan...); err != nil {
					return errors.Join(err, rows.Close())
				}
				cells := make([]SelectedDeploymentForkDiagnosticCell, len(values))
				for i, value := range values {
					cells[i] = SelectedDeploymentForkDiagnosticCell{Text: value.String, Present: value.Valid}
				}
				section.Rows = append(section.Rows, cells)
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return fmt.Errorf("read selected fork %s rows: %w", query.name, err)
			}
			if query.name == "run_control" && len(section.Rows) != 1 {
				section.Failure = sql.ErrNoRows.Error()
			}
			sections = append(sections, section)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sections, nil
}

func ReadServedRunDebugSummaryForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return "", err
	}
	sections := make([]string, 0, 10)
	for _, scope := range []string{"runs", "entity_state", "flow_instances", "events", "event_deliveries", "settled_delivery_attempts", "event_receipts", "dead_letters", "delivery_agents", "runtime_logs"} {
		section := readServedRunDebugSection(ctx, selected, string(dialect), scope, runID)
		if err := ctx.Err(); err != nil {
			return "", err
		}
		sections = append(sections, section)
	}
	return strings.Join(sections, "\n"), nil
}

func readServedRunDebugSection(ctx context.Context, selected any, backend, scope, runID string) string {
	if scope == "runs" || scope == "entity_state" || scope == "event_deliveries" || scope == "settled_delivery_attempts" || scope == "delivery_agents" {
		out, stage := []string{}, ""
		err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if scope == "runs" {
				switch owner := selected.(type) {
				case *PostgresStore:
					out, stage, err = owner.runLifecyclePostgresOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				case *SQLiteRuntimeStore:
					out, stage, err = owner.runLifecycleSQLiteOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				}
			} else if scope == "entity_state" {
				out, err = readServedEntityDebugStorageTx(ctx, tx, runID)
			} else {
				out, stage, err = delivery.ReadRunDeliveryDebugSection(ctx, tx, backend == "postgres", scope, runID)
			}
			return err
		})
		if err != nil {
			return fmt.Sprintf("%s%s: %v", scope, stage, err)
		}
		if len(out) == 0 {
			return scope + ": []"
		}
		return scope + ": " + strings.Join(out, "; ")
	}
	sqlText := servedRunDebugSQL(backend, scope)
	if sqlText == "" {
		return scope + ": unsupported debug query"
	}
	out := []string{}
	stage := ""
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, sqlText, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			stage = " columns"
			return err
		}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			scan := make([]any, len(values))
			for i := range values {
				scan[i] = &values[i]
			}
			if err := rows.Scan(scan...); err != nil {
				stage = " scan"
				return err
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
			stage = " rows"
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("%s%s: %v", scope, stage, err)
	}
	if len(out) == 0 {
		return scope + ": []"
	}
	return scope + ": " + strings.Join(out, "; ")
}

func readServedEntityDebugStorageTx(ctx context.Context, tx *sql.Tx, runID string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+` WHERE es.run_id=$1 ORDER BY es.created_at, es.entity_id LIMIT 5`, runID)
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
		data, err := workflowheader.ScanData(rows, sqliteTimeValue)
		if err != nil {
			return nil, err
		}
		var stage any
		if data.Entity.CurrentState != nil {
			stage = *data.Entity.CurrentState
		}
		out = append(out, fmt.Sprint([]any{data.Entity.EntityID, data.Entity.FlowInstance, stage}))
	}
	return out, rows.Err()
}

func servedRunDebugSQL(backend, scope string) string {
	sqlText := ""
	switch backend {
	case "postgres":
		switch scope {
		case "flow_instances":
			sqlText = `SELECT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi WHERE fi.run_id = $1::uuid ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), COALESCE(d.failure->>'class', ''), COALESCE(d.failure->'detail'->>'code', ''), COALESCE(d.failure->'detail'->'attributes'->>'validation_error', '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	case "sqlite":
		switch scope {
		case "flow_instances":
			sqlText = `SELECT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi WHERE fi.run_id = ? ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), COALESCE(json_extract(d.failure, '$.class'), ''), COALESCE(json_extract(d.failure, '$.detail.code'), ''), COALESCE(json_extract(d.failure, '$.detail.attributes.validation_error'), '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	}
	return sqlText
}
