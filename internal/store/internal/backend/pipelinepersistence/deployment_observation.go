package pipelinepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeploymentIntentProgressStorage struct{ Intents, Cursor int }

func CountAccountDeploymentReceiversStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`, runID).Scan(&count)
	return count, err
}

func CountConsumerDeploymentReceiversStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='consumer'`, runID).Scan(&count)
	return count, err
}

func CountDeploymentRouteObligationsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_card_route_obligations r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1`, runID).Scan(&count)
	return count, err
}

type DeploymentIntentMetadataStorage struct {
	Status, BundleHash, Origin                   string
	SourceKind, FeedID, Blocked, Claimant, Retry delivery.WorkloadNullableText
}
type DeploymentFeedStorage struct {
	FeedID, Status                                  string
	Cursor, Cardinality, Generation                 int64
	Claimant, Lease, RetryAt, RetryFailure, Blocked delivery.WorkloadNullableText
}
type DeploymentIntentDiagnosticStorage struct {
	Status              string
	Cardinality, Cursor int
}

func ReadDeploymentIntentVersionStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&version)
	return version, err
}

func ReadDeploymentIntentCursorStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var cursor int
	err := tx.QueryRowContext(ctx, `SELECT cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&cursor)
	return cursor, err
}

func ReadDeploymentOriginIntentVersionStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&version)
	return version, err
}

func CountDeploymentOriginIntentsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&count)
	return count, err
}

type DeploymentIntentCardinalityStorage struct{ Cursor, Cardinality int }

type DeploymentIntentStatusStorage struct {
	Cardinality int
	Status      string
}

func ReadDeploymentIntentStatusStorageTx(ctx context.Context, tx *sql.Tx, run string) (DeploymentIntentStatusStorage, error) {
	var out DeploymentIntentStatusStorage
	err := tx.QueryRowContext(ctx, `SELECT cardinality,status FROM fan_out_intents WHERE run_id=$1`, run).Scan(&out.Cardinality, &out.Status)
	return out, err
}

func ReadDeploymentIntentCardinalityStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentIntentCardinalityStorage, error) {
	var out DeploymentIntentCardinalityStorage
	err := tx.QueryRowContext(ctx, `SELECT cursor,cardinality FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&out.Cursor, &out.Cardinality)
	return out, err
}

func ReadDeploymentOriginIntentStateStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentIntentDiagnosticStorage, error) {
	var out DeploymentIntentDiagnosticStorage
	err := tx.QueryRowContext(ctx, `SELECT status,cardinality,cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&out.Status, &out.Cardinality, &out.Cursor)
	return out, err
}

func ReadDeploymentCommittedOrdinalsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]delivery.CommittedFanOutOrdinalStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,CAST(event_id AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed' ORDER BY ordinal`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []delivery.CommittedFanOutOrdinalStorage
	for rows.Next() {
		var row delivery.CommittedFanOutOrdinalStorage
		if err := rows.Scan(&row.Ordinal, &row.ID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type PinnedDeploymentFeedStorage struct {
	Declaration, Version, Status string
	Cardinality, Cursor          int
}

func ReadPinnedDeploymentFeedsStorageTx(ctx context.Context, tx *sql.Tx, run string) ([]PinnedDeploymentFeedStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT source_resource_event_name,source_resource_version_id,status,cardinality,cursor
		FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY source_resource_event_name`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PinnedDeploymentFeedStorage
	for rows.Next() {
		var row PinnedDeploymentFeedStorage
		if err := rows.Scan(&row.Declaration, &row.Version, &row.Status, &row.Cardinality, &row.Cursor); err != nil {
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

func CountPinnedPortfolioSuccessfulPipelineReceiptsStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name IN ('portfolio/account.registered','portfolio/account.notify.requested') AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, run).Scan(&out)
	return out, err
}

type DeploymentSettlementDiagnosticStorage struct{ EventID, EventName, Outcome string }

func ReadDeploymentSettlementDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, run string) ([]DeploymentSettlementDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.event_id,e.event_name,COALESCE(r.outcome,'missing')
		FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id
		AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
		WHERE e.run_id=$1 ORDER BY e.created_at`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentSettlementDiagnosticStorage
	for rows.Next() {
		var row DeploymentSettlementDiagnosticStorage
		if err := rows.Scan(&row.EventID, &row.EventName, &row.Outcome); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeploymentIntentProgressStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentIntentProgressStorage, error) {
	var out DeploymentIntentProgressStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Intents, &out.Cursor)
	return out, err
}

func ReadDeploymentIntentMetadataStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentIntentMetadataStorage, error) {
	var out DeploymentIntentMetadataStorage
	var source, feed, blocked, claimant, retry sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash,origin_kind,source_kind,deployment_feed_id,blocked_reason,claim_owner,retry_ready_at FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Status, &out.BundleHash, &out.Origin, &source, &feed, &blocked, &claimant, &retry)
	out.SourceKind, out.FeedID = delivery.WorkloadNullableText(source), delivery.WorkloadNullableText(feed)
	out.Blocked, out.Claimant, out.Retry = delivery.WorkloadNullableText(blocked), delivery.WorkloadNullableText(claimant), delivery.WorkloadNullableText(retry)
	return out, err
}

func ReadDeploymentFeedStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentFeedStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(deployment_feed_id AS TEXT),status,cursor,cardinality,CAST(claim_owner AS TEXT),claim_generation,CAST(lease_expires_at AS TEXT),CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(blocked_reason AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY deployment_feed_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentFeedStorage
	for rows.Next() {
		var row DeploymentFeedStorage
		var claimant, lease, retryAt, retryFailure, blocked sql.NullString
		if err := rows.Scan(&row.FeedID, &row.Status, &row.Cursor, &row.Cardinality, &claimant, &row.Generation, &lease, &retryAt, &retryFailure, &blocked); err != nil {
			return nil, err
		}
		row.Claimant, row.Lease, row.RetryAt = delivery.WorkloadNullableText(claimant), delivery.WorkloadNullableText(lease), delivery.WorkloadNullableText(retryAt)
		row.RetryFailure, row.Blocked = delivery.WorkloadNullableText(retryFailure), delivery.WorkloadNullableText(blocked)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeploymentIntentDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentIntentDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT status,cardinality,cursor FROM fan_out_intents WHERE run_id=$1 ORDER BY deployment_feed_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentIntentDiagnosticStorage
	for rows.Next() {
		var row DeploymentIntentDiagnosticStorage
		if err := rows.Scan(&row.Status, &row.Cardinality, &row.Cursor); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func CountDeploymentCommittedOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, runID).Scan(&out)
	return out, err
}

func CountDeploymentPipelineReceiptsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'`, runID).Scan(&out)
	return out, err
}

func CountRootReadySuccessfulPipelineReceiptsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name='root.ready' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, runID).Scan(&out)
	return out, err
}

type DeploymentReceiptStorage struct {
	EventID, SubscriberType, SubscriberID, Outcome string
	Reason, Failure                                delivery.WorkloadNullableText
}

func ReadDeploymentReceiptStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentReceiptStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(r.event_id AS TEXT),r.subscriber_type,r.subscriber_id,r.outcome,CAST(r.reason_code AS TEXT),CAST(r.failure AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 ORDER BY e.insertion_sequence,r.subscriber_type,r.subscriber_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentReceiptStorage
	for rows.Next() {
		var row DeploymentReceiptStorage
		var reason, failure sql.NullString
		if err := rows.Scan(&row.EventID, &row.SubscriberType, &row.SubscriberID, &row.Outcome, &reason, &failure); err != nil {
			return nil, err
		}
		row.Reason, row.Failure = delivery.WorkloadNullableText(reason), delivery.WorkloadNullableText(failure)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
