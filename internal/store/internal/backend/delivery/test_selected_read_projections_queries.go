package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func readPreparedTargetConflictTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
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

func readExactAgentDeliveryStatusTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID, agentID string) (string, error) {
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

func readStandaloneAgentDeliveryStorageTx(ctx context.Context, tx *sql.Tx, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
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

func readSubscriberEventNamesCreatedSince(ctx context.Context, q queryer, since time.Time, subscriberID string) ([]string, error) {
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

func readCausalDeliveryFrontier(ctx context.Context, q queryer, runID, eventID string) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	if err := q.QueryRowContext(ctx, causalDeliveryFrontierQuery, runID, eventID).Scan(&out.Observed, &out.Unsettled, &out.DeadLetters); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func probeCausalDeliveryFrontier(ctx context.Context, q queryer, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
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

func readCausalDeliveryStatusCounts(ctx context.Context, q queryer, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
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

func readNonLogRunDeliveryClaimTotals(ctx context.Context, q queryer, runID string) (deliveries, claims int64, err error) {
	err = q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(d.claim_version),0)
FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
WHERE e.run_id=$1 AND e.event_name<>'platform.runtime_log'`, runID).Scan(&deliveries, &claims)
	if err != nil {
		return 0, 0, err
	}
	return deliveries, claims, nil
}

func readEventDeliveryDiagnosticRows(ctx context.Context, q queryer, eventID string, postgres bool) ([]EventDeliveryDiagnosticRow, error) {
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

func readDeliveryAttemptDiagnosticRows(ctx context.Context, q queryer, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
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
