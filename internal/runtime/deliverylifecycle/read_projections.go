package deliverylifecycle

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	postgresAgentPendingEligibility = `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= CURRENT_TIMESTAMP)
		OR d.status = 'in_progress'
	)`
	sqliteAgentPendingEligibility = `(
		d.status = 'pending'
		OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= CURRENT_TIMESTAMP)
		OR d.status = 'in_progress'
	)`
)

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
	if a.dialect == DialectPostgres {
		args = []any{page.RunID, page.Since.UTC()}
		where := []string{
			"d.run_id = $1::uuid",
			"run.status IN ('running', 'paused')",
			"d.status = 'pending'",
			"d.created_at >= $2::timestamptz",
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
		args = []any{page.RunID, page.Since.UTC()}
		where := []string{
			"d.run_id = ?",
			"run.status IN ('running', 'paused')",
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
func (a *Adapter) AgentPendingAggregates(ctx context.Context, q queryer, agentIDs []string, since time.Time) ([]AgentPendingAggregate, error) {
	agentIDs = normalizedNonEmptyStrings(agentIDs)
	if len(agentIDs) == 0 {
		return []AgentPendingAggregate{}, nil
	}
	if since.IsZero() {
		since = time.Unix(0, 0).UTC()
	}
	var (
		query string
		args  []any
	)
	if a.dialect == DialectPostgres {
		placeholders := make([]string, 0, len(agentIDs))
		for _, agentID := range agentIDs {
			args = append(args, agentID)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		args = append(args, since.UTC())
		query = fmt.Sprintf(`
			SELECT d.subscriber_id, COUNT(*), MIN(e.created_at)
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE d.subscriber_type = 'agent'
			  AND d.subscriber_id IN (%s)
			  AND e.created_at >= $%d::timestamptz
			  AND (e.run_id IS NULL OR r.status IN ('running', 'paused'))
			  AND %s
			GROUP BY d.subscriber_id
			ORDER BY d.subscriber_id`, strings.Join(placeholders, ","), len(args), postgresAgentPendingEligibility)
	} else {
		placeholders := make([]string, 0, len(agentIDs))
		for _, agentID := range agentIDs {
			args = append(args, agentID)
			placeholders = append(placeholders, "?")
		}
		args = append(args, since.UTC())
		query = fmt.Sprintf(`
			SELECT d.subscriber_id, COUNT(*), MIN(e.created_at)
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE d.subscriber_type = 'agent'
			  AND d.subscriber_id IN (%s)
			  AND e.created_at >= ?
			  AND (e.run_id IS NULL OR r.status IN ('running', 'paused'))
			  AND %s
			GROUP BY d.subscriber_id
			ORDER BY d.subscriber_id`, strings.Join(placeholders, ","), sqliteAgentPendingEligibility)
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select agent pending aggregates: %w", err)
	}
	defer rows.Close()
	out := make([]AgentPendingAggregate, 0, len(agentIDs))
	for rows.Next() {
		var (
			item      AgentPendingAggregate
			oldestRaw any
		)
		if err := rows.Scan(&item.AgentID, &item.Count, &oldestRaw); err != nil {
			return nil, fmt.Errorf("scan agent pending aggregate: %w", err)
		}
		item.AgentID = strings.TrimSpace(item.AgentID)
		oldest, ok, err := parseNullableTime(oldestRaw)
		if err != nil {
			return nil, err
		}
		if item.AgentID == "" || item.Count <= 0 || !ok {
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
func (a *Adapter) AgentPendingReferencePage(ctx context.Context, q queryer, page AgentPendingPageQuery) (AgentPendingReferencePage, error) {
	page.AgentID = strings.TrimSpace(page.AgentID)
	if page.AgentID == "" {
		return AgentPendingReferencePage{}, fmt.Errorf("agent pending page agent id is required")
	}
	if page.Limit <= 0 {
		return AgentPendingReferencePage{}, fmt.Errorf("agent pending page limit must be positive")
	}
	if page.Since.IsZero() {
		page.Since = time.Unix(0, 0).UTC()
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
	var (
		query string
		args  []any
	)
	if a.dialect == DialectPostgres {
		args = []any{page.AgentID, page.Since.UTC()}
		where := []string{
			"d.subscriber_type = 'agent'",
			"d.subscriber_id = $1",
			"e.created_at >= $2::timestamptz",
			"(e.run_id IS NULL OR r.status IN ('running', 'paused'))",
			postgresAgentPendingEligibility,
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
		args = []any{page.AgentID, page.Since.UTC()}
		where := []string{
			"d.subscriber_type = 'agent'",
			"d.subscriber_id = ?",
			"e.created_at >= ?",
			"(e.run_id IS NULL OR r.status IN ('running', 'paused'))",
			sqliteAgentPendingEligibility,
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
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return AgentPendingReferencePage{}, err
	}
	result.References = make([]AgentPendingReference, 0, len(raw))
	for _, reference := range raw {
		record, err := a.loadByID(ctx, q, reference.deliveryID, false)
		if err != nil {
			return AgentPendingReferencePage{}, err
		}
		snapshot := snapshotAt(record, now)
		if snapshot.DeliveryID != reference.deliveryID || snapshot.EventID != reference.eventID ||
			snapshot.SubscriberClass != SubscriberAgent || snapshot.SubscriberID != page.AgentID ||
			!agentPendingSnapshotEligible(snapshot, now) {
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
func (a *Adapter) CurrentAgentSnapshots(ctx context.Context, q queryer, agentIDs []string) ([]Snapshot, error) {
	agentIDs = normalizedNonEmptyStrings(agentIDs)
	if len(agentIDs) == 0 {
		return []Snapshot{}, nil
	}
	var (
		query        string
		args         []any
		placeholders = make([]string, 0, len(agentIDs))
	)
	if a.dialect == DialectPostgres {
		for _, agentID := range agentIDs {
			args = append(args, agentID)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		query = fmt.Sprintf(`
			WITH ranked AS (
				SELECT d.delivery_id,
					ROW_NUMBER() OVER (
						PARTITION BY d.subscriber_id
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
				  AND d.subscriber_id IN (%s)
				  AND d.status IN ('pending', 'in_progress', 'failed', 'dead_letter')
			)
			SELECT delivery_id::text
			FROM ranked
			WHERE row_number = 1
			ORDER BY delivery_id`, strings.Join(placeholders, ","))
	} else {
		for _, agentID := range agentIDs {
			args = append(args, agentID)
			placeholders = append(placeholders, "?")
		}
		query = fmt.Sprintf(`
			WITH ranked AS (
				SELECT d.delivery_id,
					ROW_NUMBER() OVER (
						PARTITION BY d.subscriber_id
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
				  AND d.subscriber_id IN (%s)
				  AND d.status IN ('pending', 'in_progress', 'failed', 'dead_letter')
			)
			SELECT delivery_id
			FROM ranked
			WHERE row_number = 1
			ORDER BY delivery_id`, strings.Join(placeholders, ","))
	}
	snapshots, err := a.snapshotsByIDQuery(ctx, q, query, args...)
	if err != nil {
		return nil, err
	}
	agents := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agents[agentID] = struct{}{}
	}
	for _, snapshot := range snapshots {
		_, requested := agents[snapshot.SubscriberID]
		if !requested || snapshot.SubscriberClass != SubscriberAgent || snapshot.Status == StatusDelivered {
			return nil, fmt.Errorf("%w: current agent lifecycle reference changed during hydration", ErrConflict)
		}
	}
	return snapshots, nil
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
			OR (d.started_at IS NOT NULL AND d.status NOT IN ('delivered', 'dead_letter'))
		)`
		join = `LEFT JOIN event_delivery_attempts a
			ON a.delivery_id = d.delivery_id
		   AND a.claim_version = d.current_attempt_version
		   AND a.open_marker = TRUE`
		if a.dialect == DialectSQLite {
			predicate = `(
				d.status = 'in_progress'
				OR COALESCE(a.active_session_id, '') <> ''
				OR (d.started_at IS NOT NULL AND d.status NOT IN ('delivered', 'dead_letter'))
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
