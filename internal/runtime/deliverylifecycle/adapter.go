package deliverylifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

// Adapter contains the private SQL mechanics for the executable-delivery
// owner. Runtime consumers receive Store, never Adapter or a transaction.
type Adapter struct {
	dialect Dialect
}

func NewAdapter(dialect Dialect) (*Adapter, error) {
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return nil, fmt.Errorf("delivery lifecycle dialect %q is unsupported", dialect)
	}
	return &Adapter{dialect: dialect}, nil
}

type scanner interface {
	Scan(...any) error
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type deliveryRecord struct {
	Snapshot
	claimToken string
	eventType  string
	entityID   string
	flowID     string
	bundleHash string
}

func (a *Adapter) CommitInitial(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute) ([]DurableHandoffProof, error) {
	if tx == nil {
		return nil, fmt.Errorf("delivery initial commit transaction is required")
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return nil, err
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	proofs := make([]DurableHandoffProof, 0, len(routes))
	for _, route := range routes {
		obligation, err := NewObligation(eventID, runID, route)
		if err != nil {
			return nil, err
		}
		proof, err := a.insertExactObligation(ctx, tx, obligation)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, proof)
	}
	return proofs, nil
}

func (a *Adapter) insertExactObligation(ctx context.Context, tx *sql.Tx, obligation Obligation) (DurableHandoffProof, error) {
	target, deliveryContext, projection, err := encodeRoute(obligation.Route())
	if err != nil {
		return DurableHandoffProof{}, err
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return DurableHandoffProof{}, err
	}
	query := `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
			delivery_target_route, delivery_context, delivery_payload_projection,
			status, retry_count, max_retries, next_eligible_at, claim_version,
			created_at, updated_at
		) VALUES (
			$1::uuid, NULLIF($2, '')::uuid, $3::uuid, $4, $5, $6,
			$7::jsonb, $8::jsonb, $9::jsonb,
			'pending', 0, $10, $11, 0, $11, $11
		) ON CONFLICT (event_id, route_identity) DO NOTHING`
	args := []any{obligation.DeliveryID(), obligation.RunID(), obligation.EventID(), obligation.RouteIdentity().String(), string(obligation.SubscriberClass()), obligation.SubscriberID(), string(target), string(deliveryContext), string(projection), obligation.MaxRetries(), now}
	if a.dialect == DialectSQLite {
		query = `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
				delivery_target_route, delivery_context, delivery_payload_projection,
				status, retry_count, max_retries, next_eligible_at, claim_version,
				created_at, updated_at
			) VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, 0, ?, ?)
			ON CONFLICT(event_id, route_identity) DO NOTHING`
		args = []any{obligation.DeliveryID(), obligation.RunID(), obligation.EventID(), obligation.RouteIdentity().String(), string(obligation.SubscriberClass()), obligation.SubscriberID(), string(target), string(deliveryContext), string(projection), obligation.MaxRetries(), now, now, now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return DurableHandoffProof{}, fmt.Errorf("commit delivery obligation: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return DurableHandoffProof{}, fmt.Errorf("commit delivery obligation: read affected rows: %w", err)
	}
	record, err := a.loadByEventAndRoute(ctx, tx, obligation.EventID(), obligation.RouteIdentity(), true)
	if err != nil {
		return DurableHandoffProof{}, err
	}
	if record.DeliveryID != obligation.DeliveryID() || record.RunID != obligation.RunID() ||
		record.SubscriberClass != obligation.SubscriberClass() || record.SubscriberID != obligation.SubscriberID() ||
		record.MaxRetries != obligation.MaxRetries() || !events.SameDeliveryRouteIdentity(record.Route, obligation.Route()) {
		return DurableHandoffProof{}, fmt.Errorf("%w: delivery obligation duplicate does not exactly match admitted route", ErrConflict)
	}
	if inserted == 0 && (record.Status != StatusPending || record.RetryCount != 0 || record.ClaimVersion != 0) {
		return DurableHandoffProof{}, fmt.Errorf("%w: delivery obligation replay conflicts with existing lifecycle", ErrConflict)
	}
	return DurableHandoffProof{deliveryID: obligation.DeliveryID(), eventID: obligation.EventID(), routeIdentity: obligation.RouteIdentity().String()}, nil
}

func (a *Adapter) ClaimExact(ctx context.Context, tx *sql.Tx, event events.Event, route events.DeliveryRoute, leaseTTL time.Duration) (ClaimedObligation, error) {
	if tx == nil {
		return ClaimedObligation{}, fmt.Errorf("delivery claim transaction is required")
	}
	identity, err := route.Identity()
	if err != nil {
		return ClaimedObligation{}, err
	}
	record, err := a.loadByEventAndRoute(ctx, tx, event.ID(), identity, true)
	if err != nil {
		return ClaimedObligation{}, err
	}
	if !events.SameDeliveryRouteIdentity(record.Route, route) {
		return ClaimedObligation{}, fmt.Errorf("%w: delivery route does not exactly match durable obligation", ErrConflict)
	}
	return a.claimLocked(ctx, tx, record, leaseTTL)
}

func (a *Adapter) ClaimPendingAgent(ctx context.Context, tx *sql.Tx, agentID string, limit int, leaseTTL time.Duration) ([]ClaimedObligation, error) {
	candidates, err := a.AgentClaimCandidates(ctx, tx, agentID, limit)
	if err != nil {
		return nil, err
	}
	return a.ClaimCandidates(ctx, tx, candidates, leaseTTL)
}

func (a *Adapter) ClaimPendingNode(ctx context.Context, tx *sql.Tx, nodeID string, limit int, leaseTTL time.Duration) ([]ClaimedObligation, error) {
	candidates, err := a.NodeClaimCandidates(ctx, tx, nodeID, limit)
	if err != nil {
		return nil, err
	}
	return a.ClaimCandidates(ctx, tx, candidates, leaseTTL)
}

func (a *Adapter) AgentClaimCandidates(ctx context.Context, q queryer, agentID string, limit int) ([]ClaimCandidate, error) {
	return a.claimCandidates(ctx, q, SubscriberAgent, agentID, limit)
}

func (a *Adapter) NodeClaimCandidates(ctx context.Context, q queryer, nodeID string, limit int) ([]ClaimCandidate, error) {
	return a.claimCandidates(ctx, q, SubscriberNode, nodeID, limit)
}

func (a *Adapter) claimCandidates(ctx context.Context, q queryer, class SubscriberClass, subscriberID string, limit int) ([]ClaimCandidate, error) {
	if q == nil {
		return nil, fmt.Errorf("delivery claim candidate queryer is required")
	}
	if class != SubscriberAgent && class != SubscriberNode {
		return nil, fmt.Errorf("delivery subscriber class %q cannot claim pending work", class)
	}
	subscriberID = strings.TrimSpace(subscriberID)
	if subscriberID == "" {
		return nil, fmt.Errorf("delivery subscriber id is required")
	}
	if limit <= 0 || limit > 500 {
		return nil, fmt.Errorf("delivery claim limit must be between 1 and 500")
	}
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return nil, err
	}
	query := `
		SELECT d.delivery_id::text, d.run_id::text
		FROM event_deliveries d
		LEFT JOIN event_delivery_attempts current_attempt
		  ON current_attempt.delivery_id = d.delivery_id
		 AND current_attempt.claim_version = d.current_attempt_version
		 AND current_attempt.open_marker = TRUE
		WHERE d.subscriber_type = $1 AND d.subscriber_id = $2
		  AND (
			d.status = 'pending'
			OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= $3)
			OR (d.status = 'in_progress' AND current_attempt.lease_expires_at <= $3)
		  )
		ORDER BY CASE
			WHEN d.status = 'in_progress' THEN current_attempt.lease_expires_at
			ELSE d.next_eligible_at
		END ASC, d.created_at ASC, d.delivery_id ASC
		LIMIT $4`
	args := []any{string(class), subscriberID, now, limit}
	if a.dialect == DialectSQLite {
		query = `
			SELECT d.delivery_id, d.run_id
			FROM event_deliveries d
			LEFT JOIN event_delivery_attempts current_attempt
			  ON current_attempt.delivery_id = d.delivery_id
			 AND current_attempt.claim_version = d.current_attempt_version
			 AND current_attempt.open_marker = TRUE
			WHERE d.subscriber_type = ? AND d.subscriber_id = ?
			  AND (
				d.status = 'pending'
				OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= ?)
				OR (d.status = 'in_progress' AND current_attempt.lease_expires_at <= ?)
			  )
			ORDER BY CASE
				WHEN d.status = 'in_progress' THEN current_attempt.lease_expires_at
				ELSE d.next_eligible_at
			END ASC, d.created_at ASC, d.delivery_id ASC
			LIMIT ?`
		args = []any{string(class), subscriberID, now, now, limit}
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select claimable %s deliveries: %w", class, err)
	}
	candidates := make([]ClaimCandidate, 0, limit)
	for rows.Next() {
		candidate := ClaimCandidate{class: class, subscriberID: subscriberID}
		if err := rows.Scan(&candidate.deliveryID, &candidate.runID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan claimable %s delivery: %w", class, err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read claimable %s deliveries: %w", class, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close claimable %s deliveries: %w", class, err)
	}
	return candidates, nil
}

func (a *Adapter) ClaimCandidates(ctx context.Context, tx *sql.Tx, candidates []ClaimCandidate, leaseTTL time.Duration) ([]ClaimedObligation, error) {
	if tx == nil {
		return nil, fmt.Errorf("delivery claim transaction is required")
	}
	out := make([]ClaimedObligation, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.deliveryID == "" || candidate.runID == "" || candidate.subscriberID == "" ||
			(candidate.class != SubscriberAgent && candidate.class != SubscriberNode) {
			return nil, fmt.Errorf("delivery claim candidate is invalid")
		}
		record, err := a.loadByID(ctx, tx, candidate.deliveryID, true)
		if err != nil {
			return nil, err
		}
		if record.RunID != candidate.runID || record.SubscriberClass != candidate.class || record.SubscriberID != candidate.subscriberID {
			return nil, fmt.Errorf("%w: delivery claim candidate identity changed", ErrConflict)
		}
		claimed, err := a.claimLocked(ctx, tx, record, leaseTTL)
		if errors.Is(err, ErrIneligible) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, claimed)
	}
	return out, nil
}

func (a *Adapter) SnapshotExact(ctx context.Context, q queryer, event events.Event, route events.DeliveryRoute) (Snapshot, error) {
	obligation, err := NewObligation(event.ID(), event.RunID(), route)
	if err != nil {
		return Snapshot{}, err
	}
	return a.Snapshot(ctx, q, obligation.DeliveryID())
}

func (a *Adapter) claimLocked(ctx context.Context, tx *sql.Tx, record deliveryRecord, leaseTTL time.Duration) (ClaimedObligation, error) {
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return ClaimedObligation{}, err
	}
	switch record.Status {
	case StatusPending:
	case StatusFailed:
		if record.RetryCount > record.MaxRetries || record.NextEligibleAt.After(now) {
			return ClaimedObligation{}, ErrIneligible
		}
	case StatusInProgress:
		if record.ClaimExpiresAt.IsZero() || record.ClaimExpiresAt.After(now) {
			return ClaimedObligation{}, ErrIneligible
		}
	default:
		return ClaimedObligation{}, ErrIneligible
	}
	token := uuid.NewString()
	version := record.ClaimVersion + 1
	expiresAt := now.Add(leaseTTL)
	if err := a.insertAttempt(ctx, tx, record.DeliveryID, version, token, now, expiresAt); err != nil {
		return ClaimedObligation{}, err
	}
	query := `
		UPDATE event_deliveries
		SET status = 'in_progress', claim_version = $1,
			current_attempt_version = $1, current_attempt_open = TRUE,
			started_at = COALESCE(started_at, $2), next_eligible_at = NULL,
			reason_code = NULL, failure = NULL, updated_at = $2
		WHERE delivery_id = $3::uuid AND claim_version = $4`
	args := []any{version, now, record.DeliveryID, record.ClaimVersion}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_deliveries
			SET status = 'in_progress', claim_version = ?,
				current_attempt_version = ?, current_attempt_open = TRUE,
				started_at = COALESCE(started_at, ?), next_eligible_at = NULL,
				reason_code = NULL, failure = NULL, updated_at = ?
			WHERE delivery_id = ? AND claim_version = ?`
		args = []any{version, version, now, now, record.DeliveryID, record.ClaimVersion}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return ClaimedObligation{}, fmt.Errorf("claim delivery obligation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ClaimedObligation{}, fmt.Errorf("%w: delivery claim lost compare-and-set", ErrConflict)
	}
	if record.Status == StatusInProgress {
		if err := a.expireAttempt(ctx, tx, record, now); err != nil {
			return ClaimedObligation{}, err
		}
	}
	claim := Claim{deliveryID: record.DeliveryID, runID: record.RunID, routeIdentity: record.RouteIdentity.String(), token: token, version: version, class: record.SubscriberClass, subscriberID: record.SubscriberID}
	claimed, err := a.loadByID(ctx, tx, record.DeliveryID, false)
	if err != nil {
		return ClaimedObligation{}, err
	}
	if err := a.recordTransition(ctx, claimed, "in_progress", nil, now); err != nil {
		return ClaimedObligation{}, err
	}
	return ClaimedObligation{Snapshot: claimed.Snapshot, Claim: claim}, nil
}

func (a *Adapter) BindAgentSession(ctx context.Context, tx *sql.Tx, claim Claim, sessionID string) (Snapshot, error) {
	if tx == nil || !claim.valid() {
		return Snapshot{}, fmt.Errorf("delivery session binding requires a current claim")
	}
	if _, err := uuid.Parse(strings.TrimSpace(sessionID)); err != nil {
		return Snapshot{}, fmt.Errorf("delivery session id: %w", err)
	}
	record, _, err := a.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return Snapshot{}, err
	}
	if record.SubscriberClass != SubscriberAgent {
		return Snapshot{}, fmt.Errorf("%w: node delivery cannot bind an agent session", ErrConflict)
	}
	query := `
		UPDATE event_delivery_attempts
		SET active_session_id = $1::uuid, session_run_id = $2::uuid, session_agent_id = $3
		WHERE delivery_id = $4::uuid AND claim_version = $5 AND claim_token = $6::uuid AND open_marker = TRUE`
	args := []any{sessionID, record.RunID, record.SubscriberID, claim.deliveryID, claim.version, claim.token}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET active_session_id = ?, session_run_id = ?, session_agent_id = ?
			WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("bind delivery agent session: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery session binding lost claim", ErrConflict)
	}
	updated, err := a.loadByID(ctx, tx, claim.deliveryID, false)
	return updated.Snapshot, err
}

func (a *Adapter) RenewClaim(ctx context.Context, tx *sql.Tx, claim Claim, leaseTTL time.Duration) (Snapshot, error) {
	if tx == nil || !claim.valid() {
		return Snapshot{}, fmt.Errorf("delivery claim renewal requires a current claim")
	}
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	_, now, err := a.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return Snapshot{}, err
	}
	expiresAt := now.Add(leaseTTL)
	query := `
		UPDATE event_delivery_attempts
		SET lease_expires_at = $1
		WHERE delivery_id = $2::uuid AND claim_version = $3 AND claim_token = $4::uuid
		  AND open_marker = TRUE AND lease_expires_at > $5`
	args := []any{expiresAt, claim.deliveryID, claim.version, claim.token, now}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET lease_expires_at = ?
			WHERE delivery_id = ? AND claim_version = ? AND claim_token = ?
			  AND open_marker = TRUE AND lease_expires_at > ?`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("renew delivery claim: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery claim renewal lost claim", ErrConflict)
	}
	deliveryQuery := `
		UPDATE event_deliveries
		SET updated_at = $1
		WHERE delivery_id = $2::uuid AND status = 'in_progress' AND claim_version = $3
		  AND current_attempt_version = $3 AND current_attempt_open = TRUE`
	deliveryArgs := []any{now, claim.deliveryID, claim.version}
	if a.dialect == DialectSQLite {
		deliveryQuery = `
			UPDATE event_deliveries
			SET updated_at = ?
			WHERE delivery_id = ? AND status = 'in_progress' AND claim_version = ?
			  AND current_attempt_version = ? AND current_attempt_open = TRUE`
		deliveryArgs = []any{now, claim.deliveryID, claim.version, claim.version}
	}
	deliveryResult, err := tx.ExecContext(ctx, deliveryQuery, deliveryArgs...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("record delivery claim renewal time: %w", err)
	}
	if rows, _ := deliveryResult.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery claim renewal lost lifecycle owner", ErrConflict)
	}
	updated, err := a.loadByID(ctx, tx, claim.deliveryID, false)
	return updated.Snapshot, err
}

func (a *Adapter) SettleSuccess(ctx context.Context, tx *sql.Tx, claim Claim, sideEffects []string, duration time.Duration) (Snapshot, error) {
	return a.settle(ctx, tx, claim, Settlement{Disposition: "success", SideEffects: sideEffects, Duration: duration})
}

func (a *Adapter) SettleFailure(ctx context.Context, tx *sql.Tx, claim Claim, settlement Settlement) (Snapshot, error) {
	if settlement.Disposition != FailureRetry && settlement.Disposition != FailureDeadLetter {
		return Snapshot{}, fmt.Errorf("delivery failure disposition %q is invalid", settlement.Disposition)
	}
	if settlement.Failure == nil {
		return Snapshot{}, fmt.Errorf("delivery failure settlement requires a failure envelope")
	}
	if settlement.Disposition == FailureDeadLetter && strings.TrimSpace(settlement.ReasonCode) == "" {
		return Snapshot{}, fmt.Errorf("terminal delivery failure requires a reason code")
	}
	return a.settle(ctx, tx, claim, settlement)
}

func (a *Adapter) settle(ctx context.Context, tx *sql.Tx, claim Claim, settlement Settlement) (Snapshot, error) {
	if tx == nil {
		return Snapshot{}, fmt.Errorf("delivery settlement transaction is required")
	}
	if !claim.valid() {
		return Snapshot{}, fmt.Errorf("delivery settlement requires a current claim (delivery=%q run=%q route=%t token=%t version=%d class=%q subscriber=%q)", claim.deliveryID, claim.runID, claim.routeIdentity != "", claim.token != "", claim.version, claim.class, claim.subscriberID)
	}
	if settlement.Duration < 0 {
		return Snapshot{}, fmt.Errorf("delivery settlement duration cannot be negative")
	}
	record, now, err := a.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return Snapshot{}, err
	}
	status := StatusDelivered
	transition := "delivered"
	outcome := "delivered"
	reason := strings.TrimSpace(settlement.ReasonCode)
	effectiveFailure := settlement.Failure
	retryCount := record.RetryCount
	var nextEligible any
	if settlement.Disposition == FailureRetry {
		if record.RetryCount < record.MaxRetries {
			status = StatusFailed
			transition = "failed"
			outcome = "retry_scheduled"
			retryCount++
			base := settlement.RetryBase
			if base <= 0 {
				base = time.Second
			}
			delay := time.Duration(float64(base) * math.Pow(2, float64(retryCount-1)))
			nextEligible = now.Add(delay)
		} else {
			status = StatusDeadLetter
			transition = "dead_letter"
			outcome = "dead_letter"
			reason = "retry_exhausted"
			effectiveFailure, err = a.retryExhaustedFailure(ctx, tx, record, claim, settlement.Failure)
			if err != nil {
				return Snapshot{}, err
			}
		}
	} else if settlement.Disposition == FailureDeadLetter {
		status = StatusDeadLetter
		transition = "dead_letter"
		outcome = "dead_letter"
	}
	failureRaw, err := encodeFailure(effectiveFailure)
	if err != nil {
		return Snapshot{}, err
	}
	query := `
		UPDATE event_deliveries
		SET status = $1::text, retry_count = $2, next_eligible_at = $3,
			reason_code = NULLIF($4, ''), failure = NULLIF($5, '')::jsonb,
			current_attempt_version = NULL, current_attempt_open = NULL,
			settled_at = CASE WHEN $1::text IN ('delivered', 'dead_letter') THEN $6::timestamptz ELSE NULL END,
			updated_at = $6::timestamptz
		WHERE delivery_id = $7::uuid AND claim_version = $8 AND current_attempt_version = $8
		  AND current_attempt_open = TRUE AND status = 'in_progress'`
	args := []any{string(status), retryCount, nextEligible, reason, failureRaw, now, claim.deliveryID, claim.version}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_deliveries
			SET status = ?, retry_count = ?, next_eligible_at = ?,
				reason_code = NULLIF(?, ''), failure = NULLIF(?, ''),
				current_attempt_version = NULL, current_attempt_open = NULL,
				settled_at = CASE WHEN ? IN ('delivered', 'dead_letter') THEN ? ELSE NULL END,
				updated_at = ?
			WHERE delivery_id = ? AND claim_version = ? AND current_attempt_version = ?
			  AND current_attempt_open = TRUE AND status = 'in_progress'`
		args = []any{string(status), retryCount, nextEligible, reason, failureRaw, string(status), now, now, claim.deliveryID, claim.version, claim.version}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("settle delivery obligation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery settlement lost claim", ErrConflict)
	}
	if err := a.completeAttempt(ctx, tx, claim, outcome, reason, effectiveFailure, settlement.SideEffects, settlement.Duration, now); err != nil {
		return Snapshot{}, err
	}
	updated, err := a.loadByID(ctx, tx, claim.deliveryID, false)
	if err != nil {
		return Snapshot{}, err
	}
	if err := a.recordTransition(ctx, updated, transition, effectiveFailure, now); err != nil {
		return Snapshot{}, err
	}
	return updated.Snapshot, nil
}

func (a *Adapter) retryExhaustedFailure(ctx context.Context, tx *sql.Tx, record deliveryRecord, claim Claim, current *runtimefailures.Envelope) (*runtimefailures.Envelope, error) {
	outcomes, err := a.Outcomes(ctx, tx, claim.deliveryID)
	if err != nil {
		return nil, fmt.Errorf("load delivery retry history: %w", err)
	}
	if len(outcomes) != record.RetryCount {
		return nil, fmt.Errorf("%w: delivery retry history has %d outcomes for retry_count %d", ErrConflict, len(outcomes), record.RetryCount)
	}
	history := make([]map[string]any, 0, len(outcomes)+1)
	appendFailure := func(version int64, failure *runtimefailures.Envelope) error {
		if failure == nil {
			return fmt.Errorf("%w: delivery retry history claim %d has no failure", ErrConflict, version)
		}
		value, err := runtimefailures.EnvelopeValue(*failure)
		if err != nil {
			return fmt.Errorf("validate delivery retry history claim %d: %w", version, err)
		}
		history = append(history, map[string]any{"claim_version": version, "failure": value})
		return nil
	}
	for _, prior := range outcomes {
		if prior.Outcome != "retry_scheduled" {
			return nil, fmt.Errorf("%w: delivery retry history claim %d has outcome %q", ErrConflict, prior.ClaimVersion, prior.Outcome)
		}
		if err := appendFailure(prior.ClaimVersion, prior.Failure); err != nil {
			return nil, err
		}
	}
	if err := appendFailure(claim.version, current); err != nil {
		return nil, err
	}
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(
		runtimefailures.ClassRetryExhausted,
		"delivery_retry_exhausted",
		"delivery-lifecycle",
		"settle_failure",
		map[string]any{"max_retries": record.MaxRetries, "retry_history": history},
	))
	if !ok || failure.Class != runtimefailures.ClassRetryExhausted {
		return nil, fmt.Errorf("construct canonical retry-exhausted failure")
	}
	return &failure, nil
}

func (a *Adapter) Snapshot(ctx context.Context, q queryer, deliveryID string) (Snapshot, error) {
	record, err := a.loadByID(ctx, q, deliveryID, false)
	if err != nil {
		return Snapshot{}, err
	}
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotAt(record, now), nil
}

func (a *Adapter) Outcomes(ctx context.Context, q queryer, deliveryID string) ([]Outcome, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if _, err := uuid.Parse(deliveryID); err != nil {
		return nil, fmt.Errorf("delivery id: %w", err)
	}
	query := `
		SELECT delivery_id::text, claim_version, outcome, COALESCE(reason_code, ''),
			failure, side_effects, duration_ms, settled_at
		FROM event_delivery_outcomes
		WHERE delivery_id = $1::uuid
		ORDER BY claim_version`
	if a.dialect == DialectSQLite {
		query = `
			SELECT delivery_id, claim_version, outcome, COALESCE(reason_code, ''),
				failure, side_effects, duration_ms, settled_at
			FROM event_delivery_outcomes
			WHERE delivery_id = ?
			ORDER BY claim_version`
	}
	rows, err := q.QueryContext(ctx, query, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("list delivery outcomes: %w", err)
	}
	defer rows.Close()
	out := []Outcome{}
	for rows.Next() {
		var (
			item           Outcome
			failureRaw     []byte
			sideEffectsRaw []byte
			durationMS     int64
			settledRaw     any
		)
		if err := rows.Scan(&item.DeliveryID, &item.ClaimVersion, &item.Outcome, &item.ReasonCode, &failureRaw, &sideEffectsRaw, &durationMS, &settledRaw); err != nil {
			return nil, fmt.Errorf("scan delivery outcome: %w", err)
		}
		if item.ClaimVersion <= 0 {
			return nil, fmt.Errorf("%w: delivery outcome claim version is invalid", ErrConflict)
		}
		switch item.Outcome {
		case "delivered", "retry_scheduled", "dead_letter", "terminalized":
		default:
			return nil, fmt.Errorf("%w: delivery outcome %q is invalid", ErrConflict, item.Outcome)
		}
		if item.Failure, err = decodeFailure(failureRaw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(sideEffectsRaw, &item.SideEffects); err != nil {
			return nil, fmt.Errorf("decode delivery outcome side effects: %w", err)
		}
		item.Duration = time.Duration(durationMS) * time.Millisecond
		var present bool
		if item.SettledAt, present, err = parseNullableTime(settledRaw); err != nil {
			return nil, err
		}
		if !present {
			return nil, fmt.Errorf("%w: delivery outcome settled time is missing", ErrConflict)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list delivery outcomes: %w", err)
	}
	return out, nil
}

func (a *Adapter) ProveHandoff(ctx context.Context, q queryer, eventID string, route events.DeliveryRoute) (DurableHandoffProof, error) {
	identity, err := route.Identity()
	if err != nil {
		return DurableHandoffProof{}, err
	}
	record, err := a.loadByEventAndRoute(ctx, q, eventID, identity, false)
	if err != nil {
		return DurableHandoffProof{}, err
	}
	if !events.SameDeliveryRouteIdentity(record.Route, route) {
		return DurableHandoffProof{}, fmt.Errorf("%w: durable handoff route mismatch", ErrConflict)
	}
	return DurableHandoffProof{deliveryID: record.DeliveryID, eventID: record.EventID, routeIdentity: record.RouteIdentity.String()}, nil
}

func (a *Adapter) SummarizeRun(ctx context.Context, q queryer, runID string) (RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return RunSummary{}, fmt.Errorf("delivery run summary run id: %w", err)
	}
	query := `
		SELECT status, COUNT(*), MIN(next_eligible_at)
		FROM event_deliveries WHERE run_id = $1::uuid
		GROUP BY status`
	if a.dialect == DialectSQLite {
		query = `SELECT status, COUNT(*), MIN(next_eligible_at) FROM event_deliveries WHERE run_id = ? GROUP BY status`
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return RunSummary{}, fmt.Errorf("summarize delivery run: %w", err)
	}
	defer rows.Close()
	summary := RunSummary{RunID: runID}
	for rows.Next() {
		var rawStatus string
		var count int
		var next any
		if err := rows.Scan(&rawStatus, &count, &next); err != nil {
			return RunSummary{}, fmt.Errorf("scan delivery run summary: %w", err)
		}
		status, err := ParseStatus(rawStatus)
		if err != nil {
			return RunSummary{}, err
		}
		summary.Total += count
		switch status {
		case StatusPending:
			summary.Pending += count
		case StatusInProgress:
			summary.InProgress += count
		case StatusFailed:
			summary.RetryScheduled += count
		case StatusDelivered:
			summary.Delivered += count
		case StatusDeadLetter:
			summary.DeadLetter += count
		}
		if parsed, ok, err := parseNullableTime(next); err != nil {
			return RunSummary{}, err
		} else if ok && (summary.NextEligibleAt.IsZero() || parsed.Before(summary.NextEligibleAt)) {
			summary.NextEligibleAt = parsed
		}
	}
	return summary, rows.Err()
}

func (a *Adapter) SnapshotsForEvent(ctx context.Context, q queryer, eventID string) ([]Snapshot, error) {
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return nil, fmt.Errorf("delivery event snapshots event id: %w", err)
	}
	query := `SELECT delivery_id::text FROM event_deliveries WHERE event_id = $1::uuid ORDER BY created_at, delivery_id`
	if a.dialect == DialectSQLite {
		query = `SELECT delivery_id FROM event_deliveries WHERE event_id = ? ORDER BY created_at, delivery_id`
	}
	return a.snapshotsByIDQuery(ctx, q, query, eventID)
}

func (a *Adapter) SnapshotsForRun(ctx context.Context, q queryer, runID string) ([]Snapshot, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("delivery run snapshots run id: %w", err)
	}
	query := `SELECT delivery_id::text FROM event_deliveries WHERE run_id = $1::uuid ORDER BY created_at, delivery_id`
	if a.dialect == DialectSQLite {
		query = `SELECT delivery_id FROM event_deliveries WHERE run_id = ? ORDER BY created_at, delivery_id`
	}
	return a.snapshotsByIDQuery(ctx, q, query, runID)
}

func (a *Adapter) SnapshotsForAgent(ctx context.Context, q queryer, agentID string, since time.Time) ([]Snapshot, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("delivery agent snapshots agent id is required")
	}
	if since.IsZero() {
		since = time.Unix(0, 0).UTC()
	}
	query := `
		SELECT d.delivery_id::text
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE d.subscriber_type = 'agent' AND d.subscriber_id = $1 AND e.created_at >= $2
		ORDER BY e.created_at, e.event_id, d.delivery_id`
	if a.dialect == DialectSQLite {
		query = `
			SELECT d.delivery_id
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE d.subscriber_type = 'agent' AND d.subscriber_id = ? AND e.created_at >= ?
			ORDER BY e.created_at, e.event_id, d.delivery_id`
	}
	return a.snapshotsByIDQuery(ctx, q, query, agentID, since.UTC())
}

func (a *Adapter) LifecycleSnapshotPageForAgent(ctx context.Context, q queryer, page AgentLifecyclePageQuery) (SnapshotPage, error) {
	agentID := strings.TrimSpace(page.AgentID)
	if agentID == "" {
		return SnapshotPage{}, fmt.Errorf("delivery lifecycle page agent id is required")
	}
	runID := strings.TrimSpace(page.RunID)
	if runID != "" {
		if _, err := uuid.Parse(runID); err != nil {
			return SnapshotPage{}, fmt.Errorf("delivery lifecycle page run id: %w", err)
		}
	}
	if err := validateSnapshotPagePosition(page.BeforeCreatedAt, page.BeforeDeliveryID, page.Limit); err != nil {
		return SnapshotPage{}, fmt.Errorf("delivery lifecycle page: %w", err)
	}
	statusSelected := map[Status]bool{}
	for _, status := range page.Statuses {
		switch status {
		case StatusPending, StatusInProgress, StatusDelivered, StatusFailed, StatusDeadLetter:
			statusSelected[status] = true
		default:
			return SnapshotPage{}, fmt.Errorf("delivery lifecycle page status %q is invalid", status)
		}
	}
	selectAllStatuses := len(statusSelected) == 0
	for _, status := range []Status{StatusPending, StatusInProgress, StatusDelivered, StatusFailed, StatusDeadLetter} {
		if selectAllStatuses {
			statusSelected[status] = true
		}
	}
	var cursorAt any
	var cursorID any
	if !page.BeforeCreatedAt.IsZero() {
		cursorAt = page.BeforeCreatedAt.UTC()
		cursorID = strings.TrimSpace(page.BeforeDeliveryID)
	}
	query := `
		SELECT d.delivery_id::text
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND d.subscriber_id = $1
		  AND ($2::text = '' OR d.run_id = NULLIF($2::text, '')::uuid)
		  AND (($3 AND d.status = 'pending') OR ($4 AND d.status = 'in_progress') OR
		       ($5 AND d.status = 'delivered') OR ($6 AND d.status = 'failed') OR
		       ($7 AND d.status = 'dead_letter'))
		  AND ($8::timestamptz IS NULL OR d.created_at < $8 OR (d.created_at = $8 AND d.delivery_id < $9::uuid))
		ORDER BY d.created_at DESC, d.delivery_id DESC
		LIMIT $10`
	args := []any{
		agentID, runID,
		statusSelected[StatusPending], statusSelected[StatusInProgress], statusSelected[StatusDelivered],
		statusSelected[StatusFailed], statusSelected[StatusDeadLetter],
		cursorAt, cursorID, page.Limit + 1,
	}
	if a.dialect == DialectSQLite {
		query = `
			SELECT d.delivery_id
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND d.subscriber_id = ?
			  AND (? = '' OR d.run_id = ?)
			  AND ((? AND d.status = 'pending') OR (? AND d.status = 'in_progress') OR
			       (? AND d.status = 'delivered') OR (? AND d.status = 'failed') OR
			       (? AND d.status = 'dead_letter'))
			  AND (? IS NULL OR d.created_at < ? OR (d.created_at = ? AND d.delivery_id < ?))
			ORDER BY d.created_at DESC, d.delivery_id DESC
			LIMIT ?`
		args = []any{
			agentID, runID, runID,
			statusSelected[StatusPending], statusSelected[StatusInProgress], statusSelected[StatusDelivered],
			statusSelected[StatusFailed], statusSelected[StatusDeadLetter],
			cursorAt, cursorAt, cursorAt, cursorID, page.Limit + 1,
		}
	}
	return a.snapshotPageByIDQuery(ctx, q, page.Limit, query, args...)
}

func (a *Adapter) DiagnosticSnapshotPageForAgent(ctx context.Context, q queryer, page AgentDiagnosticPageQuery) (SnapshotPage, error) {
	agentID := strings.TrimSpace(page.AgentID)
	if agentID == "" {
		return SnapshotPage{}, fmt.Errorf("delivery diagnostic page agent id is required")
	}
	if page.Status != StatusFailed && page.Status != StatusDeadLetter {
		return SnapshotPage{}, fmt.Errorf("delivery diagnostic page status %q is invalid", page.Status)
	}
	if err := validateSnapshotPagePosition(page.BeforeOccurredAt, page.BeforeDeliveryID, page.Limit); err != nil {
		return SnapshotPage{}, fmt.Errorf("delivery diagnostic page: %w", err)
	}
	var cursorAt any
	var cursorID any
	if !page.BeforeOccurredAt.IsZero() {
		cursorAt = page.BeforeOccurredAt.UTC()
		cursorID = strings.TrimSpace(page.BeforeDeliveryID)
	}
	occurredColumn := "d.updated_at"
	if page.Status == StatusDeadLetter {
		occurredColumn = "d.settled_at"
	}
	query := fmt.Sprintf(`
		SELECT d.delivery_id::text
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND d.subscriber_id = $1 AND d.status = $2
		  AND ($3::timestamptz IS NULL OR %[1]s < $3 OR
		       (%[1]s = $3 AND d.delivery_id < $4::uuid))
		ORDER BY %[1]s DESC, d.delivery_id DESC
		LIMIT $5`, occurredColumn)
	args := []any{agentID, string(page.Status), cursorAt, cursorID, page.Limit + 1}
	if a.dialect == DialectSQLite {
		query = fmt.Sprintf(`
			SELECT d.delivery_id
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND d.subscriber_id = ? AND d.status = ?
			  AND (? IS NULL OR %[1]s < ? OR (%[1]s = ? AND d.delivery_id < ?))
			ORDER BY %[1]s DESC, d.delivery_id DESC
			LIMIT ?`, occurredColumn)
		args = []any{agentID, string(page.Status), cursorAt, cursorAt, cursorAt, cursorID, page.Limit + 1}
	}
	return a.snapshotPageByIDQuery(ctx, q, page.Limit, query, args...)
}

func (a *Adapter) DiagnosticCountsForAgentSince(ctx context.Context, q queryer, agentID string, since time.Time) (AgentDiagnosticCounts, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return AgentDiagnosticCounts{}, fmt.Errorf("delivery diagnostic counts agent id is required")
	}
	if since.IsZero() {
		return AgentDiagnosticCounts{}, fmt.Errorf("delivery diagnostic counts cutoff is required")
	}
	query := `
		SELECT COUNT(*) FILTER (WHERE d.status = 'failed'),
		       COUNT(*) FILTER (WHERE d.status = 'dead_letter')
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND d.subscriber_id = $1
		  AND d.status IN ('failed', 'dead_letter')
		  AND ((d.status = 'failed' AND d.updated_at >= $2) OR
		       (d.status = 'dead_letter' AND d.settled_at >= $2))`
	if a.dialect == DialectSQLite {
		query = `
			SELECT COALESCE(SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END), 0),
			       COALESCE(SUM(CASE WHEN d.status = 'dead_letter' THEN 1 ELSE 0 END), 0)
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND d.subscriber_id = ?
			  AND d.status IN ('failed', 'dead_letter')
			  AND ((d.status = 'failed' AND d.updated_at >= ?) OR
			       (d.status = 'dead_letter' AND d.settled_at >= ?))`
	}
	var counts AgentDiagnosticCounts
	args := []any{agentID, since.UTC()}
	if a.dialect == DialectSQLite {
		args = []any{agentID, since.UTC(), since.UTC()}
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&counts.Failures, &counts.DeadLetters); err != nil {
		return AgentDiagnosticCounts{}, fmt.Errorf("count agent delivery diagnostics: %w", err)
	}
	return counts, nil
}

func validateSnapshotPagePosition(before time.Time, deliveryID string, limit int) error {
	deliveryID = strings.TrimSpace(deliveryID)
	if limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	if before.IsZero() != (deliveryID == "") {
		return fmt.Errorf("cursor time and delivery id must be supplied together")
	}
	if deliveryID != "" {
		if _, err := uuid.Parse(deliveryID); err != nil {
			return fmt.Errorf("cursor delivery id: %w", err)
		}
	}
	return nil
}

func (a *Adapter) snapshotPageByIDQuery(ctx context.Context, q queryer, limit int, query string, args ...any) (SnapshotPage, error) {
	snapshots, err := a.snapshotsByIDQuery(ctx, q, query, args...)
	if err != nil {
		return SnapshotPage{}, err
	}
	page := SnapshotPage{Snapshots: snapshots, HasMore: len(snapshots) > limit}
	if page.HasMore {
		page.Snapshots = page.Snapshots[:limit]
	}
	return page, nil
}

func (a *Adapter) snapshotsByIDQuery(ctx context.Context, q queryer, query string, args ...any) ([]Snapshot, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select delivery snapshots: %w", err)
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan delivery snapshot id: %w", err)
		}
		ids = append(ids, strings.TrimSpace(id))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read delivery snapshot ids: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close delivery snapshot ids: %w", err)
	}
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]Snapshot, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, q, id, false)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshotAt(record, now))
	}
	return out, nil
}

func snapshotAt(record deliveryRecord, now time.Time) Snapshot {
	snapshot := record.Snapshot
	snapshot.RetryEligible = snapshot.Status == StatusPending ||
		(snapshot.Status == StatusFailed && snapshot.RetryCount <= snapshot.MaxRetries && !snapshot.NextEligibleAt.After(now))
	snapshot.ClaimReclaimable = snapshot.Status == StatusInProgress && !snapshot.ClaimExpiresAt.IsZero() && !snapshot.ClaimExpiresAt.After(now)
	return snapshot
}

func (a *Adapter) ActiveRunSnapshots(ctx context.Context, tx *sql.Tx, runID string) ([]Snapshot, error) {
	if tx == nil {
		return nil, fmt.Errorf("delivery active-run snapshot transaction is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(runID)); err != nil {
		return nil, fmt.Errorf("delivery active-run snapshot run id: %w", err)
	}
	query := `SELECT delivery_id::text FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id FOR UPDATE`
	if a.dialect == DialectSQLite {
		query = `SELECT delivery_id FROM event_deliveries WHERE run_id = ? AND status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("select active run delivery snapshots: %w", err)
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
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]Snapshot, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		out = append(out, record.Snapshot)
	}
	return out, nil
}

func (a *Adapter) ActiveSnapshots(ctx context.Context, q queryer) ([]Snapshot, error) {
	query := `SELECT delivery_id::text FROM event_deliveries WHERE status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	if a.dialect == DialectSQLite {
		query = `SELECT delivery_id FROM event_deliveries WHERE status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	}
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("select active delivery snapshots: %w", err)
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
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]Snapshot, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, q, id, false)
		if err != nil {
			return nil, err
		}
		out = append(out, record.Snapshot)
	}
	return out, nil
}

func (a *Adapter) EligibleAgentSnapshots(ctx context.Context, q queryer, agentID string, since time.Time) ([]Snapshot, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("delivery agent id is required")
	}
	if since.IsZero() {
		since = time.Unix(0, 0).UTC()
	}
	query := `
		SELECT d.delivery_id::text
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		LEFT JOIN runs r ON r.run_id = e.run_id
		WHERE d.subscriber_type = 'agent' AND d.subscriber_id = $1
		  AND e.created_at >= $2
		  AND (e.run_id IS NULL OR r.status IN ('running', 'paused'))
		  AND (
			d.status = 'pending'
			OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= CURRENT_TIMESTAMP)
			OR d.status = 'in_progress'
		  )
		ORDER BY e.created_at, e.event_id, d.delivery_id`
	if a.dialect == DialectSQLite {
		query = `
			SELECT d.delivery_id
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			LEFT JOIN runs r ON r.run_id = e.run_id
			WHERE d.subscriber_type = 'agent' AND d.subscriber_id = ?
			  AND e.created_at >= ?
			  AND (e.run_id IS NULL OR r.status IN ('running', 'paused'))
			  AND (
				d.status = 'pending'
				OR (d.status = 'failed' AND d.retry_count <= d.max_retries AND d.next_eligible_at <= CURRENT_TIMESTAMP)
				OR d.status = 'in_progress'
			  )
			ORDER BY e.created_at, e.event_id, d.delivery_id`
	}
	rows, err := q.QueryContext(ctx, query, agentID, since)
	if err != nil {
		return nil, fmt.Errorf("select eligible agent delivery snapshots: %w", err)
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
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]Snapshot, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, q, id, false)
		if err != nil {
			return nil, err
		}
		record.RetryEligible = record.Status == StatusPending || record.Status == StatusFailed
		out = append(out, record.Snapshot)
	}
	return out, nil
}

func (a *Adapter) TerminalizeRun(ctx context.Context, tx *sql.Tx, runID, reason string) ([]Terminalization, error) {
	if tx == nil {
		return nil, fmt.Errorf("delivery run terminalization transaction is required")
	}
	runID = strings.TrimSpace(runID)
	reason = strings.TrimSpace(reason)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("delivery run terminalization run id: %w", err)
	}
	if reason == "" {
		return nil, fmt.Errorf("delivery run terminalization reason is required")
	}
	query := `SELECT delivery_id::text FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id FOR UPDATE`
	if a.dialect == DialectSQLite {
		query = `SELECT delivery_id FROM event_deliveries WHERE run_id = ? AND status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("select run deliveries for terminalization: %w", err)
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	failure, err := parentTerminalizationFailure(reason)
	if err != nil {
		return nil, err
	}
	failureRaw, err := encodeFailure(&failure)
	if err != nil {
		return nil, err
	}
	out := make([]Terminalization, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		version := record.ClaimVersion + 1
		query := `UPDATE event_deliveries SET status = 'dead_letter', reason_code = $1, failure = $2::jsonb, retry_count = retry_count, next_eligible_at = NULL, claim_version = $3, current_attempt_version = NULL, current_attempt_open = NULL, settled_at = $4, updated_at = $4 WHERE delivery_id = $5::uuid AND claim_version = $6`
		args := []any{reason, failureRaw, version, now, id, record.ClaimVersion}
		if a.dialect == DialectSQLite {
			query = `UPDATE event_deliveries SET status = 'dead_letter', reason_code = ?, failure = ?, retry_count = retry_count, next_eligible_at = NULL, claim_version = ?, current_attempt_version = NULL, current_attempt_open = NULL, settled_at = ?, updated_at = ? WHERE delivery_id = ? AND claim_version = ?`
			args = []any{reason, failureRaw, version, now, now, id, record.ClaimVersion}
		}
		if result, err := tx.ExecContext(ctx, query, args...); err != nil {
			return nil, fmt.Errorf("terminalize run delivery: %w", err)
		} else if affected, _ := result.RowsAffected(); affected != 1 {
			return nil, fmt.Errorf("%w: run terminalization lost delivery claim fence", ErrConflict)
		}
		if record.claimToken != "" && record.ClaimVersion > 0 {
			claim := Claim{deliveryID: id, routeIdentity: record.RouteIdentity.String(), token: record.claimToken, version: record.ClaimVersion, class: record.SubscriberClass, subscriberID: record.SubscriberID}
			if err := a.closeAttemptForTerminalization(ctx, tx, claim, reason, &failure, now); err != nil {
				return nil, err
			}
		}
		if err := a.insertTerminalizedAttempt(ctx, tx, id, version, reason, &failure, now); err != nil {
			return nil, err
		}
		updated, err := a.loadByID(ctx, tx, id, false)
		if err != nil {
			return nil, err
		}
		if err := a.recordTransition(ctx, updated, "terminalized", &failure, now); err != nil {
			return nil, err
		}
		out = append(out, Terminalization{Previous: record.Snapshot, Current: updated.Snapshot})
	}
	return out, nil
}

func parentTerminalizationFailure(reason string) (runtimefailures.Envelope, error) {
	failureErr := runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"delivery_parent_terminalized",
		"delivery_lifecycle",
		"terminalize_run",
		map[string]any{"reason_code": reason},
	)
	failure, ok := runtimefailures.EnvelopeFromError(failureErr)
	if !ok {
		return runtimefailures.Envelope{}, fmt.Errorf("construct delivery parent terminalization failure")
	}
	return failure, nil
}

func (a *Adapter) closeAttemptForTerminalization(ctx context.Context, tx *sql.Tx, claim Claim, reason string, failure *runtimefailures.Envelope, now time.Time) error {
	failureRaw, err := encodeFailure(failure)
	if err != nil {
		return err
	}
	query := `
		UPDATE event_delivery_attempts
		SET outcome = 'terminalized', reason_code = $1, failure = $2::jsonb,
			side_effects = '[]'::jsonb, duration_ms = 0, completed_at = $3, open_marker = FALSE
		WHERE delivery_id = $4::uuid AND claim_version = $5 AND claim_token = $6::uuid AND open_marker = TRUE`
	args := []any{reason, failureRaw, now, claim.deliveryID, claim.version, claim.token}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET outcome = 'terminalized', reason_code = ?, failure = ?,
				side_effects = '[]', duration_ms = 0, completed_at = ?, open_marker = FALSE
			WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("close delivery attempt for parent terminalization: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("%w: delivery attempt is stale during parent terminalization", ErrConflict)
	}
	return nil
}

func (a *Adapter) insertTerminalizedAttempt(ctx context.Context, tx *sql.Tx, deliveryID string, version int64, reason string, failure *runtimefailures.Envelope, now time.Time) error {
	token := uuid.NewString()
	failureRaw, err := encodeFailure(failure)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO event_delivery_attempts (
			delivery_id, claim_version, claim_token, started_at, lease_expires_at,
			open_marker, outcome, reason_code, failure, side_effects, duration_ms, completed_at
		) VALUES ($1::uuid, $2, $3::uuid, $4, $5, FALSE, 'terminalized', $6, $7::jsonb, '[]'::jsonb, 0, $4)`
	args := []any{deliveryID, version, token, now, now.Add(time.Second), reason, failureRaw}
	if a.dialect == DialectSQLite {
		query = `
			INSERT INTO event_delivery_attempts (
				delivery_id, claim_version, claim_token, started_at, lease_expires_at,
				open_marker, outcome, reason_code, failure, side_effects, duration_ms, completed_at
			) VALUES (?, ?, ?, ?, ?, FALSE, 'terminalized', ?, ?, '[]', 0, ?)`
		args = []any{deliveryID, version, token, now, now.Add(time.Second), reason, failureRaw, now}
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("record terminalized delivery attempt: %w", err)
	}
	return a.insertOutcome(ctx, tx, deliveryID, version, "terminalized", reason, failure, nil, 0, now)
}

func (a *Adapter) requireCurrentClaim(ctx context.Context, tx *sql.Tx, claim Claim) (deliveryRecord, time.Time, error) {
	record, err := a.loadByID(ctx, tx, claim.deliveryID, true)
	if err != nil {
		return deliveryRecord{}, time.Time{}, err
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return deliveryRecord{}, time.Time{}, err
	}
	if record.Status != StatusInProgress || record.claimToken != claim.token || record.ClaimVersion != claim.version ||
		record.RouteIdentity.String() != claim.routeIdentity || record.ClaimExpiresAt.IsZero() || !record.ClaimExpiresAt.After(now) {
		return deliveryRecord{}, time.Time{}, fmt.Errorf("%w: delivery claim is stale", ErrConflict)
	}
	return record, now, nil
}

func (a *Adapter) insertAttempt(ctx context.Context, tx *sql.Tx, deliveryID string, version int64, token string, startedAt, expiresAt time.Time) error {
	query := `INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, open_marker) VALUES ($1::uuid, $2, $3::uuid, $4, $5, TRUE)`
	args := []any{deliveryID, version, token, startedAt, expiresAt}
	if a.dialect == DialectSQLite {
		query = `INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, open_marker) VALUES (?, ?, ?, ?, ?, TRUE)`
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("record delivery claim attempt: %w", err)
	}
	return nil
}

func (a *Adapter) expireAttempt(ctx context.Context, tx *sql.Tx, record deliveryRecord, now time.Time) error {
	if record.claimToken == "" || record.ClaimVersion <= 0 {
		return fmt.Errorf("%w: expired in-progress delivery has no current claim", ErrConflict)
	}
	query := `UPDATE event_delivery_attempts SET outcome = 'lease_expired', completed_at = $1, open_marker = FALSE WHERE delivery_id = $2::uuid AND claim_version = $3 AND claim_token = $4::uuid AND open_marker = TRUE`
	args := []any{now, record.DeliveryID, record.ClaimVersion, record.claimToken}
	if a.dialect == DialectSQLite {
		query = `UPDATE event_delivery_attempts SET outcome = 'lease_expired', completed_at = ?, open_marker = FALSE WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("expire delivery claim attempt: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("%w: expired delivery attempt is missing", ErrConflict)
	}
	return nil
}

func (a *Adapter) completeAttempt(ctx context.Context, tx *sql.Tx, claim Claim, outcome, reason string, failure *runtimefailures.Envelope, sideEffects []string, duration time.Duration, now time.Time) error {
	failureRaw, err := encodeFailure(failure)
	if err != nil {
		return err
	}
	sideEffectsRaw, err := json.Marshal(sideEffects)
	if err != nil {
		return fmt.Errorf("encode delivery side effects: %w", err)
	}
	durationMS := duration.Milliseconds()
	query := `
		UPDATE event_delivery_attempts
		SET outcome = $1, reason_code = NULLIF($2, ''), failure = NULLIF($3, '')::jsonb,
			side_effects = $4::jsonb, duration_ms = $5, completed_at = $6, open_marker = FALSE
		WHERE delivery_id = $7::uuid AND claim_version = $8 AND claim_token = $9::uuid AND open_marker = TRUE`
	args := []any{outcome, reason, failureRaw, string(sideEffectsRaw), durationMS, now, claim.deliveryID, claim.version, claim.token}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET outcome = ?, reason_code = NULLIF(?, ''), failure = NULLIF(?, ''),
				side_effects = ?, duration_ms = ?, completed_at = ?, open_marker = FALSE
			WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("complete delivery attempt: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("%w: delivery attempt is stale", ErrConflict)
	}
	return a.insertOutcome(ctx, tx, claim.deliveryID, claim.version, outcome, reason, failure, sideEffects, duration, now)
}

func (a *Adapter) insertOutcome(ctx context.Context, tx *sql.Tx, deliveryID string, version int64, outcome, reason string, failure *runtimefailures.Envelope, sideEffects []string, duration time.Duration, now time.Time) error {
	failureRaw, err := encodeFailure(failure)
	if err != nil {
		return err
	}
	sideEffectsRaw, err := json.Marshal(sideEffects)
	if err != nil {
		return fmt.Errorf("encode delivery side effects: %w", err)
	}
	query := `INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, settled_at) VALUES ($1::uuid, $2, $3, NULLIF($4, ''), NULLIF($5, '')::jsonb, $6::jsonb, $7, $8)`
	args := []any{deliveryID, version, outcome, reason, failureRaw, string(sideEffectsRaw), duration.Milliseconds(), now}
	if a.dialect == DialectSQLite {
		query = `INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, settled_at) VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?)`
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("record exact delivery outcome: %w", err)
	}
	return nil
}

func (a *Adapter) recordTransition(ctx context.Context, record deliveryRecord, transition string, failure *runtimefailures.Envelope, occurredAt time.Time) error {
	currentScope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || strings.TrimSpace(currentScope.RuntimeInstanceID) == "" {
		return fmt.Errorf("delivery lifecycle transition requires exact runtime instance scope")
	}
	transitionScope := currentScope
	if strings.TrimSpace(record.bundleHash) != "" {
		transitionScope = runtimeauthoractivity.BundleScope(currentScope.RuntimeInstanceID, record.bundleHash)
	} else if currentScope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(currentScope.BundleHash) == "" {
		return fmt.Errorf("delivery lifecycle transition requires persisted run bundle_hash or exact bundle scope")
	}
	summary, found, err := runtimeauthoractivity.PersistedAuthorSafeSummary(ctx, "emit:"+record.EventID)
	if err != nil {
		return fmt.Errorf("load delivery source author-safe summary: %w", err)
	}
	if !found {
		summary = ""
	}
	retryCount := record.RetryCount
	projection := runtimeauthoractivity.Projection{
		SubjectType: string(record.SubscriberClass), SubjectID: record.SubscriberID,
		EventType: record.eventType, SubscriberType: string(record.SubscriberClass),
		SubscriberID: record.SubscriberID, RetryCount: &retryCount, ReasonCode: record.ReasonCode,
	}
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDeliveryLifecycle, Transition: transition,
		SourceOwner: "event_deliveries", SourceIdentity: record.DeliveryID,
		DedupKey:   fmt.Sprintf("delivery:%s:v%d:%s", record.DeliveryID, record.ClaimVersion, transition),
		OccurredAt: occurredAt, RunID: record.RunID, EntityID: record.entityID,
		AgentID: agentIdentity(record), FlowID: record.flowID, Projection: projection,
		Scope:             transitionScope,
		AuthorSafeSummary: summary, Failure: runtimefailures.CloneEnvelope(failure),
	}
	if err := runtimeauthoractivity.Record(ctx, draft); err != nil {
		return fmt.Errorf("record delivery lifecycle activity: %w", err)
	}
	return nil
}

func agentIdentity(record deliveryRecord) string {
	if record.SubscriberClass == SubscriberAgent {
		return record.SubscriberID
	}
	return ""
}

func (a *Adapter) loadByID(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, deliveryID string, lock bool) (deliveryRecord, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if _, err := uuid.Parse(deliveryID); err != nil {
		return deliveryRecord{}, fmt.Errorf("delivery id: %w", err)
	}
	query := a.selectRecord() + ` WHERE d.delivery_id = $1::uuid`
	if lock && a.dialect == DialectPostgres {
		query += ` FOR UPDATE OF d`
	}
	if a.dialect == DialectSQLite {
		query = a.selectRecord() + ` WHERE d.delivery_id = ?`
	}
	return a.scanRecord(q.QueryRowContext(ctx, query, deliveryID))
}

func (a *Adapter) loadByEventAndRoute(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string, identity events.DeliveryRouteIdentity, lock bool) (deliveryRecord, error) {
	query := a.selectRecord() + ` WHERE d.event_id = $1::uuid AND d.route_identity = $2`
	if lock && a.dialect == DialectPostgres {
		query += ` FOR UPDATE OF d`
	}
	if a.dialect == DialectSQLite {
		query = a.selectRecord() + ` WHERE d.event_id = ? AND d.route_identity = ?`
	}
	return a.scanRecord(q.QueryRowContext(ctx, query, strings.TrimSpace(eventID), identity.String()))
}

func (a *Adapter) selectRecord() string {
	if a.dialect == DialectSQLite {
		return `
			SELECT d.delivery_id, d.event_id, d.run_id, d.route_identity,
				d.subscriber_type, d.subscriber_id, d.delivery_target_route, d.delivery_context,
				d.delivery_payload_projection, d.status, d.retry_count, d.max_retries,
				d.next_eligible_at, d.claim_version, COALESCE(current_attempt.claim_token, ''), current_attempt.lease_expires_at,
				COALESCE(current_attempt.active_session_id, ''), COALESCE(d.reason_code, ''), d.failure,
				d.started_at, d.settled_at, d.created_at, d.updated_at,
				e.event_name, COALESCE(e.entity_id, ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, '')
			FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
			LEFT JOIN event_delivery_attempts current_attempt
			  ON current_attempt.delivery_id = d.delivery_id
			 AND current_attempt.claim_version = d.current_attempt_version
			 AND current_attempt.open_marker = TRUE
			LEFT JOIN runs r ON r.run_id = d.run_id`
	}
	return `
		SELECT d.delivery_id::text, d.event_id::text, d.run_id::text, d.route_identity,
			d.subscriber_type, d.subscriber_id, d.delivery_target_route, d.delivery_context,
			d.delivery_payload_projection, d.status, d.retry_count, d.max_retries,
			d.next_eligible_at, d.claim_version, COALESCE(current_attempt.claim_token::text, ''), current_attempt.lease_expires_at,
			COALESCE(current_attempt.active_session_id::text, ''), COALESCE(d.reason_code, ''), d.failure,
			d.started_at, d.settled_at, d.created_at, d.updated_at,
			e.event_name, COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, '')
		FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
		LEFT JOIN event_delivery_attempts current_attempt
		  ON current_attempt.delivery_id = d.delivery_id
		 AND current_attempt.claim_version = d.current_attempt_version
		 AND current_attempt.open_marker = TRUE
		LEFT JOIN runs r ON r.run_id = d.run_id`
}

func (a *Adapter) scanRecord(row scanner) (deliveryRecord, error) {
	var record deliveryRecord
	var routeIdentity, subscriberType, status string
	var targetRaw, contextRaw, projectionRaw, failureRaw []byte
	var nextEligible, claimExpires, started, settled, created, updated any
	err := row.Scan(
		&record.DeliveryID, &record.EventID, &record.RunID, &routeIdentity,
		&subscriberType, &record.SubscriberID, &targetRaw, &contextRaw, &projectionRaw,
		&status, &record.RetryCount, &record.MaxRetries, &nextEligible, &record.ClaimVersion,
		&record.claimToken, &claimExpires, &record.ActiveSessionID, &record.ReasonCode, &failureRaw,
		&started, &settled, &created, &updated, &record.eventType, &record.entityID, &record.flowID, &record.bundleHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deliveryRecord{}, ErrNotFound
	}
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("scan delivery obligation: %w", err)
	}
	identity, err := events.ParseDeliveryRouteIdentity(routeIdentity)
	if err != nil {
		return deliveryRecord{}, err
	}
	record.RouteIdentity = identity
	class, err := ParseSubscriberClass(subscriberType)
	if err != nil {
		return deliveryRecord{}, err
	}
	record.SubscriberClass = class
	record.Status, err = ParseStatus(status)
	if err != nil {
		return deliveryRecord{}, err
	}
	record.Route, err = decodeRoute(class, record.SubscriberID, targetRaw, contextRaw, projectionRaw)
	if err != nil {
		return deliveryRecord{}, err
	}
	derived, err := record.Route.Identity()
	if err != nil || derived != identity {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery route identity mismatch", ErrConflict)
	}
	if record.MaxRetries != class.MaxRetries() || record.RetryCount < 0 || record.RetryCount > record.MaxRetries {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery retry policy mismatch", ErrConflict)
	}
	if record.Failure, err = decodeFailure(failureRaw); err != nil {
		return deliveryRecord{}, err
	}
	if record.NextEligibleAt, _, err = parseNullableTime(nextEligible); err != nil {
		return deliveryRecord{}, err
	}
	if record.ClaimExpiresAt, _, err = parseNullableTime(claimExpires); err != nil {
		return deliveryRecord{}, err
	}
	if record.StartedAt, _, err = parseNullableTime(started); err != nil {
		return deliveryRecord{}, err
	}
	if record.SettledAt, _, err = parseNullableTime(settled); err != nil {
		return deliveryRecord{}, err
	}
	if record.CreatedAt, _, err = parseNullableTime(created); err != nil {
		return deliveryRecord{}, err
	}
	if record.UpdatedAt, _, err = parseNullableTime(updated); err != nil {
		return deliveryRecord{}, err
	}
	if err := validateRecordShape(record); err != nil {
		return deliveryRecord{}, err
	}
	return record, nil
}

func validateRecordShape(record deliveryRecord) error {
	conflict := func(detail string) error {
		return fmt.Errorf("%w: persisted delivery %s %s", ErrConflict, record.DeliveryID, detail)
	}
	if _, err := uuid.Parse(record.DeliveryID); err != nil {
		return conflict("has invalid delivery identity")
	}
	if _, err := uuid.Parse(record.EventID); err != nil {
		return conflict("has invalid event identity")
	}
	if _, err := uuid.Parse(record.RunID); err != nil {
		return conflict("has invalid run identity")
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) {
		return conflict("has invalid durable timestamps")
	}
	if record.claimToken != "" {
		if _, err := uuid.Parse(record.claimToken); err != nil {
			return conflict("has invalid claim token")
		}
	}
	if record.ActiveSessionID != "" {
		if _, err := uuid.Parse(record.ActiveSessionID); err != nil {
			return conflict("has invalid active session identity")
		}
	}
	claimClear := record.claimToken == "" && record.ClaimExpiresAt.IsZero() && record.ActiveSessionID == ""
	switch record.Status {
	case StatusPending:
		if record.NextEligibleAt.IsZero() || !claimClear || !record.SettledAt.IsZero() || record.Failure != nil || record.ReasonCode != "" {
			return conflict("has invalid pending shape")
		}
	case StatusInProgress:
		if !record.NextEligibleAt.IsZero() || record.claimToken == "" || record.ClaimVersion <= 0 ||
			record.ClaimExpiresAt.IsZero() || record.StartedAt.IsZero() || !record.ClaimExpiresAt.After(record.StartedAt) ||
			!record.SettledAt.IsZero() || record.Failure != nil || record.ReasonCode != "" {
			return conflict("has invalid in-progress shape")
		}
		if record.ActiveSessionID != "" && record.SubscriberClass != SubscriberAgent {
			return conflict("binds an agent session to a node obligation")
		}
	case StatusFailed:
		if record.RetryCount <= 0 || record.NextEligibleAt.IsZero() || !claimClear || !record.SettledAt.IsZero() || record.Failure == nil {
			return conflict("has invalid retry-scheduled shape")
		}
	case StatusDelivered:
		if !record.NextEligibleAt.IsZero() || !claimClear || record.SettledAt.IsZero() || record.Failure != nil || record.ReasonCode != "" {
			return conflict("has invalid delivered shape")
		}
	case StatusDeadLetter:
		if !record.NextEligibleAt.IsZero() || !claimClear || record.SettledAt.IsZero() || strings.TrimSpace(record.ReasonCode) == "" || record.Failure == nil {
			return conflict("has invalid dead-letter shape")
		}
	default:
		return conflict("has unknown lifecycle state")
	}
	return nil
}

func (a *Adapter) databaseNow(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (time.Time, error) {
	query := `SELECT CURRENT_TIMESTAMP`
	var raw any
	if err := q.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("read delivery database time: %w", err)
	}
	now, ok, err := parseNullableTime(raw)
	if err != nil || !ok {
		return time.Time{}, fmt.Errorf("read delivery database time: %w", err)
	}
	return now, nil
}

func encodeRoute(route events.DeliveryRoute) ([]byte, []byte, []byte, error) {
	route = route.Normalized()
	target, err := json.Marshal(route.Target)
	if err != nil {
		return nil, nil, nil, err
	}
	deliveryContext, err := json.Marshal(route.Context)
	if err != nil {
		return nil, nil, nil, err
	}
	projection, err := json.Marshal(route.PayloadProjection)
	if err != nil {
		return nil, nil, nil, err
	}
	return target, deliveryContext, projection, nil
}

func decodeRoute(class SubscriberClass, subscriberID string, targetRaw, contextRaw, projectionRaw []byte) (events.DeliveryRoute, error) {
	var target events.RouteIdentity
	var deliveryContext events.DeliveryContext
	var projection events.DeliveryPayloadProjection
	if err := json.Unmarshal(targetRaw, &target); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery target: %w", err)
	}
	if err := json.Unmarshal(contextRaw, &deliveryContext); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery context: %w", err)
	}
	if err := json.Unmarshal(projectionRaw, &projection); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery projection: %w", err)
	}
	return events.DeliveryRoute{SubscriberType: string(class), SubscriberID: subscriberID, Target: target, Context: deliveryContext, PayloadProjection: projection}.Normalized(), nil
}

func encodeFailure(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return "", fmt.Errorf("encode delivery failure: %w", err)
	}
	return string(raw), nil
}

func decodeFailure(raw []byte) (*runtimefailures.Envelope, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "" || string(raw) == "null" {
		return nil, nil
	}
	var failure runtimefailures.Envelope
	if err := json.Unmarshal(raw, &failure); err != nil {
		return nil, fmt.Errorf("decode delivery failure: %w", err)
	}
	return runtimefailures.CloneEnvelope(&failure), nil
}

func parseNullableTime(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	var value string
	switch typed := raw.(type) {
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed.UTC(), true, nil
	case string:
		value = typed
	case []byte:
		value = string(typed)
	default:
		return time.Time{}, false, fmt.Errorf("delivery timestamp has unsupported type %T", raw)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("parse delivery timestamp %q", value)
}
