// Package delivery owns the private SQL adapter for executable delivery
// lifecycle persistence. Runtime packages define the typed lifecycle facts;
// only selected-store operations can construct this adapter or provide it a
// transaction.
package delivery

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
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	. "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"

	postgresDatabaseNowExpression = `clock_timestamp()`
	sqliteDatabaseNowExpression   = `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`
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

func (a *Adapter) Dialect() Dialect {
	if a == nil {
		return ""
	}
	return a.dialect
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

func (a *Adapter) CommitInitial(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority ExecutionAuthority) ([]DurableHandoffProof, error) {
	if tx == nil {
		return nil, fmt.Errorf("delivery initial commit transaction is required")
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return nil, err
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil, nil
	}
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	proofs := make([]DurableHandoffProof, 0, len(routes))
	for _, route := range routes {
		obligation, err := NewObligation(eventID, runID, route, authority)
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

// ActivateNormalAuthority is the crash/replacement handoff for nonterminal
// normal-runtime work in one exact bundle source. Startup ownership fences the
// predecessor before this selected-store operation is invoked.
func (a *Adapter) ActivateNormalAuthority(ctx context.Context, tx *sql.Tx, authority ExecutionAuthority) error {
	if tx == nil {
		return fmt.Errorf("delivery authority activation transaction is required")
	}
	if authority.Kind() != ExecutionAuthorityNormalRuntime {
		return fmt.Errorf("delivery authority activation requires normal runtime authority")
	}
	if err := authority.Validate(); err != nil {
		return err
	}
	bundleHash, bundleSource := authority.BundleSource().StorageValues()
	query := `
		UPDATE event_deliveries
		SET execution_authority_id=$1,
			execution_authority_generation=$2,
			updated_at=GREATEST(updated_at, CURRENT_TIMESTAMP)
		WHERE execution_authority_kind='normal_runtime'
		  AND authority_bundle_hash=$3
		  AND authority_bundle_source=$4
		  AND (execution_authority_id<>$1 OR execution_authority_generation<>$2)
		  AND status IN ('pending','failed','in_progress')`
	args := []any{authority.ExecutionID(), authority.Generation(), bundleHash, bundleSource}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_deliveries
			SET execution_authority_id=?,
				execution_authority_generation=?,
				updated_at=CASE
					WHEN CURRENT_TIMESTAMP > updated_at THEN CURRENT_TIMESTAMP
					ELSE updated_at
				END
			WHERE execution_authority_kind='normal_runtime'
			  AND authority_bundle_hash=?
			  AND authority_bundle_source=?
			  AND (execution_authority_id<>? OR execution_authority_generation<>?)
			  AND status IN ('pending','failed','in_progress')`
		args = append(args, authority.ExecutionID(), authority.Generation())
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("activate normal delivery execution authority: %w", err)
	}
	return nil
}

func (a *Adapter) insertExactObligation(ctx context.Context, tx *sql.Tx, obligation Obligation) (DurableHandoffProof, error) {
	target, deliveryContext, projection, connectClaim, err := encodeRoute(obligation.Route())
	if err != nil {
		return DurableHandoffProof{}, err
	}
	agentFields, err := deliveryRouteAgentStorageFields(obligation.Route())
	if err != nil {
		return DurableHandoffProof{}, err
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return DurableHandoffProof{}, err
	}
	bundleHash, bundleSource := obligation.Authority().BundleSource().StorageValues()
	var selectedExecutionID, selectedForkRunID any
	var selectedGeneration any
	if obligation.Authority().Kind() == ExecutionAuthoritySelectedContractFork {
		selectedExecutionID = obligation.Authority().ExecutionID()
		selectedForkRunID = obligation.Authority().ForkRunID()
		selectedGeneration = obligation.Authority().Generation()
	}
	query := `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
			execution_authority_kind, authority_bundle_hash, authority_bundle_source,
			execution_authority_id, execution_authority_generation,
			selected_execution_id, selected_fork_run_id, selected_execution_generation,
			status, retry_count, max_retries, next_eligible_at, claim_version,
			created_at, updated_at
		) VALUES (
			$1::uuid, NULLIF($2, '')::uuid, $3::uuid, $4, $5, $6,
			$7, $8, $9, $10, $11, $12,
			$13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
			$17, $18, $19, $20, $21,
			NULLIF($22, '')::uuid, NULLIF($23, '')::uuid, $24,
			'pending', 0, $25, $26, 0, $26, $26
		) ON CONFLICT (event_id, route_identity) DO NOTHING`
	args := []any{
		obligation.DeliveryID(), obligation.RunID(), obligation.EventID(), events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()),
		string(obligation.SubscriberClass()), obligation.SubscriberID(),
		agentFields.NameOwner, agentFields.NameSource, agentFields.RoutePresence,
		agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath,
		string(target), string(deliveryContext), string(projection), string(connectClaim),
		string(obligation.Authority().Kind()), bundleHash, bundleSource, obligation.Authority().ExecutionID(), obligation.Authority().Generation(),
		selectedExecutionID, selectedForkRunID, selectedGeneration, obligation.MaxRetries(), now,
	}
	if a.dialect == DialectSQLite {
		query = `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
				agent_name_owner, agent_name_source, agent_route_presence,
				agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
				delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
				execution_authority_kind, authority_bundle_hash, authority_bundle_source,
				execution_authority_id, execution_authority_generation,
				selected_execution_id, selected_fork_run_id, selected_execution_generation,
				status, retry_count, max_retries, next_eligible_at, claim_version,
				created_at, updated_at
			) VALUES (
				?1, NULLIF(?2, ''), ?3, ?4, ?5, ?6,
				?7, ?8, ?9, ?10, ?11, ?12,
				?13, ?14, ?15, ?16,
				?17, ?18, ?19, ?20, ?21,
				NULLIF(?22, ''), NULLIF(?23, ''), ?24,
				'pending', 0, ?25, ?26, 0, ?27, ?28
			)
			ON CONFLICT(event_id, route_identity) DO NOTHING`
		args = []any{
			obligation.DeliveryID(), obligation.RunID(), obligation.EventID(), events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()),
			string(obligation.SubscriberClass()), obligation.SubscriberID(),
			agentFields.NameOwner, agentFields.NameSource, agentFields.RoutePresence,
			agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath,
			string(target), string(deliveryContext), string(projection), string(connectClaim),
			string(obligation.Authority().Kind()), bundleHash, bundleSource, obligation.Authority().ExecutionID(), obligation.Authority().Generation(),
			selectedExecutionID, selectedForkRunID, selectedGeneration, obligation.MaxRetries(), now, now, now,
		}
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
		record.MaxRetries != obligation.MaxRetries() || !events.SameDeliveryRouteIdentity(record.Route, obligation.Route()) ||
		!record.Authority.Equal(obligation.Authority()) {
		return DurableHandoffProof{}, fmt.Errorf("%w: delivery obligation duplicate does not exactly match admitted route", ErrConflict)
	}
	if inserted == 0 && (record.Status != StatusPending || record.RetryCount != 0 || record.ClaimVersion != 0) {
		return DurableHandoffProof{}, fmt.Errorf("%w: delivery obligation replay conflicts with existing lifecycle", ErrConflict)
	}
	return AdmitDurableHandoffProof(obligation.DeliveryID(), obligation.EventID(), events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()), obligation.Authority())
}

func (a *Adapter) ClaimExactResult(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, authority ExecutionAuthority, event events.Event, route events.DeliveryRoute, leaseTTL time.Duration) (ClaimResult, error) {
	if tx == nil {
		return ClaimResult{}, fmt.Errorf("delivery claim transaction is required")
	}
	if err := authority.Validate(); err != nil {
		return ClaimResult{}, err
	}
	identity, err := route.Identity()
	if err != nil {
		return ClaimResult{}, err
	}
	record, err := a.loadByEventAndRoute(ctx, tx, event.ID(), identity, true)
	if errors.Is(err, ErrNotFound) {
		return ClaimResult{Disposition: ClaimAbsent}, nil
	}
	if errors.Is(err, ErrConflict) {
		return ClaimResult{Disposition: ClaimInvariantInvalid, Invariant: err}, nil
	}
	if err != nil {
		return ClaimResult{}, err
	}
	result := ClaimResult{Snapshot: record.Snapshot}
	if !events.SameDeliveryRouteIdentity(record.Route, route) {
		result.Disposition = ClaimInvariantInvalid
		result.Invariant = fmt.Errorf("%w: delivery route does not exactly match durable obligation", ErrConflict)
		return result, nil
	}
	if !record.Authority.Equal(authority) {
		result.Disposition = ClaimWrongAuthority
		return result, nil
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return ClaimResult{}, err
	}
	previous := ClaimDisposition("")
	switch record.Status {
	case StatusPending:
	case StatusFailed:
		if record.RetryCount > record.MaxRetries {
			result.Disposition = ClaimInvariantInvalid
			result.Invariant = fmt.Errorf("%w: retry count exceeds policy", ErrConflict)
			return result, nil
		}
		if record.NextEligibleAt.After(now) {
			result.Disposition = ClaimDeferred
			result.Snapshot = snapshotAt(record, now)
			return result, nil
		}
	case StatusInProgress:
		if record.ClaimExpiresAt.IsZero() {
			result.Disposition = ClaimInvariantInvalid
			result.Invariant = fmt.Errorf("%w: active delivery lease is missing", ErrConflict)
			return result, nil
		}
		if record.ClaimExpiresAt.After(now) {
			result.Disposition = ClaimBusy
			result.Snapshot = snapshotAt(record, now)
			return result, nil
		}
		previous = ClaimReclaimable
	case StatusDelivered, StatusDeadLetter:
		result.Disposition = ClaimTerminal
		result.Snapshot = snapshotAt(record, now)
		return result, nil
	default:
		result.Disposition = ClaimInvariantInvalid
		result.Invariant = fmt.Errorf("%w: unknown delivery status", ErrConflict)
		return result, nil
	}
	claimed, err := a.claimLocked(ctx, tx, story, record, leaseTTL)
	if err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{
		Disposition: ClaimAcquired,
		Previous:    previous,
		Snapshot:    claimed.Snapshot,
		Claimed:     claimed,
	}, nil
}

func (a *Adapter) ScanContinuations(ctx context.Context, tx *sql.Tx, authority ExecutionAuthority, cursor ContinuationCursor, limit int) (ContinuationPage, error) {
	if tx == nil {
		return ContinuationPage{}, fmt.Errorf("delivery continuation scan transaction is required")
	}
	if err := authority.Validate(); err != nil {
		return ContinuationPage{}, err
	}
	if limit <= 0 || limit > 500 {
		return ContinuationPage{}, fmt.Errorf("delivery continuation scan limit must be between 1 and 500")
	}
	cursorID := executionAuthorityCursorID(authority)
	cursorAuthorityID, cursorCreatedAt, cursorDeliveryID, cursorStarted := cursor.Position()
	if cursorStarted && cursorAuthorityID != cursorID {
		return ContinuationPage{}, fmt.Errorf("delivery continuation cursor authority mismatch")
	}
	bundleHash, bundleSource := authority.BundleSource().StorageValues()
	query := `
		SELECT d.delivery_id::text, d.created_at
		FROM event_deliveries d
		WHERE d.execution_authority_kind=$1
		  AND d.authority_bundle_hash=$2
		  AND d.authority_bundle_source=$3
		  AND d.execution_authority_id=$4
		  AND d.execution_authority_generation=$5
		  AND d.status IN ('pending','failed','in_progress')
		  AND d.continuation_handoff_at IS NOT NULL
		  AND ($6::timestamptz IS NULL OR d.created_at>$6 OR (d.created_at=$6 AND d.delivery_id>$7::uuid))
		ORDER BY d.created_at, d.delivery_id
		LIMIT $8`
	args := []any{
		string(authority.Kind()), bundleHash, bundleSource, authority.ExecutionID(), authority.Generation(),
		nil, nil, limit + 1,
	}
	if cursorStarted {
		args[5] = cursorCreatedAt
		args[6] = cursorDeliveryID
	}
	if a.dialect == DialectSQLite {
		query = `
			SELECT d.delivery_id, d.created_at
			FROM event_deliveries d
			WHERE d.execution_authority_kind=?
			  AND d.authority_bundle_hash=?
			  AND d.authority_bundle_source=?
			  AND d.execution_authority_id=?
			  AND d.execution_authority_generation=?
			  AND d.status IN ('pending','failed','in_progress')
			  AND d.continuation_handoff_at IS NOT NULL
			  AND (? IS NULL OR d.created_at>? OR (d.created_at=? AND d.delivery_id>?))
			ORDER BY d.created_at, d.delivery_id
			LIMIT ?`
		var cursorTime, cursorTime2, cursorTime3 any
		var cursorDelivery any
		if cursorStarted {
			cursorTime, cursorTime2, cursorTime3 = cursorCreatedAt, cursorCreatedAt, cursorCreatedAt
			cursorDelivery = cursorDeliveryID
		}
		args = []any{
			string(authority.Kind()), bundleHash, bundleSource, authority.ExecutionID(), authority.Generation(),
			cursorTime, cursorTime2, cursorTime3, cursorDelivery, limit + 1,
		}
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return ContinuationPage{}, fmt.Errorf("scan delivery continuations: %w", err)
	}
	type reference struct {
		id        string
		createdAt time.Time
	}
	refs := make([]reference, 0, limit+1)
	for rows.Next() {
		var id string
		var createdRaw any
		if err := rows.Scan(&id, &createdRaw); err != nil {
			_ = rows.Close()
			return ContinuationPage{}, fmt.Errorf("scan delivery continuation reference: %w", err)
		}
		createdAt, present, err := parseNullableTime(createdRaw)
		if err != nil || !present {
			_ = rows.Close()
			return ContinuationPage{}, fmt.Errorf("%w: delivery continuation created_at is invalid", ErrConflict)
		}
		refs = append(refs, reference{id: strings.TrimSpace(id), createdAt: createdAt})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return ContinuationPage{}, fmt.Errorf("read delivery continuation references: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ContinuationPage{}, fmt.Errorf("close delivery continuation references: %w", err)
	}
	page := ContinuationPage{Exhausted: len(refs) <= limit}
	if len(refs) > limit {
		refs = refs[:limit]
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return ContinuationPage{}, err
	}
	page.Items = make([]ContinuationItem, 0, len(refs))
	for _, ref := range refs {
		item := ContinuationItem{DeliveryID: ref.id}
		record, loadErr := a.loadByID(ctx, tx, ref.id, false)
		if errors.Is(loadErr, ErrNotFound) {
			item.Disposition = ClaimAbsent
			page.Items = append(page.Items, item)
			continue
		}
		if errors.Is(loadErr, ErrConflict) {
			item.Disposition = ClaimInvariantInvalid
			item.Invariant = loadErr
			page.Items = append(page.Items, item)
			continue
		}
		if loadErr != nil {
			return ContinuationPage{}, loadErr
		}
		item.Snapshot = snapshotAt(record, now)
		if !record.Authority.Equal(authority) {
			item.Disposition = ClaimWrongAuthority
		} else {
			item.Disposition = continuationDisposition(record, now)
			item.Wake = continuationWake(record, item.Disposition, now)
		}
		page.Items = append(page.Items, item)
	}
	if len(refs) > 0 {
		last := refs[len(refs)-1]
		page.Next, err = AdmitContinuationCursor(cursorID, last.createdAt, last.id)
		if err != nil {
			return ContinuationPage{}, err
		}
	} else {
		page.Next = cursor
	}
	return page, nil
}

func (a *Adapter) ObserveContinuation(
	ctx context.Context,
	q queryer,
	authority ExecutionAuthority,
	deliveryID string,
) (ContinuationObservation, error) {
	if err := authority.Validate(); err != nil {
		return ContinuationObservation{}, err
	}
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return ContinuationObservation{}, errors.New("delivery continuation identity is required")
	}
	record, err := a.loadByID(ctx, q, deliveryID, false)
	if errors.Is(err, ErrNotFound) {
		return ContinuationObservation{DeliveryID: deliveryID, Disposition: ClaimAbsent}, nil
	}
	if errors.Is(err, ErrConflict) {
		return ContinuationObservation{
			DeliveryID: deliveryID, Disposition: ClaimInvariantInvalid, Invariant: err,
		}, nil
	}
	if err != nil {
		return ContinuationObservation{}, err
	}
	if !record.Authority.Equal(authority) {
		return ContinuationObservation{DeliveryID: deliveryID, Disposition: ClaimWrongAuthority}, nil
	}
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return ContinuationObservation{}, err
	}
	disposition := continuationDisposition(record, now)
	return ContinuationObservation{
		DeliveryID:  deliveryID,
		Disposition: disposition,
		Wake:        continuationWake(record, disposition, now),
	}, nil
}

func (a *Adapter) InspectRecovery(
	ctx context.Context,
	q queryer,
	source runtimecorrelation.BundleSourceFact,
) (RecoveryInventory, error) {
	if err := source.Validate(); err != nil {
		return RecoveryInventory{}, err
	}
	bundleHash, bundleSource := source.StorageValues()
	query := `
		SELECT
			COUNT(*) FILTER (WHERE status='pending'),
			COUNT(*) FILTER (WHERE status='failed'),
			COUNT(*) FILTER (WHERE status='in_progress')
		FROM event_deliveries
		WHERE execution_authority_kind='normal_runtime'
		  AND authority_bundle_hash=$1
		  AND authority_bundle_source=$2
		  AND status IN ('pending','failed','in_progress')`
	args := []any{bundleHash, bundleSource}
	if a.dialect == DialectSQLite {
		query = `
			SELECT
				SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END)
			FROM event_deliveries
			WHERE execution_authority_kind='normal_runtime'
			  AND authority_bundle_hash=?
			  AND authority_bundle_source=?
			  AND status IN ('pending','failed','in_progress')`
	}
	var pending, failed, inProgress sql.NullInt64
	if err := q.QueryRowContext(ctx, query, args...).Scan(&pending, &failed, &inProgress); err != nil {
		return RecoveryInventory{}, fmt.Errorf("inspect delivery recovery inventory: %w", err)
	}
	return RecoveryInventory{
		Pending: int(pending.Int64), Failed: int(failed.Int64), InProgress: int(inProgress.Int64),
	}, nil
}

// CommitPipelineHandoff records the durable transition from event-level
// processing to executable delivery continuation ownership.
func (a *Adapter) CommitPipelineHandoff(ctx context.Context, tx *sql.Tx, eventID string) error {
	if tx == nil {
		return errors.New("delivery continuation handoff transaction is required")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return errors.New("delivery continuation handoff event identity is required")
	}
	query := `
		UPDATE event_deliveries
		SET continuation_handoff_at = COALESCE(continuation_handoff_at, CURRENT_TIMESTAMP)
		WHERE event_id = $1::uuid`
	args := []any{eventID}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_deliveries
			SET continuation_handoff_at = COALESCE(continuation_handoff_at, CURRENT_TIMESTAMP)
			WHERE event_id = ?`
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("transfer delivery continuations after pipeline acknowledgement: %w", err)
	}
	return nil
}

func executionAuthorityCursorID(authority ExecutionAuthority) string {
	bundleHash, bundleSource := authority.BundleSource().StorageValues()
	return strings.Join([]string{
		string(authority.Kind()), bundleHash, bundleSource, authority.ExecutionID(),
		authority.ForkRunID(), fmt.Sprint(authority.Generation()),
	}, "\x00")
}

func continuationDisposition(record deliveryRecord, now time.Time) ClaimDisposition {
	switch record.Status {
	case StatusPending:
		return ClaimAcquired
	case StatusFailed:
		if record.NextEligibleAt.After(now) {
			return ClaimDeferred
		}
		return ClaimAcquired
	case StatusInProgress:
		if record.ClaimExpiresAt.After(now) {
			return ClaimBusy
		}
		return ClaimReclaimable
	case StatusDelivered, StatusDeadLetter:
		return ClaimTerminal
	default:
		return ClaimInvariantInvalid
	}
}

func continuationWake(record deliveryRecord, disposition ClaimDisposition, now time.Time) ContinuationWake {
	switch disposition {
	case ClaimDeferred:
		return AdmitContinuationWake(record.NextEligibleAt.Sub(now))
	case ClaimBusy:
		return AdmitContinuationWake(record.ClaimExpiresAt.Sub(now))
	default:
		return ContinuationWake{}
	}
}

func (a *Adapter) SnapshotExact(ctx context.Context, q queryer, event events.Event, route events.DeliveryRoute) (Snapshot, error) {
	deliveryID, err := DeliveryID(event.ID(), route)
	if err != nil {
		return Snapshot{}, err
	}
	return a.Snapshot(ctx, q, deliveryID)
}

func (a *Adapter) claimLocked(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, record deliveryRecord, leaseTTL time.Duration) (ClaimedObligation, error) {
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
			return ClaimedObligation{}, fmt.Errorf("%w: deferred delivery reached claim mutation", ErrConflict)
		}
	case StatusInProgress:
		if record.ClaimExpiresAt.IsZero() || record.ClaimExpiresAt.After(now) {
			return ClaimedObligation{}, fmt.Errorf("%w: busy delivery reached claim mutation", ErrConflict)
		}
	default:
		return ClaimedObligation{}, fmt.Errorf("%w: terminal delivery reached claim mutation", ErrConflict)
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
	claim, err := AdmitPersistedClaim(record.DeliveryID, record.RunID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), token, version, record.SubscriberClass, record.SubscriberID)
	if err != nil {
		return ClaimedObligation{}, err
	}
	claimed, err := a.loadByID(ctx, tx, record.DeliveryID, false)
	if err != nil {
		return ClaimedObligation{}, err
	}
	if err := a.recordTransition(ctx, story, claimed, "in_progress", nil, now); err != nil {
		return ClaimedObligation{}, err
	}
	return ClaimedObligation{Snapshot: claimed.Snapshot, Claim: claim}, nil
}

func (a *Adapter) BindAgentSession(ctx context.Context, tx *sql.Tx, claim Claim, sessionID string) (Snapshot, error) {
	if tx == nil || claim.Validate() != nil {
		return Snapshot{}, fmt.Errorf("delivery session binding requires a current claim")
	}
	sessionID = strings.TrimSpace(sessionID)
	if _, err := uuid.Parse(sessionID); err != nil {
		return Snapshot{}, fmt.Errorf("delivery session id: %w", err)
	}
	record, _, err := a.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return Snapshot{}, err
	}
	if record.SubscriberClass != SubscriberAgent {
		return Snapshot{}, fmt.Errorf("%w: node delivery cannot bind an agent session", ErrConflict)
	}
	fields, err := record.Route.AgentIdentity.StorageFields()
	if err != nil {
		return Snapshot{}, fmt.Errorf("bind delivery agent identity: %w", err)
	}
	query := `
		UPDATE event_delivery_attempts
		SET active_session_id = $1::uuid, session_delivery_id = $4::uuid,
			session_run_id = $2::uuid, session_subscriber_type = 'agent', session_agent_id = $3,
			session_agent_name_owner = $7, session_agent_name_source = $8,
			session_agent_route_presence = $9, session_agent_flow_scope_key = $10,
			session_agent_flow_instance_id = $11, session_agent_flow_instance_path = $12
		WHERE delivery_id = $4::uuid AND claim_version = $5 AND claim_token = $6::uuid AND open_marker = TRUE
		  AND EXISTS (
			SELECT 1 FROM agent_sessions session
			WHERE session.session_id = $1::uuid AND session.run_id = $2::uuid
			  AND session.agent_id = $3
			  AND session.agent_name_owner = $7 AND session.agent_name_source = $8
			  AND session.agent_route_presence = $9 AND session.flow_scope_key = $10
			  AND session.flow_instance_id = $11 AND session.flow_instance = $12
			  AND session.status = 'active'
		  )`
	args := []any{
		sessionID, record.RunID, record.SubscriberID, claim.DeliveryID(), claim.Version(), claim.PersistenceToken(),
		fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
	}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET active_session_id = ?1, session_delivery_id = ?4,
				session_run_id = ?2, session_subscriber_type = 'agent', session_agent_id = ?3,
				session_agent_name_owner = ?7, session_agent_name_source = ?8,
				session_agent_route_presence = ?9, session_agent_flow_scope_key = ?10,
				session_agent_flow_instance_id = ?11, session_agent_flow_instance_path = ?12
			WHERE delivery_id = ?4 AND claim_version = ?5 AND claim_token = ?6 AND open_marker = TRUE
			  AND EXISTS (
				SELECT 1 FROM agent_sessions session
				WHERE session.session_id = ?1 AND session.run_id = ?2
				  AND session.agent_id = ?3
				  AND session.agent_name_owner = ?7 AND session.agent_name_source = ?8
				  AND session.agent_route_presence = ?9 AND session.flow_scope_key = ?10
				  AND session.flow_instance_id = ?11 AND session.flow_instance = ?12
				  AND session.status = 'active'
			  )`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("bind delivery agent session: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery session binding lost claim", ErrConflict)
	}
	updated, err := a.loadByID(ctx, tx, claim.DeliveryID(), false)
	return updated.Snapshot, err
}

func (a *Adapter) RenewClaim(ctx context.Context, tx *sql.Tx, claim Claim, leaseTTL time.Duration) (Snapshot, error) {
	if tx == nil || claim.Validate() != nil {
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
	args := []any{expiresAt, claim.DeliveryID(), claim.Version(), claim.PersistenceToken(), now}
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
	deliveryArgs := []any{now, claim.DeliveryID(), claim.Version()}
	if a.dialect == DialectSQLite {
		deliveryQuery = `
			UPDATE event_deliveries
			SET updated_at = ?
			WHERE delivery_id = ? AND status = 'in_progress' AND claim_version = ?
			  AND current_attempt_version = ? AND current_attempt_open = TRUE`
		deliveryArgs = []any{now, claim.DeliveryID(), claim.Version(), claim.Version()}
	}
	deliveryResult, err := tx.ExecContext(ctx, deliveryQuery, deliveryArgs...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("record delivery claim renewal time: %w", err)
	}
	if rows, _ := deliveryResult.RowsAffected(); rows != 1 {
		return Snapshot{}, fmt.Errorf("%w: delivery claim renewal lost lifecycle owner", ErrConflict)
	}
	updated, err := a.loadByID(ctx, tx, claim.DeliveryID(), false)
	return updated.Snapshot, err
}

func (a *Adapter) SettleSuccess(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, claim Claim, sideEffects []string, duration time.Duration) (Snapshot, error) {
	return a.settle(ctx, tx, story, claim, Settlement{Disposition: "success", SideEffects: sideEffects, Duration: duration})
}

func (a *Adapter) SettleFailure(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, claim Claim, settlement Settlement) (Snapshot, error) {
	if settlement.Disposition != FailureRetry && settlement.Disposition != FailureDeadLetter {
		return Snapshot{}, fmt.Errorf("delivery failure disposition %q is invalid", settlement.Disposition)
	}
	if settlement.Failure == nil {
		return Snapshot{}, fmt.Errorf("delivery failure settlement requires a failure envelope")
	}
	if settlement.Disposition == FailureDeadLetter && strings.TrimSpace(settlement.ReasonCode) == "" {
		return Snapshot{}, fmt.Errorf("terminal delivery failure requires a reason code")
	}
	return a.settle(ctx, tx, story, claim, settlement)
}

func (a *Adapter) settle(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, claim Claim, settlement Settlement) (Snapshot, error) {
	if tx == nil {
		return Snapshot{}, fmt.Errorf("delivery settlement transaction is required")
	}
	if claim.Validate() != nil {
		return Snapshot{}, fmt.Errorf("delivery settlement requires a current claim (delivery=%q run=%q route=%t token=%t version=%d class=%q subscriber=%q)", claim.DeliveryID(), claim.RunID(), claim.RouteIdentity() != "", claim.PersistenceToken() != "", claim.Version(), claim.SubscriberClass(), claim.SubscriberID())
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
	args := []any{string(status), retryCount, nextEligible, reason, failureRaw, now, claim.DeliveryID(), claim.Version()}
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
		args = []any{string(status), retryCount, nextEligible, reason, failureRaw, string(status), now, now, claim.DeliveryID(), claim.Version(), claim.Version()}
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
	updated, err := a.loadByID(ctx, tx, claim.DeliveryID(), false)
	if err != nil {
		return Snapshot{}, err
	}
	if err := a.recordTransition(ctx, story, updated, transition, effectiveFailure, now); err != nil {
		return Snapshot{}, err
	}
	return snapshotAt(updated, now), nil
}

func (a *Adapter) retryExhaustedFailure(ctx context.Context, tx *sql.Tx, record deliveryRecord, claim Claim, current *runtimefailures.Envelope) (*runtimefailures.Envelope, error) {
	outcomes, err := a.Outcomes(ctx, tx, claim.DeliveryID())
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
	if err := appendFailure(claim.Version(), current); err != nil {
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
	return AdmitDurableHandoffProof(record.DeliveryID, record.EventID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), record.Authority)
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
	if err := rows.Err(); err != nil {
		return RunSummary{}, err
	}
	return summary, summary.Validate()
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

// RunDiagnosticCounts aggregates run-debug delivery counts in storage without
// hydrating each canonical snapshot.
func (a *Adapter) RunDiagnosticCounts(ctx context.Context, q queryer, runID string) ([]RunDiagnosticCount, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("delivery run diagnostic counts run id: %w", err)
	}
	query := `
		SELECT subscriber_id, status, COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		GROUP BY subscriber_id, status
		ORDER BY subscriber_id, status`
	if a.dialect == DialectSQLite {
		query = `
			SELECT subscriber_id, status, COUNT(*)
			FROM event_deliveries
			WHERE run_id = ?
			GROUP BY subscriber_id, status
			ORDER BY subscriber_id, status`
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("select delivery run diagnostic counts: %w", err)
	}
	defer rows.Close()
	counts := []RunDiagnosticCount{}
	for rows.Next() {
		var item RunDiagnosticCount
		var rawStatus string
		if err := rows.Scan(&item.SubscriberID, &rawStatus, &item.Count); err != nil {
			return nil, fmt.Errorf("scan delivery run diagnostic count: %w", err)
		}
		item.SubscriberID = strings.TrimSpace(item.SubscriberID)
		if item.SubscriberID == "" || item.Count <= 0 {
			return nil, fmt.Errorf("%w: delivery run diagnostic count violates structural policy", ErrConflict)
		}
		item.Status, err = ParseStatus(rawStatus)
		if err != nil {
			return nil, err
		}
		counts = append(counts, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read delivery run diagnostic counts: %w", err)
	}
	return counts, nil
}

// RunDiagnosticFailures applies failure classification, ordering, and limit
// before canonical snapshot hydration. Diagnostic occurrence time is the
// lifecycle owner's settled -> updated -> created fallback.
func (a *Adapter) RunDiagnosticFailures(ctx context.Context, q queryer, runID string, limit int) ([]Snapshot, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("delivery run diagnostic failures run id: %w", err)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("delivery run diagnostic failures limit must be positive")
	}
	query := `
		SELECT delivery_id::text
		FROM event_deliveries
		WHERE run_id = $1::uuid AND status IN ('failed', 'dead_letter')
		ORDER BY COALESCE(settled_at, updated_at, created_at) DESC, delivery_id DESC
		LIMIT $2`
	if a.dialect == DialectSQLite {
		query = `
			SELECT delivery_id
			FROM event_deliveries
			WHERE run_id = ? AND status IN ('failed', 'dead_letter')
			ORDER BY COALESCE(settled_at, updated_at, created_at) DESC, delivery_id DESC
			LIMIT ?`
	}
	return a.snapshotsByIDQuery(ctx, q, query, runID, limit)
}

// RunTraceReferencePage applies trace filtering, keyset ordering, and the
// limit before canonical snapshot hydration. This keeps executable-delivery
// SQL private while bounding every run-debug page by its requested size.
func (a *Adapter) RunTraceReferencePage(ctx context.Context, q queryer, page RunTracePageQuery) (RunTraceReferencePage, error) {
	page.RunID = strings.TrimSpace(page.RunID)
	if _, err := uuid.Parse(page.RunID); err != nil {
		return RunTraceReferencePage{}, fmt.Errorf("delivery run trace page run id: %w", err)
	}
	if page.Limit <= 0 {
		return RunTraceReferencePage{}, fmt.Errorf("delivery run trace page limit must be positive")
	}
	if page.After != nil && (page.After.EventCreatedAt.IsZero() || strings.TrimSpace(page.After.EventID) == "") {
		return RunTraceReferencePage{}, fmt.Errorf("delivery run trace page cursor requires event time and id")
	}
	for _, status := range page.DeliveryStatuses {
		if _, err := ParseStatus(string(status)); err != nil {
			return RunTraceReferencePage{}, err
		}
	}
	for _, class := range page.SubscriberClasses {
		if _, err := ParseSubscriberClass(string(class)); err != nil {
			return RunTraceReferencePage{}, err
		}
	}

	type rawReference struct {
		eventID    string
		deliveryID string
		turnID     string
	}
	var (
		rows *sql.Rows
		err  error
	)
	if a.dialect == DialectPostgres {
		rows, err = a.queryPostgresRunTraceReferences(ctx, q, page)
	} else {
		rows, err = a.querySQLiteRunTraceReferences(ctx, q, page)
	}
	if err != nil {
		return RunTraceReferencePage{}, err
	}
	raw := make([]rawReference, 0, page.Limit+1)
	for rows.Next() {
		var reference rawReference
		if err := rows.Scan(&reference.eventID, &reference.deliveryID, &reference.turnID); err != nil {
			_ = rows.Close()
			return RunTraceReferencePage{}, fmt.Errorf("scan delivery run trace reference: %w", err)
		}
		reference.eventID = strings.TrimSpace(reference.eventID)
		reference.deliveryID = strings.TrimSpace(reference.deliveryID)
		reference.turnID = strings.TrimSpace(reference.turnID)
		raw = append(raw, reference)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return RunTraceReferencePage{}, fmt.Errorf("read delivery run trace references: %w", err)
	}
	if err := rows.Close(); err != nil {
		return RunTraceReferencePage{}, fmt.Errorf("close delivery run trace references: %w", err)
	}

	result := RunTraceReferencePage{HasMore: len(raw) > page.Limit}
	if result.HasMore {
		raw = raw[:page.Limit]
	}
	result.References = make([]RunTraceReference, len(raw))
	needsSnapshots := false
	for index, reference := range raw {
		result.References[index] = RunTraceReference{EventID: reference.eventID, TurnID: reference.turnID}
		needsSnapshots = needsSnapshots || reference.deliveryID != ""
	}
	if !needsSnapshots {
		return result, nil
	}

	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return RunTraceReferencePage{}, err
	}
	cache := map[string]Snapshot{}
	for index, reference := range raw {
		if reference.deliveryID == "" {
			continue
		}
		snapshot, ok := cache[reference.deliveryID]
		if !ok {
			record, err := a.loadByID(ctx, q, reference.deliveryID, false)
			if err != nil {
				return RunTraceReferencePage{}, err
			}
			snapshot = snapshotAt(record, now)
			if snapshot.EventID != reference.eventID || snapshot.RunID != page.RunID {
				return RunTraceReferencePage{}, fmt.Errorf("delivery run trace reference %s does not belong to event %s in run %s", reference.deliveryID, reference.eventID, page.RunID)
			}
			cache[reference.deliveryID] = snapshot
		}
		snapshotCopy := snapshot
		result.References[index].Delivery = &snapshotCopy
	}
	return result, nil
}

func (a *Adapter) queryPostgresRunTraceReferences(ctx context.Context, q queryer, page RunTracePageQuery) (*sql.Rows, error) {
	where := []string{"e.run_id = $1::uuid"}
	args := []any{page.RunID}
	addArg := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if page.After != nil {
		eventAt := addArg(page.After.EventCreatedAt.UTC())
		eventID := addArg(strings.TrimSpace(page.After.EventID))
		deliveryAt := addArg(traceNullableTimestamp(page.After.DeliveryCreatedAt))
		deliveryID := addArg(strings.TrimSpace(page.After.DeliveryID))
		turnAt := addArg(traceNullableTimestamp(page.After.TurnCreatedAt))
		turnID := addArg(strings.TrimSpace(page.After.TurnID))
		where = append(where, fmt.Sprintf(`(
			e.created_at,
			e.event_id::text,
			COALESCE(d.created_at, '-infinity'::timestamptz),
			COALESCE(d.delivery_id::text, ''),
			COALESCE(t.created_at, '-infinity'::timestamptz),
			COALESCE(t.turn_id::text, '')
		) > ($%d::timestamptz, $%d, $%d::timestamptz, $%d, $%d::timestamptz, $%d)`, eventAt, eventID, deliveryAt, deliveryID, turnAt, turnID))
	}
	watermark := `GREATEST(
		e.created_at,
		COALESCE(d.created_at, '-infinity'::timestamptz),
		COALESCE(d.started_at, '-infinity'::timestamptz),
		COALESCE(d.settled_at, '-infinity'::timestamptz),
		COALESCE(sess.updated_at, '-infinity'::timestamptz),
		COALESCE(t.created_at, '-infinity'::timestamptz)
	)`
	if page.Since != nil {
		where = append(where, fmt.Sprintf("%s > $%d::timestamptz", watermark, addArg(page.Since.UTC())))
	}
	if page.Until != nil {
		where = append(where, fmt.Sprintf("%s <= $%d::timestamptz", watermark, addArg(page.Until.UTC())))
	}
	addTextFilter := func(expression string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			placeholders = append(placeholders, fmt.Sprintf("$%d", addArg(value)))
		}
		where = append(where, expression+" IN ("+strings.Join(placeholders, ",")+")")
	}
	addTextFilter("e.event_name", page.EventNames)
	addTextFilter("e.entity_id::text", page.EntityIDs)
	statuses := make([]string, 0, len(page.DeliveryStatuses))
	for _, status := range page.DeliveryStatuses {
		statuses = append(statuses, string(status))
	}
	addTextFilter("d.status", statuses)
	addTextFilter("d.subscriber_id", page.SubscriberIDs)
	classes := make([]string, 0, len(page.SubscriberClasses))
	for _, class := range page.SubscriberClasses {
		classes = append(classes, string(class))
	}
	addTextFilter("d.subscriber_type", classes)
	if page.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	limit := addArg(page.Limit + 1)
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`
		WITH trace_sessions AS (
			SELECT session_id, run_id, updated_at FROM agent_sessions
			UNION ALL
			SELECT session_id, run_id, updated_at FROM agent_conversation_audits
		)
		SELECT e.event_id::text, COALESCE(d.delivery_id::text, ''), COALESCE(t.turn_id::text, '')
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		LEFT JOIN event_delivery_attempts current_attempt
			ON current_attempt.delivery_id = d.delivery_id
		   AND current_attempt.claim_version = d.current_attempt_version
		   AND current_attempt.open_marker = TRUE
		LEFT JOIN agent_turns t
			ON t.run_id = e.run_id
		   AND t.trigger_event_id = e.event_id
		   AND (d.delivery_id IS NULL OR (d.subscriber_type = 'agent' AND d.subscriber_id <> '' AND t.agent_id = d.subscriber_id))
		LEFT JOIN trace_sessions sess
			ON sess.session_id = COALESCE(t.session_id, current_attempt.active_session_id)
		   AND (sess.run_id = e.run_id OR sess.run_id IS NULL)
		WHERE %s
		ORDER BY e.created_at, e.event_id, d.created_at NULLS FIRST, d.delivery_id NULLS FIRST, t.created_at NULLS FIRST, t.turn_id NULLS FIRST
		LIMIT $%d`, strings.Join(where, " AND "), limit), args...)
	if err != nil {
		return nil, fmt.Errorf("select postgres delivery run trace references: %w", err)
	}
	return rows, nil
}

func (a *Adapter) querySQLiteRunTraceReferences(ctx context.Context, q queryer, page RunTracePageQuery) (*sql.Rows, error) {
	where := []string{"e.run_id = ?"}
	args := []any{page.RunID}
	if page.After != nil {
		eventAt := sqliteTraceSQLTime(page.After.EventCreatedAt)
		deliveryAt := sqliteTraceSQLTimeOrFloor(page.After.DeliveryCreatedAt)
		turnAt := sqliteTraceSQLTimeOrFloor(page.After.TurnCreatedAt)
		floor := sqliteTraceCursorFloor
		where = append(where, `(
			`+sqliteTraceTimeExpression("e.created_at")+` > julianday(?)
			OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id > ?)
			OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id = ? AND `+sqliteTraceCoalescedTimeExpression("d.created_at")+` > julianday(?))
			OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id = ? AND `+sqliteTraceCoalescedTimeExpression("d.created_at")+` = julianday(?) AND COALESCE(d.delivery_id, '') > ?)
			OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id = ? AND `+sqliteTraceCoalescedTimeExpression("d.created_at")+` = julianday(?) AND COALESCE(d.delivery_id, '') = ? AND `+sqliteTraceCoalescedTimeExpression("t.created_at")+` > julianday(?))
			OR (`+sqliteTraceTimeExpression("e.created_at")+` = julianday(?) AND e.event_id = ? AND `+sqliteTraceCoalescedTimeExpression("d.created_at")+` = julianday(?) AND COALESCE(d.delivery_id, '') = ? AND `+sqliteTraceCoalescedTimeExpression("t.created_at")+` = julianday(?) AND COALESCE(t.turn_id, '') > ?)
		)`)
		args = append(args,
			eventAt,
			eventAt, page.After.EventID,
			eventAt, page.After.EventID, floor, floor, floor, deliveryAt,
			eventAt, page.After.EventID, floor, floor, floor, deliveryAt, page.After.DeliveryID,
			eventAt, page.After.EventID, floor, floor, floor, deliveryAt, page.After.DeliveryID, floor, floor, floor, turnAt,
			eventAt, page.After.EventID, floor, floor, floor, deliveryAt, page.After.DeliveryID, floor, floor, floor, turnAt, page.After.TurnID,
		)
	}
	if page.Since != nil {
		where = append(where, sqliteTraceWatermarkExpression()+" > julianday(?)")
		args = append(args, sqliteTraceSQLTime(page.Since.UTC()))
	}
	if page.Until != nil {
		where = append(where, sqliteTraceWatermarkExpression()+" <= julianday(?)")
		args = append(args, sqliteTraceSQLTime(page.Until.UTC()))
	}
	addTextFilter := func(expression string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		where = append(where, expression+" IN ("+strings.Join(placeholders, ",")+")")
	}
	addTextFilter("e.event_name", page.EventNames)
	addTextFilter("COALESCE(e.entity_id, '')", page.EntityIDs)
	statuses := make([]string, 0, len(page.DeliveryStatuses))
	for _, status := range page.DeliveryStatuses {
		statuses = append(statuses, string(status))
	}
	addTextFilter("COALESCE(d.status, '')", statuses)
	addTextFilter("COALESCE(d.subscriber_id, '')", page.SubscriberIDs)
	classes := make([]string, 0, len(page.SubscriberClasses))
	for _, class := range page.SubscriberClasses {
		classes = append(classes, string(class))
	}
	addTextFilter("COALESCE(d.subscriber_type, '')", classes)
	if page.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	args = append(args, page.Limit+1)
	rows, err := q.QueryContext(ctx, `
		WITH trace_sessions AS (
			SELECT session_id, run_id, updated_at FROM agent_sessions
			UNION ALL
			SELECT session_id, run_id, updated_at FROM agent_conversation_audits
		)
		SELECT e.event_id, COALESCE(d.delivery_id, ''), COALESCE(t.turn_id, '')
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		LEFT JOIN event_delivery_attempts current_attempt
			ON current_attempt.delivery_id = d.delivery_id
		   AND current_attempt.claim_version = d.current_attempt_version
		   AND current_attempt.open_marker = 1
		LEFT JOIN agent_turns t
			ON t.run_id = e.run_id
		   AND t.trigger_event_id = e.event_id
		   AND (d.delivery_id IS NULL OR (d.subscriber_type = 'agent' AND d.subscriber_id <> '' AND t.agent_id = d.subscriber_id))
		LEFT JOIN trace_sessions sess
			ON sess.session_id = COALESCE(t.session_id, current_attempt.active_session_id)
		   AND (sess.run_id = e.run_id OR sess.run_id IS NULL)
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY `+sqliteTraceTimeExpression("e.created_at")+`, e.event_id, `+sqliteTraceTimeExpression("d.created_at")+`, d.delivery_id, `+sqliteTraceTimeExpression("t.created_at")+`, t.turn_id
		LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("select sqlite delivery run trace references: %w", err)
	}
	return rows, nil
}

const sqliteTraceCursorFloor = "0001-01-01T00:00:00Z"

func traceNullableTimestamp(value time.Time) string {
	if value.IsZero() {
		return "-infinity"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func sqliteTraceSQLTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func sqliteTraceSQLTimeOrFloor(value time.Time) string {
	if value.IsZero() {
		return sqliteTraceCursorFloor
	}
	return sqliteTraceSQLTime(value)
}

func sqliteTraceTimeExpression(expression string) string {
	return `COALESCE(julianday(` + expression + `), julianday(substr(CAST(` + expression + ` AS TEXT), 1, instr(CAST(` + expression + ` AS TEXT), ' +') - 1)))`
}

func sqliteTraceCoalescedTimeExpression(expression string) string {
	coalesced := `COALESCE(` + expression + `, ?)`
	return sqliteTraceTimeExpression(coalesced)
}

func sqliteTraceWatermarkExpression() string {
	return `max(
		` + sqliteTraceTimeExpression("e.created_at") + `,
		` + sqliteTraceTimeExpression("COALESCE(d.created_at, '"+sqliteTraceCursorFloor+"')") + `,
		` + sqliteTraceTimeExpression("COALESCE(d.started_at, '"+sqliteTraceCursorFloor+"')") + `,
		` + sqliteTraceTimeExpression("COALESCE(d.settled_at, '"+sqliteTraceCursorFloor+"')") + `,
		` + sqliteTraceTimeExpression("COALESCE(sess.updated_at, '"+sqliteTraceCursorFloor+"')") + `,
		` + sqliteTraceTimeExpression("COALESCE(t.created_at, '"+sqliteTraceCursorFloor+"')") + `
	)`
}

func (a *Adapter) LifecycleSnapshotPageForAgent(ctx context.Context, q queryer, page AgentLifecyclePageQuery) (SnapshotPage, error) {
	page.AgentIdentity = page.AgentIdentity.Normalize()
	if err := page.AgentIdentity.Validate(); err != nil {
		return SnapshotPage{}, fmt.Errorf("delivery lifecycle page agent identity: %w", err)
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
	identityPredicate, identityArgs, err := agentIdentityPredicate(a.dialect, "d", []agentidentity.Identity{page.AgentIdentity}, 1)
	if err != nil {
		return SnapshotPage{}, err
	}
	query := fmt.Sprintf(`
		SELECT d.delivery_id::text
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND (%s)
		  AND ($8::text = '' OR d.run_id = NULLIF($8::text, '')::uuid)
		  AND (($9 AND d.status = 'pending') OR ($10 AND d.status = 'in_progress') OR
		       ($11 AND d.status = 'delivered') OR ($12 AND d.status = 'failed') OR
		       ($13 AND d.status = 'dead_letter'))
		  AND ($14::timestamptz IS NULL OR d.created_at < $14 OR (d.created_at = $14 AND d.delivery_id < $15::uuid))
		ORDER BY d.created_at DESC, d.delivery_id DESC
		LIMIT $16`, identityPredicate)
	args := append(identityArgs,
		runID,
		statusSelected[StatusPending], statusSelected[StatusInProgress], statusSelected[StatusDelivered],
		statusSelected[StatusFailed], statusSelected[StatusDeadLetter],
		cursorAt, cursorID, page.Limit+1,
	)
	if a.dialect == DialectSQLite {
		query = fmt.Sprintf(`
			SELECT d.delivery_id
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND (%s)
			  AND (? = '' OR d.run_id = ?)
			  AND ((? AND d.status = 'pending') OR (? AND d.status = 'in_progress') OR
			       (? AND d.status = 'delivered') OR (? AND d.status = 'failed') OR
			       (? AND d.status = 'dead_letter'))
			  AND (? IS NULL OR d.created_at < ? OR (d.created_at = ? AND d.delivery_id < ?))
			ORDER BY d.created_at DESC, d.delivery_id DESC
			LIMIT ?`, identityPredicate)
		args = append(identityArgs,
			runID, runID,
			statusSelected[StatusPending], statusSelected[StatusInProgress], statusSelected[StatusDelivered],
			statusSelected[StatusFailed], statusSelected[StatusDeadLetter],
			cursorAt, cursorAt, cursorAt, cursorID, page.Limit+1,
		)
	}
	return a.snapshotPageByIDQuery(ctx, q, page.Limit, query, args...)
}

func (a *Adapter) DiagnosticSnapshotPageForAgent(ctx context.Context, q queryer, page AgentDiagnosticPageQuery) (SnapshotPage, error) {
	page.AgentIdentity = page.AgentIdentity.Normalize()
	if err := page.AgentIdentity.Validate(); err != nil {
		return SnapshotPage{}, fmt.Errorf("delivery diagnostic page agent identity: %w", err)
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
	identityPredicate, identityArgs, err := agentIdentityPredicate(a.dialect, "d", []agentidentity.Identity{page.AgentIdentity}, 1)
	if err != nil {
		return SnapshotPage{}, err
	}
	query := fmt.Sprintf(`
		SELECT d.delivery_id::text
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND (%[2]s) AND d.status = $8
		  AND ($9::timestamptz IS NULL OR %[1]s < $9 OR
		       (%[1]s = $9 AND d.delivery_id < $10::uuid))
		ORDER BY %[1]s DESC, d.delivery_id DESC
		LIMIT $11`, occurredColumn, identityPredicate)
	args := append(identityArgs, string(page.Status), cursorAt, cursorID, page.Limit+1)
	if a.dialect == DialectSQLite {
		query = fmt.Sprintf(`
			SELECT d.delivery_id
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND (%[2]s) AND d.status = ?
			  AND (? IS NULL OR %[1]s < ? OR (%[1]s = ? AND d.delivery_id < ?))
			ORDER BY %[1]s DESC, d.delivery_id DESC
			LIMIT ?`, occurredColumn, identityPredicate)
		args = append(identityArgs, string(page.Status), cursorAt, cursorAt, cursorAt, cursorID, page.Limit+1)
	}
	return a.snapshotPageByIDQuery(ctx, q, page.Limit, query, args...)
}

func (a *Adapter) DiagnosticCountsForAgentSince(ctx context.Context, q queryer, identity agentidentity.Identity, since time.Time) (AgentDiagnosticCounts, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return AgentDiagnosticCounts{}, fmt.Errorf("delivery diagnostic counts agent identity: %w", err)
	}
	if since.IsZero() {
		return AgentDiagnosticCounts{}, fmt.Errorf("delivery diagnostic counts cutoff is required")
	}
	identityPredicate, identityArgs, err := agentIdentityPredicate(a.dialect, "d", []agentidentity.Identity{identity}, 1)
	if err != nil {
		return AgentDiagnosticCounts{}, err
	}
	query := fmt.Sprintf(`
		SELECT COUNT(*) FILTER (WHERE d.status = 'failed'),
		       COUNT(*) FILTER (WHERE d.status = 'dead_letter')
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent' AND (%s)
		  AND d.status IN ('failed', 'dead_letter')
		  AND ((d.status = 'failed' AND d.updated_at >= $8) OR
		       (d.status = 'dead_letter' AND d.settled_at >= $8))`, identityPredicate)
	if a.dialect == DialectSQLite {
		query = fmt.Sprintf(`
			SELECT COALESCE(SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END), 0),
			       COALESCE(SUM(CASE WHEN d.status = 'dead_letter' THEN 1 ELSE 0 END), 0)
			FROM event_deliveries d
			WHERE d.subscriber_type = 'agent' AND (%s)
			  AND d.status IN ('failed', 'dead_letter')
			  AND ((d.status = 'failed' AND d.updated_at >= ?) OR
			       (d.status = 'dead_letter' AND d.settled_at >= ?))`, identityPredicate)
	}
	var counts AgentDiagnosticCounts
	args := append(identityArgs, since.UTC())
	if a.dialect == DialectSQLite {
		args = append(identityArgs, since.UTC(), since.UTC())
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
	snapshot.RetryScheduled = snapshot.Status == StatusFailed
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

func (a *Adapter) TerminalizeRun(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID, reason string) ([]Terminalization, error) {
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
			claim, err := AdmitPersistedClaim(id, record.RunID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), record.claimToken, record.ClaimVersion, record.SubscriberClass, record.SubscriberID)
			if err != nil {
				return nil, err
			}
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
		if err := a.recordTransition(ctx, story, updated, "terminalized", &failure, now); err != nil {
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
			side_effects = '[]'::jsonb, duration_ms = 0, completed_at = $3,
			current_delivery_id = NULL, open_marker = FALSE
		WHERE delivery_id = $4::uuid AND claim_version = $5 AND claim_token = $6::uuid AND open_marker = TRUE`
	args := []any{reason, failureRaw, now, claim.DeliveryID(), claim.Version(), claim.PersistenceToken()}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET outcome = 'terminalized', reason_code = ?, failure = ?,
				side_effects = '[]', duration_ms = 0, completed_at = ?,
				current_delivery_id = NULL, open_marker = FALSE
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
	record, err := a.loadByID(ctx, tx, claim.DeliveryID(), true)
	if err != nil {
		return deliveryRecord{}, time.Time{}, err
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return deliveryRecord{}, time.Time{}, err
	}
	if record.Status != StatusInProgress || record.claimToken != claim.PersistenceToken() || record.ClaimVersion != claim.Version() ||
		events.EncodeDeliveryRouteIdentity(record.RouteIdentity) != claim.RouteIdentity() || record.ClaimExpiresAt.IsZero() || !record.ClaimExpiresAt.After(now) {
		return deliveryRecord{}, time.Time{}, fmt.Errorf("%w: delivery claim is stale", ErrConflict)
	}
	return record, now, nil
}

func (a *Adapter) insertAttempt(ctx context.Context, tx *sql.Tx, deliveryID string, version int64, token string, startedAt, expiresAt time.Time) error {
	query := `INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $1::uuid, TRUE)`
	args := []any{deliveryID, version, token, startedAt, expiresAt}
	if a.dialect == DialectSQLite {
		query = `INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES (?1, ?2, ?3, ?4, ?5, ?1, TRUE)`
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
	query := `UPDATE event_delivery_attempts SET outcome = 'lease_expired', completed_at = $1, current_delivery_id = NULL, open_marker = FALSE WHERE delivery_id = $2::uuid AND claim_version = $3 AND claim_token = $4::uuid AND open_marker = TRUE`
	args := []any{now, record.DeliveryID, record.ClaimVersion, record.claimToken}
	if a.dialect == DialectSQLite {
		query = `UPDATE event_delivery_attempts SET outcome = 'lease_expired', completed_at = ?, current_delivery_id = NULL, open_marker = FALSE WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
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
			side_effects = $4::jsonb, duration_ms = $5, completed_at = $6,
			current_delivery_id = NULL, open_marker = FALSE
		WHERE delivery_id = $7::uuid AND claim_version = $8 AND claim_token = $9::uuid AND open_marker = TRUE`
	args := []any{outcome, reason, failureRaw, string(sideEffectsRaw), durationMS, now, claim.DeliveryID(), claim.Version(), claim.PersistenceToken()}
	if a.dialect == DialectSQLite {
		query = `
			UPDATE event_delivery_attempts
			SET outcome = ?, reason_code = NULLIF(?, ''), failure = NULLIF(?, ''),
				side_effects = ?, duration_ms = ?, completed_at = ?,
				current_delivery_id = NULL, open_marker = FALSE
			WHERE delivery_id = ? AND claim_version = ? AND claim_token = ? AND open_marker = TRUE`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("complete delivery attempt: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("%w: delivery attempt is stale", ErrConflict)
	}
	return a.insertOutcome(ctx, tx, claim.DeliveryID(), claim.Version(), outcome, reason, failure, sideEffects, duration, now)
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

func (a *Adapter) recordTransition(ctx context.Context, story runtimeauthoractivity.Mutation, record deliveryRecord, transition string, failure *runtimefailures.Envelope, occurredAt time.Time) error {
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
	var summary string
	var found bool
	var err error
	if story == nil {
		return fmt.Errorf("delivery transition requires private story ownership")
	}
	summary, found, err = story.PersistedAuthorSafeSummary(ctx, "emit:"+record.EventID)
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
	err = story.Record(ctx, draft)
	if err != nil {
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
	return a.scanRecord(q.QueryRowContext(ctx, query, strings.TrimSpace(eventID), events.EncodeDeliveryRouteIdentity(identity)))
}

func (a *Adapter) selectRecord() string {
	if a.dialect == DialectSQLite {
		return `
			SELECT d.delivery_id, d.event_id, d.run_id, d.route_identity,
				d.subscriber_type, d.subscriber_id,
				d.agent_name_owner, d.agent_name_source, d.agent_route_presence,
				d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path,
				d.delivery_target_route, d.delivery_context,
				d.delivery_payload_projection, d.connect_execution_claim, d.execution_authority_kind, d.authority_bundle_hash,
				d.authority_bundle_source, d.execution_authority_id, d.execution_authority_generation,
				COALESCE(d.selected_execution_id, ''), COALESCE(d.selected_fork_run_id, ''),
				COALESCE(d.selected_execution_generation, 0),
				d.status, d.retry_count, d.max_retries,
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
			d.subscriber_type, d.subscriber_id,
			d.agent_name_owner, d.agent_name_source, d.agent_route_presence,
			d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path,
			d.delivery_target_route, d.delivery_context,
			d.delivery_payload_projection, d.connect_execution_claim, d.execution_authority_kind, d.authority_bundle_hash,
			d.authority_bundle_source, d.execution_authority_id, d.execution_authority_generation,
			COALESCE(d.selected_execution_id::text, ''), COALESCE(d.selected_fork_run_id::text, ''),
			COALESCE(d.selected_execution_generation, 0),
			d.status, d.retry_count, d.max_retries,
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
	var routeIdentity, subscriberType, status, authorityKind string
	var agentNameOwner, agentNameSource, agentRoutePresence string
	var agentFlowScopeKey, agentFlowInstanceID, agentFlowInstancePath string
	var authorityBundleHash, authorityBundleSource, authorityExecutionID, selectedExecutionID, selectedForkRunID string
	var authorityGeneration, selectedGeneration uint64
	var targetRaw, contextRaw, projectionRaw, connectClaimRaw, failureRaw []byte
	var nextEligible, claimExpires, started, settled, created, updated any
	err := row.Scan(
		&record.DeliveryID, &record.EventID, &record.RunID, &routeIdentity,
		&subscriberType, &record.SubscriberID,
		&agentNameOwner, &agentNameSource, &agentRoutePresence,
		&agentFlowScopeKey, &agentFlowInstanceID, &agentFlowInstancePath,
		&targetRaw, &contextRaw, &projectionRaw, &connectClaimRaw,
		&authorityKind, &authorityBundleHash, &authorityBundleSource, &authorityExecutionID, &authorityGeneration,
		&selectedExecutionID, &selectedForkRunID, &selectedGeneration,
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
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery route identity: %v", ErrConflict, err)
	}
	record.RouteIdentity = identity
	class, err := ParseSubscriberClass(subscriberType)
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery subscriber class: %v", ErrConflict, err)
	}
	record.SubscriberClass = class
	authorityExecutionIdentity := authorityExecutionID
	authorityForkRunID := ""
	if ExecutionAuthorityKind(authorityKind) == ExecutionAuthoritySelectedContractFork {
		if selectedExecutionID != authorityExecutionID || selectedGeneration != authorityGeneration {
			return deliveryRecord{}, fmt.Errorf("%w: selected delivery authority projection mismatch", ErrConflict)
		}
		authorityExecutionIdentity = selectedExecutionID
		authorityForkRunID = selectedForkRunID
	}
	record.Authority, err = DecodeExecutionAuthority(
		ExecutionAuthorityKind(authorityKind), authorityBundleHash, authorityBundleSource,
		authorityExecutionIdentity, authorityForkRunID, authorityGeneration,
	)
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery execution authority: %v", ErrConflict, err)
	}
	record.Status, err = ParseStatus(status)
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery status: %v", ErrConflict, err)
	}
	record.Route, err = decodeRoute(
		class,
		record.SubscriberID,
		agentidentity.StorageFields{
			AgentID:          record.SubscriberID,
			NameOwner:        agentNameOwner,
			NameSource:       agentNameSource,
			RoutePresence:    agentRoutePresence,
			FlowScopeKey:     agentFlowScopeKey,
			FlowInstanceID:   agentFlowInstanceID,
			FlowInstancePath: agentFlowInstancePath,
		},
		targetRaw,
		contextRaw,
		projectionRaw,
		connectClaimRaw,
	)
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery route: %v", ErrConflict, err)
	}
	derived, err := record.Route.Identity()
	if err != nil || derived != identity {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery route identity mismatch", ErrConflict)
	}
	if record.MaxRetries != class.MaxRetries() || record.RetryCount < 0 || record.RetryCount > record.MaxRetries {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery retry policy mismatch", ErrConflict)
	}
	if record.Failure, err = decodeFailure(failureRaw); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery failure: %v", ErrConflict, err)
	}
	if record.NextEligibleAt, _, err = parseNullableTime(nextEligible); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery next eligibility: %v", ErrConflict, err)
	}
	if record.ClaimExpiresAt, _, err = parseNullableTime(claimExpires); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery claim expiry: %v", ErrConflict, err)
	}
	if record.StartedAt, _, err = parseNullableTime(started); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery start: %v", ErrConflict, err)
	}
	if record.SettledAt, _, err = parseNullableTime(settled); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery settlement: %v", ErrConflict, err)
	}
	if record.CreatedAt, _, err = parseNullableTime(created); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery creation: %v", ErrConflict, err)
	}
	if record.UpdatedAt, _, err = parseNullableTime(updated); err != nil {
		return deliveryRecord{}, fmt.Errorf("%w: persisted delivery update: %v", ErrConflict, err)
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
	if a != nil && a.dialect == DialectSQLite {
		// CURRENT_TIMESTAMP drops fractional seconds in SQLite, which can make a
		// one-second retry eligible almost immediately at a wall-clock boundary.
		query = `SELECT ` + sqliteDatabaseNowExpression
	} else if a != nil && a.dialect == DialectPostgres {
		query = `SELECT ` + postgresDatabaseNowExpression
	}
	var raw any
	if err := q.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("read delivery database time: %w", err)
	}
	now, ok, err := parseNullableTime(raw)
	if err != nil || !ok {
		return time.Time{}, fmt.Errorf("read delivery database time: %w", err)
	}
	return now.UTC().Truncate(time.Microsecond), nil
}

func encodeRoute(route events.DeliveryRoute) ([]byte, []byte, []byte, []byte, error) {
	route = route.Normalized()
	target, err := json.Marshal(route.Target)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	deliveryContext, err := json.Marshal(route.Context)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	projection, err := json.Marshal(route.PayloadProjection)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	connectClaim, err := json.Marshal(route.ConnectClaim)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return target, deliveryContext, projection, connectClaim, nil
}

func deliveryRouteAgentStorageFields(route events.DeliveryRoute) (agentidentity.StorageFields, error) {
	route = route.Normalized()
	switch {
	case route.Recipient.IsNode():
		if !route.AgentIdentity.IsZero() {
			return agentidentity.StorageFields{}, fmt.Errorf("node delivery route cannot carry agent identity")
		}
		return agentidentity.StorageFields{}, nil
	case route.Recipient.IsAgent():
		fields, err := route.AgentIdentity.StorageFields()
		if err != nil {
			return agentidentity.StorageFields{}, fmt.Errorf("agent delivery route identity fields: %w", err)
		}
		if fields.AgentID != route.Recipient.ID() {
			return agentidentity.StorageFields{}, fmt.Errorf("agent delivery route subscriber does not match identity")
		}
		return fields, nil
	default:
		return agentidentity.StorageFields{}, fmt.Errorf("delivery route subscriber type %q is unsupported", route.Recipient.Code())
	}
}

func decodeRoute(
	class SubscriberClass,
	subscriberID string,
	agentFields agentidentity.StorageFields,
	targetRaw, contextRaw, projectionRaw, connectClaimRaw []byte,
) (events.DeliveryRoute, error) {
	var target events.DeliveryTargetOwnership
	var deliveryContext events.DeliveryContext
	var projection events.DeliveryPayloadProjection
	var connectClaim events.ConnectExecutionClaim
	if err := json.Unmarshal(targetRaw, &target); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery target: %w", err)
	}
	if err := json.Unmarshal(contextRaw, &deliveryContext); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery context: %w", err)
	}
	if err := json.Unmarshal(projectionRaw, &projection); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode delivery projection: %w", err)
	}
	if err := json.Unmarshal(connectClaimRaw, &connectClaim); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("decode connect execution claim: %w", err)
	}
	identity := agentidentity.Identity{}
	switch class {
	case SubscriberAgent:
		var err error
		identity, err = agentidentity.FromStorageFields(agentFields)
		if err != nil {
			return events.DeliveryRoute{}, fmt.Errorf("decode delivery agent identity: %w", err)
		}
		if identity.AgentID() != strings.TrimSpace(subscriberID) {
			return events.DeliveryRoute{}, fmt.Errorf("delivery subscriber does not match stored agent identity")
		}
	case SubscriberNode:
		if agentFields.NameOwner != "" || agentFields.NameSource != "" || agentFields.RoutePresence != "" ||
			agentFields.FlowScopeKey != "" || agentFields.FlowInstanceID != "" || agentFields.FlowInstancePath != "" {
			return events.DeliveryRoute{}, fmt.Errorf("node delivery row carries agent identity fields")
		}
	}
	var recipient events.DeliveryRecipient
	switch class {
	case SubscriberAgent:
		recipient = events.MustAgentDeliveryRecipient(subscriberID)
	case SubscriberNode:
		node, err := runtimeidentity.ParseExecutableNodeKey(subscriberID)
		if err != nil {
			return events.DeliveryRoute{}, fmt.Errorf("decode delivery node identity: %w", err)
		}
		recipient = events.MustNodeDeliveryRecipient(node)
	default:
		return events.DeliveryRoute{}, fmt.Errorf("unsupported delivery subscriber class %q", class)
	}
	return events.DeliveryRoute{
		Recipient:         recipient,
		AgentIdentity:     identity,
		Target:            target,
		Context:           deliveryContext,
		PayloadProjection: projection,
		ConnectClaim:      connectClaim,
	}.Normalized(), nil
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
