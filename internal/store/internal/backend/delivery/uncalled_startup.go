package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

type UncalledStartupKind int

const (
	UncalledStartupFirstCall UncalledStartupKind = iota + 1
	UncalledStartupInterruptedRetry
	UncalledStartupFailedRetry
)

type UncalledStartupClaim struct {
	Claim      deliverylifecycle.Claim
	Actor      agentidentity.Identity
	BundleHash string
	Kind       UncalledStartupKind
}

func (s *DeliveryPostgresOwner) UncalledStartupClaims(ctx context.Context, attempt *mutationprotocol.Attempt, lock bool) ([]UncalledStartupClaim, error) {
	return postgresDeliveryAdapter.uncalledStartupClaims(ctx, attempt, lock)
}

func (s *DeliverySQLiteOwner) UncalledStartupClaims(ctx context.Context, attempt *mutationprotocol.Attempt, lock bool) ([]UncalledStartupClaim, error) {
	return sqliteDeliveryAdapter.uncalledStartupClaims(ctx, attempt, lock)
}

func (a *Adapter) uncalledStartupClaims(ctx context.Context, attempt *mutationprotocol.Attempt, lock bool) ([]UncalledStartupClaim, error) {
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return nil, err
	}
	authority, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, err
	}
	if authority.HasRuntimeGrant() && authority.GrantEvidence().SelectedFork != nil {
		return nil, nil
	}
	processID := authority.ProcessAuthority().LeaseStamp().LeaseID
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) ([]UncalledStartupClaim, error) {
		return a.uncalledStartupClaimsTx(ctx, tx, processID, lock)
	})
}

// An admitted ordinary grant from a replaced process is positive predecessor
// evidence. A missing grant or an expired claim lease is not such proof.
const uncalledStartupClaimsSQL = "WITH " + uncalledStartupClaimsSelection

const uncalledStartupClaimsSelection = `startup_claims AS (
	SELECT CAST(d.delivery_id AS TEXT) AS delivery_id, d.run_id, CASE
		WHEN NOT EXISTS (SELECT 1 FROM runtime_agent_turn_lifetimes t WHERE t.origin_kind='delivery' AND t.origin_id=d.delivery_id)
		  AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts e WHERE e.origin_kind='delivery' AND e.origin_delivery_id=d.delivery_id)
		THEN 1
		WHEN d.claim_version=2 AND d.retry_count=1 AND d.max_retries=1
		  AND (SELECT COUNT(*) FROM runtime_external_effect_attempts e
			WHERE e.origin_kind='delivery' AND e.origin_delivery_id=d.delivery_id)=1
		  AND EXISTS (
		    SELECT 1 FROM event_delivery_attempts prior
		    JOIN runtime_external_effect_attempts e ON e.origin_delivery_id=prior.delivery_id
		      AND e.origin_claim_version=prior.claim_version AND e.origin_claim_token=prior.claim_token
		    JOIN runtime_external_effect_operations o ON o.operation_id=e.operation_id
		    JOIN runtime_agent_turn_lifetimes t ON t.origin_kind='delivery' AND t.origin_id=d.delivery_id
		      AND t.run_id=d.run_id AND t.agent_id=d.subscriber_id
		      AND t.first_attempt_id=e.attempt_id AND t.current_attempt_id=e.attempt_id
		    WHERE prior.delivery_id=d.delivery_id AND prior.claim_version=1
		      AND prior.closure_kind='settled' AND prior.open_marker=FALSE AND prior.completed_at IS NOT NULL
		      AND prior.outcome='retry_scheduled' AND prior.reason_code='effect_recovery_outcome_unconfirmed'
		      AND e.origin_kind='delivery' AND e.origin_run_id=d.run_id AND e.origin_route_identity=d.route_identity
		      AND e.origin_subscriber_type=d.subscriber_type AND e.origin_subscriber_id=d.subscriber_id
		      AND e.state='outcome_uncertain' AND e.launched_at IS NOT NULL
		      AND e.completion_projection_phase IS NULL AND e.completion_continuation_active=FALSE
		      AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent' AND o.state='outcome_uncertain'
		      AND o.agent_run_id=d.run_id AND o.agent_id=d.subscriber_id AND o.bundle_hash=d.authority_bundle_hash
		      AND t.first_launched_at IS NOT NULL AND t.cancel_reason IS NULL AND t.settled_at IS NULL AND t.origin_evidence IS NULL)
		THEN 2
		WHEN d.claim_version=2 AND d.retry_count=1 AND d.max_retries=1
		  AND r.origin_kind<>'fork_materialization'
		  AND EXISTS (
		    SELECT 1 FROM event_delivery_attempts prior
		    JOIN runtime_agent_turn_lifetimes t ON t.origin_kind='delivery' AND t.origin_id=d.delivery_id
		      AND t.run_id=d.run_id AND t.agent_id=d.subscriber_id
		    JOIN runtime_external_effect_attempts anchor ON anchor.attempt_id=t.admitted_attempt_id
		      AND anchor.origin_kind='delivery' AND anchor.origin_delivery_id=d.delivery_id
		    JOIN runtime_external_effect_attempts bound ON bound.attempt_id=t.current_attempt_id
		      AND bound.origin_kind='delivery' AND bound.origin_delivery_id=d.delivery_id
		    WHERE prior.delivery_id=d.delivery_id AND prior.claim_version=1
		      AND prior.closure_kind='settled' AND prior.open_marker=FALSE AND prior.completed_at IS NOT NULL
		      AND prior.outcome='retry_scheduled' AND prior.reason_code='handler_failure' AND prior.failure IS NOT NULL
		      AND t.cancel_reason IS NULL AND t.origin_evidence IS NULL
		      AND NOT EXISTS (
		        SELECT 1 FROM runtime_external_effect_attempts e
		        LEFT JOIN runtime_external_effect_operations o ON o.operation_id=e.operation_id
		        WHERE e.origin_delivery_id=d.delivery_id AND (
		          e.origin_kind='delivery' AND e.origin_claim_version=prior.claim_version AND e.origin_claim_token=prior.claim_token
		          AND e.origin_run_id=d.run_id AND e.origin_route_identity=d.route_identity
		          AND e.origin_subscriber_type=d.subscriber_type AND e.origin_subscriber_id=d.subscriber_id
		          AND e.state='terminal_failure' AND e.completed_at IS NOT NULL
		          AND e.completion_projection_phase IS NULL AND e.completion_continuation_active=FALSE
		          AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent' AND o.state='terminal_failure'
		          AND o.agent_run_id=d.run_id AND o.agent_id=d.subscriber_id AND o.bundle_hash=d.authority_bundle_hash
		        ) IS NOT TRUE)
		      AND NOT EXISTS (
		        SELECT 1 FROM runtime_provider_attempt_drains drain
		        WHERE drain.origin_kind='delivery' AND drain.origin_delivery_id=d.delivery_id))
		THEN 3 ELSE 0 END AS arm
		FROM event_deliveries d JOIN runs r ON r.run_id=d.run_id AND r.bundle_hash=d.authority_bundle_hash
		JOIN event_delivery_attempts c ON c.delivery_id=d.delivery_id AND c.claim_version=d.claim_version AND c.open_marker=TRUE
		WHERE d.status='in_progress' AND d.subscriber_type='agent' AND d.execution_authority_kind='normal_runtime'
		  AND d.current_attempt_version=d.claim_version AND d.current_attempt_open=TRUE
		  AND r.status IN (` + runstate.ActiveStateSQLValues + `)
		  AND EXISTS (SELECT 1 FROM runtime_generation_grants g WHERE CAST(g.grant_id AS TEXT)=d.execution_authority_id
		    AND g.runtime_generation=d.execution_authority_generation AND g.bundle_hash=d.authority_bundle_hash
		    AND g.selected_execution_id IS NULL AND g.state='admitted' AND g.process_authority_id<>$1)
	)
	SELECT delivery_id, arm FROM startup_claims WHERE arm<>0 ORDER BY run_id,delivery_id`

func (a *Adapter) uncalledStartupClaimsTx(ctx context.Context, tx *sql.Tx, processID string, lock bool) ([]UncalledStartupClaim, error) {
	rows, err := tx.QueryContext(ctx, uncalledStartupClaimsSQL, processID)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id   string
		kind UncalledStartupKind
	}
	var candidates []candidate
	for rows.Next() {
		var row candidate
		if err := rows.Scan(&row.id, &row.kind); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		candidates = append(candidates, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	out := make([]UncalledStartupClaim, 0, len(candidates))
	for _, candidate := range candidates {
		record, err := a.loadByID(ctx, tx, candidate.id, lock)
		if err != nil {
			return nil, err
		}
		actor := record.Route.AgentIdentity
		if record.Status != deliverylifecycle.StatusInProgress || record.SubscriberClass != deliverylifecycle.SubscriberAgent || actor.RunID != record.RunID || actor.Name.AgentID != record.SubscriberID {
			return nil, fmt.Errorf("uncalled startup candidate differs from its exact claimed actor")
		}
		claim, err := deliverylifecycle.AdmitPersistedClaim(candidate.id, record.RunID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), record.claimToken, record.ClaimVersion, record.SubscriberClass, record.SubscriberID)
		if err != nil {
			return nil, err
		}
		switch candidate.kind {
		case UncalledStartupFirstCall:
		case UncalledStartupInterruptedRetry, UncalledStartupFailedRetry:
			if claim.Version() != 2 || record.RetryCount != 1 || record.MaxRetries != 1 {
				return nil, fmt.Errorf("uncalled startup retry candidate differs from its exact exhausted claim")
			}
		default:
			return nil, fmt.Errorf("invalid uncalled startup claim kind %d", candidate.kind)
		}
		out = append(out, UncalledStartupClaim{Claim: claim, Actor: actor, BundleHash: record.Authority.SourceArtifact().BundleHash(), Kind: candidate.kind})
	}
	return out, nil
}
