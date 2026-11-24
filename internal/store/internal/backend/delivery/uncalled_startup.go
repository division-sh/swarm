package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

type UncalledStartupClaim struct {
	Claim      deliverylifecycle.Claim
	Actor      agentidentity.Identity
	BundleHash string
}

func (s *DeliveryPostgresOwner) UncalledStartupClaimsTx(ctx context.Context, tx *sql.Tx, processID string, lock bool) ([]UncalledStartupClaim, error) {
	return postgresDeliveryAdapter.uncalledStartupClaimsTx(ctx, tx, processID, lock)
}

func (s *DeliverySQLiteOwner) UncalledStartupClaimsTx(ctx context.Context, tx *sql.Tx, processID string, lock bool) ([]UncalledStartupClaim, error) {
	return sqliteDeliveryAdapter.uncalledStartupClaimsTx(ctx, tx, processID, lock)
}

func (a *Adapter) uncalledStartupClaimsTx(ctx context.Context, tx *sql.Tx, processID string, lock bool) ([]UncalledStartupClaim, error) {
	// An admitted ordinary grant from a replaced process is positive predecessor
	// evidence. A missing grant or an expired claim lease is not such proof.
	rows, err := tx.QueryContext(ctx, `SELECT CAST(d.delivery_id AS TEXT)
		FROM event_deliveries d JOIN runs r ON r.run_id=d.run_id AND r.bundle_hash=d.authority_bundle_hash
		JOIN event_delivery_attempts c ON c.delivery_id=d.delivery_id AND c.claim_version=d.claim_version AND c.open_marker=TRUE
		WHERE d.status='in_progress' AND d.subscriber_type='agent' AND d.execution_authority_kind='normal_runtime'
		  AND d.current_attempt_version=d.claim_version AND d.current_attempt_open=TRUE
		  AND r.status IN (`+runstate.ActiveStateSQLValues+`)
		  AND EXISTS (SELECT 1 FROM runtime_generation_grants g WHERE CAST(g.grant_id AS TEXT)=d.execution_authority_id
		    AND g.runtime_generation=d.execution_authority_generation AND g.bundle_hash=d.authority_bundle_hash
		    AND g.selected_execution_id IS NULL AND g.state='admitted' AND g.process_authority_id<>$1)
		  AND NOT EXISTS (SELECT 1 FROM runtime_agent_turn_lifetimes t WHERE t.origin_kind='delivery' AND t.origin_id=d.delivery_id)
		  AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts e WHERE e.origin_kind='delivery' AND e.origin_delivery_id=d.delivery_id)
		ORDER BY d.run_id,d.delivery_id`, processID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	out := make([]UncalledStartupClaim, 0, len(ids))
	for _, id := range ids {
		record, err := a.loadByID(ctx, tx, id, lock)
		if err != nil {
			return nil, err
		}
		actor := record.Route.AgentIdentity
		if record.Status != deliverylifecycle.StatusInProgress || record.SubscriberClass != deliverylifecycle.SubscriberAgent || actor.RunID != record.RunID || actor.Name.AgentID != record.SubscriberID {
			return nil, fmt.Errorf("uncalled startup candidate differs from its exact claimed actor")
		}
		claim, err := deliverylifecycle.AdmitPersistedClaim(id, record.RunID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), record.claimToken, record.ClaimVersion, record.SubscriberClass, record.SubscriberID)
		if err != nil {
			return nil, err
		}
		out = append(out, UncalledStartupClaim{Claim: claim, Actor: actor, BundleHash: record.Authority.SourceArtifact().BundleHash()})
	}
	return out, nil
}
