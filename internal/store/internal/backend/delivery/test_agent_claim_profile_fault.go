package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type AgentClaimProfileFault string

const (
	AgentClaimProfileMissing AgentClaimProfileFault = "missing"
	AgentClaimProfileCorrupt AgentClaimProfileFault = "corrupt"
	AgentClaimProfileForeign AgentClaimProfileFault = "foreign"
)

// Only capture evidence changes. The native claim owner retains every lease,
// version, token and settlement right; selected preparation is never mutated.
func ApplyAgentClaimProfileFaultTx(ctx context.Context, tx *sql.Tx, dialect Dialect, claim deliverylifecycle.Claim, expected deliverylifecycle.AgentClaimProfile, fault AgentClaimProfileFault, foreign deliverylifecycle.Claim) error {
	if tx == nil || claim.Validate() != nil || claim.Version() != 1 ||
		claim.SubscriberClass() != deliverylifecycle.SubscriberAgent || expected.Validate() != nil {
		return fmt.Errorf("agent profile fault requires an exact first agent claim and original capture")
	}
	if ctx == nil {
		return fmt.Errorf("agent profile fault requires its transaction context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fault != AgentClaimProfileMissing && fault != AgentClaimProfileCorrupt && fault != AgentClaimProfileForeign {
		return fmt.Errorf("agent profile fault has no named variant")
	}
	if fault != AgentClaimProfileForeign && foreign != (deliverylifecycle.Claim{}) {
		return fmt.Errorf("non-foreign agent profile fault cannot carry another claim")
	}
	adapter, err := NewAdapter(dialect)
	if err != nil {
		return err
	}
	record, _, err := adapter.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if record.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || !expected.MatchesDelivery(record.Snapshot) {
		return fmt.Errorf("agent profile fault refuses foreign or selected preparation authority")
	}
	profile, err := readFirstAgentClaimProfileTx(ctx, tx, record.Snapshot)
	if err != nil {
		return err
	}
	if profile != expected {
		return fmt.Errorf("agent profile fault original capture differs")
	}
	var prior []byte
	if err := tx.QueryRowContext(ctx, `SELECT agent_claim_profile FROM event_delivery_attempts
		WHERE delivery_id=$1 AND claim_version=1 AND claim_token=$2 AND open_marker=TRUE
		AND current_delivery_id=delivery_id`, claim.DeliveryID(), claim.PersistenceToken()).Scan(&prior); err != nil {
		return err
	}
	replacement, err := agentClaimProfileFaultReplacement(ctx, tx, adapter, record, fault, foreign)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET agent_claim_profile=$1
		WHERE delivery_id=$2 AND claim_version=1 AND claim_token=$3 AND open_marker=TRUE
		AND current_delivery_id=delivery_id AND agent_claim_profile=$4
		AND EXISTS (SELECT 1 FROM event_deliveries d WHERE d.delivery_id=$2 AND d.run_id=$5
		AND d.event_id=$6 AND d.route_identity=$7 AND d.status='in_progress' AND d.claim_version=1
		AND d.current_attempt_version=1 AND d.current_attempt_open=TRUE AND d.subscriber_type='agent'
		AND d.subscriber_id=$8 AND d.execution_authority_kind='normal_runtime'
		AND d.authority_bundle_hash=$9 AND d.execution_authority_id=$10 AND d.execution_authority_generation=$11)`,
		replacement, claim.DeliveryID(), claim.PersistenceToken(), string(prior), record.RunID, record.EventID,
		claim.RouteIdentity(), claim.SubscriberID(), record.Authority.SourceArtifact().BundleHash(),
		record.Authority.ExecutionID(), record.Authority.Generation())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("agent profile fault lost exact capture CAS: rows=%d error=%v", count, err)
	}
	return nil
}

func agentClaimProfileFaultReplacement(ctx context.Context, tx *sql.Tx, adapter *Adapter, target deliveryRecord, fault AgentClaimProfileFault, foreign deliverylifecycle.Claim) (any, error) {
	switch fault {
	case AgentClaimProfileMissing:
		return nil, nil
	case AgentClaimProfileCorrupt:
		return `{"profile_id":"owner-built-invalid-capture"}`, nil
	case AgentClaimProfileForeign:
		if foreign.Validate() != nil || foreign.Version() != 1 || foreign.SubscriberClass() != deliverylifecycle.SubscriberAgent {
			return nil, fmt.Errorf("foreign agent profile fault requires another exact first agent claim")
		}
		other, _, err := adapter.requireCurrentClaim(ctx, tx, foreign)
		if err != nil {
			return nil, err
		}
		if other.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || other.DeliveryID == target.DeliveryID ||
			other.EventID == target.EventID && other.RunID == target.RunID && other.Authority.SourceArtifact().Matches(target.Authority.SourceArtifact()) {
			return nil, fmt.Errorf("foreign agent profile fault requires independent delivery/source evidence, not a display-name difference")
		}
		profile, err := readFirstAgentClaimProfileTx(ctx, tx, other.Snapshot)
		if err != nil {
			return nil, err
		}
		raw, err := canonicaljson.MarshalPreservingNumberKinds(profile)
		if err != nil {
			return nil, err
		}
		return string(raw), nil
	default:
		return nil, fmt.Errorf("agent profile fault has no named variant")
	}
}
