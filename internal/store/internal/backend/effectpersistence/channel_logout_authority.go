package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
)

func requireChannelLogoutAuthorityTx(ctx context.Context, tx *sql.Tx, a runtimeeffects.Authority, postgres bool) error {
	if err := channeldelivery.LockPrincipalTx(ctx, tx, a.ChannelLogout.PrincipalID, postgres); err != nil {
		return invalidExternalAuthority(a, "principal_not_current")
	}
	current, err := channelonboarding.LogoutEffectAuthorityCurrent(ctx, tx, a, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(a, "stale")
	}
	return nil
}

func requireChannelLogoutSettlementAuthorityTx(ctx context.Context, tx *sql.Tx, s runtimeeffects.Settlement, postgres bool) error {
	a := s.Authority
	if !a.Valid() || a.Kind != runtimeeffects.AuthorityChannelLogout || s.OperationID != a.ChannelLogout.EffectOperationID {
		return fmt.Errorf("logout settlement authority is invalid")
	}
	query := `SELECT attempt.authority_evidence,original.authority_evidence,operation.authority_evidence,
		operation.authority_kind,operation.effect_kind,operation.bundle_hash,attempt.state,attempt.execution_owner,attempt.fence_generation
		FROM runtime_external_effect_operations operation
		JOIN runtime_external_effect_attempts attempt ON attempt.operation_id=operation.operation_id
		JOIN runtime_external_effect_attempts original ON original.operation_id=operation.operation_id AND original.attempt_ordinal=1
		WHERE operation.operation_id=$1 AND attempt.attempt_id=$2`
	if postgres {
		query += ` FOR UPDATE OF operation,attempt`
	}
	var attempt, original, operation []byte
	var kind, effectKind, bundle, state, owner string
	var fence uint64
	if err := tx.QueryRowContext(ctx, query, s.OperationID, s.AttemptID).Scan(
		&attempt, &original, &operation, &kind, &effectKind, &bundle, &state, &owner, &fence); err != nil {
		return fmt.Errorf("load exact logout settlement attempt: %w", err)
	}
	if kind != string(runtimeeffects.AuthorityChannelLogout) || effectKind != string(runtimeeffects.KindChannelLogout) ||
		bundle != a.ChannelLogout.BundleHash || owner != a.ExecutionOwner || fence != a.FenceGeneration {
		return fmt.Errorf("logout settlement contradicts original journal coordinates")
	}
	if err := requireChannelSettlementEvidence(a, attempt, original, operation); err != nil {
		return err
	}
	switch s.State {
	case runtimeeffects.StateSettled, runtimeeffects.StateOutcomeUncertain:
		if state == string(runtimeeffects.StateLaunched) || state == string(runtimeeffects.StateResponseObserved) || state == string(s.State) {
			return nil
		}
	case runtimeeffects.StateTerminalFailure:
		if state == string(runtimeeffects.StateAuthorized) || state == string(s.State) {
			return nil
		}
	}
	return fmt.Errorf("logout cannot settle %s from %s", s.State, state)
}
