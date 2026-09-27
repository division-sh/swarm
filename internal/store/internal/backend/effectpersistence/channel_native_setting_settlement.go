package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// A launched native write may finish after its activation retires. Settlement
// verifies the original journal attempt and physical setting, not a successor
// activation, and cannot authorize another provider write.
func requireChannelNativeSettingSettlementAuthorityTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement, postgres bool) error {
	authority := settlement.Authority
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthorityChannelNativeSetting ||
		settlement.OperationID != authority.ChannelNativeSetting.EffectOperationID {
		return fmt.Errorf("native setting settlement authority is invalid")
	}
	query := `SELECT operation.authority_evidence, operation.authority_kind, operation.effect_kind,
		operation.bundle_hash, attempt.state, attempt.execution_owner, attempt.fence_generation,
		setting.generation, setting.install_operation_id
		FROM runtime_external_effect_operations operation
		JOIN runtime_external_effect_attempts attempt ON attempt.operation_id=operation.operation_id
		JOIN channel_native_settings setting ON setting.setting_id=?
		WHERE operation.operation_id=? AND attempt.attempt_id=?`
	if postgres {
		query = `SELECT operation.authority_evidence, operation.authority_kind, operation.effect_kind,
			operation.bundle_hash, attempt.state, attempt.execution_owner, attempt.fence_generation,
			setting.generation, setting.install_operation_id::text
			FROM runtime_external_effect_operations operation
			JOIN runtime_external_effect_attempts attempt ON attempt.operation_id=operation.operation_id
			JOIN channel_native_settings setting ON setting.setting_id=$1::uuid
			WHERE operation.operation_id=$2::uuid AND attempt.attempt_id=$3::uuid
			FOR UPDATE OF operation, attempt, setting`
	}
	var storedEvidence []byte
	var kind, effectKind, bundleHash, state, owner, installOperationID string
	var fence uint64
	var generation int64
	if err := tx.QueryRowContext(ctx, query, authority.ChannelNativeSetting.SettingID, settlement.OperationID, settlement.AttemptID).Scan(
		&storedEvidence, &kind, &effectKind, &bundleHash, &state, &owner, &fence, &generation, &installOperationID,
	); err != nil {
		return fmt.Errorf("load exact native setting settlement attempt: %w", err)
	}
	if kind != string(runtimeeffects.AuthorityChannelNativeSetting) || effectKind != string(runtimeeffects.KindChannelNativeSetting) ||
		bundleHash != authority.ChannelNativeSetting.BundleHash || owner != authority.ExecutionOwner ||
		fence != authority.FenceGeneration || generation != authority.ChannelNativeSetting.SettingGeneration ||
		installOperationID != settlement.OperationID {
		return fmt.Errorf("native setting settlement attempt contradicts authority")
	}
	wantRaw, err := json.Marshal(authority.Evidence())
	if err != nil {
		return err
	}
	want, err := canonicaljson.Canonicalize(wantRaw)
	if err != nil {
		return err
	}
	got, err := canonicaljson.Canonicalize(storedEvidence)
	if err != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("native setting settlement evidence contradicts original authority")
	}
	switch settlement.State {
	case runtimeeffects.StateSettled, runtimeeffects.StateOutcomeUncertain:
		if state != string(runtimeeffects.StateLaunched) && state != string(runtimeeffects.StateResponseObserved) && state != string(settlement.State) {
			return fmt.Errorf("native setting cannot settle %s from %s", settlement.State, state)
		}
	case runtimeeffects.StateTerminalFailure:
		if state != string(runtimeeffects.StateAuthorized) && state != string(runtimeeffects.StateTerminalFailure) {
			return fmt.Errorf("native setting prelaunch failure contradicts %s", state)
		}
	default:
		return fmt.Errorf("unsupported native setting settlement %s", settlement.State)
	}
	return nil
}

func projectChannelNativeSettingSettlementTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement, postgres bool) error {
	if settlement.Authority.Kind != runtimeeffects.AuthorityChannelNativeSetting {
		return nil
	}
	s := settlement.Authority.ChannelNativeSetting
	if !settlement.Authority.Valid() || settlement.OperationID != s.EffectOperationID {
		return fmt.Errorf("native setting settlement has contradictory authority")
	}
	if settlement.State == runtimeeffects.StateTerminalFailure {
		return nil
	}
	state, readbackHash := "uncertain", any(nil)
	if settlement.State == runtimeeffects.StateSettled {
		provided, ok := settlement.Evidence["readback_hash"].(string)
		desired, err := channelnative.DesiredCommands()
		if err != nil {
			return err
		}
		if !ok || provided != runtimeeffects.Fingerprint(desired) {
			return fmt.Errorf("native setting success lacks exact compiled readback")
		}
		query := `SELECT COUNT(*) FROM runtime_external_effect_operations prior
			WHERE prior.authority_kind='channel_native_setting'
			AND json_extract(prior.authority_evidence, '$.setting_id')=?
			AND prior.operation_id<>?
			AND prior.state IN ('launched','response_observed','outcome_uncertain')`
		if postgres {
			query = `SELECT COUNT(*) FROM runtime_external_effect_operations prior
				WHERE prior.authority_kind='channel_native_setting'
				AND prior.authority_evidence->>'setting_id'=$1
				AND prior.operation_id<>$2::uuid
				AND prior.state IN ('launched','response_observed','outcome_uncertain')`
		}
		var predecessors int64
		if err := tx.QueryRowContext(ctx, query, s.SettingID, settlement.OperationID).Scan(&predecessors); err != nil {
			return err
		}
		if predecessors != 0 {
			return fmt.Errorf("native setting has unresolved predecessor write")
		}
		state, readbackHash = "installed", provided
	}
	query := `UPDATE channel_native_settings SET state=?, readback_hash=?, updated_at=?
		WHERE setting_id=? AND generation=? AND install_operation_id=?
		AND state IN ('planned','uncertain','retired','installed')`
	if postgres {
		query = `UPDATE channel_native_settings SET state=$1, readback_hash=$2, updated_at=$3
			WHERE setting_id=$4::uuid AND generation=$5 AND install_operation_id=$6::uuid
			AND state IN ('planned','uncertain','retired','installed')`
	}
	if state == "installed" {
		query = `UPDATE channel_native_settings SET state=CASE WHEN EXISTS (
			SELECT 1 FROM channel_native_setting_consumers c WHERE c.setting_id=channel_native_settings.setting_id AND c.state='current')
			THEN 'installed' ELSE 'retired' END, readback_hash=?, updated_at=?
			WHERE setting_id=? AND generation=? AND install_operation_id=?
			AND state IN ('planned','uncertain','retired','installed')`
		if postgres {
			query = `UPDATE channel_native_settings SET state=CASE WHEN EXISTS (
				SELECT 1 FROM channel_native_setting_consumers c WHERE c.setting_id=channel_native_settings.setting_id AND c.state='current')
				THEN 'installed' ELSE 'retired' END, readback_hash=$1, updated_at=$2
				WHERE setting_id=$3::uuid AND generation=$4 AND install_operation_id=$5::uuid
				AND state IN ('planned','uncertain','retired','installed')`
		}
		result, err := tx.ExecContext(ctx, query, readbackHash, settlement.Now.UTC(), s.SettingID, s.SettingGeneration, settlement.OperationID)
		if err != nil {
			return err
		}
		return requireExternalAttemptTransition(result, nil)
	}
	result, err := tx.ExecContext(ctx, query, state, readbackHash, settlement.Now.UTC(), s.SettingID, s.SettingGeneration, settlement.OperationID)
	if err != nil {
		return err
	}
	return requireExternalAttemptTransition(result, nil)
}
