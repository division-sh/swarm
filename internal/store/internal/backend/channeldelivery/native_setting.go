package channeldelivery

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

func AttachNativeInboxSettingTx(ctx context.Context, tx *sql.Tx, admission channelnative.Admission, postgres bool) (channelnative.Setting, error) {
	if tx == nil {
		return channelnative.Setting{}, fmt.Errorf("native inbox setting requires selected transaction")
	}
	if err := admission.Validate(); err != nil {
		return channelnative.Setting{}, err
	}
	query := `SELECT a.provider, a.principal_id, a.interface_key, a.binding_revision,
		a.activation_revision, a.context_publication_generation, a.channel_pack_id,
		a.channel_pack_version, a.channel_manifest_hash, a.conversation_reference
		FROM connected_channel_activations a
		JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
		JOIN operator_channel_bindings b ON b.interface_key=a.interface_key
		WHERE a.activation_id=? AND a.status='current' AND o.phase='succeeded'
		AND o.identity_operation_id=b.operation_id AND b.status='current'
		AND b.principal_id=a.principal_id AND b.binding_revision=a.binding_revision
		AND b.conversation_reference=a.conversation_reference`
	if postgres {
		query = `SELECT a.provider, a.principal_id::text, a.interface_key, a.binding_revision,
			a.activation_revision, a.context_publication_generation, a.channel_pack_id,
			a.channel_pack_version, a.channel_manifest_hash, a.conversation_reference
			FROM connected_channel_activations a
			JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
			JOIN operator_channel_bindings b ON b.interface_key=a.interface_key
			WHERE a.activation_id=$1::uuid AND a.status='current' AND o.phase='succeeded'
			AND o.identity_operation_id=b.operation_id AND b.status='current'
			AND b.principal_id=a.principal_id AND b.binding_revision=a.binding_revision
			AND b.conversation_reference=a.conversation_reference
			FOR UPDATE OF a, o, b`
	}
	var provider, principalID, interfaceKey, packID, packVersion, packHash, conversation string
	var bindingRevision, activationRevision, contextGeneration int64
	err := tx.QueryRowContext(ctx, query, admission.ActivationID).Scan(&provider, &principalID, &interfaceKey,
		&bindingRevision, &activationRevision, &contextGeneration, &packID, &packVersion, &packHash, &conversation)
	if errors.Is(err, sql.ErrNoRows) {
		return channelnative.Setting{}, fmt.Errorf("native inbox activation is not exact-current")
	}
	if err != nil {
		return channelnative.Setting{}, err
	}
	if provider != admission.Provider || principalID != admission.PrincipalID || interfaceKey != admission.InterfaceKey ||
		bindingRevision != admission.BindingRevision || activationRevision != admission.ActivationRevision ||
		contextGeneration != admission.ContextPublicationGeneration || packID != admission.PackID ||
		packVersion != admission.PackVersion || packHash != admission.PackManifestHash ||
		conversation != admission.ConversationReference {
		return channelnative.Setting{}, fmt.Errorf("native inbox setting contradicts current activation")
	}
	if err := retireStaleNativeInboxConsumersTx(ctx, tx, postgres); err != nil {
		return channelnative.Setting{}, err
	}
	desired, err := channelnative.DesiredCommands()
	if err != nil {
		return channelnative.Setting{}, err
	}
	setting := channelnative.Setting{
		Provider: admission.Provider, ResourceSlotID: admission.ResourceSlotID,
		ConversationRef: admission.ConversationReference, EntryContractHash: admission.EntryContractHash,
		PrincipalID: admission.PrincipalID,
	}
	query = `SELECT setting_id, principal_id, pack_id, pack_version, pack_manifest_hash,
		entry_contract_hash, desired_commands, generation, state, COALESCE(install_operation_id, '')
		FROM channel_native_settings WHERE provider=? AND resource_slot_id=? AND conversation_reference=?
		AND scope_kind='chat' AND language_code=''`
	if postgres {
		query = `SELECT setting_id::text, principal_id::text, pack_id, pack_version, pack_manifest_hash,
			entry_contract_hash, desired_commands, generation, state, COALESCE(install_operation_id::text, '')
			FROM channel_native_settings WHERE provider=$1 AND resource_slot_id=$2 AND conversation_reference=$3
			AND scope_kind='chat' AND language_code='' FOR UPDATE`
	}
	var existingPrincipal, existingPack, existingVersion, existingHash, contract string
	var raw []byte
	err = tx.QueryRowContext(ctx, query, admission.Provider, admission.ResourceSlotID, admission.ConversationReference).
		Scan(&setting.SettingID, &existingPrincipal, &existingPack, &existingVersion, &existingHash,
			&contract, &raw, &setting.Generation, &setting.State, &setting.InstallOperationID)
	if errors.Is(err, sql.ErrNoRows) {
		setting.SettingID, setting.Generation, setting.State = uuid.NewString(), 1, "planned"
		query = `INSERT INTO channel_native_settings
			(setting_id, provider, resource_slot_id, conversation_reference, scope_kind, language_code,
			pack_id, pack_version, pack_manifest_hash, entry_contract_hash, desired_commands,
			principal_id, generation, state, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'chat', '', ?, ?, ?, ?, ?, ?, 1, 'planned', ?, ?)`
		if postgres {
			query = `INSERT INTO channel_native_settings
				(setting_id, provider, resource_slot_id, conversation_reference, scope_kind, language_code,
				pack_id, pack_version, pack_manifest_hash, entry_contract_hash, desired_commands,
				principal_id, generation, state, created_at, updated_at)
				VALUES ($1::uuid, $2, $3, $4, 'chat', '', $5, $6, $7, $8, $9::jsonb, $10::uuid, 1, 'planned', $11, $12)`
		}
		now := time.Now().UTC()
		if _, err := tx.ExecContext(ctx, query, setting.SettingID, admission.Provider, admission.ResourceSlotID,
			admission.ConversationReference, admission.PackID, admission.PackVersion, admission.PackManifestHash,
			admission.EntryContractHash, string(desired), admission.PrincipalID, now, now); err != nil {
			return channelnative.Setting{}, fmt.Errorf("create physical native inbox setting: %w", err)
		}
	} else if err != nil {
		return channelnative.Setting{}, err
	} else {
		canonical, err := canonicaljson.Canonicalize(raw)
		if err != nil {
			return channelnative.Setting{}, err
		}
		compatible := existingPrincipal == admission.PrincipalID && existingPack == admission.PackID &&
			existingVersion == admission.PackVersion && existingHash == admission.PackManifestHash &&
			contract == admission.EntryContractHash && bytes.Equal(canonical, desired)
		if setting.State == "uncertain" || setting.State == "unavailable" {
			return channelnative.Setting{}, fmt.Errorf("native inbox setting has unresolved or foreign provider state")
		}
		if !compatible || setting.State == "retired" {
			count, err := currentNativeConsumersTx(ctx, tx, setting.SettingID, postgres)
			if err != nil {
				return channelnative.Setting{}, err
			}
			if count != 0 {
				return channelnative.Setting{}, fmt.Errorf("incompatible native inbox setting has current consumers")
			}
			query = `UPDATE channel_native_settings SET principal_id=?, pack_id=?, pack_version=?,
				pack_manifest_hash=?, entry_contract_hash=?, desired_commands=?, generation=generation+1,
				state='planned', install_operation_id=NULL, readback_hash=NULL, updated_at=?
				WHERE setting_id=? AND state <> 'uncertain'`
			if postgres {
				query = `UPDATE channel_native_settings SET principal_id=$1::uuid, pack_id=$2, pack_version=$3,
					pack_manifest_hash=$4, entry_contract_hash=$5, desired_commands=$6::jsonb, generation=generation+1,
					state='planned', install_operation_id=NULL, readback_hash=NULL, updated_at=$7
					WHERE setting_id=$8::uuid AND state <> 'uncertain'`
			}
			result, err := tx.ExecContext(ctx, query, admission.PrincipalID, admission.PackID, admission.PackVersion,
				admission.PackManifestHash, admission.EntryContractHash, string(desired), time.Now().UTC(), setting.SettingID)
			if err != nil {
				return channelnative.Setting{}, err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				return channelnative.Setting{}, fmt.Errorf("native inbox setting generation did not advance: %w", err)
			}
			setting.Generation++
			setting.State = "planned"
			setting.InstallOperationID = ""
		}
	}
	query = `INSERT INTO channel_native_setting_consumers
		(setting_id, activation_id, activation_revision, interface_key, binding_revision,
		context_publication_generation, state, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'current', ?)
		ON CONFLICT (setting_id, activation_id) DO UPDATE SET
		activation_revision=excluded.activation_revision, interface_key=excluded.interface_key,
		binding_revision=excluded.binding_revision,
		context_publication_generation=excluded.context_publication_generation,
		state='current', updated_at=excluded.updated_at`
	if postgres {
		query = `INSERT INTO channel_native_setting_consumers
			(setting_id, activation_id, activation_revision, interface_key, binding_revision,
			context_publication_generation, state, updated_at)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'current', $7)
			ON CONFLICT (setting_id, activation_id) DO UPDATE SET
			activation_revision=excluded.activation_revision, interface_key=excluded.interface_key,
			binding_revision=excluded.binding_revision,
			context_publication_generation=excluded.context_publication_generation,
			state='current', updated_at=excluded.updated_at`
	}
	if _, err := tx.ExecContext(ctx, query, setting.SettingID, admission.ActivationID, admission.ActivationRevision,
		admission.InterfaceKey, admission.BindingRevision, admission.ContextPublicationGeneration, time.Now().UTC()); err != nil {
		return channelnative.Setting{}, fmt.Errorf("attach native inbox activation consumer: %w", err)
	}
	setting.CurrentConsumerCount, err = currentNativeConsumersTx(ctx, tx, setting.SettingID, postgres)
	return setting, err
}

func currentNativeConsumersTx(ctx context.Context, tx *sql.Tx, settingID string, postgres bool) (int64, error) {
	query := `SELECT COUNT(*) FROM channel_native_setting_consumers WHERE setting_id=? AND state='current'`
	if postgres {
		query = `SELECT COUNT(*) FROM channel_native_setting_consumers WHERE setting_id=$1::uuid AND state='current'`
	}
	var count int64
	err := tx.QueryRowContext(ctx, query, settingID).Scan(&count)
	return count, err
}

func RetireStaleNativeInboxConsumersTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("native inbox retirement requires selected transaction")
	}
	return retireStaleNativeInboxConsumersTx(ctx, tx, postgres)
}

func retireStaleNativeInboxConsumersTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	query := `UPDATE channel_native_setting_consumers SET state='retired', updated_at=?
		WHERE state='current' AND NOT EXISTS (
			SELECT 1 FROM connected_channel_activations a
			JOIN operator_channel_bindings b ON b.interface_key=a.interface_key
			WHERE a.activation_id=channel_native_setting_consumers.activation_id
			AND a.status='current' AND b.status='current'
			AND a.activation_revision=channel_native_setting_consumers.activation_revision
			AND a.context_publication_generation=channel_native_setting_consumers.context_publication_generation
			AND a.interface_key=channel_native_setting_consumers.interface_key
			AND a.binding_revision=channel_native_setting_consumers.binding_revision
			AND b.binding_revision=a.binding_revision AND b.principal_id=a.principal_id
		)`
	if postgres {
		query = `UPDATE channel_native_setting_consumers SET state='retired', updated_at=$1
			WHERE state='current' AND NOT EXISTS (
				SELECT 1 FROM connected_channel_activations a
				JOIN operator_channel_bindings b ON b.interface_key=a.interface_key
				WHERE a.activation_id=channel_native_setting_consumers.activation_id
				AND a.status='current' AND b.status='current'
				AND a.activation_revision=channel_native_setting_consumers.activation_revision
				AND a.context_publication_generation=channel_native_setting_consumers.context_publication_generation
				AND a.interface_key=channel_native_setting_consumers.interface_key
				AND a.binding_revision=channel_native_setting_consumers.binding_revision
				AND b.binding_revision=a.binding_revision AND b.principal_id=a.principal_id
			)`
	}
	if _, err := tx.ExecContext(ctx, query, time.Now().UTC()); err != nil {
		return err
	}
	query = `UPDATE channel_native_settings SET state='retired', updated_at=?
		WHERE state IN ('planned','installed') AND NOT EXISTS (
			SELECT 1 FROM channel_native_setting_consumers c
			WHERE c.setting_id=channel_native_settings.setting_id AND c.state='current')`
	if postgres {
		query = `UPDATE channel_native_settings SET state='retired', updated_at=$1
			WHERE state IN ('planned','installed') AND NOT EXISTS (
				SELECT 1 FROM channel_native_setting_consumers c
				WHERE c.setting_id=channel_native_settings.setting_id AND c.state='current')`
	}
	_, err := tx.ExecContext(ctx, query, time.Now().UTC())
	return err
}
