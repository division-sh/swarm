package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func requireChannelNativeSettingAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, postgres bool) error {
	if err := channeldelivery.LockPrincipalTx(ctx, tx, authority.ChannelNativeSetting.PrincipalID, postgres); err != nil {
		return invalidExternalAuthority(authority, "principal_not_current")
	}
	current, err := channelNativeSettingAuthorityCurrent(ctx, tx, authority, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(authority, "stale")
	}
	return nil
}

func channelNativeSettingAuthorityCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthorityChannelNativeSetting {
		return false, nil
	}
	s := authority.ChannelNativeSetting
	query := `SELECT setting.setting_id, setting.provider, setting.resource_slot_id,
		setting.conversation_reference, setting.scope_kind, setting.member_reference,
		setting.principal_id, setting.generation,
		setting.state, setting.install_operation_id, setting.entry_contract_hash, setting.entry_command,
		setting.pack_id, setting.pack_version, setting.pack_manifest_hash, setting.desired_commands,
		consumer.activation_revision, consumer.binding_revision, consumer.context_publication_generation,
		activation.activation_revision, activation.binding_revision, activation.context_publication_generation,
		activation.provider, activation.principal_id, activation.conversation_reference,
		activation.channel_pack_id, activation.channel_pack_version, activation.channel_manifest_hash,
		activation.bundle_hash, activation.bundle_identity, activation.pack_inventory_generation,
		activation.runtime_instance_id, activation.plan_generation, activation.target_generation
		FROM channel_native_settings setting
		JOIN channel_native_setting_consumers consumer ON consumer.setting_id=setting.setting_id
		JOIN connected_channel_activations activation ON activation.activation_id=consumer.activation_id
		JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
		JOIN operator_channel_bindings binding ON binding.interface_key=activation.interface_key
		WHERE setting.setting_id=? AND consumer.activation_id=?
		AND setting.language_code=''
		AND consumer.state='current' AND activation.status='current'
		AND consumer.activation_revision=activation.activation_revision
		AND consumer.binding_revision=activation.binding_revision
		AND consumer.context_publication_generation=activation.context_publication_generation
		AND consumer.interface_key=activation.interface_key
		AND onboarding.phase='succeeded' AND onboarding.identity_operation_id=binding.operation_id
		AND binding.status='current' AND binding.principal_id=activation.principal_id
		AND binding.binding_revision=activation.binding_revision
		AND binding.conversation_reference=activation.conversation_reference
		AND ((setting.scope_kind='chat' AND binding.conversation_scope='direct' AND setting.member_reference='')
			OR (setting.scope_kind='chat_member' AND binding.conversation_scope='shared'
			AND setting.member_reference=binding.external_account_reference))
		AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations prior
			WHERE prior.authority_kind='channel_native_setting'
			AND json_extract(prior.authority_evidence, '$.setting_id')=setting.setting_id
			AND prior.operation_id<>setting.install_operation_id
			AND prior.state IN ('launched','response_observed','outcome_uncertain'))`
	if postgres {
		query = `SELECT setting.setting_id::text, setting.provider, setting.resource_slot_id,
			setting.conversation_reference, setting.scope_kind, setting.member_reference,
			setting.principal_id::text, setting.generation,
			setting.state, setting.install_operation_id::text, setting.entry_contract_hash, setting.entry_command,
			setting.pack_id, setting.pack_version, setting.pack_manifest_hash, setting.desired_commands,
			consumer.activation_revision, consumer.binding_revision, consumer.context_publication_generation,
			activation.activation_revision, activation.binding_revision, activation.context_publication_generation,
			activation.provider, activation.principal_id::text, activation.conversation_reference,
			activation.channel_pack_id, activation.channel_pack_version, activation.channel_manifest_hash,
			activation.bundle_hash, activation.bundle_identity, activation.pack_inventory_generation,
			activation.runtime_instance_id::text, activation.plan_generation, activation.target_generation
			FROM channel_native_settings setting
			JOIN channel_native_setting_consumers consumer ON consumer.setting_id=setting.setting_id
			JOIN connected_channel_activations activation ON activation.activation_id=consumer.activation_id
			JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
			JOIN operator_channel_bindings binding ON binding.interface_key=activation.interface_key
			WHERE setting.setting_id=$1::uuid AND consumer.activation_id=$2::uuid
			AND setting.language_code=''
			AND consumer.state='current' AND activation.status='current'
			AND consumer.activation_revision=activation.activation_revision
			AND consumer.binding_revision=activation.binding_revision
			AND consumer.context_publication_generation=activation.context_publication_generation
			AND consumer.interface_key=activation.interface_key
			AND onboarding.phase='succeeded' AND onboarding.identity_operation_id=binding.operation_id
			AND binding.status='current' AND binding.principal_id=activation.principal_id
			AND binding.binding_revision=activation.binding_revision
			AND binding.conversation_reference=activation.conversation_reference
			AND ((setting.scope_kind='chat' AND binding.conversation_scope='direct' AND setting.member_reference='')
				OR (setting.scope_kind='chat_member' AND binding.conversation_scope='shared'
				AND setting.member_reference=binding.external_account_reference))
			AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations prior
				WHERE prior.authority_kind='channel_native_setting'
				AND prior.authority_evidence->>'setting_id'=setting.setting_id::text
				AND prior.operation_id<>setting.install_operation_id
				AND prior.state IN ('launched','response_observed','outcome_uncertain'))`
		if lock {
			query += ` FOR UPDATE OF setting, consumer, activation, onboarding, binding`
		}
	}
	var settingID, provider, slotID, conversation, scopeKind, memberReference, principalID, state, operationID, contract, entryCommand string
	var packID, packVersion, packHash string
	var desired []byte
	var generation, consumerActivationRevision, consumerBindingRevision, consumerContextGeneration int64
	var activationRevision, activationBindingRevision, activationContextGeneration int64
	var activationProvider, activationPrincipal, activationConversation, activationPackID, activationPackVersion, activationPackHash string
	var bundleHash, bundleIdentity, inventoryGeneration, runtimeInstanceID, planGeneration string
	var targetGeneration int64
	err := q.QueryRowContext(ctx, query, s.SettingID, s.ActivationID).Scan(
		&settingID, &provider, &slotID, &conversation, &scopeKind, &memberReference, &principalID, &generation,
		&state, &operationID, &contract, &entryCommand, &packID, &packVersion, &packHash, &desired,
		&consumerActivationRevision, &consumerBindingRevision, &consumerContextGeneration,
		&activationRevision, &activationBindingRevision, &activationContextGeneration,
		&activationProvider, &activationPrincipal, &activationConversation,
		&activationPackID, &activationPackVersion, &activationPackHash,
		&bundleHash, &bundleIdentity, &inventoryGeneration, &runtimeInstanceID, &planGeneration, &targetGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check physical native inbox setting authority: %w", err)
	}
	canonical, err := canonicaljson.Canonicalize(desired)
	if err != nil {
		return false, err
	}
	expected, err := channelnative.DesiredCommands(s.SettingID, s.SettingGeneration)
	if err != nil {
		return false, err
	}
	return state == "planned" && settingID == s.SettingID && provider == s.Provider &&
		slotID == s.ResourceSlotID && conversation == s.ConversationRef && scopeKind == s.ScopeKind &&
		memberReference == s.MemberReference && principalID == s.PrincipalID && entryCommand == s.EntryCommand &&
		generation == s.SettingGeneration && operationID == s.EffectOperationID &&
		contract == s.EntryContractHash && packID == s.PackID && packVersion == s.PackVersion &&
		packHash == s.PackManifestHash && bytes.Equal(canonical, expected) &&
		consumerActivationRevision == s.ActivationRevision && consumerBindingRevision == s.BindingRevision &&
		consumerContextGeneration == int64(s.ContextPublicationGeneration) &&
		activationRevision == s.ActivationRevision && activationBindingRevision == s.BindingRevision &&
		activationContextGeneration == int64(s.ContextPublicationGeneration) &&
		activationProvider == s.Provider && activationPrincipal == s.PrincipalID &&
		activationConversation == s.ConversationRef && activationPackID == s.PackID &&
		activationPackVersion == s.PackVersion && activationPackHash == s.PackManifestHash &&
		bundleHash == s.BundleHash && bundleIdentity == s.BundleIdentity &&
		inventoryGeneration == s.PackInventoryGeneration && runtimeInstanceID == s.RuntimeInstanceID &&
		planGeneration == s.PlanGeneration.Diagnostic() && targetGeneration == int64(s.TargetGeneration), nil
}
