package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func deploymentChannelDeliveryCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	d := authority.ChannelDelivery
	current, err := channeldelivery.DeploymentTransportCurrent(ctx, q, d.Parent, d.PrincipalID, d.InterfaceKey,
		d.BindingRevision, d.ExternalAccountRef, d.ConversationRef, d.PlanGeneration.Diagnostic(), postgres)
	if err != nil || !current {
		return false, err
	}
	plan, found, err := channeldelivery.LoadPlan(ctx, q, d.DeliveryID, postgres)
	if err != nil || !found {
		return false, err
	}
	if plan.SourceKind == channeldelivery.PlanResponse && plan.RequestParent != d.Parent {
		return false, nil
	}
	query := `SELECT r.render_input FROM channel_delivery_plans p
		JOIN channel_delivery_renders r ON r.render_id=p.current_render_id AND r.delivery_id=p.delivery_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
		JOIN operator_channel_bindings b ON b.interface_key=p.interface_key AND b.status='current'
		WHERE p.delivery_id=$1 AND p.current_render_id=$2 AND r.render_hash=$3 AND p.state='rendered'
		AND p.principal_id=$4 AND p.interface_key=$5 AND p.delivery_epoch=$6
		AND p.external_account=$7 AND p.conversation_reference=$8
		AND b.principal_id=p.principal_id AND b.binding_revision=$9
		AND b.external_account=p.external_account AND b.conversation_reference=p.conversation_reference
		AND b.conversation_scope=p.conversation_scope
		AND selected.principal_id=p.principal_id AND selected.delivery_epoch=p.delivery_epoch
		AND ((p.source_kind='response' AND p.binding_revision<=b.binding_revision)
		 OR (p.source_kind<>'response' AND selected.interface_key=p.interface_key
		 AND selected.binding_revision=b.binding_revision AND selected.external_account=p.external_account
		 AND selected.conversation_reference=p.conversation_reference AND selected.conversation_scope=p.conversation_scope))
		AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
		 WHERE other.authority_kind='channel_delivery' AND json_extract(other.authority_evidence,'$.delivery_id')=p.delivery_id
		 AND other.operation_id<>$10 AND other.state IN ('authorized','launched','response_observed'))
		AND ` + channeldelivery.SendEligibilityPredicate
	if postgres {
		query = `SELECT r.render_input FROM channel_delivery_plans p
			JOIN channel_delivery_renders r ON r.render_id=p.current_render_id AND r.delivery_id=p.delivery_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
			JOIN operator_channel_bindings b ON b.interface_key=p.interface_key AND b.status='current'
			WHERE p.delivery_id=$1::uuid AND p.current_render_id=$2::uuid AND r.render_hash=$3 AND p.state='rendered'
			AND p.principal_id=$4::uuid AND p.interface_key=$5 AND p.delivery_epoch=$6
			AND p.external_account=$7 AND p.conversation_reference=$8
			AND b.principal_id=p.principal_id AND b.binding_revision=$9
			AND b.external_account=p.external_account AND b.conversation_reference=p.conversation_reference
			AND b.conversation_scope=p.conversation_scope
			AND selected.principal_id=p.principal_id AND selected.delivery_epoch=p.delivery_epoch
			AND ((p.source_kind='response' AND p.binding_revision<=b.binding_revision)
			 OR (p.source_kind<>'response' AND selected.interface_key=p.interface_key
			 AND selected.binding_revision=b.binding_revision AND selected.external_account=p.external_account
			 AND selected.conversation_reference=p.conversation_reference AND selected.conversation_scope=p.conversation_scope))
			AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
			 WHERE other.authority_kind='channel_delivery' AND other.authority_evidence->>'delivery_id'=p.delivery_id::text
			 AND other.operation_id<>$10::uuid AND other.state IN ('authorized','launched','response_observed'))
			AND ` + channeldelivery.SendEligibilityPredicate
	}
	args := []any{d.DeliveryID, d.RenderID, d.RenderHash, d.PrincipalID, d.InterfaceKey, d.DeliveryEpoch,
		d.ExternalAccountRef, d.ConversationRef, d.BindingRevision, d.EffectOperationID}
	if d.PreviousReceiptOperationID == "" {
		query += ` AND p.current_receipt_operation_id IS NULL`
	} else {
		query += ` AND p.current_receipt_operation_id=$11 AND EXISTS (SELECT 1 FROM channel_delivery_receipts previous
			WHERE previous.effect_operation_id=$11 AND previous.delivery_id=p.delivery_id AND previous.state='sent')`
		args = append(args, d.PreviousReceiptOperationID)
	}
	if postgres && lock {
		query += ` FOR UPDATE OF p, selected, b`
	}
	var raw []byte
	err = q.QueryRowContext(ctx, query, args...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check exact deployment channel delivery: %w", err)
	}
	if plan.SourceKind == channeldelivery.PlanResponse {
		raw, err = canonicaljson.Canonicalize(raw)
		if err != nil {
			return false, err
		}
		frozen, err := render.Decode(raw, d.RenderHash)
		if err != nil {
			return false, err
		}
		return channeldelivery.ResponseSourceCurrent(ctx, q, frozen, postgres, lock)
	}
	_, err = canonicaljson.Decode(raw)
	return err == nil, err
}

func deploymentNativeSettingCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	s := authority.ChannelNativeSetting
	query := `SELECT CAST(setting.setting_id AS TEXT), setting.provider, setting.resource_slot_id,
		setting.conversation_reference, setting.scope_kind, setting.member_reference, CAST(setting.principal_id AS TEXT), setting.generation,
		setting.state, CAST(setting.install_operation_id AS TEXT), setting.entry_contract_hash, setting.entry_command,
		setting.pack_id, setting.pack_version, setting.pack_manifest_hash, setting.desired_commands,
		consumer.parent_evidence, b.interface_key, b.external_account
		FROM channel_native_settings setting
		JOIN channel_native_setting_consumers consumer ON consumer.setting_id=setting.setting_id
		JOIN deployment_channel_operations o ON o.operation_id=consumer.activation_id
		JOIN operator_channel_bindings b ON b.interface_key=consumer.interface_key
		WHERE setting.setting_id=$1 AND consumer.activation_id=$2 AND consumer.parent_kind='deployment_registration'
		AND consumer.state='current' AND consumer.activation_revision=$3 AND consumer.binding_revision=$4
		AND consumer.context_publication_generation IS NULL AND setting.language_code=''
		AND setting.pack_id=o.channel_pack_id AND setting.pack_version=o.channel_pack_version AND setting.pack_manifest_hash=o.channel_manifest_hash
		AND ((setting.scope_kind='chat' AND b.conversation_scope='direct' AND setting.member_reference='')
		 OR (setting.scope_kind='chat_member' AND b.conversation_scope='shared' AND setting.member_reference=b.external_account))
		AND NOT EXISTS (SELECT 1 FROM channel_native_setting_consumers other
		 WHERE other.setting_id=setting.setting_id AND other.state='current' AND other.activation_id<>consumer.activation_id)
		AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations prior
		 WHERE prior.authority_kind='channel_native_setting' AND json_extract(prior.authority_evidence,'$.setting_id')=setting.setting_id
		 AND prior.operation_id<>setting.install_operation_id AND prior.state IN ('launched','response_observed','outcome_uncertain'))`
	if postgres {
		query = strings.Replace(query, "json_extract(prior.authority_evidence,'$.setting_id')=setting.setting_id",
			"prior.authority_evidence->>'setting_id'=setting.setting_id::text", 1)
		if lock {
			query += ` FOR UPDATE OF setting, consumer, b`
		}
	}
	var snapshot nativeSettingAuthoritySnapshot
	var raw []byte
	var key, account string
	err := q.QueryRowContext(ctx, query, s.SettingID, s.Parent.ID, s.Parent.Revision, s.BindingRevision).Scan(
		&snapshot.settingID, &snapshot.provider, &snapshot.slotID, &snapshot.conversation, &snapshot.scopeKind,
		&snapshot.memberReference, &snapshot.principalID, &snapshot.generation, &snapshot.state, &snapshot.operationID,
		&snapshot.contract, &snapshot.entryCommand, &snapshot.packID, &snapshot.packVersion, &snapshot.packHash, &snapshot.desired,
		&raw, &key, &account)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	parent, err := channelnative.DecodeTransportParent(raw)
	if err != nil || parent != s.Parent {
		return false, err
	}
	current, err := channeldelivery.DeploymentTransportCurrent(ctx, q, parent, s.PrincipalID, key, s.BindingRevision,
		account, s.ConversationRef, s.PlanGeneration.Diagnostic(), postgres)
	if err != nil || !current {
		return false, err
	}
	canonical, err := canonicaljson.Canonicalize(snapshot.desired)
	if err != nil {
		return false, err
	}
	expected, err := channelnative.DesiredCommands(s.SettingID, s.SettingGeneration)
	return err == nil && snapshot.matchesSetting(s, canonical, expected), err
}
