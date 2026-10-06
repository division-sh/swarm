package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

func ResolveCurrentTextTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (render.ResolvedText, bool, error) {
	if tx == nil {
		return render.ResolvedText{}, false, fmt.Errorf("channel text requires selected transaction")
	}
	if err := text.Validate(); err != nil {
		return render.ResolvedText{}, false, err
	}
	query := `SELECT b.principal_id, b.interface_key, b.binding_revision
		FROM operator_channel_bindings b
		WHERE b.interface_key=? AND b.status='current'
		AND b.external_account_reference=? AND b.conversation_reference=? AND b.conversation_scope=?
		AND EXISTS (SELECT 1 FROM connected_channel_activations a
			JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
			WHERE a.interface_key=b.interface_key AND a.status='current' AND o.phase='succeeded'
			AND o.identity_operation_id=b.operation_id AND a.provider=?
			AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
			AND a.conversation_reference=b.conversation_reference)
		LIMIT 2`
	if postgres {
		query = `SELECT b.principal_id::text, b.interface_key, b.binding_revision
			FROM operator_channel_bindings b
			WHERE b.interface_key=$1 AND b.status='current'
			AND b.external_account_reference=$2 AND b.conversation_reference=$3 AND b.conversation_scope=$4
			AND EXISTS (SELECT 1 FROM connected_channel_activations a
				JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
				WHERE a.interface_key=b.interface_key AND a.status='current' AND o.phase='succeeded'
				AND o.identity_operation_id=b.operation_id AND a.provider=$5
				AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
				AND a.conversation_reference=b.conversation_reference)
			LIMIT 2`
	}
	rows, err := tx.QueryContext(ctx, query, text.Interface.Key(), text.ExternalAccountRef,
		text.ConversationRef, string(text.ConversationScope), text.Provider)
	if err != nil {
		return render.ResolvedText{}, false, err
	}
	defer rows.Close()
	var result render.ResolvedText
	if !rows.Next() {
		return render.ResolvedText{}, false, rows.Err()
	}
	if err := rows.Scan(&result.PrincipalID, &result.InterfaceKey, &result.BindingRevision); err != nil {
		return render.ResolvedText{}, false, err
	}
	if rows.Next() {
		return render.ResolvedText{}, false, fmt.Errorf("channel text resolves multiple current bindings")
	}
	return result, true, rows.Err()
}

func ResolveCurrentInboxEntryTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (render.ResolvedInboxEntry, bool, error) {
	if tx == nil {
		return render.ResolvedInboxEntry{}, false, fmt.Errorf("native inbox entry requires selected transaction")
	}
	if err := RequireTextIntentTx(ctx, tx, text, postgres); err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	if text.EntryReference == "" {
		return render.ResolvedInboxEntry{}, false, nil
	}
	if text.EntryReference == render.TextReplyInboxReference {
		bound, current, err := ResolveCurrentTextTx(ctx, tx, text, postgres)
		if err != nil || !current {
			return render.ResolvedInboxEntry{}, false, err
		}
		activationID, bindingRevision, audience, err := currentBoundTextAudienceTx(ctx, tx, text, postgres, false)
		if err != nil {
			return render.ResolvedInboxEntry{}, false, err
		}
		if bindingRevision != bound.BindingRevision || audience.PrincipalID != bound.PrincipalID || audience.InterfaceKey != bound.InterfaceKey {
			return render.ResolvedInboxEntry{}, false, fmt.Errorf("text/reply inbox authority changed")
		}
		return render.ResolvedInboxEntry{
			Kind: render.InboxEntryTextReply, PrincipalID: bound.PrincipalID, InterfaceKey: bound.InterfaceKey,
			BindingRevision: bound.BindingRevision, ActivationID: activationID, EntryReference: text.EntryReference,
		}, true, nil
	}
	query := `SELECT b.principal_id, b.interface_key, b.binding_revision, a.activation_id,
		s.setting_id, s.resource_slot_id, s.generation, s.entry_command
		FROM operator_channel_bindings b
		JOIN connected_channel_activations a ON a.interface_key=b.interface_key
			AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
			AND a.conversation_reference=b.conversation_reference AND a.status='current'
		JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
			AND o.identity_operation_id=b.operation_id AND o.phase='succeeded'
		JOIN channel_native_setting_consumers c ON c.activation_id=a.activation_id
			AND c.activation_revision=a.activation_revision AND c.interface_key=a.interface_key
			AND c.binding_revision=a.binding_revision
			AND c.context_publication_generation=a.context_publication_generation AND c.state='current'
		JOIN channel_native_settings s ON s.setting_id=c.setting_id
			AND s.principal_id=b.principal_id AND s.provider=a.provider
			AND s.conversation_reference=b.conversation_reference AND s.language_code=''
			AND s.pack_id=a.channel_pack_id AND s.pack_version=a.channel_pack_version
			AND s.pack_manifest_hash=a.channel_manifest_hash
			AND s.scope_kind=CASE WHEN b.conversation_scope='direct' THEN 'chat' ELSE 'chat_member' END
			AND s.member_reference=CASE WHEN b.conversation_scope='direct' THEN '' ELSE b.external_account_reference END
		WHERE b.interface_key=? AND b.status='current' AND b.external_account_reference=?
			AND b.conversation_reference=? AND b.conversation_scope=? AND a.provider=?
			AND s.entry_command=? LIMIT 2`
	if postgres {
		query = `SELECT b.principal_id::text, b.interface_key, b.binding_revision, a.activation_id::text,
			s.setting_id::text, s.resource_slot_id, s.generation, s.entry_command
			FROM operator_channel_bindings b
			JOIN connected_channel_activations a ON a.interface_key=b.interface_key
				AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
				AND a.conversation_reference=b.conversation_reference AND a.status='current'
			JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
				AND o.identity_operation_id=b.operation_id AND o.phase='succeeded'
			JOIN channel_native_setting_consumers c ON c.activation_id=a.activation_id
				AND c.activation_revision=a.activation_revision AND c.interface_key=a.interface_key
				AND c.binding_revision=a.binding_revision
				AND c.context_publication_generation=a.context_publication_generation AND c.state='current'
			JOIN channel_native_settings s ON s.setting_id=c.setting_id
				AND s.principal_id=b.principal_id AND s.provider=a.provider
				AND s.conversation_reference=b.conversation_reference AND s.language_code=''
				AND s.pack_id=a.channel_pack_id AND s.pack_version=a.channel_pack_version
				AND s.pack_manifest_hash=a.channel_manifest_hash
				AND s.scope_kind=CASE WHEN b.conversation_scope='direct' THEN 'chat' ELSE 'chat_member' END
				AND s.member_reference=CASE WHEN b.conversation_scope='direct' THEN '' ELSE b.external_account_reference END
			WHERE b.interface_key=$1 AND b.status='current' AND b.external_account_reference=$2
				AND b.conversation_reference=$3 AND b.conversation_scope=$4 AND a.provider=$5
				AND s.entry_command=$6 LIMIT 2`
	}
	rows, err := tx.QueryContext(ctx, query, text.Interface.Key(), text.ExternalAccountRef,
		text.ConversationRef, string(text.ConversationScope), text.Provider, text.EntryReference)
	if err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	defer rows.Close()
	var result render.ResolvedInboxEntry
	if !rows.Next() {
		return render.ResolvedInboxEntry{}, false, rows.Err()
	}
	if err := rows.Scan(&result.PrincipalID, &result.InterfaceKey, &result.BindingRevision,
		&result.ActivationID, &result.SettingID, &result.ResourceSlotID, &result.SettingGeneration, &result.EntryReference); err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	if rows.Next() {
		return render.ResolvedInboxEntry{}, false, fmt.Errorf("native inbox entry resolves multiple current settings")
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	count, err := currentNativeConsumersTx(ctx, tx, result.SettingID, postgres)
	if err != nil || count != 1 {
		return render.ResolvedInboxEntry{}, false, errors.Join(err, fmt.Errorf("native inbox entry requires one current connection"))
	}
	// Discovery keeps a known generation retryable. Usability is admitted only
	// by fresh provider qualification and PlanInboxResponseTx.
	result.Kind = render.InboxEntryNative
	return result, true, nil
}
