package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

func resolveDeploymentText(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText) (render.ResolvedText, bool, error) {
	parent, found, err := deploymentBindingParent(ctx, tx, text.Interface.Key())
	if err != nil || !found {
		return render.ResolvedText{}, false, err
	}
	var result render.ResolvedText
	err = tx.QueryRowContext(ctx, `SELECT CAST(b.principal_id AS TEXT), b.interface_key, b.binding_revision `+
		deploymentTransportJoin+` AND o.operation_id=$1 AND o.provider=$2 AND b.interface_key=$3
		AND b.external_account=$4 AND b.conversation_reference=$5 AND b.conversation_scope=$6`,
		parent.ID, text.Provider, text.Interface.Key(), text.ExternalAccountRef, text.ConversationRef, string(text.ConversationScope)).
		Scan(&result.PrincipalID, &result.InterfaceKey, &result.BindingRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return result, false, nil
	}
	result.Parent = parent
	return result, err == nil, err
}

func resolveDeploymentInbox(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (render.ResolvedInboxEntry, bool, error) {
	bound, found, err := resolveDeploymentText(ctx, tx, text)
	if err != nil || !found {
		return render.ResolvedInboxEntry{}, false, err
	}
	qualification, found, err := loadDeploymentNativeQualification(ctx, tx, bound.Parent, postgres, false)
	if err != nil || !found {
		return render.ResolvedInboxEntry{}, false, err
	}
	result := render.ResolvedInboxEntry{Kind: render.InboxEntryNative, Parent: bound.Parent,
		PrincipalID: bound.PrincipalID, InterfaceKey: bound.InterfaceKey, BindingRevision: bound.BindingRevision,
		SettingID: qualification.projection.SettingID, SettingGeneration: qualification.projection.SettingGeneration}
	err = tx.QueryRowContext(ctx, `SELECT resource_slot_id,entry_command FROM channel_native_settings WHERE setting_id=$1`, result.SettingID).
		Scan(&result.ResourceSlotID, &result.EntryReference)
	if err != nil {
		return result, false, err
	}
	if text.EntryReference != result.EntryReference {
		return render.ResolvedInboxEntry{}, false, nil
	}
	if result.Parent.Validate() != nil {
		return result, false, fmt.Errorf("deployment inbox parent is incomplete")
	}
	return result, true, nil
}
