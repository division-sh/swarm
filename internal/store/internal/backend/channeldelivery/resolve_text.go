package channeldelivery

import (
	"context"
	"database/sql"
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
