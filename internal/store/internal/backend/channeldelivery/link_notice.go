package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func AdmitLinkNoticeTx(ctx context.Context, tx *sql.Tx, id, text string, postgres bool) (bool, error) {
	binding, supplied, create := render.LinkNoticeBinding(ctx)
	if !supplied || binding.Status != operatorchannel.BindingCurrent || binding.Interface.Validate() != nil || uuid.Validate(id) != nil || !render.ValidLinkNoticeText(text) {
		return false, fmt.Errorf("link notice requires its exact current destination")
	}
	principal, found, err := LockCurrentPrincipalTx(ctx, tx, postgres)
	if err != nil || !found || principal != binding.PrincipalID {
		return false, errors.Join(fmt.Errorf("link notice principal is not current"), err)
	}
	if found, err := replayLinkNotice(ctx, tx, id, text, binding, postgres); found || err != nil {
		return false, err
	}
	if !create {
		return false, render.ErrLinkNoticeAbsent
	}
	selected, found, err := LockDefaultTx(ctx, tx, postgres)
	if err != nil || !found {
		return false, errors.Join(fmt.Errorf("link notice destination is absent"), err)
	}
	if selected.State != StateCurrent || selected.PrincipalID != binding.PrincipalID || selected.InterfaceKey != binding.Interface.Key() || selected.BindingRevision != binding.Revision || selected.ExternalAccountRef != binding.ExternalAccountRef || selected.ConversationRef != binding.ConversationRef || selected.ConversationScope != binding.ConversationScope {
		return false, fmt.Errorf("link notice destination changed")
	}
	query := `INSERT INTO mailbox (item_id, scope, item_type, severity, summary, payload, status, notified, created_at)
		VALUES (?, 'global', ?, 'normal', ?, '{}', 'pending', false, ?) RETURNING item_id`
	if postgres {
		query = `INSERT INTO mailbox (item_id, scope, item_type, severity, summary, payload, status, notified, created_at)
			VALUES ($1::uuid, 'global', $2, 'normal', $3, '{}'::jsonb, 'pending', false, $4) RETURNING item_id`
	}
	var inserted string
	if err := tx.QueryRowContext(ctx, query, id, render.LinkNoticeType, text, time.Now().UTC()).Scan(&inserted); err != nil {
		return false, err
	}
	return insertPlanTx(ctx, tx, PlanNotice, id, selected, postgres)
}

func replayLinkNotice(ctx context.Context, tx *sql.Tx, id, text string, binding operatorchannel.Binding, postgres bool) (bool, error) {
	query := `SELECT m.item_type, m.summary, CAST(m.payload AS TEXT), COUNT(p.delivery_id), MIN(CAST(p.principal_id AS TEXT)), MIN(p.interface_key), MIN(p.binding_revision),
		MIN(p.external_account), MIN(p.conversation_reference), MIN(p.conversation_scope) FROM mailbox m LEFT JOIN channel_delivery_plans p
		ON p.source_kind='notice' AND p.source_id=m.item_id WHERE m.item_id=`
	argument := "?"
	if postgres {
		argument = "$1::uuid"
	}
	count := 0
	var kind, summary, payload string
	var saved Default
	err := tx.QueryRowContext(ctx, query+argument+` GROUP BY m.item_id,m.item_type,m.summary,m.payload`, id).Scan(&kind, &summary, &payload, &count, &saved.PrincipalID, &saved.InterfaceKey, &saved.BindingRevision, &saved.ExternalAccountRef, &saved.ConversationRef, &saved.ConversationScope)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || count != 1 || kind != render.LinkNoticeType || summary != text || payload != "{}" || saved.PrincipalID != binding.PrincipalID || saved.InterfaceKey != binding.Interface.Key() || saved.BindingRevision != binding.Revision || saved.ExternalAccountRef != binding.ExternalAccountRef || saved.ConversationRef != binding.ConversationRef || saved.ConversationScope != binding.ConversationScope {
		return false, errors.Join(fmt.Errorf("link notice replay identity conflicts"), err)
	}
	return true, nil
}
