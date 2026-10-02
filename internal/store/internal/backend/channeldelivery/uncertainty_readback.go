package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/google/uuid"
)

// appendUncertaintyReadbackTx is disclosure, not resend eligibility. A linked
// successor or terminal source cannot erase its predecessor's uncertain write.
func appendUncertaintyReadbackTx(ctx context.Context, tx *sql.Tx, audience render.Audience, text string, postgres bool) (string, error) {
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil {
		return "", err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != audience.PrincipalID ||
		selected.InterfaceKey != audience.InterfaceKey || selected.DeliveryEpoch != audience.DeliveryEpoch ||
		selected.ExternalAccountRef != audience.ExternalAccountRef || selected.ConversationRef != audience.ConversationRef ||
		selected.ConversationScope != audience.ConversationScope {
		return text, nil
	}
	query := `SELECT DISTINCT p.source_kind, p.source_id FROM channel_delivery_plans p
		WHERE p.state='uncertain' AND p.source_kind IN ('notice','card')
		AND p.principal_id=? AND p.interface_key=? AND p.delivery_epoch=?
		AND p.external_account_reference=? AND p.conversation_reference=? AND p.conversation_scope=?
		ORDER BY p.source_kind, p.source_id`
	if postgres {
		query = `SELECT DISTINCT p.source_kind, p.source_id::text FROM channel_delivery_plans p
			WHERE p.state='uncertain' AND p.source_kind IN ('notice','card')
			AND p.principal_id=$1::uuid AND p.interface_key=$2 AND p.delivery_epoch=$3
			AND p.external_account_reference=$4 AND p.conversation_reference=$5 AND p.conversation_scope=$6
			ORDER BY p.source_kind, p.source_id::text`
	}
	rows, err := tx.QueryContext(ctx, query, selected.PrincipalID, selected.InterfaceKey, selected.DeliveryEpoch,
		selected.ExternalAccountRef, selected.ConversationRef, string(selected.ConversationScope))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	type source struct{ kind, id string }
	var sources []source
	for rows.Next() {
		var item source
		if err := rows.Scan(&item.kind, &item.id); err != nil {
			return "", err
		}
		if uuid.Validate(item.id) != nil {
			return "", fmt.Errorf("uncertain channel source identity is invalid")
		}
		sources = append(sources, item)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	if len(sources) == 0 {
		return text, nil
	}
	lines := []string{text, "Older copies may be outdated. Current state:"}
	for _, item := range sources {
		outcome, err := uncertaintySourceOutcomeTx(ctx, tx, item.kind, item.id, postgres)
		if err != nil {
			return "", err
		}
		lines = append(lines, item.kind+" ["+item.id[:8]+"] "+outcome)
	}
	return strings.Join(lines, "\n"), nil
}

func uncertaintySourceOutcomeTx(ctx context.Context, tx *sql.Tx, kind, id string, postgres bool) (string, error) {
	if kind == PlanCard {
		card, err := decisionpersistence.LoadDecisionCardInTx(ctx, tx, id, postgres)
		if err != nil {
			return "", err
		}
		return render.CardDecisionReadback(card)
	}
	query := `SELECT COALESCE(notified, false) FROM mailbox WHERE item_id=?`
	if postgres {
		query = `SELECT COALESCE(notified, false) FROM mailbox WHERE item_id=$1::uuid FOR UPDATE`
	}
	var acknowledged bool
	if err := tx.QueryRowContext(ctx, query, id).Scan(&acknowledged); err != nil {
		return "", err
	}
	if acknowledged {
		return "Acknowledged", nil
	}
	return "Not acknowledged", nil
}
