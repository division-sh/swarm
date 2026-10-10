package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

type intentObservationQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func ObserveIntent(ctx context.Context, q intentObservationQueryer, demand render.IntentObservationQuery, postgres bool) (render.IntentObservation, bool, error) {
	byMessage := demand.MessageReference != ""
	if q == nil {
		return render.IntentObservation{}, false, fmt.Errorf("channel intent observation requires its selected reader")
	}
	if err := demand.Validate(); err != nil {
		return render.IntentObservation{}, false, err
	}
	var table string
	switch demand.Kind {
	case render.IntentText:
		table = "operator_channel_text_intents"
	case render.IntentAction:
		table = "operator_channel_action_intents"
	default:
		return render.IntentObservation{}, false, fmt.Errorf("channel intent observation kind is invalid")
	}
	query := `SELECT state, COALESCE(disposition,'') FROM ` + table + ` WHERE provider=? AND provider_event_id=? AND interface_key=? LIMIT 2`
	if postgres {
		query = `SELECT state, COALESCE(disposition,'') FROM ` + table + ` WHERE provider=$1 AND provider_event_id=$2 AND interface_key=$3 LIMIT 2`
	}
	args := []any{demand.Provider, demand.ProviderEventID, demand.InterfaceKey}
	if byMessage {
		query = `SELECT state, COALESCE(disposition,'') FROM operator_channel_text_intents
			WHERE provider=? AND json_extract(fact,'$.provider_message_reference')=? AND interface_key=?
			AND json_extract(fact,'$.conversation_reference')=? LIMIT 2`
		if postgres {
			query = `SELECT state, COALESCE(disposition,'') FROM operator_channel_text_intents
				WHERE provider=$1 AND fact->>'provider_message_reference'=$2 AND interface_key=$3
				AND fact->>'conversation_reference'=$4 LIMIT 2`
		}
		args = []any{demand.Provider, demand.MessageReference, demand.InterfaceKey, demand.ConversationReference}
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return render.IntentObservation{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return render.IntentObservation{}, false, rows.Err()
	}
	var result render.IntentObservation
	if err := rows.Scan(&result.State, &result.Disposition); err != nil {
		return render.IntentObservation{}, false, err
	}
	if rows.Next() {
		return render.IntentObservation{}, false, fmt.Errorf("channel intent observation identity is not unique")
	}
	return result, true, rows.Err()
}
