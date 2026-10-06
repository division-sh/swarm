package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

type intentObservationQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func ObserveIntent(ctx context.Context, q intentObservationQueryer, demand render.IntentObservationQuery, postgres bool) (render.IntentObservation, bool, error) {
	if q == nil || strings.TrimSpace(demand.Provider) == "" || strings.TrimSpace(demand.ProviderEventID) == "" || demand.InterfaceKey == "" {
		return render.IntentObservation{}, false, fmt.Errorf("channel intent observation requires exact provider, event and interface")
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
	rows, err := q.QueryContext(ctx, query, demand.Provider, demand.ProviderEventID, demand.InterfaceKey)
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
