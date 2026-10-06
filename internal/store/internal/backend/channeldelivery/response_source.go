package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

// ResponseSourceCurrent admits a response from one settled responsibility.
// A reply transfer retains text evidence, not a second response authority.
func ResponseSourceCurrent(ctx context.Context, q intentQueryer, frozen render.Frozen, postgres, lock bool) (bool, error) {
	publicationID := frozen.SourceID
	if q == nil || uuid.Validate(publicationID) != nil {
		return false, fmt.Errorf("channel response requires an exact source occurrence")
	}
	if err := frozen.Validate(); err != nil {
		return false, err
	}
	currentPurpose := func() (bool, error) {
		if frozen.InputCard != nil {
			return inputPromptCurrent(ctx, q, frozen, postgres, lock)
		}
		return true, nil
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
		FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
			FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var action operatorchannel.InboundAction
	action.PublicationID = publicationID
	var key, state, disposition string
	var raw []byte
	err := q.QueryRowContext(ctx, query, publicationID).Scan(&action.Provider, &action.ProviderEventID,
		&key, &raw, &action.ProviderAuthorization, &state, &disposition)
	if err == nil {
		current, err := responseActionRowCurrent(ctx, q, action, raw, key, state, disposition, frozen.InputCard != nil, postgres, lock)
		if err != nil || !current {
			return false, err
		}
		return currentPurpose()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
		FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
			FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var text operatorchannel.InboundText
	text.PublicationID = publicationID
	err = q.QueryRowContext(ctx, query, publicationID).Scan(&text.Provider, &text.ProviderEventID,
		&key, &raw, &text.ProviderAuthorization, &state, &disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &text.TextFact); err != nil {
		return false, err
	}
	if err := text.Validate(); err != nil {
		return false, err
	}
	if key != text.Interface.Key() || state != "settled" ||
		(disposition != "entry" && disposition != "teaching" && disposition != "chooser" &&
			!(frozen.InputCard != nil && disposition == "input_progressed")) {
		return false, nil
	}
	return currentPurpose()
}

func responseActionRowCurrent(ctx context.Context, q intentQueryer, action operatorchannel.InboundAction,
	raw []byte, key, state, disposition string, inputPrompt, postgres, lock bool) (bool, error) {
	if err := json.Unmarshal(raw, &action.ActionFact); err != nil {
		return false, err
	}
	if err := action.Validate(); err != nil {
		return false, err
	}
	if key != action.Interface.Key() || state != "settled" ||
		(disposition != "navigation" && !(inputPrompt && (disposition == "input_started" || disposition == "applied"))) {
		return false, nil
	}
	if _, err := RequireActionIntentTx(ctx, q, action, postgres, lock); err != nil {
		return false, err
	}
	if action.Kind == operatorchannel.ActionSourceReply {
		return true, nil
	}
	query := `SELECT COUNT(*) FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT COUNT(*) FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
	}
	var count int
	if err := q.QueryRowContext(ctx, query, action.PublicationID).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}
