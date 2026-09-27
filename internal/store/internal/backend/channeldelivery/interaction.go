package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

// InsertActionIntentTx reserves a verified callback in the same selected-store
// transaction as its inbound publication. It conveys no mutation authority.
func InsertActionIntentTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, receivedAt time.Time, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("channel action intent requires a selected transaction")
	}
	if err := action.Validate(); err != nil {
		return err
	}
	if receivedAt.IsZero() {
		return fmt.Errorf("channel action intent requires received_at")
	}
	fact, err := canonicaljson.Bytes(action.ActionFact)
	if err != nil {
		return err
	}
	query := `INSERT INTO operator_channel_action_intents
		(publication_id, provider, provider_event_id, interface_key, fact, provider_authorization, state, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)`
	if postgres {
		query = `INSERT INTO operator_channel_action_intents
			(publication_id, provider, provider_event_id, interface_key, fact, provider_authorization, state, recorded_at)
			VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, 'pending', $7)`
	}
	if _, err := tx.ExecContext(ctx, query, action.PublicationID, action.Provider, action.ProviderEventID,
		action.Interface.Key(), string(fact), action.ProviderAuthorization, receivedAt.UTC()); err != nil {
		return fmt.Errorf("insert verified channel action intent: %w", err)
	}
	return nil
}

func InsertTextIntentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, receivedAt time.Time, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("channel text intent requires a selected transaction")
	}
	if err := text.Validate(); err != nil {
		return err
	}
	if receivedAt.IsZero() {
		return fmt.Errorf("channel text intent requires received_at")
	}
	fact, err := canonicaljson.Bytes(text.TextFact)
	if err != nil {
		return err
	}
	query := `INSERT INTO operator_channel_text_intents
		(publication_id, provider, provider_event_id, interface_key, fact, provider_authorization, state, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)`
	if postgres {
		query = `INSERT INTO operator_channel_text_intents
			(publication_id, provider, provider_event_id, interface_key, fact, provider_authorization, state, recorded_at)
			VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, 'pending', $7)`
	}
	if _, err := tx.ExecContext(ctx, query, text.PublicationID, text.Provider, text.ProviderEventID,
		text.Interface.Key(), string(fact), text.ProviderAuthorization, receivedAt.UTC()); err != nil {
		return fmt.Errorf("insert verified channel text intent: %w", err)
	}
	return nil
}

func ListPendingActionIntents(ctx context.Context, tx *sql.Tx, afterPublicationID string, limit int, postgres bool) ([]render.PendingAction, error) {
	if tx == nil || limit < 1 || limit > 500 || (afterPublicationID != "" && uuid.Validate(afterPublicationID) != nil) {
		return nil, fmt.Errorf("channel action scan requires a store, valid cursor and bounded limit")
	}
	cursor := afterPublicationID
	if cursor == "" {
		cursor = "00000000-0000-0000-0000-000000000000"
	}
	query := `SELECT intent.publication_id, intent.provider, intent.provider_event_id,
		intent.interface_key, intent.fact, intent.provider_authorization, intent.recorded_at
		FROM operator_channel_action_intents intent
		WHERE intent.state='pending' AND intent.publication_id>?
		ORDER BY intent.publication_id LIMIT ?`
	if postgres {
		query = `SELECT intent.publication_id::text, intent.provider, intent.provider_event_id,
			intent.interface_key, intent.fact, intent.provider_authorization, intent.recorded_at
			FROM operator_channel_action_intents intent
			WHERE intent.state='pending' AND intent.publication_id>$1::uuid
			ORDER BY intent.publication_id LIMIT $2`
	}
	rows, err := tx.QueryContext(ctx, query, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pending := make([]render.PendingAction, 0, limit)
	for rows.Next() {
		var item render.PendingAction
		var interfaceKey string
		var raw []byte
		var recorded any
		if err := rows.Scan(&item.PublicationID, &item.Fact.Provider, &item.Fact.ProviderEventID,
			&interfaceKey, &raw, &item.Fact.ProviderAuthorization, &recorded); err != nil {
			return nil, err
		}
		var err error
		item.ReceivedAt, err = decodeActionTime(recorded)
		if err != nil {
			return nil, err
		}
		item.Fact.PublicationID = item.PublicationID
		if err := json.Unmarshal(raw, &item.Fact.ActionFact); err != nil {
			return nil, fmt.Errorf("decode pending channel action fact: %w", err)
		}
		if err := item.Fact.Validate(); err != nil {
			return nil, err
		}
		if item.Fact.Interface.Key() != interfaceKey {
			return nil, fmt.Errorf("pending channel action interface contradicts fact")
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

func ListPendingTextIntents(ctx context.Context, tx *sql.Tx, afterPublicationID string, limit int, postgres bool) ([]render.PendingText, error) {
	if tx == nil || limit < 1 || limit > 500 || (afterPublicationID != "" && uuid.Validate(afterPublicationID) != nil) {
		return nil, fmt.Errorf("channel text scan requires a store, valid cursor and bounded limit")
	}
	cursor := afterPublicationID
	if cursor == "" {
		cursor = "00000000-0000-0000-0000-000000000000"
	}
	query := `SELECT intent.publication_id, intent.provider, intent.provider_event_id,
		intent.interface_key, intent.fact, intent.provider_authorization, intent.recorded_at
		FROM operator_channel_text_intents intent
		WHERE intent.state='pending' AND intent.publication_id>?
		ORDER BY intent.publication_id LIMIT ?`
	if postgres {
		query = `SELECT intent.publication_id::text, intent.provider, intent.provider_event_id,
			intent.interface_key, intent.fact, intent.provider_authorization, intent.recorded_at
			FROM operator_channel_text_intents intent
			WHERE intent.state='pending' AND intent.publication_id>$1::uuid
			ORDER BY intent.publication_id LIMIT $2`
	}
	rows, err := tx.QueryContext(ctx, query, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pending := make([]render.PendingText, 0, limit)
	for rows.Next() {
		var item render.PendingText
		var interfaceKey string
		var raw []byte
		var recorded any
		if err := rows.Scan(&item.PublicationID, &item.Fact.Provider, &item.Fact.ProviderEventID,
			&interfaceKey, &raw, &item.Fact.ProviderAuthorization, &recorded); err != nil {
			return nil, err
		}
		item.ReceivedAt, err = decodeActionTime(recorded)
		if err != nil {
			return nil, err
		}
		item.Fact.PublicationID = item.PublicationID
		if err := json.Unmarshal(raw, &item.Fact.TextFact); err != nil {
			return nil, fmt.Errorf("decode pending channel text fact: %w", err)
		}
		if err := item.Fact.Validate(); err != nil {
			return nil, err
		}
		if item.Fact.Interface.Key() != interfaceKey {
			return nil, fmt.Errorf("pending channel text interface contradicts fact")
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

func RequireTextIntentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("channel text admission requires a selected transaction")
	}
	if err := text.Validate(); err != nil {
		return err
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
		FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
			FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
	}
	var provider, providerEventID, interfaceKey, authorization, state string
	var raw []byte
	err := tx.QueryRowContext(ctx, query, text.PublicationID).Scan(&provider, &providerEventID, &interfaceKey, &raw, &authorization, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("verified channel text intent is absent")
	}
	if err != nil {
		return err
	}
	var stored operatorchannel.TextFact
	if err := json.Unmarshal(raw, &stored); err != nil {
		return err
	}
	if provider != text.Provider || providerEventID != text.ProviderEventID ||
		interfaceKey != text.Interface.Key() || authorization != text.ProviderAuthorization ||
		stored != text.TextFact || state != "pending" {
		return fmt.Errorf("verified channel text intent contradicts admitted fact")
	}
	return nil
}

func decodeActionTime(value any) (time.Time, error) {
	switch value := value.(type) {
	case time.Time:
		return value.UTC(), nil
	case []byte:
		return decodeActionTime(string(value))
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
			if parsed, err := time.Parse(layout, value); err == nil {
				return parsed.UTC(), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("decode channel action time %T", value)
}

func RequireActionIntentTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, postgres, lock bool) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("channel action admission requires a selected transaction")
	}
	if err := action.Validate(); err != nil {
		return "", err
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
		FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
			FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var provider, providerEventID, interfaceKey, authorization, state string
	var raw []byte
	err := tx.QueryRowContext(ctx, query, action.PublicationID).Scan(&provider, &providerEventID, &interfaceKey, &raw, &authorization, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("verified channel action intent is absent")
	}
	if err != nil {
		return "", err
	}
	var stored operatorchannel.ActionFact
	if err := json.Unmarshal(raw, &stored); err != nil {
		return "", err
	}
	if provider != action.Provider || providerEventID != action.ProviderEventID ||
		interfaceKey != action.Interface.Key() || authorization != action.ProviderAuthorization ||
		stored != action.ActionFact || (state != "pending" && state != "settled") {
		return "", fmt.Errorf("verified channel action intent contradicts admitted fact")
	}
	return state, nil
}

func SettleAppliedActionIntentTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, disposition render.ActionDisposition, postgres bool) error {
	if disposition != render.ActionApplied && disposition != render.ActionInputStarted {
		return fmt.Errorf("channel card mutation requires an applied disposition")
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return err
	}
	if state != "pending" {
		return fmt.Errorf("completed channel action cannot mutate again")
	}
	query := `UPDATE operator_channel_action_intents SET state='settled', disposition=?, settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_action_intents SET state='settled', disposition=$1, settled_at=$2
			WHERE publication_id=$3::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, string(disposition), time.Now().UTC(), action.PublicationID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("channel action disposition did not commit exactly once")
	}
	return nil
}
