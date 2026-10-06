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
	state, _, err := requireExactTextIntentTx(ctx, tx, text, postgres, false)
	if err != nil {
		return err
	}
	if state != "pending" {
		return fmt.Errorf("verified channel text intent is not pending")
	}
	return nil
}

type intentQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func requireExactTextIntentTx(ctx context.Context, tx intentQueryer, text operatorchannel.InboundText, postgres, lock bool) (string, string, error) {
	if tx == nil {
		return "", "", fmt.Errorf("channel text admission requires a selected transaction")
	}
	if err := text.Validate(); err != nil {
		return "", "", err
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition, '')
		FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition, '')
			FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var provider, providerEventID, interfaceKey, authorization, state, disposition string
	var raw []byte
	err := tx.QueryRowContext(ctx, query, text.PublicationID).Scan(&provider, &providerEventID, &interfaceKey, &raw, &authorization, &state, &disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", fmt.Errorf("verified channel text intent is absent")
	}
	if err != nil {
		return "", "", err
	}
	var stored operatorchannel.TextFact
	if err := json.Unmarshal(raw, &stored); err != nil {
		return "", "", err
	}
	if provider != text.Provider || providerEventID != text.ProviderEventID ||
		interfaceKey != text.Interface.Key() || authorization != text.ProviderAuthorization ||
		stored != text.TextFact || (state != "pending" && state != "settled") {
		return "", "", fmt.Errorf("verified channel text intent contradicts admitted fact")
	}
	return state, disposition, nil
}

// RejectNativeEntryTx settles an exact verified occurrence without publishing
// a response or granting principal, draft, mailbox or business authority.
func RejectNativeEntryTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) error {
	if text.EntryReference == "" {
		return fmt.Errorf("native entry rejection requires an entry fact")
	}
	state, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, true)
	if err != nil {
		return err
	}
	if state == "settled" {
		if disposition != "entry_rejected" {
			return fmt.Errorf("native entry already has another disposition")
		}
		return nil
	}
	query := `UPDATE operator_channel_text_intents SET state='settled', disposition='entry_rejected', settled_at=? WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_text_intents SET state='settled', disposition='entry_rejected', settled_at=$1 WHERE publication_id=$2::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, time.Now().UTC(), text.PublicationID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("native entry rejection lost its pending occurrence")
	}
	return nil
}

func LoadChooserTextIntentTx(ctx context.Context, tx *sql.Tx, publicationID string, postgres, lock bool) (render.PendingText, error) {
	if tx == nil || uuid.Validate(publicationID) != nil {
		return render.PendingText{}, fmt.Errorf("draft choice requires exact retained text intent")
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, recorded_at, state, disposition
		FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, recorded_at, state, disposition
			FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var pending render.PendingText
	var interfaceKey, state string
	var disposition sql.NullString
	var raw []byte
	var recorded any
	if err := tx.QueryRowContext(ctx, query, publicationID).Scan(&pending.Fact.Provider, &pending.Fact.ProviderEventID,
		&interfaceKey, &raw, &pending.Fact.ProviderAuthorization, &recorded, &state, &disposition); err != nil {
		return render.PendingText{}, err
	}
	if state != "settled" || !disposition.Valid || disposition.String != "chooser" {
		return render.PendingText{}, fmt.Errorf("draft choice has no retained chooser text")
	}
	pending.PublicationID = publicationID
	pending.Fact.PublicationID = publicationID
	var err error
	pending.ReceivedAt, err = decodeActionTime(recorded)
	if err != nil {
		return render.PendingText{}, err
	}
	if err := json.Unmarshal(raw, &pending.Fact.TextFact); err != nil {
		return render.PendingText{}, err
	}
	if err := pending.Fact.Validate(); err != nil || pending.Fact.Interface.Key() != interfaceKey {
		return render.PendingText{}, fmt.Errorf("draft choice retained text contradicts its verified fact: %w", err)
	}
	return pending, nil
}

func RejectUnboundTextTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) error {
	if text.EntryReference != "" {
		return fmt.Errorf("unbound text rejection does not interpret inbox entries")
	}
	state, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, true)
	if err != nil {
		return err
	}
	if state == "settled" {
		if disposition != "rejected" {
			return fmt.Errorf("unbound text already has another disposition")
		}
		return nil
	}
	_, current, err := ResolveCurrentTextTx(ctx, tx, text, postgres)
	if err != nil {
		return err
	}
	if current {
		return fmt.Errorf("unbound text rejection cannot reject a current operator binding")
	}
	return SettleTextIntentTx(ctx, tx, text, "rejected", postgres)
}

func SettleTextIntentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, disposition string, postgres bool) error {
	if disposition != "input_progressed" && disposition != "input_complete" && disposition != "teaching" && disposition != "chooser" && disposition != "control" && disposition != "rejected" {
		return fmt.Errorf("unsupported channel text disposition %q", disposition)
	}
	if err := RequireTextIntentTx(ctx, tx, text, postgres); err != nil {
		return err
	}
	query := `UPDATE operator_channel_text_intents SET state='settled', disposition=?, settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_text_intents SET state='settled', disposition=$1, settled_at=$2
			WHERE publication_id=$3::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, disposition, time.Now().UTC(), text.PublicationID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("verified channel text intent lost its pending state")
	}
	return nil
}

// Only the verified occurrence is settled; no card, response or execution
// authority is created. A retained chooser answer requires its exact callback.
func SettleUnsupportedTextIntentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) error {
	return settleUnsupportedTextIntentTx(ctx, tx, text, false, postgres)
}

func settleUnsupportedTextIntentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, chooser, postgres bool) error {
	state, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, true)
	if err != nil {
		return err
	}
	if state == "settled" && disposition == "unsupported" {
		return nil
	}
	if state != "pending" && !(chooser && state == "settled" && disposition == "chooser") {
		return fmt.Errorf("channel input already has another disposition")
	}
	query := `UPDATE operator_channel_text_intents SET state='settled', disposition='unsupported', settled_at=?
		WHERE publication_id=? AND state=? AND COALESCE(disposition,'')=?`
	if postgres {
		query = `UPDATE operator_channel_text_intents SET state='settled', disposition='unsupported', settled_at=$1
			WHERE publication_id=$2::uuid AND state=$3 AND COALESCE(disposition,'')=$4`
	}
	result, err := tx.ExecContext(ctx, query, time.Now().UTC(), text.PublicationID, state, disposition)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("unsupported channel input lost its exact occurrence")
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

func RequireActionIntentTx(ctx context.Context, tx intentQueryer, action operatorchannel.InboundAction, postgres, lock bool) (string, error) {
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
	if action.Kind == operatorchannel.ActionSourceReply {
		text := operatorchannel.InboundText{TextFact: action.TextSource, Provider: action.Provider,
			ProviderEventID: action.ProviderEventID, PublicationID: action.PublicationID, ProviderAuthorization: action.ProviderAuthorization}
		textState, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, lock)
		if err != nil || textState != "settled" || disposition != "control" {
			return "", errors.Join(err, fmt.Errorf("reply action lacks its exact transferred text intent"))
		}
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

// SettleUnappliedActionIntentTx records a terminal non-mutation result for one
// verified callback. It cannot acknowledge a verdict or grant response content.
func SettleUnappliedActionIntentTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, disposition render.ActionDisposition, postgres bool) error {
	if disposition != render.ActionStale && disposition != render.ActionRejected && disposition != render.ActionUnsupported {
		return fmt.Errorf("channel action requires a non-mutation disposition")
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return err
	}
	if state == "settled" {
		query := `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=?`
		if postgres {
			query = `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		}
		var previous string
		if err := tx.QueryRowContext(ctx, query, action.PublicationID).Scan(&previous); err != nil {
			return err
		}
		if previous != string(disposition) {
			return fmt.Errorf("channel action already settled with a different disposition")
		}
		return nil
	}
	if disposition == render.ActionUnsupported {
		text, found, err := loadUnsupportedChooserTextTx(ctx, tx, action.ActionFact, postgres)
		if err != nil {
			return err
		}
		if found {
			if err := settleUnsupportedTextIntentTx(ctx, tx, text.Fact, true, postgres); err != nil {
				return err
			}
		}
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
		return fmt.Errorf("channel action non-mutation disposition did not commit")
	}
	return nil
}
