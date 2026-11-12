package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

type ChannelCallbackRollbackEvidence struct {
	Actions, Acknowledgments int
}

type ChannelCallbackRollbackFault interface {
	ReadEvidence(context.Context) (ChannelCallbackRollbackEvidence, error)
	CardState(context.Context, string) (string, error)
	Close() error
}

type channelCallbackRollbackFault struct {
	owner *SQLiteRuntimeStore
	event string
}

// Only this native SQLite fixture owns the fixed write fault; an independent
// read-only inspection can never install it or receive its connection.
func OpenChannelCallbackRollbackFaultForTest(selected any, event string) (ChannelCallbackRollbackFault, error) {
	decoded, err := hex.DecodeString(event)
	owner, ok := selected.(*SQLiteRuntimeStore)
	if !ok || owner == nil || owner.backend == nil || !owner.backend.Valid() || err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("callback rollback fault requires its original SQLite owner and exact provider event")
	}
	fault := &channelCallbackRollbackFault{owner: owner, event: event}
	if err := fault.set(true); err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *channelCallbackRollbackFault) set(enabled bool) error {
	statement := `DROP TRIGGER reject_deployment_callback`
	if enabled {
		statement = `CREATE TRIGGER reject_deployment_callback BEFORE UPDATE ON operator_channel_action_intents
			WHEN NEW.provider_event_id='` + f.event + `' BEGIN SELECT RAISE(ABORT,'deployment callback rollback probe'); END`
	}
	return f.owner.backend.RunTransaction(context.Background(), "channel callback rollback fault", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}

func (f *channelCallbackRollbackFault) ReadEvidence(ctx context.Context) (ChannelCallbackRollbackEvidence, error) {
	var observed ChannelCallbackRollbackEvidence
	if f.owner == nil {
		return observed, errors.New("callback rollback fault is closed")
	}
	for _, entry := range []struct {
		query string
		count *int
	}{
		{`SELECT COUNT(*) FROM operator_channel_action_intents WHERE provider_event_id=$1`, &observed.Actions},
		{`SELECT COUNT(*) FROM runtime_external_effect_operations WHERE authority_kind='channel_action_ack' AND json_extract(authority_evidence,'$.provider_event_id')=$1`, &observed.Acknowledgments},
	} {
		if err := f.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, entry.query, f.event).Scan(entry.count)
		}); err != nil {
			return observed, err
		}
	}
	return observed, nil
}

func (f *channelCallbackRollbackFault) CardState(ctx context.Context, token string) (string, error) {
	if f.owner == nil {
		return "", errors.New("callback rollback fault is closed")
	}
	var state string
	err := f.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT card.status FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN decision_cards card ON card.card_id=plan.source_id WHERE action.action_token=$1`, token).Scan(&state)
	})
	return state, err
}

func (f *channelCallbackRollbackFault) Close() error {
	if f.owner == nil {
		return errors.New("callback rollback fault is closed")
	}
	err := f.set(false)
	f.owner = nil
	return err
}
