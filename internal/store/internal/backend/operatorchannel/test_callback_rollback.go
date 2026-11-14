package operatorchannel

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

type CallbackRollbackEvidence struct {
	Actions, Acknowledgments int
}

type CallbackRollbackFixtureFault struct {
	owner *SQLiteOwner
	event string
	ctx   context.Context
}

func OpenCallbackRollbackFaultForTest(ctx context.Context, owner *SQLiteOwner, event string) (*CallbackRollbackFixtureFault, error) {
	decoded, err := hex.DecodeString(event)
	if owner == nil || owner.backend == nil || !owner.backend.Valid() || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != event {
		return nil, fmt.Errorf("callback rollback fault requires its original SQLite owner and exact provider event")
	}
	fault := &CallbackRollbackFixtureFault{owner: owner, event: event, ctx: ctx}
	if err := fault.set(true); err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *CallbackRollbackFixtureFault) set(enabled bool) error {
	return f.owner.SetCallbackRollbackFixtureFault(f.ctx, f.event, enabled)
}

func (f *CallbackRollbackFixtureFault) ReadEvidence(ctx context.Context) (CallbackRollbackEvidence, error) {
	if f.owner == nil {
		return CallbackRollbackEvidence{}, errors.New("callback rollback fault is closed")
	}
	return f.owner.ReadCallbackRollbackFixtureEvidence(ctx, f.event)
}

func (f *CallbackRollbackFixtureFault) CardState(ctx context.Context, token string) (string, error) {
	if f.owner == nil {
		return "", errors.New("callback rollback fault is closed")
	}
	return f.owner.ReadCallbackRollbackFixtureCardState(ctx, token)
}

func (f *CallbackRollbackFixtureFault) Close() error {
	if f.owner == nil {
		return errors.New("callback rollback fault is closed")
	}
	err := f.set(false)
	f.owner = nil
	return err
}

func (s *SQLiteOwner) SetCallbackRollbackFixtureFault(ctx context.Context, event string, enabled bool) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("callback rollback fault requires its original SQLite owner")
	}
	statement := `DROP TRIGGER reject_deployment_callback`
	if enabled {
		statement = `CREATE TRIGGER reject_deployment_callback BEFORE UPDATE ON operator_channel_action_intents
			WHEN NEW.provider_event_id='` + event + `' BEGIN SELECT RAISE(ABORT,'deployment callback rollback probe'); END`
	}
	return s.backend.RunTransaction(ctx, "channel callback rollback fault", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}

func (s *SQLiteOwner) ReadCallbackRollbackFixtureEvidence(ctx context.Context, event string) (CallbackRollbackEvidence, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return CallbackRollbackEvidence{}, fmt.Errorf("callback rollback fault requires its original SQLite owner")
	}
	var observed CallbackRollbackEvidence
	for _, entry := range []struct {
		query string
		count *int
	}{
		{`SELECT COUNT(*) FROM operator_channel_action_intents WHERE provider_event_id=$1`, &observed.Actions},
		{`SELECT COUNT(*) FROM runtime_external_effect_operations WHERE authority_kind='channel_action_ack' AND json_extract(authority_evidence,'$.provider_event_id')=$1`, &observed.Acknowledgments},
	} {
		if err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, entry.query, event).Scan(entry.count)
		}); err != nil {
			return observed, err
		}
	}
	return observed, nil
}

func (s *SQLiteOwner) ReadCallbackRollbackFixtureCardState(ctx context.Context, token string) (string, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return "", fmt.Errorf("callback rollback fault requires its original SQLite owner")
	}
	var state string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT card.status FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN decision_cards card ON card.card_id=plan.source_id WHERE action.action_token=$1`, token).Scan(&state)
	})
	return state, err
}
