package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type ConversationForkStorage struct{ Snapshots, Turns int }
type ConversationForkTurnStorage struct {
	TurnID, State                 string
	Failure                       json.RawMessage
	Completions, KeyedOccurrences int
}
type ConversationForkDomainStorage struct{ Runs, Events, Mailbox, Mutations int }

type ConversationForkTurnDiagnostic struct {
	Index          int
	State, Failure string
}

func ReadConversationForkTurnDiagnosticsForTest(ctx context.Context, selected any, forkID string) ([]ConversationForkTurnDiagnostic, error) {
	if err := validateSelectedForkStorageIdentity(forkID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []ConversationForkTurnDiagnostic
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT turn_index,state,COALESCE(CAST(failure AS TEXT),'') FROM conversation_fork_turns WHERE fork_id=$1 ORDER BY turn_index`, forkID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ConversationForkTurnDiagnostic
			if err := rows.Scan(&row.Index, &row.State, &row.Failure); err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadConversationForkStorageForTest(ctx context.Context, selected any, forkID string) (ConversationForkStorage, error) {
	var out ConversationForkStorage
	if err := validateSelectedForkStorageIdentity(forkID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM conversation_fork_snapshots WHERE fork_id=$1),(SELECT COUNT(*) FROM conversation_fork_turns WHERE fork_id=$1)`, forkID).Scan(&out.Snapshots, &out.Turns)
	})
	if err != nil {
		return ConversationForkStorage{}, err
	}
	return out, nil
}

func ReadConversationForkTurnStorageForTest(ctx context.Context, selected any, forkID, key string) (ConversationForkTurnStorage, error) {
	var out ConversationForkTurnStorage
	if err := validateSelectedForkStorageIdentity(forkID); err != nil {
		return out, err
	}
	if key == "" {
		return out, fmt.Errorf("fork turn observation requires an exact request key")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var failure []byte
		err := tx.QueryRowContext(ctx, `SELECT t.fork_turn_id,t.state,t.failure,
		(SELECT COUNT(*) FROM conversation_fork_turn_completions WHERE fork_turn_id=t.fork_turn_id),
		(SELECT COUNT(*) FROM conversation_fork_turns WHERE fork_id=$1 AND idempotency_key=$2)
		FROM conversation_fork_turns t WHERE t.fork_id=$1 AND t.idempotency_key=$2`, forkID, key).Scan(&out.TurnID, &out.State, &failure, &out.Completions, &out.KeyedOccurrences)
		out.Failure = failure
		return err
	})
	if err != nil {
		return ConversationForkTurnStorage{}, err
	}
	return out, nil
}

func ReadConversationForkDomainStorageForTest(ctx context.Context, selected any, runID string) (ConversationForkDomainStorage, error) {
	var out ConversationForkDomainStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM runs),(SELECT COUNT(*) FROM events WHERE event_name <> 'platform.runtime_log'),
		(SELECT COUNT(*) FROM mailbox),(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1)`, runID).Scan(&out.Runs, &out.Events, &out.Mailbox, &out.Mutations)
	})
	if err != nil {
		return ConversationForkDomainStorage{}, err
	}
	return out, nil
}
