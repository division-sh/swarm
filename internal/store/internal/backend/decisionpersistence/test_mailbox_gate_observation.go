package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
)

type MailboxGateCardStorage struct {
	Status, Verdict, DecidedBy, DecisionEventID, Anchor string
}

func (s *DecisionPostgresOwner) ReadMailboxGateCardStorageForTest(ctx context.Context, cardID string) (out MailboxGateCardStorage, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateCardStorageTx(ctx, tx, cardID)
		return err
	})
	return
}

func (s *DecisionSQLiteOwner) ReadMailboxGateCardStorageForTest(ctx context.Context, cardID string) (out MailboxGateCardStorage, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateCardStorageTx(ctx, tx, cardID)
		return err
	})
	return
}

func readMailboxGateCardStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (out MailboxGateCardStorage, err error) {
	err = tx.QueryRowContext(ctx, `SELECT status, COALESCE(verdict,''), COALESCE(decided_by,''), COALESCE(CAST(decision_event_id AS TEXT),''), CAST(anchor AS TEXT) FROM decision_cards WHERE card_id=$1`, cardID).Scan(&out.Status, &out.Verdict, &out.DecidedBy, &out.DecisionEventID, &out.Anchor)
	return
}

func (s *DecisionPostgresOwner) ReadMailboxGateChangesStorageForTest(ctx context.Context, cardID string) (out []string, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateChangesStorageTx(ctx, tx, cardID)
		return err
	})
	return
}

func (s *DecisionSQLiteOwner) ReadMailboxGateChangesStorageForTest(ctx context.Context, cardID string) (out []string, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateChangesStorageTx(ctx, tx, cardID)
		return err
	})
	return
}

func readMailboxGateChangesStorageTx(ctx context.Context, tx *sql.Tx, cardID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(change_id AS TEXT), change_type, CAST(payload AS TEXT) FROM decision_card_changes WHERE card_id=$1 ORDER BY change_id`, cardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, kind, payload string
		if err := rows.Scan(&id, &kind, &payload); err != nil {
			return nil, err
		}
		out = append(out, id+" "+kind+" "+payload)
	}
	return out, errors.Join(rows.Err(), rows.Close())
}
