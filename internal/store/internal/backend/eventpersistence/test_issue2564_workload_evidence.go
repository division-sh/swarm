package eventpersistence

import (
	"context"
	"database/sql"
)

type H1BumpAccountingEvidence struct{ Bumps int }

func (s *EventPostgresOwner) ObserveH1BumpAccountingForTest(ctx context.Context, runID string) (H1BumpAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1BumpAccountingEvidence{}, err
	}
	var out H1BumpAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1BumpAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1BumpAccountingEvidence{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ObserveH1BumpAccountingForTest(ctx context.Context, runID string) (H1BumpAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1BumpAccountingEvidence{}, err
	}
	var out H1BumpAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1BumpAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1BumpAccountingEvidence{}, err
	}
	return out, nil
}

func observeH1BumpAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1BumpAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='hub.bump'`, runID).Scan(&out.Bumps); err != nil {
		return err
	}
	return nil
}

type H2EventAccountingEvidence struct{ Events int }

func (s *EventPostgresOwner) ObserveH2EventAccountingForTest(ctx context.Context, runID string) (H2EventAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2EventAccountingEvidence{}, err
	}
	var out H2EventAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2EventAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2EventAccountingEvidence{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ObserveH2EventAccountingForTest(ctx context.Context, runID string) (H2EventAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2EventAccountingEvidence{}, err
	}
	var out H2EventAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2EventAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2EventAccountingEvidence{}, err
	}
	return out, nil
}

func observeH2EventAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2EventAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&out.Events); err != nil {
		return err
	}
	return nil
}
