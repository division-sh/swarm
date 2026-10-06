package delivery

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

// DeliveryEventEvidence retains every attempt, including abandoned claims,
// and selection rows independently of successful settlement.
type DeliveryEventEvidence struct {
	Deliveries  []DeliveryRowEvidence
	DeadLetters int
}

type DeliveryRowEvidence struct {
	DeliveryID, RunID, EventID, SubscriberType, SubscriberID, Status string
	RetryCount                                                       int
	ClaimVersion                                                     int64
	HandoffAt                                                        time.Time
	HandoffPresent                                                   bool
	HandlerSelections                                                int
	Attempts                                                         []DeliveryAttemptEvidence
}

type DeliveryAttemptEvidence struct {
	ClaimVersion         int64
	ClosureKind, Outcome string
}

type WriterRunDeliveryEvidence struct{ Total, DeliveredAgents, DeadLetters, Retries, BadClaims, SettledDelivered int }

func (s *DeliveryPostgresOwner) ObserveWriterRunForTest(ctx context.Context, runID string) (WriterRunDeliveryEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterRunDeliveryEvidence{}, err
	}
	var out WriterRunDeliveryEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeWriterRun(ctx, tx, runID, &out) })
	if err != nil {
		return WriterRunDeliveryEvidence{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveWriterRunForTest(ctx context.Context, runID string) (WriterRunDeliveryEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterRunDeliveryEvidence{}, err
	}
	var out WriterRunDeliveryEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeWriterRun(ctx, tx, runID, &out) })
	if err != nil {
		return WriterRunDeliveryEvidence{}, err
	}
	return out, nil
}

func observeWriterRun(ctx context.Context, tx *sql.Tx, runID string, out *WriterRunDeliveryEvidence) error {
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'),
		(SELECT COALESCE(SUM(retry_count),0) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.claim_version<>1 OR (a.closure_kind='settled' AND a.outcome<>'delivered'))),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND a.closure_kind='settled' AND a.outcome='delivered')`, runID).Scan(&out.Total, &out.DeliveredAgents, &out.DeadLetters, &out.Retries, &out.BadClaims, &out.SettledDelivered)
}

func (s *DeliveryPostgresOwner) ObserveEventEvidenceForTest(ctx context.Context, eventID string) (DeliveryEventEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return DeliveryEventEvidence{}, err
	}
	var out DeliveryEventEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeDeliveryEventEvidence(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveEventEvidenceForTest(ctx context.Context, eventID string) (DeliveryEventEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return DeliveryEventEvidence{}, err
	}
	var out DeliveryEventEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeDeliveryEventEvidence(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	return out, nil
}

func observeDeliveryEventEvidence(ctx context.Context, tx *sql.Tx, eventID string) (DeliveryEventEvidence, error) {
	var out DeliveryEventEvidence
	if _, err := uuid.Parse(eventID); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id,run_id,event_id,subscriber_type,subscriber_id,status,retry_count,claim_version,continuation_handoff_at,
		(SELECT COUNT(*) FROM event_delivery_handler_rule_selections s WHERE s.delivery_id=d.delivery_id)
		FROM event_deliveries d WHERE event_id=$1 ORDER BY delivery_id`, eventID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var row DeliveryRowEvidence
		var handed any
		if err := rows.Scan(&row.DeliveryID, &row.RunID, &row.EventID, &row.SubscriberType, &row.SubscriberID, &row.Status, &row.RetryCount, &row.ClaimVersion, &handed, &row.HandlerSelections); err != nil {
			return DeliveryEventEvidence{}, errors.Join(err, rows.Close())
		}
		row.HandoffPresent = handed != nil
		row.HandoffAt, _, err = parseNullableTime(handed)
		if err != nil {
			return DeliveryEventEvidence{}, errors.Join(err, rows.Close())
		}
		out.Deliveries = append(out.Deliveries, row)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	for i := range out.Deliveries {
		row := &out.Deliveries[i]
		attempts, err := tx.QueryContext(ctx, `SELECT claim_version,COALESCE(closure_kind,''),COALESCE(outcome,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`, row.DeliveryID)
		if err != nil {
			return DeliveryEventEvidence{}, err
		}
		for attempts.Next() {
			var attempt DeliveryAttemptEvidence
			if err := attempts.Scan(&attempt.ClaimVersion, &attempt.ClosureKind, &attempt.Outcome); err != nil {
				return DeliveryEventEvidence{}, errors.Join(err, attempts.Close())
			}
			row.Attempts = append(row.Attempts, attempt)
		}
		err = errors.Join(attempts.Err(), attempts.Close())
		if err != nil {
			return DeliveryEventEvidence{}, err
		}
	}
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, eventID).Scan(&out.DeadLetters)
	if err != nil {
		return DeliveryEventEvidence{}, err
	}
	return out, nil
}
