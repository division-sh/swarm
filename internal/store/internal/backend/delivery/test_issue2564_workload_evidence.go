package delivery

import (
	"context"
	"database/sql"
	"errors"
)

type H1DeliveryAccountingEvidence struct{ DeliveredBumps, DeliveredAgents, Undelivered, Retries, DeadLetters, BadAttempts int }

func (s *DeliveryPostgresOwner) ObserveH1DeliveryAccountingForTest(ctx context.Context, runID string) (H1DeliveryAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1DeliveryAccountingEvidence{}, err
	}
	var out H1DeliveryAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1DeliveryAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1DeliveryAccountingEvidence{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH1DeliveryAccountingForTest(ctx context.Context, runID string) (H1DeliveryAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1DeliveryAccountingEvidence{}, err
	}
	var out H1DeliveryAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1DeliveryAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1DeliveryAccountingEvidence{}, err
	}
	return out, nil
}

func observeH1DeliveryAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1DeliveryAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node' AND e.event_name='hub.bump' AND d.status='delivered'`, runID).Scan(&out.DeliveredBumps); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'`, runID).Scan(&out.DeliveredAgents); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status<>'delivered'`, runID).Scan(&out.Undelivered); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(retry_count),0) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&out.Retries); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1`, runID).Scan(&out.DeadLetters); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.claim_version<>1 OR a.closure_kind<>'settled' OR a.outcome<>'delivered')`, runID).Scan(&out.BadAttempts); err != nil {
		return err
	}
	return nil
}

type H2DeliveryAccountingEvidence struct{ DeadLetters, Retried int }

func (s *DeliveryPostgresOwner) ObserveH2DeliveryAccountingForTest(ctx context.Context, runID string) (H2DeliveryAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2DeliveryAccountingEvidence{}, err
	}
	var out H2DeliveryAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2DeliveryAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2DeliveryAccountingEvidence{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH2DeliveryAccountingForTest(ctx context.Context, runID string) (H2DeliveryAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2DeliveryAccountingEvidence{}, err
	}
	var out H2DeliveryAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2DeliveryAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2DeliveryAccountingEvidence{}, err
	}
	return out, nil
}

func observeH2DeliveryAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2DeliveryAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID).Scan(&out.DeadLetters); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.outcome='retry_scheduled' OR a.outcome='dead_letter')`, runID).Scan(&out.Retried); err != nil {
		return err
	}
	return nil
}

type H2PendingAccountingEvidence struct{ Pending int }

func (s *DeliveryPostgresOwner) ObserveH2PendingAccountingForTest(ctx context.Context, runID string) (H2PendingAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2PendingAccountingEvidence{}, err
	}
	var out H2PendingAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2PendingAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2PendingAccountingEvidence{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH2PendingAccountingForTest(ctx context.Context, runID string) (H2PendingAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2PendingAccountingEvidence{}, err
	}
	var out H2PendingAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2PendingAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2PendingAccountingEvidence{}, err
	}
	return out, nil
}

func observeH2PendingAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2PendingAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id IS NOT NULL AND status IN ('pending','in_progress')`).Scan(&out.Pending); err != nil {
		return err
	}
	return nil
}

type H1NodeFailuresEvidence struct {
	Delivery string
	Event    string
	Name     string
	Status   string
	Retries  int
	Reason   string
	Failure  string
}

func (s *DeliveryPostgresOwner) ObserveH1NodeFailuresForTest(ctx context.Context, runID string) ([]H1NodeFailuresEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1NodeFailuresEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1NodeFailuresForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH1NodeFailuresForTest(ctx context.Context, runID string) ([]H1NodeFailuresEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1NodeFailuresEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1NodeFailuresForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1NodeFailuresForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1NodeFailuresEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,e.event_id,e.event_name,d.status,d.retry_count,COALESCE(d.reason_code,''),COALESCE(CAST(d.failure AS TEXT),'null')
		FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (d.retry_count<>0 OR d.status='dead_letter' OR d.failure IS NOT NULL)
		ORDER BY d.created_at,d.delivery_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1NodeFailuresEvidence
	for rows.Next() {
		var row H1NodeFailuresEvidence
		if err := rows.Scan(&row.Delivery, &row.Event, &row.Name, &row.Status, &row.Retries, &row.Reason, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1AttemptFailuresEvidence struct {
	Delivery string
	Event    string
	Name     string
	Version  int
	Closure  string
	Outcome  string
	Reason   string
	Failure  string
}

func (s *DeliveryPostgresOwner) ObserveH1AttemptFailuresForTest(ctx context.Context, runID string) ([]H1AttemptFailuresEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1AttemptFailuresEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1AttemptFailuresForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH1AttemptFailuresForTest(ctx context.Context, runID string) ([]H1AttemptFailuresEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1AttemptFailuresEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1AttemptFailuresForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1AttemptFailuresForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1AttemptFailuresEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,e.event_id,e.event_name,a.claim_version,a.closure_kind,COALESCE(a.outcome,''),COALESCE(a.reason_code,''),COALESCE(CAST(a.failure AS TEXT),'null')
		FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (a.outcome IN ('retry_scheduled','dead_letter') OR a.failure IS NOT NULL)
		ORDER BY d.delivery_id,a.claim_version`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1AttemptFailuresEvidence
	for rows.Next() {
		var row H1AttemptFailuresEvidence
		if err := rows.Scan(&row.Delivery, &row.Event, &row.Name, &row.Version, &row.Closure, &row.Outcome, &row.Reason, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1DeadLettersEvidence struct {
	ID       string
	Event    string
	Name     string
	Delivery string
	Version  int
	Retries  int
	Node     string
	Failure  string
}

func (s *DeliveryPostgresOwner) ObserveH1DeadLettersForTest(ctx context.Context, runID string) ([]H1DeadLettersEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1DeadLettersEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1DeadLettersForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH1DeadLettersForTest(ctx context.Context, runID string) ([]H1DeadLettersEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1DeadLettersEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1DeadLettersForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1DeadLettersForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1DeadLettersEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT l.dead_letter_id,e.event_id,e.event_name,COALESCE(CAST(l.delivery_id AS TEXT),''),COALESCE(l.claim_version,0),l.retry_count,COALESCE(l.handler_node,''),CAST(l.failure AS TEXT)
		FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1 ORDER BY l.created_at,l.dead_letter_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1DeadLettersEvidence
	for rows.Next() {
		var row H1DeadLettersEvidence
		if err := rows.Scan(&row.ID, &row.Event, &row.Name, &row.Delivery, &row.Version, &row.Retries, &row.Node, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H2ResponseQueueEvidence struct {
	Name   string
	Status WorkloadNullableText
	Count  int
}

func (s *DeliveryPostgresOwner) ObserveH2ResponseQueueForTest(ctx context.Context, runID string) ([]H2ResponseQueueEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2ResponseQueueEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2ResponseQueueForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH2ResponseQueueForTest(ctx context.Context, runID string) ([]H2ResponseQueueEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2ResponseQueueEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2ResponseQueueForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2ResponseQueueForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2ResponseQueueEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.event_name,d.status,COUNT(*) FROM events e LEFT JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 GROUP BY e.event_name,d.status`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2ResponseQueueEvidence
	for rows.Next() {
		var row H2ResponseQueueEvidence
		var status sql.NullString
		if err := rows.Scan(&row.Name, &status, &row.Count); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Status = WorkloadNullableText{String: status.String, Valid: status.Valid}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H2NodeDeliveriesEvidence struct {
	Event   string
	Status  string
	Retries int
	Name    string
	Payload []byte
}

func (s *DeliveryPostgresOwner) ObserveH2NodeDeliveriesForTest(ctx context.Context, runID string) ([]H2NodeDeliveriesEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2NodeDeliveriesEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2NodeDeliveriesForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ObserveH2NodeDeliveriesForTest(ctx context.Context, runID string) ([]H2NodeDeliveriesEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2NodeDeliveriesEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2NodeDeliveriesForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2NodeDeliveriesForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2NodeDeliveriesEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.event_id,d.status,d.retry_count,e.event_name,e.payload FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2NodeDeliveriesEvidence
	for rows.Next() {
		var row H2NodeDeliveriesEvidence
		if err := rows.Scan(&row.Event, &row.Status, &row.Retries, &row.Name, &row.Payload); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type WorkloadNullableText struct {
	String string
	Valid  bool
}
