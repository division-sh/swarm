package delivery

import (
	"context"
	"database/sql"
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

type WorkloadNullableText struct {
	String string
	Valid  bool
}
