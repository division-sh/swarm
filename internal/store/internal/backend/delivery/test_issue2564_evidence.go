package delivery

import (
	"context"
	"database/sql"
	"time"
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
