package delivery

import (
	"context"
	"database/sql"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *DeliveryPostgresOwner) ReadServedRunDeliverySummaryForTest(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	var out runtimedelivery.RunSummary
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = s.SummarizeRunTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadServedRunDeliverySummaryForTest(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	var out runtimedelivery.RunSummary
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = s.SummarizeRunTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadNodeDeliveryTargetEncodingForTest(ctx context.Context, eventID, nodeID string) (string, error) {
	var out string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadNodeDeliveryTargetEncodingTx(ctx, tx, true, eventID, nodeID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadNodeDeliveryTargetEncodingForTest(ctx context.Context, eventID, nodeID string) (string, error) {
	var out string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadNodeDeliveryTargetEncodingTx(ctx, tx, false, eventID, nodeID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadCausalDeliveryFrontierForTest(ctx context.Context, runID, eventID string) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadCausalDeliveryFrontier(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadCausalDeliveryFrontierForTest(ctx context.Context, runID, eventID string) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadCausalDeliveryFrontier(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ProbeCausalDeliveryFrontierForTest(ctx context.Context, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ProbeCausalDeliveryFrontier(ctx, tx, probe)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ProbeCausalDeliveryFrontierForTest(ctx context.Context, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	var out CausalDeliveryFrontier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ProbeCausalDeliveryFrontier(ctx, tx, probe)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadEventDeliveryDiagnosticRowsForTest(ctx context.Context, eventID string) ([]EventDeliveryDiagnosticRow, error) {
	var out []EventDeliveryDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadEventDeliveryDiagnosticRows(ctx, tx, eventID, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadEventDeliveryDiagnosticRowsForTest(ctx context.Context, eventID string) ([]EventDeliveryDiagnosticRow, error) {
	var out []EventDeliveryDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadEventDeliveryDiagnosticRows(ctx, tx, eventID, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadSubscriberEventNamesCreatedSinceForTest(ctx context.Context, since time.Time, subscriberID string) ([]string, error) {
	var out []string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadSubscriberEventNamesCreatedSince(ctx, tx, since, subscriberID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadSubscriberEventNamesCreatedSinceForTest(ctx context.Context, since time.Time, subscriberID string) ([]string, error) {
	var out []string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadSubscriberEventNamesCreatedSince(ctx, tx, since, subscriberID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadCausalDeliveryStatusCountsForTest(ctx context.Context, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	var out []CausalDeliveryStatusCount
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadCausalDeliveryStatusCounts(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadCausalDeliveryStatusCountsForTest(ctx context.Context, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	var out []CausalDeliveryStatusCount
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadCausalDeliveryStatusCounts(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadNonLogRunDeliveryClaimTotalsForTest(ctx context.Context, runID string) (int64, int64, error) {
	var deliveries, claims int64
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		deliveries, claims, err = ReadNonLogRunDeliveryClaimTotals(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	return deliveries, claims, nil
}

func (s *DeliverySQLiteOwner) ReadNonLogRunDeliveryClaimTotalsForTest(ctx context.Context, runID string) (int64, int64, error) {
	var deliveries, claims int64
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		deliveries, claims, err = ReadNonLogRunDeliveryClaimTotals(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	return deliveries, claims, nil
}

func (s *DeliveryPostgresOwner) ReadDeliveryAttemptDiagnosticRowsForTest(ctx context.Context, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	var out []DeliveryAttemptDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadDeliveryAttemptDiagnosticRows(ctx, tx, deliveryID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadDeliveryAttemptDiagnosticRowsForTest(ctx context.Context, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	var out []DeliveryAttemptDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadDeliveryAttemptDiagnosticRows(ctx, tx, deliveryID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
