package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

type SelectedCausalDiagnosticStorage struct {
	Events     []eventrecord.LifecycleDiagnosticEventLineage
	Projection []byte
}

type SelectedCausalDiagnosticConservation struct {
	Events, Pending int
}

func (s *EventPostgresOwner) ReadSelectedCausalDiagnosticStorageForTest(ctx context.Context, outboxID string) (SelectedCausalDiagnosticStorage, error) {
	var out SelectedCausalDiagnosticStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.ReadLifecycleDiagnosticEventLineage(ctx, tx, true, outboxID)
		if err != nil {
			return err
		}
		out.Projection, err = ReadLifecycleDiagnosticProjection(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadSelectedCausalDiagnosticStorageForTest(ctx context.Context, outboxID string) (SelectedCausalDiagnosticStorage, error) {
	var out SelectedCausalDiagnosticStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.ReadLifecycleDiagnosticEventLineage(ctx, tx, false, outboxID)
		if err != nil {
			return err
		}
		out.Projection, err = ReadLifecycleDiagnosticProjection(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	return out, nil
}

func (s *EventPostgresOwner) ReadSelectedCausalDiagnosticConservationForTest(ctx context.Context, outboxID string) (SelectedCausalDiagnosticConservation, error) {
	var out SelectedCausalDiagnosticConservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.CountLifecycleDiagnosticEvents(ctx, tx, true, outboxID)
		if err != nil {
			return err
		}
		out.Pending, err = CountPendingLifecycleDiagnostic(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadSelectedCausalDiagnosticConservationForTest(ctx context.Context, outboxID string) (SelectedCausalDiagnosticConservation, error) {
	var out SelectedCausalDiagnosticConservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.CountLifecycleDiagnosticEvents(ctx, tx, false, outboxID)
		if err != nil {
			return err
		}
		out.Pending, err = CountPendingLifecycleDiagnostic(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	return out, nil
}

func (s *EventPostgresOwner) ReadGlobalEventChronologyForTest(ctx context.Context) ([]eventrecord.GlobalEventChronologyRow, error) {
	var out []eventrecord.GlobalEventChronologyRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGlobalEventChronologyTx(ctx, tx, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadGlobalEventChronologyForTest(ctx context.Context) ([]eventrecord.GlobalEventChronologyRow, error) {
	var out []eventrecord.GlobalEventChronologyRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGlobalEventChronologyTx(ctx, tx, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventPostgresOwner) CountRunEventNameStorageForTest(ctx context.Context, run, name string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountRunEventNameStorage(ctx, tx, run, name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *EventSQLiteOwner) CountRunEventNameStorageForTest(ctx context.Context, run, name string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountRunEventNameStorage(ctx, tx, run, name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *EventPostgresOwner) ReadSourceRouteSettlementStorageForTest(ctx context.Context, runID, eventID string) (eventrecord.SourceRouteSettlementStorage, error) {
	var out eventrecord.SourceRouteSettlementStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadSourceRouteSettlementStorage(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return eventrecord.SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadSourceRouteSettlementStorageForTest(ctx context.Context, runID, eventID string) (eventrecord.SourceRouteSettlementStorage, error) {
	var out eventrecord.SourceRouteSettlementStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadSourceRouteSettlementStorage(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return eventrecord.SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func (s *EventPostgresOwner) CountChainDepthDiagnosticStorageForTest(ctx context.Context, runID, entityID, handlerNode string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountChainDepthDiagnosticStorage(ctx, tx, true, runID, entityID, handlerNode)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *EventSQLiteOwner) CountChainDepthDiagnosticStorageForTest(ctx context.Context, runID, entityID, handlerNode string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountChainDepthDiagnosticStorage(ctx, tx, false, runID, entityID, handlerNode)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *EventPostgresOwner) ReadLatestHandlerErrorLogForTest(ctx context.Context) (json.RawMessage, error) {
	var out json.RawMessage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadLatestHandlerErrorLog(ctx, tx, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadLatestHandlerErrorLogForTest(ctx context.Context) (json.RawMessage, error) {
	var out json.RawMessage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadLatestHandlerErrorLog(ctx, tx, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventPostgresOwner) ReadCausalEventStorageSinceForTest(ctx context.Context, since time.Time) ([]eventrecord.CausalObservationRow, error) {
	var out []eventrecord.CausalObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadCausalObservationSince(ctx, tx, true, since)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadCausalEventStorageSinceForTest(ctx context.Context, since time.Time) ([]eventrecord.CausalObservationRow, error) {
	var out []eventrecord.CausalObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadCausalObservationSince(ctx, tx, false, since)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
