package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type APIEventPublicationRefusalStorage struct{ Runs, MatchingEvents, APICompletions int }
type APIEventReplayStorage struct {
	OriginalAgentDeliveries, ReplayAgentDeliveries, AuditEventCount int
	ReplaySourceEventID, AuditSourceEventID, AuditPayload           string
}

func ReadDecisionRouteStatusStorageForTest(ctx context.Context, selected any, eventID string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return "", fmt.Errorf("decision route evidence requires an exact canonical event")
	}
	var status string
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = pipelinepersistence.FixtureDecisionRouteStatusTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func ReadLatestNamedEventIdentityStorageForTest(ctx context.Context, selected any, eventName, excludedEventID string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	var eventID string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1 AND CAST(event_id AS TEXT)<>$2 ORDER BY created_at DESC LIMIT 1`, eventName, excludedEventID).Scan(&eventID)
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func ReadPipelineReceiptOutcomeStorageForTest(ctx context.Context, selected any, eventID string) (string, *runtimefailures.Envelope, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", nil, err
	}
	var outcome string
	var failure *runtimefailures.Envelope
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		outcome, failure, err = pipelinepersistence.FixturePipelineReceiptOutcomeTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", nil, err
	}
	return outcome, failure, nil
}

func CountPhysicalEventDeliveriesForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = storedelivery.FixtureDeliveryCardinalityTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountAgentEventDeliveryStorageForTest(ctx context.Context, selected any, eventID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = storedelivery.FixtureAgentEventCardinalityTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
func CountPipelineEventReceiptStorageForTest(ctx context.Context, selected any, eventID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.FixturePipelineReceiptCardinalityTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountPhysicalRunIdentityForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountPhysicalRunEventsForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountPhysicalEventsForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountPhysicalRunsForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountAPICommandReceiptsForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type APIPublicationCardinalityStorage struct{ Runs, Events, APICompletions int }

func ReadAPIFlowPublicationRefusalStorageForTest(ctx context.Context, selected any) (APIPublicationCardinalityStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return APIPublicationCardinalityStorage{}, err
	}
	var out APIPublicationCardinalityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM runs),(SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM api_idempotency)`).Scan(&out.Runs, &out.Events, &out.APICompletions)
	})
	if err != nil {
		return APIPublicationCardinalityStorage{}, err
	}
	return out, nil
}

func ReadAPIRunStartRefusalStorageForTest(ctx context.Context, selected any, runID string) (APIPublicationCardinalityStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return APIPublicationCardinalityStorage{}, err
	}
	var out APIPublicationCardinalityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM runs WHERE run_id=$1),(SELECT COUNT(*) FROM events WHERE run_id=$1),(SELECT COUNT(*) FROM api_idempotency)`, runID).Scan(&out.Runs, &out.Events, &out.APICompletions)
	})
	if err != nil {
		return APIPublicationCardinalityStorage{}, err
	}
	return out, nil
}

func CountEventNameStorageForTest(ctx context.Context, selected any, eventName string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name=$1`, eventName).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadAPIEventPublicationRefusalStorageForTest(ctx context.Context, selected any, eventName string) (APIEventPublicationRefusalStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return APIEventPublicationRefusalStorage{}, err
	}
	var out APIEventPublicationRefusalStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM runs),(SELECT COUNT(*) FROM events WHERE event_name=$1),(SELECT COUNT(*) FROM api_idempotency)`, eventName).Scan(&out.Runs, &out.MatchingEvents, &out.APICompletions)
	})
	if err != nil {
		return APIEventPublicationRefusalStorage{}, err
	}
	return out, nil
}

func ReadAPIEventReplayStorageForTest(ctx context.Context, selected any, originalEventID, replayEventID, auditEventID string) (APIEventReplayStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return APIEventReplayStorage{}, err
	}
	var out APIEventReplayStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.OriginalAgentDeliveries, err = storedelivery.FixtureAgentEventCardinalityTx(ctx, tx, originalEventID)
		if err != nil {
			return err
		}
		out.ReplayAgentDeliveries, err = storedelivery.FixtureAgentEventCardinalityTx(ctx, tx, replayEventID)
		if err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name='event.replayed'`).Scan(&out.AuditEventCount); err != nil {
			return err
		}
		out.ReplaySourceEventID, _, err = readFixtureEventLineageStorage(ctx, selected, tx, replayEventID)
		if err != nil {
			return err
		}
		out.AuditSourceEventID, out.AuditPayload, err = readFixtureEventLineageStorage(ctx, selected, tx, auditEventID)
		return err
	})
	if err != nil {
		return APIEventReplayStorage{}, err
	}
	return out, nil
}
