package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

// Read the physical identity at the caller's existing run/name cut. An
// ambiguous fixture is a refusal, never permission to choose an arbitrary row.
func ReadRunNamedEventIdentityStorageForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	if err := validateRunNamedEventObservation(selected, runID, eventName); err != nil {
		return "", err
	}
	var eventID string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		ids, err := eventrecord.ReadRunNamedEventIdentityCandidatesTx(ctx, tx, runID, eventName)
		if err != nil {
			return err
		}
		switch len(ids) {
		case 0:
			return sql.ErrNoRows
		case 1:
			eventID = ids[0]
			return nil
		default:
			return fmt.Errorf("ambiguous physical event identity for run %s and event %s", runID, eventName)
		}
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func ReadRunNamedEventIdentitiesStorageForTest(ctx context.Context, selected any, runID, eventName string) ([]string, error) {
	if err := validateRunNamedEventObservation(selected, runID, eventName); err != nil {
		return nil, err
	}
	var ids []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		ids, err = eventrecord.ReadRunNamedEventIdentityHistoryTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func validateRunNamedEventObservation(selected any, runID, eventName string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID || eventName == "" || strings.TrimSpace(eventName) != eventName {
		return fmt.Errorf("run event identity read requires a canonical run and exact event name")
	}
	return nil
}
