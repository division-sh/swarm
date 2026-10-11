package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/google/uuid"
)

type StandaloneRunStorage = runlifecycle.StandaloneRunStorage
type StandaloneAgentDeliveryStorage = delivery.StandaloneAgentDeliveryStorage

func ReadExactAgentDeliveryStatusForTest(ctx context.Context, selected any, eventID, agentID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	if agentID == "" || strings.TrimSpace(agentID) != agentID {
		return "", fmt.Errorf("delivery status evidence requires an exact agent identity")
	}
	_, postgres := selected.(*PostgresStore)
	var status string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = delivery.ReadExactAgentDeliveryStatusTx(ctx, tx, postgres, eventID, agentID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func validateStandaloneStorageEvent(selected any, eventID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return fmt.Errorf("standalone evidence requires an exact canonical event")
	}
	return nil
}

func ReadStandaloneRunStorageForTest(ctx context.Context, selected any, eventID string) (StandaloneRunStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return StandaloneRunStorage{}, err
	}
	var out StandaloneRunStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadStandaloneRunStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}

func ReadStandaloneAgentDeliveryStorageForTest(ctx context.Context, selected any, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	if agentID == "" || strings.TrimSpace(agentID) != agentID {
		return StandaloneAgentDeliveryStorage{}, fmt.Errorf("standalone evidence requires an exact agent identity")
	}
	var out StandaloneAgentDeliveryStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadStandaloneAgentDeliveryStorageTx(ctx, tx, eventID, agentID)
		return err
	})
	if err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	return out, nil
}

func ReadStandaloneCompletionCandidateForTest(ctx context.Context, selected any, eventID string) (runtimerunlifecycle.Candidate, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out runtimerunlifecycle.Candidate
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadStandaloneCompletionCandidateTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	return out, nil
}
