package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
)

func CountStageCompletionNodeDeliveriesForTest(ctx context.Context, selected any, runID, eventName, nodeID, rootNodeID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.CountStageCompletionNodeDeliveriesTx(ctx, tx, runID, eventName, nodeID, rootNodeID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadStageCompletionDiscoveryStateForTest(ctx context.Context, selected any, runID, entityID string) (string, error) {
	for _, id := range []string{runID, entityID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return "", err
		}
	}
	var state string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		state, err = entitystore.ReadStageCompletionDiscoveryStateTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return "", err
	}
	return state, nil
}

type StageCompletionFieldlessStorage struct{ Headers, Fields int }

func ReadStageCompletionFieldlessStorageForTest(ctx context.Context, selected any, runID, stage string) (StageCompletionFieldlessStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return StageCompletionFieldlessStorage{}, err
	}
	var out StageCompletionFieldlessStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND current_state=$2),(SELECT COUNT(*) FROM entity_state WHERE run_id=$1)`, runID, stage).Scan(&out.Headers, &out.Fields)
	})
	if err != nil {
		return StageCompletionFieldlessStorage{}, err
	}
	return out, nil
}
