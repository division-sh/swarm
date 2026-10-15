package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"strings"
)

func CountConnectorRunNodeDeliveriesForTest(ctx context.Context, selected any, runID, nodeID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	if nodeID == "" || strings.TrimSpace(nodeID) != nodeID {
		return 0, fmt.Errorf("connector storage observation requires nodeID")
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deliveryadapter.CountConnectorRunNodeDeliveriesTx(ctx, tx, runID, nodeID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadConnectorLatestNodeDeliveryDiagnosticForTest(ctx context.Context, selected any, runID, nodeID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}
	if strings.TrimSpace(nodeID) == "" {
		return "", fmt.Errorf("connector storage observation requires nodeID")
	}
	_, postgres := selected.(*PostgresStore)
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deliveryadapter.ReadConnectorLatestNodeDeliveryDiagnosticTx(ctx, tx, postgres, runID, nodeID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountConnectorEntityDeadLettersForTest(ctx context.Context, selected any, entityID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, entityID); err != nil {
		return 0, err
	}

	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deliveryadapter.CountConnectorEntityDeadLettersTx(ctx, tx, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadConnectorLatestEntityDeadLetterDiagnosticForTest(ctx context.Context, selected any, entityID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, entityID); err != nil {
		return "", err
	}

	_, postgres := selected.(*PostgresStore)
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deliveryadapter.ReadConnectorLatestEntityDeadLetterDiagnosticTx(ctx, tx, postgres, entityID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadConnectorLatestActivityReceiptDiagnosticForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}

	_, postgres := selected.(*PostgresStore)
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadConnectorLatestActivityReceiptDiagnosticTx(ctx, tx, postgres, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountConnectorRunActivityAttemptsForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}

	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = activityjournal.CountConnectorRunActivityAttemptsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountChannelSucceededToolAttemptsForTest(ctx context.Context, selected any, runID, tool string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	if tool == "" || strings.TrimSpace(tool) != tool {
		return 0, fmt.Errorf("channel journal requires exact compiled tool")
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = activityjournal.CountChannelSucceededToolAttemptsTx(ctx, tx, runID, tool)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
