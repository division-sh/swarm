package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type SelectedForkReceiverExecutionProofRequest = runforkpersistence.SelectedForkReceiverExecutionProofRequest
type SelectedForkReceiverExecutionProof = runforkpersistence.SelectedForkReceiverExecutionProof

func ReadSelectedForkReceiverExecutionProofForTest(ctx context.Context, selected any, req SelectedForkReceiverExecutionProofRequest) (SelectedForkReceiverExecutionProof, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedForkReceiverExecutionProof{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.runForkPostgresOwner.ReadSelectedForkReceiverExecutionProofForTest(ctx, req)
	case *SQLiteRuntimeStore:
		return owner.runForkSQLiteOwner.ReadSelectedForkReceiverExecutionProofForTest(ctx, req)
	default:
		return SelectedForkReceiverExecutionProof{}, fmt.Errorf("storage observation requires the original selected owner, got %T", selected)
	}
}
