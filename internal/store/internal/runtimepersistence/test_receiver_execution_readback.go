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
		return runforkpersistence.ReadPostgresSelectedForkReceiverExecutionProofForTest(ctx, owner.backend, req)
	case *SQLiteRuntimeStore:
		return runforkpersistence.ReadSQLiteSelectedForkReceiverExecutionProofForTest(ctx, owner.backend, req)
	default:
		return SelectedForkReceiverExecutionProof{}, fmt.Errorf("storage observation requires the original selected owner, got %T", selected)
	}
}
