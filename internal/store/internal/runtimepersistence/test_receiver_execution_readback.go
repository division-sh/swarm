package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type SelectedForkReceiverExecutionProofRequest = runforkpersistence.SelectedForkReceiverExecutionProofRequest
type SelectedForkReceiverExecutionProof = runforkpersistence.SelectedForkReceiverExecutionProof

func ReadSelectedForkReceiverExecutionProofForTest(ctx context.Context, selected any, req SelectedForkReceiverExecutionProofRequest) (SelectedForkReceiverExecutionProof, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedForkReceiverExecutionProof{}, err
	}
	var out SelectedForkReceiverExecutionProof
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedForkReceiverExecutionProofTx(ctx, tx, req)
		return err
	})
	if err != nil {
		return SelectedForkReceiverExecutionProof{}, err
	}
	return out, nil
}
