package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedForkReceiverExecutionProofRequest = private.SelectedForkReceiverExecutionProofRequest
type SelectedForkReceiverExecutionProof = private.SelectedForkReceiverExecutionProof

func ReadSelectedForkReceiverExecutionProof(ctx context.Context, selected any, req SelectedForkReceiverExecutionProofRequest) (SelectedForkReceiverExecutionProof, error) {
	return private.ReadSelectedForkReceiverExecutionProofForTest(ctx, selected, req)
}
