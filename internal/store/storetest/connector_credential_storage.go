package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ConnectorCredentialLeakStorageEvidence = private.ConnectorCredentialLeakStorageEvidence

func ReadConnectorCredentialLeakStorage(ctx context.Context, selected any, runID, secret string) (ConnectorCredentialLeakStorageEvidence, error) {
	return private.ReadConnectorCredentialLeakStorageForTest(ctx, selected, runID, secret)
}
