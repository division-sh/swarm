package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type HandlerSelectionStorageEvidence = private.HandlerSelectionStorageEvidence

func ReadHandlerSelectionStorage(ctx context.Context, selected any, eventID string) ([]HandlerSelectionStorageEvidence, error) {
	return private.ReadHandlerSelectionStorageForTest(ctx, selected, eventID)
}
