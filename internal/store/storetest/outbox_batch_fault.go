package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetOutboxAfterFirstEventWriteFault(ctx context.Context, selected any, run, first, second string, enabled bool) error {
	return private.SetOutboxAfterFirstEventWriteFaultForTest(ctx, selected, run, first, second, enabled)
}

func SetOutboxDeadLetterInsertFault(ctx context.Context, selected any, eventID string, enabled bool) error {
	return private.SetOutboxDeadLetterInsertFaultForTest(ctx, selected, eventID, enabled)
}
