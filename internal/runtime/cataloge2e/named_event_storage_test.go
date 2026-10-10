package cataloge2e

import (
	"context"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func (h *runtimeHarness) readRunNamedEventIdentity(ctx context.Context, runID, eventName string) (string, error) {
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		return "", err
	}
	return storetest.ReadRunNamedEventIdentityStorage(ctx, reader, runID, eventName)
}
