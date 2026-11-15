package storetest

import (
	"context"
	"encoding/json"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CorruptRetainedReplyOrigin(ctx context.Context, selected any, runID, contextID string, origin json.RawMessage) error {
	return private.CorruptRetainedReplyOriginForTest(ctx, selected, runID, contextID, origin)
}
