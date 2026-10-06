package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ActivityClaimReplyLossPersistenceFault(t testing.TB, ctx context.Context, selected any, run, request string, fault error) (pipeline.WorkflowPersistence, func() int32) {
	t.Helper()
	persistence, count, err := private.ActivityClaimReplyLossPersistenceFaultForTest(ctx, selected, run, request, fault)
	if err != nil {
		t.Fatalf("install exact native activity claim reply loss: %v", err)
	}
	return persistence, count
}
