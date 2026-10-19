package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ActivityClaimReplyLossBoundFault(t testing.TB, ctx context.Context, selected any, original pipeline.ActivityAttemptJournal, run, request string, fault error) (pipeline.ActivityAttemptJournal, func() int32) {
	t.Helper()
	journal, count, err := private.ActivityClaimReplyLossBoundFaultForTest(ctx, selected, original, run, request, fault)
	if err != nil {
		t.Fatalf("install exact captured activity claim reply loss: %v", err)
	}
	return journal, count
}
