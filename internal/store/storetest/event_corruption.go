package storetest

import (
	"context"
	"testing"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/testsql"
)

func CorruptTargetRouteScalar(t testing.TB, ctx context.Context, selected any, claim testsql.EventCorruptionClaim, eventID string) {
	t.Helper()
	testsql.RequireEventCorruptionClaim(t, claim)
	testsql.RequireEventCorruptionResult(t, claim, private.CorruptTargetRouteScalarForTest(ctx, selected, eventID))
}

func CorruptInboundDuplicatePayload(t testing.TB, ctx context.Context, selected any, claim testsql.EventCorruptionClaim, eventID string) {
	t.Helper()
	testsql.RequireEventCorruptionClaim(t, claim)
	testsql.RequireEventCorruptionResult(t, claim, private.CorruptInboundDuplicatePayloadForTest(ctx, selected, eventID))
}

func RequireEventRowCount(t testing.TB, ctx context.Context, selected any, eventID string, want int) {
	t.Helper()
	got, err := private.CountExactEventForTest(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("count event fixture %s: %v", eventID, err)
	}
	if got != want {
		t.Fatalf("event fixture rows for %s = %d, want %d", eventID, got, want)
	}
}
