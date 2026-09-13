package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SupersedeDecisionCardAnchor(t testing.TB, ctx context.Context, selected any, card decisioncard.Card, now time.Time) {
	t.Helper()
	if err := private.SupersedeDecisionCardAnchorForTest(ctx, selected, card, now); err != nil {
		t.Fatalf("pin pending card with superseded anchor: %v", err)
	}
}
