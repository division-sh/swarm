package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ConversationForkSourceFixture = private.ConversationForkSourceFixture

func SeedConversationForkSource(t testing.TB, ctx context.Context, selected any, fixture ConversationForkSourceFixture) [2]events.Event {
	t.Helper()
	events, err := private.SeedConversationForkSourceForTest(ctx, selected, fixture)
	if err != nil {
		t.Fatalf("seed conversation fork source: %v", err)
	}
	return events
}
