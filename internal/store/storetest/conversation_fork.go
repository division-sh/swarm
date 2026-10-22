package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ConversationForkSourceFixture = private.ConversationForkSourceFixture
type ConversationForkSourceResult = private.ConversationForkSourceResult
type ConversationForkConstruction = private.ConversationForkConstruction

func PrepareConversationForkConstruction(t testing.TB, ctx context.Context, selected any, source semanticview.Source, planner *manager.AgentManager, fixture ConversationForkSourceFixture) ConversationForkConstruction {
	t.Helper()
	plan, err := private.PrepareConversationForkConstructionForTest(ctx, selected, source, planner, fixture)
	if err != nil {
		t.Fatalf("prepare conversation fork construction: %v", err)
	}
	return plan
}

func SeedConversationForkSource(t testing.TB, ctx context.Context, selected any, fixture ConversationForkSourceFixture) ConversationForkSourceResult {
	t.Helper()
	events, err := private.SeedConversationForkSourceForTest(ctx, selected, fixture)
	if err != nil {
		t.Fatalf("seed conversation fork source: %v", err)
	}
	return events
}
