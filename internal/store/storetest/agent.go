package storetest

import (
	"context"
	"testing"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

type AgentFixtureStore = agentfixture.Store

// AgentLifecycleFixture adapts a selected store for tests that construct an
// AgentManager without the production process-composition owner.
func AgentLifecycleFixture(selected AgentFixtureStore) runtimemanager.AgentLifecyclePersistence {
	return agentfixture.Lifecycle(selected)
}

func UpsertAgentFixture(ctx context.Context, selected agentfixture.Store, rec runtimemanager.PersistedAgent) error {
	return agentfixture.Upsert(ctx, selected, rec)
}

func CommitAgentLifecycleFixture(ctx context.Context, selected agentfixture.Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return agentfixture.Commit(ctx, selected, req)
}

func RequireAgentFixture(t testing.TB, ctx context.Context, selected agentfixture.Store, rec runtimemanager.PersistedAgent) {
	t.Helper()
	if err := agentfixture.Upsert(ctx, selected, rec); err != nil {
		t.Fatalf("admit agent fixture %s: %v", rec.Config.ID, err)
	}
}
