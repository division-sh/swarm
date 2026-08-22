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
func AgentLifecycleFixture(t testing.TB, selected AgentFixtureStore) runtimemanager.AgentLifecyclePersistence {
	t.Helper()
	return agentfixture.Lifecycle(t, selected)
}

func UpsertAgentFixture(t testing.TB, ctx context.Context, selected agentfixture.Store, rec runtimemanager.PersistedAgent) error {
	t.Helper()
	return agentfixture.Upsert(t, ctx, selected, rec)
}

func CommitAgentLifecycleFixture(t testing.TB, ctx context.Context, selected agentfixture.Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	t.Helper()
	return agentfixture.Commit(t, ctx, selected, req)
}

func RequireAgentFixture(t testing.TB, ctx context.Context, selected agentfixture.Store, rec runtimemanager.PersistedAgent) {
	t.Helper()
	if err := agentfixture.Upsert(t, ctx, selected, rec); err != nil {
		t.Fatalf("admit agent fixture %s: %v", rec.Config.ID, err)
	}
}
