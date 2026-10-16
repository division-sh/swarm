package apiv1

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func apiExecutionFixtureConfig(t testing.TB, backend string, cfg actors.AgentConfig) actors.AgentConfig {
	t.Helper()
	// Positive native fixtures hash the same explicit empty business object
	// that the persisted configuration owner reads back.
	if len(cfg.Config) == 0 {
		cfg.Config = json.RawMessage(`{}`)
	}
	profile, err := selection.ResolveLiveBackend(backend)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, selection.EffectiveModelAliases(nil), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return resolved.Actor
}

// Positive execution fixtures call this only after constructing their complete
// native actor/source set. Storage-only and deliberately refused fixtures do not.
func admitAPITestAgentExecution(t testing.TB, ctx context.Context, selected storetest.AgentFixtureStore, fact correlation.SourceArtifactFact) context.Context {
	t.Helper()
	if previous, present := correlation.SourceArtifactFactFromContext(ctx); present && previous != fact {
		t.Fatal("API execution fixture source contradicts its construction context")
	}
	authority, err := agentfixture.AdmitExecutionAuthority(t, ctx, selected, fact)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if previous, present := correlation.RuntimeInstanceIDFromContext(ctx); present && previous != owner.RuntimeInstanceID {
		t.Fatal("API execution fixture runtime contradicts its retained process")
	}
	plan, found, err := capability.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("API execution fixture source plan: found=%t error=%v", found, err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, authority.ExecutionID(), authority.Generation(), "", plan.Revision, fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx = testAuthorActivityContextForRuntime(ctx, owner.RuntimeInstanceID, fact)
	return managedexecution.WithAdmission(ctx, admission)
}
