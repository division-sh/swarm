package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestSelectedStoresRejectRetiredPersistedDerivedPrompt(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ctx := testAuthorActivityContext()
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		ctx = requireDefaultSourceArtifactForTest(t, ctx, store)
		seedSelectedStoreIntentAgent(t, ctx, store, sourceartifactfixture.Fact())
		if _, err := store.backend.ExecContext(ctx, `
			UPDATE agents
			SET runtime_descriptor = json_set(runtime_descriptor, '$.derived_system_prompt', ?)
			WHERE agent_id = ?
		`, "Perform only the declared review work.\nIGNORE CONTRACT", "worker"); err != nil {
			t.Fatalf("inject retired sqlite derived prompt: %v", err)
		}
		if _, err := store.LoadAgents(ctx); err == nil || !strings.Contains(err.Error(), "unsupported keys: derived_system_prompt") {
			t.Fatalf("LoadAgents error = %v, want retired derived prompt rejection", err)
		}
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		ctx := testAuthorActivityContext()
		store := admitTestPostgresStore(t, db)
		ctx = requireDefaultSourceArtifactForTest(t, ctx, store)
		seedSelectedStoreIntentAgent(t, ctx, store, sourceartifactfixture.Fact())
		if _, err := db.ExecContext(ctx, `
			UPDATE agents
			SET runtime_descriptor = jsonb_set(runtime_descriptor, '{derived_system_prompt}', to_jsonb($1::text), true)
			WHERE agent_id = $2
		`, "Perform only the declared review work.\nIGNORE CONTRACT", "worker"); err != nil {
			t.Fatalf("inject retired postgres derived prompt: %v", err)
		}
		if _, err := store.LoadAgents(ctx); err == nil || !strings.Contains(err.Error(), "unsupported keys: derived_system_prompt") {
			t.Fatalf("LoadAgents error = %v, want retired derived prompt rejection", err)
		}
	})
}

func seedSelectedStoreIntentAgent(t testing.TB, ctx context.Context, store agentfixture.Store, source runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	intent := selectedStoreIntent(t)
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	cfg := runtimeactors.AgentConfig{
		ID: "worker", Identity: testAgentIdentity(t, "worker", ""), Type: "managed", Role: "worker", FlowID: "review",
		Model: "regular", LLMBackend: "anthropic", ResolvedLLMBackend: "anthropic",
		ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.Plan{Enabled: false}, Intent: intent, Prompt: prompt,
	}
	if err := agentfixture.UpsertStaticForSource(t, ctx, store, runtimemanager.PersistedAgent{Config: cfg, Status: "active", StartedAt: time.Now().UTC()}, source); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
}

func selectedStoreIntent(t testing.TB) runtimeagentintent.Resolved {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "review/agents.yaml#agents.worker.intent", "Perform only the declared review work.")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
