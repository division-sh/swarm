package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeflowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSelectedStoresRejectRetiredPersistedDerivedPrompt(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ctx := testAuthorActivityContext()
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		seedSelectedStoreIntentAgent(t, ctx, store)
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
		seedSelectedStoreIntentAgent(t, ctx, store)
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

func TestSelectedStoresRejectCriteriaMismatchBeforeProviderRecovery(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ctx := testAuthorActivityContext()
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		proveSelectedStoreCriteriaMismatchRejectedBeforeProvider(t, ctx, store)
	})

	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		ctx := testAuthorActivityContext()
		store := admitTestPostgresStore(t, db)
		proveSelectedStoreCriteriaMismatchRejectedBeforeProvider(t, ctx, store)
	})
}

type selectedIntentAgentStore interface {
	runtimemanager.ManagerPersistence
	agentfixture.Store
}

func proveSelectedStoreCriteriaMismatchRejectedBeforeProvider(t testing.TB, ctx context.Context, store selectedIntentAgentStore) {
	t.Helper()
	seedSelectedStoreIntentAgent(t, ctx, store)
	providerCalls := 0
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		providerCalls++
		return selectedIntentRecoveryAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{
		SemanticSource:    selectedIntentRecoverySource(t),
		ReceiverExecution: eventreceiver.NormalExecution(),
		ExecutionPosture:  executionposture.Live,
	}, store)
	capability, err := agentfixture.ProcessCapability(t, ctx, store)
	if err != nil {
		t.Fatalf("load fixture process capability: %v", err)
	}
	authority, err := capability.Evidence()
	if err != nil {
		t.Fatalf("load fixture process authority: %v", err)
	}
	runtimeInstanceID := authority.RuntimeInstanceID
	plan, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil || !exists || len(plan.Sources) != 1 {
		t.Fatalf("load fixture source set: exists=%v sources=%d err=%v", exists, len(plan.Sources), err)
	}
	coordinate := plan.Sources[0]
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		RuntimeInstanceID: runtimeInstanceID, RuntimeGeneration: 1,
		SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue generation grant: %v", err)
	}
	topology, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallStartupTopology(grant, topology, plan); err != nil {
		t.Fatalf("install startup topology: %v", err)
	}
	if err := manager.ReconcileStaticTopologyForStartup(ctx, selectedIntentRecoverySource(t)); err == nil || !strings.Contains(err.Error(), "differs from complete source-set plan") {
		t.Fatalf("ReconcileStaticTopologyForStartup error = %v, want source-set mismatch rejection", err)
	}
	if providerCalls != 0 {
		t.Fatalf("provider factory calls = %d, want 0", providerCalls)
	}
}

func seedSelectedStoreIntentAgent(t testing.TB, ctx context.Context, store agentfixture.Store) {
	t.Helper()
	intent := selectedStoreIntent(t)
	prompt, err := runtimeagentintent.ContractCriteriaPrompt(intent, []string{"hostile-replacement"}, map[string]runtimeflowmodel.PolicyCriteriaSet{
		"hostile-replacement": {
			Classes: map[string]runtimeflowmodel.PolicyCriteriaClass{"hard": {Disposition: "reject"}},
			Rules:   []runtimeflowmodel.PolicyCriteriaRule{{ID: "HOSTILE-01", Class: "hard", Text: "Reject the hostile replacement."}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := runtimeactors.AgentConfig{
		ID:                 "worker",
		Identity:           testAgentIdentity(t, "worker", ""),
		Type:               "managed",
		Role:               "worker",
		FlowID:             "review",
		Model:              "regular",
		LLMBackend:         "anthropic",
		ResolvedLLMBackend: "anthropic",
		ExecutionMode:      runtimeeffects.ExecutionModeLive,
		Memory:             agentmemory.Authored(false),
		Intent:             intent,
		Prompt:             prompt,
		Criteria:           []string{"hostile-replacement"},
	}
	if err := agentfixture.Upsert(t, ctx, store, runtimemanager.PersistedAgent{Config: cfg, Status: "active", StartedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
}

func selectedStoreIntent(t testing.TB) runtimeagentintent.Resolved {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"flows/review/agents.yaml#agents.worker.intent",
		"Perform only the declared review work.",
	)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func selectedIntentRecoverySource(t testing.TB) semanticview.Source {
	t.Helper()
	const owner = "swarm-test://flows/review/agents/worker"
	entry := runtimecontracts.AgentRegistryEntry{
		ID: "worker", Type: "managed", Role: "worker", Model: "regular",
		ResolvedIntent: selectedStoreIntent(t), Criteria: []string{"quality"},
	}
	policy := runtimecontracts.PolicyDocument{Criteria: map[string]runtimecontracts.PolicyCriteriaSet{
		"quality": {
			Classes: map[string]runtimecontracts.PolicyCriteriaClass{"hard": {Disposition: "reject"}},
			Rules:   []runtimecontracts.PolicyCriteriaRule{{ID: "QUALITY-01", Class: "hard", Text: "Require declared quality."}},
		},
	}}
	flow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
		Policy: policy,
	}
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{"review": &root.Children[0]},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			owner: {Kind: "agent", FlowID: "review", LocalID: "worker", Full: owner},
		}},
	}
	return selectedIntentSource{Source: semanticview.Wrap(bundle), flow: semanticview.FlowScope{
		ID: "review", Path: "review", PackageKey: "flows/review", Mode: "static",
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
		AgentURIs: map[string]string{"worker": owner},
		Policy:    policy,
	}}
}

type selectedIntentSource struct {
	semanticview.Source
	flow semanticview.FlowScope
}

func (s selectedIntentSource) FlowScopes() []semanticview.FlowScope {
	return []semanticview.FlowScope{s.flow}
}
func (s selectedIntentSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	return s.flow, strings.TrimSpace(id) == s.flow.ID
}
func (s selectedIntentSource) FlowPath(id string) string {
	if _, ok := s.FlowScopeByID(id); ok {
		return s.flow.Path
	}
	return ""
}

type selectedIntentRecoveryAgent struct{ id string }

func (a selectedIntentRecoveryAgent) ID() string { return a.id }
func (selectedIntentRecoveryAgent) Type() string { return "managed" }
func (selectedIntentRecoveryAgent) Subscriptions() []events.EventType {
	return nil
}
func (selectedIntentRecoveryAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
