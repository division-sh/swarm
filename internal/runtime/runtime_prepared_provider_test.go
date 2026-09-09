package runtime

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

type preparedProtocolProbe struct {
	startupVisibleSurfaceProbeStub
	before         func(context.Context) error
	replaceReceipt bool
	authorities    []effects.Authority
}

func (p *preparedProtocolProbe) ProbeStartupVisibleToolSurface(ctx context.Context, actor actors.AgentConfig, prompt string, tools []llm.ToolDefinition) (*llm.Response, error) {
	if p.before != nil {
		if err := p.before(ctx); err != nil {
			return nil, err
		}
	}
	authority, ok := effects.AuthorityFromContext(ctx)
	if !ok || !authority.Valid() || authority.StartupProbe.Preparation == nil {
		return nil, errors.New("prepared probe omitted exact authority")
	}
	p.authorities = append(p.authorities, authority)
	handle, err := effects.BeginStartupProbe(ctx, "claude_cli_startup_probe", []byte(authority.ID), nil)
	if err != nil {
		return nil, err
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		return nil, err
	}
	response, err := p.startupVisibleSurfaceProbeStub.ProbeStartupVisibleToolSurface(ctx, actor, prompt, tools)
	if err != nil {
		return nil, err
	}
	if err := handle.MarkResponseObserved(ctx, nil); err != nil {
		return nil, err
	}
	if err := handle.Succeed(ctx, nil); err != nil {
		return nil, err
	}
	if p.replaceReceipt {
		old := response.CapabilitySurface
		foreign := old.Authority
		foreign.ID = uuid.NewString()
		surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorPlan: old.ActorPlan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "independently-valid-replacement", Authority: foreign, CreatedAt: time.Now().UTC()})
		if err != nil {
			return nil, err
		}
		response.CapabilitySurface = &surface
	}
	return response, nil
}

func newPreparedProviderTestPlans(t *testing.T) (startupownership.ProcessCapability, []PreparedSelectedForkProviderProbe) {
	t.Helper()
	evidence, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{OwnerID: "prepared-process", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}, "runtime_test")
	if err != nil {
		t.Fatal(err)
	}
	process, err := startupownership.NewProcessCapability(&runtimeTestRetainedSession{authority: evidence})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	blueprints, err := manager.StaticAgentMaterializationBlueprints(claudeStartupAgentSource())
	if err != nil || len(blueprints) != 1 {
		t.Fatalf("blueprints: %d %v", len(blueprints), err)
	}
	blueprint, err := manager.ResolveAgentMaterializationBlueprint(manager.AgentManagerOptions{LLMBackend: "claude_cli"}, blueprints[0])
	if err != nil {
		t.Fatal(err)
	}
	var probes []PreparedSelectedForkProviderProbe
	for _, owner := range []string{"target-one", "target-two"} {
		name, err := agentidentity.DeclaredName(blueprint.Config.ID, owner)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := agentidentity.NewPlan(name, blueprint.Identity.Route)
		if err != nil {
			t.Fatal(err)
		}
		actor, err := plan.Fingerprint()
		if err != nil {
			t.Fatal(err)
		}
		blueprint.Identity = plan
		probes = append(probes, PreparedSelectedForkProviderProbe{Agent: blueprint, Authority: managedcapabilities.PreparedSelectedForkProbeAuthority{
			SelectedForkPreparationCoordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: evidence.AuthorityID, ProcessOwnerID: evidence.OwnerID, ProcessBootID: evidence.BootID,
				BundleHash: runtimeTestBundleHash, SourceFingerprint: strings.Repeat("a", 64), AdmittedPlanFingerprint: strings.Repeat("b", 64),
				ConfigurationFingerprint: strings.Repeat("c", 64), CatalogFingerprint: strings.Repeat("d", 64),
			},
			ActorPlanFingerprint: actor,
		}})
	}
	return process, probes
}

func preparedProviderTestCatalog(t *testing.T, runtimes *llm.AgentRuntimeSet, tools claudeStartupToolSource, probes []PreparedSelectedForkProviderProbe) *PreparedSelectedForkProviderCatalog {
	t.Helper()
	blueprints := make([]manager.AgentMaterializationBlueprint, len(probes))
	for i, probe := range probes {
		blueprints[i] = probe.Agent
	}
	catalog, err := PrepareSelectedForkProviderCatalog(testAuthorActivityContext(context.Background()), runtimes, tools, blueprints)
	if err != nil {
		t.Fatal(err)
	}
	for i := range probes {
		probes[i].Authority.CatalogFingerprint = catalog.Fingerprint()
	}
	return catalog
}

func TestPreparedProviderPreflightUsesRunlessPlansWithoutManager(t *testing.T) {
	process, plans := newPreparedProviderTestPlans(t)
	exec := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
	turns := mcp.NewTurnContextRegistry(actors.ActorFromContext)
	// No manager or concrete actor resolver exists for this protocol.
	gateway := mcp.NewGateway(exec, "probe-token", RuntimeMCPGatewayHooks(nil, nil, nil, nil, turns))
	server := httptest.NewServer(gateway.Handler())
	t.Cleanup(server.Close)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	binding := testToolGatewayBinding(server.URL, server.URL, "probe-token")
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		t.Fatal(err)
	}
	probe := &preparedProtocolProbe{}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: probe})
	if err != nil {
		t.Fatal(err)
	}
	store := &startupCapabilityStore{}
	preparationID := uuid.NewString()
	catalog := preparedProviderTestCatalog(t, runtimes, exec, plans)
	ids, err := ValidatePreparedSelectedForkProviderPreflight(testAuthorActivityContext(context.Background()), cfg, binding, catalog, turns, preparationID, process, plans, liveTestEffectController(&startupEffectStore{}), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] == ids[1] || len(probe.authorities) != 2 || len(exec.executed) != 2 {
		t.Fatalf("lost same-name prospective actors: ids=%v authorities=%d tool calls=%v", ids, len(probe.authorities), exec.executed)
	}
	for _, id := range ids {
		surface := store.surfaces[id]
		if !surface.ActorIdentity.IsZero() || surface.ActorPlan.IsZero() || surface.Authority.ExecutionAuthorityID != preparationID || surface.Authority.Preparation == nil {
			t.Fatalf("not preparation evidence: %+v", surface)
		}
		if err := surface.ValidateEffective(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreparedProviderPreflightRejectsChangedGatewayCatalog(t *testing.T) {
	process, plans := newPreparedProviderTestPlans(t)
	plans = plans[:1]
	exec := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
	turns := mcp.NewTurnContextRegistry(actors.ActorFromContext)
	server := httptest.NewServer(mcp.NewGateway(exec, "probe-token", RuntimeMCPGatewayHooks(nil, nil, nil, nil, turns)).Handler())
	t.Cleanup(server.Close)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		t.Fatal(err)
	}
	probe := &preparedProtocolProbe{}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: probe})
	if err != nil {
		t.Fatal(err)
	}
	catalog := preparedProviderTestCatalog(t, runtimes, exec, plans)
	for i := range exec.defs {
		if exec.defs[i].Name == "health_check" {
			exec.defs[i].Description += " changed after preparation"
		}
	}
	store := &startupCapabilityStore{}
	ids, err := ValidatePreparedSelectedForkProviderPreflight(testAuthorActivityContext(context.Background()), cfg, testToolGatewayBinding(server.URL, server.URL, "probe-token"), catalog, turns, uuid.NewString(), process, plans, liveTestEffectController(&startupEffectStore{}), store)
	if err == nil || len(ids) != 0 || len(probe.calls) != 1 || len(exec.executed) != 0 {
		t.Fatalf("changed gateway catalog admitted: ids=%v err=%v probes=%d executions=%v", ids, err, len(probe.calls), exec.executed)
	}
	if !strings.Contains(err.Error(), "mcp tools/list") {
		t.Fatalf("refusal did not reach authenticated catalog comparison: %v", err)
	}
}

func TestPreparedProviderPreflightRejectsCensusBeforeProviderResolution(t *testing.T) {
	for _, name := range []string{"actor", "live_actor", "process", "boot", "mixed_target", "duplicate", "config_actor", "retired"} {
		t.Run(name, func(t *testing.T) {
			process, plans := newPreparedProviderTestPlans(t)
			switch name {
			case "actor":
				plans[0].Authority.ActorPlanFingerprint = strings.Repeat("f", 64)
			case "live_actor":
				plans[0].Agent.Config.Identity, _ = plans[0].Agent.Identity.Live(uuid.NewString())
			case "process":
				plans[0].Authority.ProcessAuthorityID = uuid.NewString()
			case "boot":
				plans[0].Authority.ProcessBootID = uuid.NewString()
			case "mixed_target":
				plans[1].Authority.CatalogFingerprint = strings.Repeat("f", 64)
			case "duplicate":
				plans[1] = plans[0]
			case "config_actor":
				plans[0].Agent.Config.ID = "other"
			case "retired":
				if err := process.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			_, err := ValidatePreparedSelectedForkProviderPreflight(context.Background(), nil, toolgateway.Binding{}, nil, nil, uuid.NewString(), process, plans, liveTestEffectController(&startupEffectStore{}), &startupCapabilityStore{})
			if err == nil || strings.Contains(err.Error(), "runtime resolver") {
				t.Fatalf("census reached provider resolution: %v", err)
			}
		})
	}
}

func TestPreparedProviderPreflightFailureDoesNotReturnReceipts(t *testing.T) {
	for _, scenario := range []string{"provider_failure", "replaced_receipt", "retired_during_last_probe", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			process, plans := newPreparedProviderTestPlans(t)
			plans = plans[:1]
			probe := &preparedProtocolProbe{}
			ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
			defer cancel()
			switch scenario {
			case "provider_failure":
				probe.err = errors.New("provider-rejected-probe")
			case "replaced_receipt":
				probe.replaceReceipt = true
			case "retired_during_last_probe":
				probe.before = process.Release
			case "canceled":
				cancel()
			}
			cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
			profile, err := cfg.LLMBackendProfile()
			if err != nil {
				t.Fatal(err)
			}
			runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: probe})
			if err != nil {
				t.Fatal(err)
			}
			store := &startupCapabilityStore{}
			catalog := preparedProviderTestCatalog(t, runtimes, &startupProbeToolExecutor{}, plans)
			// Empty tools keep this proof at the provider boundary. The positive
			// matrix above independently exercises the real MCP HTTP protocol.
			ids, err := ValidatePreparedSelectedForkProviderPreflight(ctx, cfg, testToolGatewayBinding("http://127.0.0.1:1", "http://127.0.0.1:1", "probe-token"), catalog, mcp.NewTurnContextRegistry(actors.ActorFromContext), uuid.NewString(), process, plans, liveTestEffectController(&startupEffectStore{}), store)
			if err == nil || len(ids) != 0 {
				t.Fatalf("failed preparation returned receipts: %v %v", ids, err)
			}
			if scenario == "provider_failure" && !strings.Contains(err.Error(), "provider-rejected-probe") {
				t.Fatal(err)
			}
			if scenario == "replaced_receipt" && !strings.Contains(err.Error(), "changed startup plan") {
				t.Fatal(err)
			}
			if scenario == "canceled" {
				if !errors.Is(err, context.Canceled) || len(probe.calls) != 0 || len(store.surfaces) != 0 {
					t.Fatalf("canceled preparation advanced: %v calls=%v surfaces=%d", err, probe.calls, len(store.surfaces))
				}
			} else if len(probe.calls) != 1 || len(store.surfaces) != 1 {
				t.Fatalf("lost diagnostic or retried provider: calls=%v surfaces=%d err=%v", probe.calls, len(store.surfaces), err)
			}
		})
	}
}
