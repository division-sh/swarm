package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

func TestPreparedProviderCatalogExactIdentity(t *testing.T) {
	_, probes := newPreparedProviderTestPlans(t)
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		t.Fatal(err)
	}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: &preparedProtocolProbe{}})
	if err != nil {
		t.Fatal(err)
	}
	build := func(blueprints []manager.AgentMaterializationBlueprint, tools *startupProbeToolExecutor) *PreparedSelectedForkProviderCatalog {
		t.Helper()
		catalog, err := PrepareSelectedForkProviderCatalog(testAuthorActivityContext(context.Background()), runtimes, tools, blueprints)
		if err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	blueprints := []manager.AgentMaterializationBlueprint{probes[0].Agent, probes[1].Agent}
	tools := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
	base := build(blueprints, tools)
	if base.Fingerprint() == "" || len(base.targets) != 2 {
		t.Fatal("lost exact same-name census")
	}
	if reordered := build([]manager.AgentMaterializationBlueprint{blueprints[1], blueprints[0]}, tools); reordered.Fingerprint() != base.Fingerprint() {
		t.Fatal("actor order changed catalog identity")
	}
	for _, scenario := range []string{"schema", "description", "usage", "capability", "configuration", "declaration_owner"} {
		t.Run(scenario, func(t *testing.T) {
			changed := append([]manager.AgentMaterializationBlueprint(nil), blueprints...)
			exec := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
			switch scenario {
			case "schema":
				exec.defs[0].Schema = map[string]any{"type": "object", "additionalProperties": false}
			case "description":
				exec.defs[0].Description += " changed"
			case "usage":
				exec.defs[0].Usage = "new exact usage"
			case "capability":
				for name, capability := range exec.caps {
					capability.Callable = false
					capability.DenialReason = "denied"
					exec.caps[name] = capability
				}
			case "configuration":
				changed[0].Config.Role += " changed"
			case "declaration_owner":
				name, err := agentidentity.DeclaredName(changed[0].Config.ID, "replacement-owner")
				if err != nil {
					t.Fatal(err)
				}
				changed[0].Identity, err = agentidentity.NewPlan(name, changed[0].Identity.Route)
				if err != nil {
					t.Fatal(err)
				}
			}
			if build(changed, exec).Fingerprint() == base.Fingerprint() {
				t.Fatalf("%s did not change catalog identity", scenario)
			}
		})
	}
}

func TestPreparedProviderCatalogOwnsSnapshots(t *testing.T) {
	_, probes := newPreparedProviderTestPlans(t)
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
	profile, _ := cfg.LLMBackendProfile()
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: &preparedProtocolProbe{}})
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{"type": "object", "title": "original"}
	exec := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
	exec.defs[0].Schema = schema
	catalog := preparedProviderTestCatalog(t, runtimes, exec, probes)
	key := probes[0].Agent.Identity
	before := catalog.targets[key]
	expectedCapabilities := make(map[string]toolcapabilities.Capability, len(before.capabilities.ByName))
	for name, capability := range before.capabilities.ByName {
		expectedCapabilities[name] = capability
	}
	schema["title"] = "changed"
	exec.defs[0].Description = "changed"
	for name, capability := range exec.caps {
		capability.Callable = false
		exec.caps[name] = capability
	}
	probes[0].Agent.Config.Role = "changed"
	after := catalog.targets[key]
	if after.resolved.Actor.Role == "changed" || after.tools[0].Description == "changed" || after.tools[0].Schema.(map[string]any)["title"] != "original" {
		t.Fatal("caller mutation changed frozen configuration or tool definition")
	}
	for name, capability := range expectedCapabilities {
		if after.capabilities.ByName[name] != capability {
			t.Fatal("caller mutation changed frozen capabilities")
		}
	}
	consumer, err := after.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	consumer.tools[0].Schema.(map[string]any)["title"] = "consumer changed"
	consumer.resolved.Actor.Role = "consumer changed"
	if after.tools[0].Schema.(map[string]any)["title"] != "original" || after.resolved.Actor.Role == "consumer changed" {
		t.Fatal("consumer mutated its canonical catalog")
	}
}

func TestPreparedProviderCatalogRejectsCensusAndCancellation(t *testing.T) {
	_, probes := newPreparedProviderTestPlans(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareSelectedForkProviderCatalog(ctx, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled catalog: %v", err)
	}
	// A zero-value runtime set would fail resolution. Invalid census must win.
	for _, scenario := range []string{"duplicate", "live", "foreign_actor", "noncanonical"} {
		t.Run(scenario, func(t *testing.T) {
			blueprints := []manager.AgentMaterializationBlueprint{probes[0].Agent, probes[1].Agent}
			switch scenario {
			case "duplicate":
				blueprints[1] = blueprints[0]
			case "live":
				blueprints[1].Config.Identity, _ = blueprints[1].Identity.Live(uuid.NewString())
			case "foreign_actor":
				blueprints[1].Config.ID = "other"
			case "noncanonical":
				blueprints[1].Identity.Name.Owner += " "
			}
			_, err := PrepareSelectedForkProviderCatalog(context.Background(), &llm.AgentRuntimeSet{}, &startupProbeToolExecutor{}, blueprints)
			if err == nil || (!strings.Contains(err.Error(), "duplicate") && !strings.Contains(err.Error(), "runless") && !strings.Contains(err.Error(), "canonical")) {
				t.Fatalf("invalid census reached resolution: %v", err)
			}
		})
	}
}

func TestPreparedProviderCatalogRejectsInvalidSchemaValues(t *testing.T) {
	for _, value := range []any{string([]byte{0xff}), uint64(9007199254740993)} {
		_, _, err := clonePreparedProviderTools([]llm.ToolDefinition{{Name: "probe", Schema: map[string]any{"invalid": value}}}, toolcapabilities.Set{})
		if err == nil {
			t.Fatalf("invalid semantic schema value was coerced: %#v", value)
		}
	}
}

func TestPreparedProviderPreflightRejectsChangedCatalogBeforeDispatch(t *testing.T) {
	for _, scenario := range []string{"fingerprint", "configuration", "missing_actor", "foreign_owner"} {
		t.Run(scenario, func(t *testing.T) {
			process, probes := newPreparedProviderTestPlans(t)
			cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
			profile, _ := cfg.LLMBackendProfile()
			provider := &preparedProtocolProbe{}
			runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: provider})
			if err != nil {
				t.Fatal(err)
			}
			catalog := preparedProviderTestCatalog(t, runtimes, &startupProbeToolExecutor{}, probes)
			switch scenario {
			case "fingerprint":
				for i := range probes {
					probes[i].Authority.CatalogFingerprint = strings.Repeat("f", 64)
				}
			case "configuration":
				probes[1].Agent.Config.Role += " changed"
			case "missing_actor":
				probes = probes[:1]
			case "foreign_owner":
				name, _ := agentidentity.DeclaredName(probes[1].Agent.Config.ID, "foreign-owner")
				probes[1].Agent.Identity, _ = agentidentity.NewPlan(name, probes[1].Agent.Identity.Route)
				probes[1].Authority.ActorPlanFingerprint, _ = probes[1].Agent.Identity.Fingerprint()
			}
			store := &startupCapabilityStore{}
			_, err = ValidatePreparedSelectedForkProviderPreflight(context.Background(), cfg, toolgateway.Binding{}, catalog, nil, uuid.NewString(), process, probes, liveTestEffectController(&startupEffectStore{}), store)
			if err == nil || !strings.Contains(err.Error(), "catalog") || len(provider.calls) != 0 || len(store.surfaces) != 0 {
				t.Fatalf("changed catalog reached probe: %v calls=%d surfaces=%d", err, len(provider.calls), len(store.surfaces))
			}
		})
	}
}
