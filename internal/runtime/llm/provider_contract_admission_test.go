package llm

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestProviderContractAdmissionMatchesEveryShippedAdapter(t *testing.T) {
	cases := map[string]ProviderContractProvider{
		llmselection.BackendAnthropic:        &AnthropicAPIRuntime{},
		llmselection.BackendClaudeCLI:        &ClaudeCLIRuntime{},
		llmselection.BackendOpenAICompatible: &OpenAICompatibleRuntime{},
		llmselection.BackendOpenAIResponses:  &OpenAIResponsesRuntime{},
		llmselection.BackendMock:             &MockRuntime{},
	}
	for id, runtime := range cases {
		t.Run(id, func(t *testing.T) {
			profile, err := llmselection.ResolveActiveBackend(id)
			if err != nil {
				t.Fatal(err)
			}
			contract, err := ProviderContractForProfile(profile)
			if err != nil || contract != runtime.ProviderContract() {
				t.Fatalf("admission contract = %#v, %v; runtime = %#v", contract, err, runtime.ProviderContract())
			}
		})
	}
}

func TestProviderContractAdmissionRejectsNonCanonicalProfiles(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*llmselection.Profile){
		func(p *llmselection.Profile) { p.Provider = llmselection.ProviderAnthropic },
		func(p *llmselection.Profile) { p.Transport = llmselection.TransportAPI },
		func(p *llmselection.Profile) { p.RuntimeMode = llmselection.BackendMock },
		func(p *llmselection.Profile) { p.Active = false },
		func(p *llmselection.Profile) { p.ID = llmselection.BackendLocal },
	} {
		changed := profile
		mutate(&changed)
		if _, err := ProviderContractForProfile(changed); err == nil {
			t.Fatalf("admitted changed profile %#v", changed)
		}
	}
	if _, err := (AgentProviderContracts{}).ResolveAgentProviderContract(resolvedClaudeAgent("unconfigured")); err == nil {
		t.Fatal("admitted an unconfigured contract resolver")
	}
}

func TestAgentProviderContractAdmissionNeverConstructsRuntimeSlots(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	buildCalls := 0
	unavailable := errors.New("runtime construction is not admission")
	prepare := func(RuntimeFactory) (RuntimeFactory, error) {
		buildCalls++
		return RuntimeFactory{}, unavailable
	}
	runtimes, err := newAgentRuntimeSet(profile, RuntimeFactory{}, nil, prepare, prepare)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := NewAgentProviderContracts(profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range []models.AgentConfig{resolvedClaudeAgent("live"), resolvedMockAgent("mock")} {
		contract, err := runtimes.ResolveAgentProviderContract(actor)
		if err != nil {
			t.Fatal(err)
		}
		inspection, err := providers.ResolveAgentProviderContract(actor)
		if err != nil || inspection != contract {
			t.Fatalf("inspection = %#v, %v; boot = %#v", inspection, err, contract)
		}
	}
	if buildCalls != 0 || runtimes.defaultSlot.runtime != nil || runtimes.mockSlot.runtime != nil {
		t.Fatalf("admission constructed a runtime: calls=%d, live=%T, mock=%T", buildCalls, runtimes.defaultSlot.runtime, runtimes.mockSlot.runtime)
	}
	if _, err := runtimes.ResolveAgentRuntime(resolvedClaudeAgent("execution")); !errors.Is(err, unavailable) || buildCalls != 1 {
		t.Fatalf("execution no longer constructs its adapter: %v, calls=%d", err, buildCalls)
	}
}

func TestAgentProviderContractAdmissionPreservesDescriptorRejection(t *testing.T) {
	for _, id := range []string{llmselection.BackendAnthropic, llmselection.BackendClaudeCLI, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses} {
		t.Run(id, func(t *testing.T) {
			profile, err := llmselection.ResolveLiveBackend(id)
			if err != nil {
				t.Fatal(err)
			}
			providers, err := NewAgentProviderContracts(profile)
			if err != nil {
				t.Fatal(err)
			}
			runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			projected, err := ResolveAgentExecution(executionposture.Live, profile, nil, models.AgentConfig{ID: "actor"})
			if err != nil {
				t.Fatal(err)
			}
			for _, mutate := range []func(*models.AgentConfig){
				func(a *models.AgentConfig) { a.ResolvedLLMProvider = "wrong-provider" },
				func(a *models.AgentConfig) { a.ResolvedLLMTransport = "wrong-transport" },
				func(a *models.AgentConfig) { a.ResolvedLLMBackend = llmselection.BackendMock },
				func(a *models.AgentConfig) { a.ExecutionMode = "mock" },
				func(a *models.AgentConfig) { a.Model, a.ResolvedModel = "regular", "" },
			} {
				actor := projected.Actor
				mutate(&actor)
				_, inspectErr := providers.ResolveAgentProviderContract(actor)
				_, bootErr := runtimes.ResolveAgentProviderContract(actor)
				if inspectErr == nil || bootErr == nil || inspectErr.Error() != bootErr.Error() {
					t.Fatalf("descriptor refusal diverged: inspection=%v, boot=%v", inspectErr, bootErr)
				}
			}
		})
	}
}

type admissionContractOverride struct {
	observedClaudeRuntime
	contract ProviderContract
}

func (r *admissionContractOverride) ProviderContract() ProviderContract { return r.contract }

func TestAgentProviderContractAdmissionUsesInjectedContractWithoutPreparingRuntime(t *testing.T) {
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	contract := ClaudeCLIProviderContract()
	contract.NativeTools.Capabilities.WebSearch = false
	injected := &admissionContractOverride{contract: contract}
	prepare := func(RuntimeFactory) (RuntimeFactory, error) {
		return RuntimeFactory{}, fmt.Errorf("unexpected factory preparation")
	}
	runtimes, err := newAgentRuntimeSet(profile, RuntimeFactory{}, injected, prepare, prepare)
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtimes.ResolveAgentProviderContract(resolvedClaudeAgent("live"))
	if err != nil || got != contract || runtimes.defaultSlot.runtime != nil {
		t.Fatalf("injected contract = %#v, %v; prepared=%T", got, err, runtimes.defaultSlot.runtime)
	}
	injected.contract = AnthropicAPIProviderContract()
	if _, err := runtimes.ResolveAgentProviderContract(resolvedClaudeAgent("mismatched")); err == nil || !strings.Contains(err.Error(), "runtime mode") {
		t.Fatalf("mismatched injected contract was admitted: %v", err)
	}
}
