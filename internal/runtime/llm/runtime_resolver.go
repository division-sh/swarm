package llm

import (
	"fmt"
	"strings"
	"sync"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type runtimeFactoryBuilder struct {
	mu       sync.Mutex
	factory  RuntimeFactory
	prepared *RuntimeFactory
	err      error
}

func (b *runtimeFactoryBuilder) build(profile llmselection.Profile) (Runtime, error) {
	b.mu.Lock()
	if b.prepared == nil && b.err == nil {
		prepared, err := b.factory.prepare()
		if err != nil {
			b.err = err
		} else {
			b.prepared = &prepared
		}
	}
	prepared, err := b.prepared, b.err
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return prepared.buildProfile(profile)
}

type runtimeSlot struct {
	profile  llmselection.Profile
	injected Runtime
	builder  *runtimeFactoryBuilder
	once     sync.Once
	runtime  Runtime
	err      error
}

func (s *runtimeSlot) get() (Runtime, error) {
	if s == nil {
		return nil, fmt.Errorf("llm runtime slot is not configured")
	}
	s.once.Do(func() {
		if s.injected != nil {
			s.runtime = s.injected
			if _, ok := s.runtime.(ProviderContractProvider); ok {
				_, s.err = RequireProviderContractForProfile(s.profile, s.runtime)
			}
			return
		}
		if s.builder == nil {
			s.err = fmt.Errorf("llm runtime builder is not configured for profile %q", s.profile.ID)
			return
		}
		s.runtime, s.err = s.builder.build(s.profile)
	})
	return s.runtime, s.err
}

type AgentRuntimeResolver interface {
	RuntimeForAgent(models.AgentConfig) (Runtime, error)
}

// AgentRuntimeSet binds one configured live default and the exact mock
// adapter to already-resolved agent descriptors. Slots are built lazily so a
// fully mocked source never constructs the unreachable live adapter.
type AgentRuntimeSet struct {
	configuredDefault llmselection.Profile
	defaultSlot       *runtimeSlot
	mockSlot          *runtimeSlot
}

func NewAgentRuntimeSet(configuredDefault llmselection.Profile, factory RuntimeFactory, injectedDefault Runtime) (*AgentRuntimeSet, error) {
	profile, err := llmselection.ResolveActiveBackend(configuredDefault.ID)
	if err != nil {
		return nil, err
	}
	mockProfile, err := llmselection.ResolveActiveBackend(llmselection.BackendMock)
	if err != nil {
		return nil, err
	}
	builder := &runtimeFactoryBuilder{factory: factory}
	defaultSlot := &runtimeSlot{profile: profile, injected: injectedDefault, builder: builder}
	mockSlot := &runtimeSlot{profile: mockProfile, builder: builder}
	if profile.ID == llmselection.BackendMock {
		mockSlot = defaultSlot
	}
	return &AgentRuntimeSet{
		configuredDefault: profile,
		defaultSlot:       defaultSlot,
		mockSlot:          mockSlot,
	}, nil
}

func (r *AgentRuntimeSet) ConfiguredDefault() llmselection.Profile {
	if r == nil {
		return llmselection.Profile{}
	}
	return r.configuredDefault
}

func (r *AgentRuntimeSet) SelectionForArtifact(mockConfigured bool) (llmselection.AgentExecutionSelection, error) {
	if r == nil {
		return llmselection.AgentExecutionSelection{}, fmt.Errorf("agent llm runtime resolver is required")
	}
	return llmselection.ResolveAgentExecutionSelection(r.configuredDefault, mockConfigured)
}

func (r *AgentRuntimeSet) RuntimeForSelection(selection llmselection.AgentExecutionSelection) (Runtime, error) {
	if r == nil {
		return nil, fmt.Errorf("agent llm runtime resolver is required")
	}
	switch selection.Profile.ID {
	case llmselection.BackendMock:
		return r.mockSlot.get()
	case r.configuredDefault.ID:
		return r.defaultSlot.get()
	default:
		return nil, fmt.Errorf("agent selects llm backend %q outside configured default %q", selection.Profile.ID, r.configuredDefault.ID)
	}
}

func (r *AgentRuntimeSet) RuntimeForAgent(actor models.AgentConfig) (Runtime, error) {
	selection, err := r.SelectionForArtifact(actor.Mock.Configured())
	if err != nil {
		return nil, fmt.Errorf("agent %s execution selection: %w", agentRuntimeLabel(actor), err)
	}
	if strings.TrimSpace(actor.LLMBackend) != selection.Profile.ID {
		return nil, fmt.Errorf("agent %s llm backend %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.LLMBackend), selection.Profile.ID)
	}
	if actor.ExecutionMode != selection.Mode {
		return nil, fmt.Errorf("agent %s execution mode %q conflicts with effective selection %q", agentRuntimeLabel(actor), actor.ExecutionMode, selection.Mode)
	}
	if strings.TrimSpace(actor.ResolvedLLMProvider) != selection.Profile.Provider {
		return nil, fmt.Errorf("agent %s provider %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.ResolvedLLMProvider), selection.Profile.Provider)
	}
	if strings.TrimSpace(actor.ResolvedLLMTransport) != selection.Profile.Transport {
		return nil, fmt.Errorf("agent %s transport %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.ResolvedLLMTransport), selection.Profile.Transport)
	}
	if selection.ArtifactRequirement == llmselection.ArtifactRequired &&
		(actor.Mock.Kind != mockperformance.KindPython || len(actor.Mock.Source) == 0 || strings.TrimSpace(actor.Mock.Digest) == "") {
		return nil, fmt.Errorf("agent %s selects mock execution but has no compiled Python performance", agentRuntimeLabel(actor))
	}
	return r.RuntimeForSelection(selection)
}

func agentRuntimeLabel(actor models.AgentConfig) string {
	if id := strings.TrimSpace(actor.ID); id != "" {
		return id
	}
	if role := strings.TrimSpace(actor.Role); role != "" {
		return role
	}
	return "unknown-agent"
}
