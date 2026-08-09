package llm

import (
	"fmt"
	"strings"
	"sync"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type runtimeFactoryBuilder struct {
	mu       sync.Mutex
	factory  RuntimeFactory
	prepare  func(RuntimeFactory) (RuntimeFactory, error)
	prepared *RuntimeFactory
	err      error
}

func (b *runtimeFactoryBuilder) build(profile llmselection.Profile) (Runtime, error) {
	b.mu.Lock()
	if b.prepared == nil && b.err == nil {
		prepare := b.prepare
		if prepare == nil {
			prepare = RuntimeFactory.prepare
		}
		prepared, err := prepare(b.factory)
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
	ResolveAgentRuntime(models.AgentConfig) (AgentRuntimeResolution, error)
}

type AgentRuntimeResolution struct {
	Actor     models.AgentConfig
	Selection llmselection.AgentExecutionSelection
	Runtime   Runtime
}

// AgentRuntimeSet binds one configured live default and the exact mock
// adapter to already-resolved agent descriptors. Slots are built lazily so a
// fully mocked source never constructs the unreachable live adapter.
type AgentRuntimeSet struct {
	configuredDefault llmselection.Profile
	modelAliases      llmselection.ModelAliases
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
	liveBuilder := &runtimeFactoryBuilder{factory: factory, prepare: RuntimeFactory.prepare}
	mockBuilder := &runtimeFactoryBuilder{factory: factory, prepare: RuntimeFactory.prepareMock}
	defaultSlot := &runtimeSlot{profile: profile, injected: injectedDefault, builder: liveBuilder}
	mockSlot := &runtimeSlot{profile: mockProfile, builder: mockBuilder}
	if profile.ID == llmselection.BackendMock {
		mockSlot = defaultSlot
	}
	return &AgentRuntimeSet{
		configuredDefault: profile,
		modelAliases: func() llmselection.ModelAliases {
			if factory.Cfg == nil {
				return nil
			}
			return factory.Cfg.LLM.Models
		}(),
		defaultSlot: defaultSlot,
		mockSlot:    mockSlot,
	}, nil
}

func (r *AgentRuntimeSet) ConfiguredDefault() llmselection.Profile {
	if r == nil {
		return llmselection.Profile{}
	}
	return r.configuredDefault
}

func (r *AgentRuntimeSet) runtimeForSelection(selection llmselection.AgentExecutionSelection) (Runtime, error) {
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

func (r *AgentRuntimeSet) ResolveAgentRuntime(actor models.AgentConfig) (AgentRuntimeResolution, error) {
	if r == nil {
		return AgentRuntimeResolution{}, fmt.Errorf("agent llm runtime resolver is required")
	}
	resolved, err := ResolveAgentExecution(r.configuredDefault, r.modelAliases, actor)
	if err != nil {
		return AgentRuntimeResolution{}, fmt.Errorf("agent %s execution selection: %w", agentRuntimeLabel(actor), err)
	}
	actor = resolved.Actor
	selection := resolved.Selection
	if strings.TrimSpace(actor.ResolvedLLMBackend) != selection.Profile.ID {
		return AgentRuntimeResolution{}, fmt.Errorf("agent %s resolved llm backend %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.ResolvedLLMBackend), selection.Profile.ID)
	}
	if actor.ExecutionMode != selection.Mode {
		return AgentRuntimeResolution{}, fmt.Errorf("agent %s execution mode %q conflicts with effective selection %q", agentRuntimeLabel(actor), actor.ExecutionMode, selection.Mode)
	}
	if strings.TrimSpace(actor.ResolvedLLMProvider) != selection.Profile.Provider {
		return AgentRuntimeResolution{}, fmt.Errorf("agent %s provider %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.ResolvedLLMProvider), selection.Profile.Provider)
	}
	if strings.TrimSpace(actor.ResolvedLLMTransport) != selection.Profile.Transport {
		return AgentRuntimeResolution{}, fmt.Errorf("agent %s transport %q conflicts with effective selection %q", agentRuntimeLabel(actor), strings.TrimSpace(actor.ResolvedLLMTransport), selection.Profile.Transport)
	}
	modelRuntime, err := r.runtimeForSelection(selection)
	if err != nil {
		return AgentRuntimeResolution{}, err
	}
	return AgentRuntimeResolution{Actor: actor, Selection: selection, Runtime: modelRuntime}, nil
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
