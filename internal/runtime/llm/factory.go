package llm

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type RuntimeFactory struct {
	Cfg                  *config.Config
	Sessions             sessions.Registry
	LiveSessions         LiveSessionAcquirer
	Conversations        ConversationPersistence
	LockOwner            string
	Workspaces           workspace.Resolver
	Events               EventPublisher
	MCPTurns             MCPTurnContextStore
	ToolGateway          toolgateway.Binding
	Credentials          runtimecredentials.Store
	CompletionController *runtimeeffects.Controller
}

func (f RuntimeFactory) Build() (Runtime, error) {
	if f.Cfg == nil {
		return nil, fmt.Errorf("llm runtime config is required")
	}
	profile, err := f.Cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
	}
	prepared, err := f.prepare()
	if err != nil {
		return nil, err
	}
	return prepared.buildProfile(profile)
}

func (f RuntimeFactory) prepare() (RuntimeFactory, error) {
	if f.Cfg == nil {
		return RuntimeFactory{}, fmt.Errorf("llm runtime config is required")
	}
	if _, err := f.Cfg.LLMBackendProfile(); err != nil {
		return RuntimeFactory{}, err
	}
	if f.CompletionController == nil || !f.CompletionController.CompletionEnabled() {
		return RuntimeFactory{}, fmt.Errorf("llm completion execution controller is required")
	}
	if f.Sessions == nil {
		f.Sessions = sessions.NewInMemoryRegistry(f.Cfg.LLM.Session.LockTTL)
	}
	if f.LiveSessions == nil {
		return RuntimeFactory{}, fmt.Errorf("llm live session acquirer is required")
	}
	if f.LockOwner == "" {
		f.LockOwner = defaultLockOwner()
	}
	return f, nil
}

func (f RuntimeFactory) prepareMock() (RuntimeFactory, error) {
	if f.Cfg == nil {
		return RuntimeFactory{}, fmt.Errorf("llm runtime config is required")
	}
	if f.CompletionController == nil || !f.CompletionController.CompletionEnabled() {
		return RuntimeFactory{}, fmt.Errorf("llm completion execution controller is required")
	}
	if f.Sessions == nil {
		f.Sessions = sessions.NewInMemoryRegistry(f.Cfg.LLM.Session.LockTTL)
	}
	if f.LiveSessions == nil {
		f.LiveSessions = NewTransientLiveSessionAcquirer(f.Sessions)
	}
	if f.LockOwner == "" {
		f.LockOwner = defaultLockOwner()
	}
	return f, nil
}

func (f RuntimeFactory) buildProfile(profile llmselection.Profile) (Runtime, error) {
	profile, err := llmselection.ResolveActiveBackend(profile.ID)
	if err != nil {
		return nil, err
	}

	providerAdmission := NewProviderAdmissionRegistry(f.Cfg)
	providerCredentials := NewProviderCredentialResolver(f.Credentials)

	var runtime Runtime
	switch profile.ID {
	case llmselection.BackendAnthropic:
		runtime = NewAnthropicAPIRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, providerCredentials)
		runtime.(*AnthropicAPIRuntime).providerAdmission = providerAdmission
		runtime.(*AnthropicAPIRuntime).completionController = f.CompletionController
		runtime.(*AnthropicAPIRuntime).liveSessions = f.LiveSessions
	case llmselection.BackendClaudeCLI:
		runtime = NewClaudeCLIRuntimeWithOptions(f.Cfg, f.Sessions, f.LockOwner, f.Workspaces, f.Conversations, f.Events, ClaudeCLIRuntimeOptions{
			MCPTurnContextStore:  f.MCPTurns,
			ToolGateway:          f.ToolGateway,
			ProviderCredentials:  providerCredentials,
			CompletionController: f.CompletionController,
		})
		runtime.(*ClaudeCLIRuntime).providerAdmission = providerAdmission
		runtime.(*ClaudeCLIRuntime).liveSessions = f.LiveSessions
	case llmselection.BackendOpenAICompatible:
		runtime = NewOpenAICompatibleRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, providerCredentials)
		runtime.(*OpenAICompatibleRuntime).providerAdmission = providerAdmission
		runtime.(*OpenAICompatibleRuntime).completionController = f.CompletionController
		runtime.(*OpenAICompatibleRuntime).liveSessions = f.LiveSessions
	case llmselection.BackendOpenAIResponses:
		runtime = NewOpenAIResponsesRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, providerCredentials)
		runtime.(*OpenAIResponsesRuntime).providerAdmission = providerAdmission
		runtime.(*OpenAIResponsesRuntime).completionController = f.CompletionController
		runtime.(*OpenAIResponsesRuntime).liveSessions = f.LiveSessions
	case llmselection.BackendMock:
		runtime = NewMockRuntime(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, f.CompletionController)
		runtime.(*MockRuntime).liveSessions = f.LiveSessions
	default:
		return nil, fmt.Errorf("unsupported llm backend profile: %s", profile.ID)
	}
	if _, err := RequireProviderContractForProfile(profile, runtime); err != nil {
		return nil, err
	}
	return runtime, nil
}

// NoopRuntime is useful in early bootstrap phases and tests. Its provider
// contract is explicit so a test double cannot silently impersonate a
// differently selected backend.
type NoopRuntime struct {
	contract ProviderContract
}

func NewNoopRuntime(contract ProviderContract) NoopRuntime {
	return NoopRuntime{contract: contract}
}

func (r NoopRuntime) ProviderContract() ProviderContract { return r.contract }

func (NoopRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	return &Session{
		ID: "noop", AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]ToolDefinition(nil), tools...), Memory: agentmemory.PlatformDefault(),
	}, nil
}

func (NoopRuntime) PrepareManagedSession(context.Context, *Session) error { return nil }

func (NoopRuntime) ContinueManagedSession(_ context.Context, _ *Session, call ManagedCall) (*Response, error) {
	message, err := call.providerMessage()
	if err != nil {
		return nil, err
	}
	return &Response{Message: Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func (NoopRuntime) ContinueForkChatSession(_ context.Context, _ *Session, call ForkChatCall) (*Response, error) {
	message := call.providerMessage()
	return &Response{Message: Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func (NoopRuntime) PersistConversationSnapshot(context.Context, *Session) error { return nil }

type EventPublisher interface {
	Publish(ctx context.Context, evt events.Event) error
	MarkDeliveryInProgress(ctx context.Context, agentID, sessionID string) (bool, error)
}

func defaultLockOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}
