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
	if f.CompletionController == nil || !f.CompletionController.CompletionEnabled() {
		return nil, fmt.Errorf("llm completion execution controller is required")
	}
	if f.Sessions == nil {
		f.Sessions = sessions.NewInMemoryRegistry(f.Cfg.LLM.Session.LockTTL)
	}
	if f.LockOwner == "" {
		f.LockOwner = defaultLockOwner()
	}

	profile, err := f.Cfg.LLMBackendProfile()
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
	case llmselection.BackendClaudeCLI:
		runtime = NewClaudeCLIRuntimeWithOptions(f.Cfg, f.Sessions, f.LockOwner, f.Workspaces, f.Conversations, f.Events, ClaudeCLIRuntimeOptions{
			MCPTurnContextStore:  f.MCPTurns,
			ToolGateway:          f.ToolGateway,
			ProviderCredentials:  providerCredentials,
			CompletionController: f.CompletionController,
		})
		runtime.(*ClaudeCLIRuntime).providerAdmission = providerAdmission
	case llmselection.BackendOpenAICompatible:
		runtime = NewOpenAICompatibleRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, providerCredentials)
		runtime.(*OpenAICompatibleRuntime).providerAdmission = providerAdmission
		runtime.(*OpenAICompatibleRuntime).completionController = f.CompletionController
	case llmselection.BackendOpenAIResponses:
		runtime = NewOpenAIResponsesRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, providerCredentials)
		runtime.(*OpenAIResponsesRuntime).providerAdmission = providerAdmission
		runtime.(*OpenAIResponsesRuntime).completionController = f.CompletionController
	case llmselection.BackendMock:
		runtime = NewMockRuntime(f.Cfg, f.Sessions, f.LockOwner, f.Conversations, f.Events, f.CompletionController)
	default:
		return nil, fmt.Errorf("unsupported llm backend profile: %s", profile.ID)
	}
	if _, err := RequireProviderContractForProfile(profile, runtime); err != nil {
		return nil, err
	}
	return runtime, nil
}

// NoopRuntime is useful in early bootstrap phases and tests.
type NoopRuntime struct{}

func (NoopRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "noop", AgentID: agentID, Memory: agentmemory.PlatformDefault()}, nil
}

func (NoopRuntime) ContinueSession(_ context.Context, _ *Session, message Message) (*Response, error) {
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
