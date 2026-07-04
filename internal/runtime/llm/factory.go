package llm

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type RuntimeFactory struct {
	Cfg           *config.Config
	Sessions      sessions.Registry
	Turns         TurnPersistence
	Conversations ConversationPersistence
	Budget        BudgetGuard
	LockOwner     string
	Workspaces    workspace.Resolver
	Events        EventPublisher
	MCPTurns      MCPTurnContextStore
	ToolGateway   toolgateway.Binding
	Credentials   runtimecredentials.Store
}

func (f RuntimeFactory) Build() (Runtime, error) {
	if f.Cfg == nil {
		return nil, fmt.Errorf("llm runtime config is required")
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
		runtime = NewAnthropicAPIRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Conversations, f.Budget, f.Events, providerCredentials)
		runtime.(*AnthropicAPIRuntime).providerAdmission = providerAdmission
	case llmselection.BackendClaudeCLI:
		runtime = NewClaudeCLIRuntimeWithOptions(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Budget, f.Workspaces, f.Conversations, f.Events, ClaudeCLIRuntimeOptions{
			MCPTurnContextStore: f.MCPTurns,
			ToolGateway:         f.ToolGateway,
			ProviderCredentials: providerCredentials,
		})
		runtime.(*ClaudeCLIRuntime).providerAdmission = providerAdmission
	case llmselection.BackendOpenAICompatible:
		runtime = NewOpenAICompatibleRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Conversations, f.Budget, f.Events, providerCredentials)
		runtime.(*OpenAICompatibleRuntime).providerAdmission = providerAdmission
	case llmselection.BackendOpenAIResponses:
		runtime = NewOpenAIResponsesRuntimeWithProviderCredentials(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Conversations, f.Budget, f.Events, providerCredentials)
		runtime.(*OpenAIResponsesRuntime).providerAdmission = providerAdmission
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
	return &Session{ID: "noop", AgentID: agentID, RuntimeMode: "noop"}, nil
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
