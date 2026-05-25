package llm

import (
	"context"
	"fmt"
	"os"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	llmselection "swarm/internal/runtime/llm/selection"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
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

	var runtime Runtime
	switch profile.ID {
	case llmselection.BackendAPI:
		runtime = NewAnthropicAPIRuntime(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Conversations, f.Budget, f.Events)
	case llmselection.BackendCLITest:
		runtime = NewClaudeCLIRuntimeWithOptions(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Budget, f.Workspaces, f.Conversations, f.Events, ClaudeCLIRuntimeOptions{
			MCPTurnContextStore: f.MCPTurns,
		})
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
