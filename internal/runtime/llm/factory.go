package llm

import (
	"context"
	"fmt"
	"os"
	"time"

	"empireai/internal/config"
	"empireai/internal/runtime/sessions"
	workspace "empireai/internal/runtime/workspace"
)

type RuntimeFactory struct {
	Cfg           *config.Config
	Sessions      sessions.Registry
	Turns         TurnPersistence
	Conversations ConversationPersistence
	Budget        BudgetGuard
	LockOwner     string
	Workspaces    workspace.Resolver
}

func (f RuntimeFactory) Build() (Runtime, error) {
	if f.Sessions == nil {
		f.Sessions = sessions.NewInMemoryRegistry(f.Cfg.LLM.Session.LockTTL)
	}
	if f.LockOwner == "" {
		f.LockOwner = defaultLockOwner()
	}

	switch f.Cfg.LLM.RuntimeMode {
	case "api":
		return NewAnthropicAPIRuntime(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Conversations, f.Budget), nil
	case "cli_test":
		return NewClaudeCLIRuntime(f.Cfg, f.Sessions, f.LockOwner, f.Turns, f.Budget, f.Workspaces, f.Conversations), nil
	default:
		return nil, fmt.Errorf("unsupported llm runtime mode: %s", f.Cfg.LLM.RuntimeMode)
	}
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

func defaultLockOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}
