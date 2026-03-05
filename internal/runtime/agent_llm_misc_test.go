package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

type captureRuntime struct {
	started      int
	startPrompts []string
	seen         []Message
}

func (c *captureRuntime) StartSession(_ context.Context, agentID, systemPrompt string, _ []ToolDefinition) (*Session, error) {
	c.started++
	c.startPrompts = append(c.startPrompts, systemPrompt)
	return &Session{ID: "s1", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (c *captureRuntime) ContinueSession(_ context.Context, _ *Session, message Message) (*Response, error) {
	c.seen = append(c.seen, message)
	return &Response{Message: Message{Role: "assistant", Content: "ok"}}, nil
}

func TestLLMAgentFactory_IDTypeSubscriptionsAndBoardStep(t *testing.T) {
	rt := &captureRuntime{}
	factory := NewLLMAgentFactory(rt, &fakeToolExec{}, nil)
	cfgJSON, _ := json.Marshal(map[string]any{
		"system_prompt": "You are a1.",
	})

	a, err := factory(models.AgentConfig{
		ID:            "a1",
		Type:          "worker",
		Role:          "pm-agent",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"foo.*", "bar.baz"},
		Config:        cfgJSON,
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	la, ok := a.(*LLMAgent)
	if !ok {
		t.Fatalf("expected *LLMAgent, got %T", a)
	}
	if la.ID() != "a1" {
		t.Fatalf("ID: expected a1, got %s", la.ID())
	}
	if la.Type() != "worker" {
		t.Fatalf("Type: expected worker, got %s", la.Type())
	}
	subs := la.Subscriptions()
	if len(subs) != 2 {
		t.Fatalf("Subscriptions: expected 2, got %+v", subs)
	}

	// BoardStep should call StepWithRole with "board_directive".
	out, err := la.BoardStep(context.Background(), "do x")
	if err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	if out != "ok" {
		t.Fatalf("expected ok output, got %q", out)
	}
	if len(rt.seen) == 0 || rt.seen[0].Role != "board_directive" {
		t.Fatalf("expected board_directive message, got %+v", rt.seen)
	}
}

func TestLLMAgentFactory_RejectsMissingSystemPrompt(t *testing.T) {
	rt := &captureRuntime{}
	factory := NewLLMAgentFactory(rt, &fakeToolExec{}, nil)

	_, err := factory(models.AgentConfig{
		ID:   "missing-prompt",
		Role: "pm-agent",
	})
	if err == nil {
		t.Fatal("expected missing system_prompt to fail")
	}
}

func TestLLMAgent_UsesModePromptVariantFromContracts(t *testing.T) {
	promptsDir := t.TempDir()
	t.Setenv("EMPIREAI_PROMPTS_DIR", promptsDir)
	if err := os.WriteFile(filepath.Join(promptsDir, "market-research-agent.md"), []byte("default-prompt"), 0o644); err != nil {
		t.Fatalf("write default prompt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(promptsDir, "market-research-agent.corpus.md"), []byte("corpus-prompt"), 0o644); err != nil {
		t.Fatalf("write mode prompt: %v", err)
	}

	rt := &captureRuntime{}
	agent := NewLLMAgent(models.AgentConfig{
		ID:            "market-research-agent-shard-0-abc123",
		Role:          "market-research-agent",
		Mode:          "factory",
		ParentAgent:   "market-research-agent",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config: mustJSON(map[string]any{
			"system_prompt": "stale-default",
		}),
	}, rt, &fakeToolExec{}, nil)

	if _, err := agent.OnEvent(context.Background(), events.Event{
		ID:   "evt-1",
		Type: events.EventType("market_research.scan_assigned"),
		Payload: mustJSON(map[string]any{
			"mode": "corpus",
		}),
	}); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(rt.startPrompts) == 0 {
		t.Fatal("expected start prompt to be captured")
	}
	if rt.startPrompts[0] != "corpus-prompt" {
		t.Fatalf("expected corpus mode prompt, got %q", rt.startPrompts[0])
	}
}
