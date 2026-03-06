package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"

	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sessions"
)

type echoRuntime struct {
	mu    sync.Mutex
	turns map[string]int
}

func (r *echoRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	_ = systemPrompt
	_ = tools
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.turns == nil {
		r.turns = make(map[string]int)
	}
	r.turns[agentID] = 0
	return &llm.Session{
		ID:          "sess-" + agentID,
		AgentID:     agentID,
		RuntimeMode: "test",
	}, nil
}

func (r *echoRuntime) ContinueSession(_ context.Context, s *llm.Session, message llm.Message) (*llm.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turns[s.AgentID]++
	n := r.turns[s.AgentID]
	return &llm.Response{
		Message: llm.Message{
			Role:    "assistant",
			Content: fmt.Sprintf("turn=%d echo=%s", n, message.Content),
		},
	}, nil
}

func TestConversationLongRunContinuity(t *testing.T) {
	rt := &echoRuntime{}
	c := llm.NewConversation(
		"agent-soak",
		"",
		"soak test prompt",
		nil,
		llm.SessionScoped,
		200,
		rt,
	)

	for i := 1; i <= 60; i++ {
		resp, err := c.Step(context.Background(), fmt.Sprintf("message-%d", i))
		if err != nil {
			t.Fatalf("step %d failed: %v", i, err)
		}
		if resp == nil || resp.Message.Content == "" {
			t.Fatalf("step %d returned empty response", i)
		}
	}
	if c.TurnCount != 60 {
		t.Fatalf("expected 60 turns, got %d", c.TurnCount)
	}
	if c.Session == nil || c.Session.ID == "" {
		t.Fatal("expected non-empty session after soak conversation")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if got := rt.turns["agent-soak"]; got != 60 {
		t.Fatalf("expected runtime turn counter=60, got %d", got)
	}
}

func TestSessionRegistryLockContentionSoak(t *testing.T) {
	sr := sessions.NewInMemoryRegistry(0)

	first, err := sr.Acquire("agent-lock", "cli_test", "holder", "")
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := sr.Acquire("agent-lock", "cli_test", fmt.Sprintf("worker-%d", i), "")
			if err == nil {
				errs <- fmt.Errorf("worker-%d unexpectedly acquired lock while holder active", i)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if err := sr.Release(first); err != nil {
		t.Fatalf("release initial lease failed: %v", err)
	}
	if _, err := sr.Acquire("agent-lock", "cli_test", "after-release", ""); err != nil {
		t.Fatalf("expected acquire after release, got: %v", err)
	}
}

func TestConversationConcurrentTurnLoad(t *testing.T) {
	const (
		agents = 12
		turns  = 80
	)

	rt := &echoRuntime{}
	var wg sync.WaitGroup
	errs := make(chan error, agents)

	for i := 0; i < agents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			agentID := fmt.Sprintf("agent-soak-%02d", i)
			c := llm.NewConversation(
				agentID,
				"",
				"concurrent soak prompt",
				nil,
				llm.SessionScoped,
				turns+20,
				rt,
			)
			for step := 1; step <= turns; step++ {
				resp, err := c.Step(context.Background(), fmt.Sprintf("a=%d step=%d", i, step))
				if err != nil {
					errs <- fmt.Errorf("agent=%s step=%d: %w", agentID, step, err)
					return
				}
				if resp == nil || resp.Message.Content == "" {
					errs <- fmt.Errorf("agent=%s step=%d returned empty response", agentID, step)
					return
				}
			}
			if c.Session == nil || c.Session.ID == "" {
				errs <- fmt.Errorf("agent=%s missing session id after run", agentID)
				return
			}
			if c.TurnCount != turns {
				errs <- fmt.Errorf("agent=%s turn_count=%d want=%d", agentID, c.TurnCount, turns)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	if got := len(rt.turns); got != agents {
		t.Fatalf("expected %d runtime agent counters, got %d", agents, got)
	}
	for i := 0; i < agents; i++ {
		agentID := fmt.Sprintf("agent-soak-%02d", i)
		if got := rt.turns[agentID]; got != turns {
			t.Fatalf("agent=%s runtime turns=%d want=%d", agentID, got, turns)
		}
	}
}
