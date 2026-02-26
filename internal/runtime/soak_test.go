package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type echoRuntime struct {
	mu    sync.Mutex
	turns map[string]int
}

func (r *echoRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	_ = systemPrompt
	_ = tools
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.turns == nil {
		r.turns = make(map[string]int)
	}
	r.turns[agentID] = 0
	return &Session{
		ID:          "sess-" + agentID,
		AgentID:     agentID,
		RuntimeMode: "test",
	}, nil
}

func (r *echoRuntime) ContinueSession(_ context.Context, s *Session, message Message) (*Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turns[s.AgentID]++
	n := r.turns[s.AgentID]
	return &Response{
		Message: Message{
			Role:    "assistant",
			Content: fmt.Sprintf("turn=%d echo=%s", n, message.Content),
		},
	}, nil
}

func TestConversationLongRunContinuity(t *testing.T) {
	rt := &echoRuntime{}
	c := NewConversation(
		"agent-soak",
		"",
		"soak test prompt",
		nil,
		SessionScoped,
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
}

func TestSessionRegistryLockContentionSoak(t *testing.T) {
	sr := NewInMemorySessionRegistry(0)

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
