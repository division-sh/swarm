package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

func TestAgentContextHelpers_ExtractContextIDs_AndTransitionFallback(t *testing.T) {
	evt := events.Event{
		VerticalID: "",
		TaskID:     "",
		Payload: mustJSON(map[string]any{
			"vertical_ref": "v-ref",
			"task_ref":     "t-ref",
		}),
	}
	verticalID, taskID := extractContextIDs(evt)
	if verticalID != "v-ref" || taskID != "t-ref" {
		t.Fatalf("extractContextIDs payload fallback mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	// Existing top-level fields should win over payload.
	evt = events.Event{
		VerticalID: "v-top",
		TaskID:     "t-top",
		Payload: mustJSON(map[string]any{
			"vertical_id": "v-payload",
			"task_id":     "t-payload",
		}),
	}
	verticalID, taskID = extractContextIDs(evt)
	if verticalID != "v-top" || taskID != "t-top" {
		t.Fatalf("extractContextIDs top-level precedence mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	// Invalid payload JSON should not break extraction.
	verticalID, taskID = extractContextIDs(events.Event{VerticalID: "v", TaskID: "t", Payload: []byte("{")})
	if verticalID != "v" || taskID != "t" {
		t.Fatalf("extractContextIDs invalid-json mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	key := transitionContextKey(
		events.Event{Payload: mustJSON(map[string]any{"vertical_id": "v1"})},
		events.Event{Payload: mustJSON(map[string]any{"vertical_id": "v2", "task_id": "t2"})},
	)
	if key != "v1|t2" {
		t.Fatalf("transitionContextKey fallback mismatch: %q", key)
	}
}

func TestAgentContractHelpers_BudgetExpectationAndRemediation(t *testing.T) {
	agent := &LLMAgent{cfg: models.AgentConfig{Role: "empire-coordinator"}}
	inbound := events.Event{Type: events.EventType("budget.threshold_crossed")}

	// Missing budget emission should fail contract.
	recorder := NewEmittedEventsRecorder()
	err := agent.enforcePostTurnExpectations(inbound, recorder)
	if err == nil || !strings.Contains(err.Error(), "must emit one budget.* event") {
		t.Fatalf("expected budget contract error, got %v", err)
	}

	// Emitting a budget event should satisfy contract.
	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := agent.enforcePostTurnExpectations(inbound, recorder); err != nil {
		t.Fatalf("expected budget contract satisfied, got %v", err)
	}

	// Remediation prompt for budget thresholds.
	prompt, ok := contractRemediationPrompt(agent.cfg, inbound, errors.New("x"))
	if !ok || !strings.Contains(prompt, "emit_budget_*") {
		t.Fatalf("expected budget remediation prompt, ok=%v prompt=%q", ok, prompt)
	}

	// Non-matching role/event should not emit remediation prompt.
	if prompt, ok := contractRemediationPrompt(models.AgentConfig{Role: "backend-agent"}, events.Event{Type: events.EventType("board.chat")}, errors.New("x")); ok || prompt != "" {
		t.Fatalf("expected no remediation prompt for non-coordinator event, ok=%v prompt=%q", ok, prompt)
	}
}

type failingContinueRuntime struct{}

func (failingContinueRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "s", AgentID: agentID, RuntimeMode: "api"}, nil
}
func (failingContinueRuntime) ContinueSession(context.Context, *Session, Message) (*Response, error) {
	return nil, errors.New("continue failed")
}
func (failingContinueRuntime) PersistConversationSnapshot(context.Context, *Session) error { return nil }

func TestAttemptPostTurnContractRemediation_Branches(t *testing.T) {
	// No remediation prompt for non-coordinator board event -> returns original error.
	noPromptAgent := &LLMAgent{cfg: models.AgentConfig{Role: "backend-agent"}}
	originalErr := errors.New("contract failed")
	got := noPromptAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("board.chat")},
		NewEmittedEventsRecorder(),
		originalErr,
	)
	if !errors.Is(got, originalErr) {
		t.Fatalf("expected original contract error, got %v", got)
	}

	// Remediation prompt path with successful follow-up check.
	okAgent := &LLMAgent{
		cfg: models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"},
		conversation: NewConversation(
			"empire-coordinator",
			"",
			"prompt",
			nil,
			SessionScoped,
			10,
			&llmNoToolRuntime{},
		),
	}
	recorder := NewEmittedEventsRecorder()
	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := okAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("budget.threshold_crossed")},
		recorder,
		errors.New("budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool"),
	); err != nil {
		t.Fatalf("expected remediation success, got %v", err)
	}

	// Remediation prompt path where follow-up step fails.
	failAgent := &LLMAgent{
		cfg: models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"},
		conversation: NewConversation(
			"empire-coordinator",
			"",
			"prompt",
			nil,
			SessionScoped,
			10,
			failingContinueRuntime{},
		),
	}
	recorder = NewEmittedEventsRecorder()
	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := failAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("budget.threshold_crossed")},
		recorder,
		errors.New("budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool"),
	); err == nil || !strings.Contains(err.Error(), "continue failed") {
		t.Fatalf("expected remediation step failure, got %v", err)
	}
}
