package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestConversationToolUncertaintyUsesAgentPresentation(t *testing.T) {
	for _, stage := range []string{"transport", "read_response"} {
		t.Run(stage, func(t *testing.T) {
			cause := errors.New("interrupted " + stage)
			failure := failures.Wrap(failures.ClassOutcomeUncertain, "http_tool_attempt_outcome_unconfirmed", "tool-executor", "execute_http_tool", map[string]any{"tool": "probe", "stage": stage}, cause)
			executor := &selectiveToolExec{errors: map[string]error{"probe": failure}}
			conversation := newTestConversation("a1", "t1", "sys", nil, testMemory(), 4, &fakeRuntime{})
			conversation.SetToolExecutor(executor)
			raw, executed, err := conversation.executeToolCalls(context.Background(), []ToolCall{{ID: "one-call", Name: "probe", Arguments: map[string]any{}}})
			if err != nil || len(executed) != 1 || executed[0].OK || len(executor.calls) != 1 {
				t.Fatalf("ordinary error result not delivered exactly once: executed=%+v calls=%v err=%v", executed, executor.calls, err)
			}
			if !strings.Contains(raw, "result unknown: the call may or may not have reached the system; it will not be retried automatically") || strings.Contains(strings.ToLower(raw), "reconcile") {
				t.Fatalf("complete model-visible result has the wrong uncertainty instructions: %s", raw)
			}
			var entries []struct {
				Failure struct {
					Class  failures.Class  `json:"class"`
					Detail failures.Detail `json:"detail"`
				} `json:"failure"`
			}
			if err := json.Unmarshal([]byte(raw), &entries); err != nil || len(entries) != 1 || entries[0].Failure.Class != failures.ClassOutcomeUncertain || entries[0].Failure.Detail.Code != "http_tool_attempt_outcome_unconfirmed" || entries[0].Failure.Detail.Attributes["stage"] != stage {
				t.Fatalf("agent projection lost machine identity: %s: %v", raw, err)
			}
			canonical := failures.Normalize(failure, "test", "control")
			if !strings.Contains(canonical.Remediation, "Reconcile the authoritative operation state") || !errors.Is(failure, cause) || failures.ValidateEnvelope(canonical) != nil {
				t.Fatalf("presentation changed canonical evidence/cause: %+v %v", canonical, failure)
			}
		})
	}
}
