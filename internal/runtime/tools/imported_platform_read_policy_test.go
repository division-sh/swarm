package tools

import (
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestImportedPlatformReadUsesAuthoredOnceOnlyPolicy(t *testing.T) {
	for _, toolID := range []string{"telegram.identify_bot", "telegram.read_inbox_commands"} {
		for _, outcome := range []string{"transport_interruption", "complete_503"} {
			t.Run(toolID+"/"+outcome, func(t *testing.T) {
				tool := packfixture.ConnectorTool(t, "telegram", toolID).Tool
				credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := credentials.Set(t.Context(), "telegram_bot_token", "proof-bot-token"); err != nil {
					t.Fatal(err)
				}
				executor := NewExecutorWithOptions(nil, ExecutorOptions{
					Credentials: credentials,
					WorkflowSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
						Tools: map[string]runtimecontracts.ToolSchemaEntry{toolID: tool},
					}),
				})
				calls := 0
				original := errors.New("provider transport interrupted")
				executor.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls++
					if !strings.Contains(request.URL.Path, "proof-bot-token") {
						t.Fatalf("imported connector omitted its admitted credential: %s", request.URL)
					}
					if outcome == "transport_interruption" {
						return nil, original
					}
					return &http.Response{
						StatusCode: http.StatusServiceUnavailable,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(`{"ok":false,"description":"temporarily unavailable"}`)),
					}, nil
				})
				h := effecttest.New()
				ctx := WithActor(managedEffectCommittedContext(h, toolID+"/"+outcome), models.AgentConfig{
					ID: h.Token.AgentID, Identity: h.Token.Identity, ExecutionMode: "live", Tools: []string{toolID},
				})
				input := map[string]any{}
				if toolID == "telegram.read_inbox_commands" {
					input = map[string]any{"chat_id": "42", "language_code": "en"}
				}
				output, err := executor.Execute(ctx, toolID, input)
				failure, ok := runtimefailures.As(err)
				if output != nil || !ok || calls != 1 || len(h.Attempts) != 1 || len(h.Settlements) != 1 {
					t.Fatalf("authored call used platform read policy: calls=%d attempts=%v settlements=%v output=%v err=%v", calls, h.Attempts, h.Settlements, output, err)
				}
				if outcome == "transport_interruption" {
					if failure.Failure.Detail.Code != "http_tool_attempt_outcome_unconfirmed" || !errors.Is(err, original) {
						t.Fatalf("launched interruption lost its original outcome: %v", err)
					}
				} else if failure.Failure.Detail.Code != "http_tool_status_effect_outcome_unconfirmed" || failure.Failure.Detail.Attributes["status"] != http.StatusServiceUnavailable {
					t.Fatalf("complete response was confused with an interrupted launch: %v", err)
				}
				if failure.Failure.Class != runtimefailures.ClassOutcomeUncertain || failure.Failure.Retryable {
					t.Fatalf("imported platform read changed authored interruption behavior: %v", err)
				}
				for _, attempt := range h.Attempts {
					if attempt.Class != runtimeeffects.EffectWriteOrUnknown || attempt.Adapter != "authored_http_tool" {
						t.Fatalf("connector category granted platform read authority to a flow: %+v", attempt)
					}
				}
				if replay, err := executor.Execute(ctx, toolID, input); replay != nil || err == nil || calls != 1 || len(h.Attempts) != 1 || len(h.Settlements) != 1 {
					t.Fatalf("authored replay dispatched another connector read: calls=%d output=%v err=%v", calls, replay, err)
				}
			})
		}
	}
}
