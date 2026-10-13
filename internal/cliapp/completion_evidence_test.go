package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestCompletion2270CLIReadbackRendered(t *testing.T) {
	for _, format := range []string{"human", "json"} {
		t.Run(format, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			evidence := completionEvidenceFixtureForCLI()
			for i := 0; i < 32; i++ {
				evidence.Models = append(evidence.Models, runtimeeffects.CompletionModelObservation{Model: fmt.Sprintf("reported-%02d-%s", i, strings.Repeat("label", 16)), Source: runtimeeffects.CompletionModelCLIUsage})
			}
			turn := validConversationTurn(1, "turn-1", "event-1", "task.completed")
			turn["completion_evidence"] = evidence
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request.Method != conversationListTurnsMethod {
					t.Errorf("unexpected method %q", request.Method)
					return
				}
				writeJSONRPCResult(t, w, request.ID, map[string]any{"conversation": validConversationSummary("sess-1"), "turns": []map[string]any{turn}})
			}))
			defer server.Close()
			args := []string{"conversation", "view", "sess-1"}
			if format == "json" {
				args = append(args, "--json")
			}
			var out, errOut bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server)); code != 0 {
				t.Fatalf("CLI readback failed: code=%d, %s", code, errOut.String())
			}
			for _, observation := range evidence.Models {
				if !strings.Contains(out.String(), observation.Model) {
					t.Fatalf("rendered readback hid contributor %q: %s", observation.Model, out.String())
				}
			}
			if !strings.Contains(out.String(), "request-A") || !strings.Contains(out.String(), "claude.result.modelUsage") {
				t.Fatal("rendered readback erased request/source qualification")
			}
		})
	}
}

func TestCompletion2270CLIForkSnapshotEvidenceRefused(t *testing.T) {
	for _, name := range []string{"request-fallback", "duplicate-evidence"} {
		t.Run(name, func(t *testing.T) {
			result := validForkChatChatResult("fork-1", "turn-1")
			source := result["snapshot"].(map[string]any)["source_turn"].(map[string]any)
			if name == "request-fallback" {
				source["resolved_model"] = "request-A"
			} else {
				source["completion_evidence"] = json.RawMessage(`{"request":{},"request":{}}`)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var decoded forkChatChatResult
			decodeErr := json.Unmarshal(raw, &decoded)
			if decodeErr == nil && validateForkChatSnapshot(decoded.Snapshot) == nil {
				t.Fatal("fork snapshot bypassed canonical evidence validation")
			}
		})
	}
}

func completionEvidenceFixtureForCLI() runtimeeffects.CompletionEvidence {
	return (runtimeeffects.CompletionEvidence{Request: runtimeeffects.CompletionRequest{
		ModelAlias: "regular", RequestedModel: "request-A", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"},
		Models: []runtimeeffects.CompletionModelObservation{}, CostProvenance: runtimeeffects.CompletionCostUnavailable}).Canonical()
}

func TestCompletion2270CLIReadback(t *testing.T) {
	e := completionEvidenceFixtureForCLI()
	e.Models = []runtimeeffects.CompletionModelObservation{{Model: "reported-B", Source: runtimeeffects.CompletionModelCLIUsage}, {Model: "reported-C", Source: runtimeeffects.CompletionModelCLIUsage}}
	if err := validateCLICompletionEvidence("test", e, ""); err != nil {
		t.Fatal(err)
	}
	if err := validateCLICompletionEvidence("test", e, "request-A"); err == nil {
		t.Fatal("accepted request fallback in observed model readback")
	}
	text := cliCompletionEvidenceSummary(e)
	for _, want := range []string{"requested request-A", "reported-B", "reported-C", "claude.result.modelUsage", "cost unavailable"} {
		if !strings.Contains(text, want) {
			t.Fatalf("human evidence omitted %q: %s", want, text)
		}
	}
}
