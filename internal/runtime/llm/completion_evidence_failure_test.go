package llm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestCompletion2270TerminalSelection(t *testing.T) {
	earlier := `{"type":"result","model":"earlier","usage":{"input_tokens":1,"output_tokens":2},"total_cost_usd":0.1}`
	for _, tc := range []struct {
		name, last, model string
		invalid           bool
	}{
		{"duplicate-type", `{"type":"result","type":"result","model":"last","usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.3}`, "", true},
		{"contradictory-type", `{"type":"result","type":"system","model":"last","usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.3}`, "", true},
		{"truncated-result", `{"type":"result","model":"last","usage":`, "", true},
		{"case-variant-metadata", `{"type":"result","model":"last","Type":false,"usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.3}`, "last", false},
		{"last-valid-result", `{"type":"result","model":"last","usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.3}`, "last", false},
		{"trailing-metadata", `{"type":"system","Type":false,"model":"not-a-result"}`, "earlier", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := claudeCompletionUsageFromRaw([]byte(earlier + "\n" + tc.last))
			if usage.ResolvedModel != tc.model || (err != nil) != tc.invalid {
				t.Fatalf("final-terminal attribution: %+v, %v", usage, err)
			}
			if tc.invalid && (len(usage.Evidence.Problems) == 0 || usage.ProviderReportedCostUSD != nil || usage.InputTokens != nil) {
				t.Fatalf("invalid final terminal certified stale facts: %+v", usage)
			}
			if !tc.invalid && usage.ProviderReportedCostUSD == nil {
				t.Fatal("valid terminal price was discarded")
			}
		})
	}
}

func TestCompletion2270ContributorUnicodeStrictness(t *testing.T) {
	for _, tc := range []struct {
		name, key, model string
		invalid          bool
	}{
		{"high-surrogate", `"\ud800"`, "", true},
		{"low-surrogate", `"\udc00"`, "", true},
		{"surrogate-pair", `"\ud83d\ude80"`, "\U0001f680", false},
		{"genuine-replacement-character", `"\ufffd"`, "\ufffd", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := claudeCompletionUsageFromRaw([]byte(`{"type":"result","modelUsage":{` + tc.key + `:{"inputTokens":10},"valid-sibling":{"inputTokens":2}},"usage":{"input_tokens":12,"output_tokens":2},"total_cost_usd":0.3}`))
			wantModels := 2
			if tc.invalid {
				wantModels = 1
			}
			if (err != nil) != tc.invalid || len(usage.Evidence.Models) != wantModels || usage.InputTokens == nil || *usage.InputTokens != 12 || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.3 {
				t.Fatalf("Unicode admission erased or fabricated independent facts: %+v, %v", usage, err)
			}
			for _, observation := range usage.Evidence.Models {
				if observation.Model != "valid-sibling" && observation.Model != tc.model {
					t.Fatalf("fabricated contributor label: %q", observation.Model)
				}
			}
		})
	}
}

func TestCompletion2270TokenProtocolKeysAreExact(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		invalid   bool
		cache     bool
	}{
		{name: "token-case-override", raw: `{"input_tokens":12,"INPUT_TOKENS":999,"output_tokens":3}`},
		{name: "token-case-type-conflict", raw: `{"input_tokens":12,"INPUT_TOKENS":false,"output_tokens":3}`},
		{name: "wrong-case-only", raw: `{"INPUT_TOKENS":12,"OUTPUT_TOKENS":3}`, invalid: true},
		{name: "cache-case-override", raw: `{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"EPHEMERAL_5M_INPUT_TOKENS":false,"ephemeral_1h_input_tokens":3,"EPHEMERAL_1H_INPUT_TOKENS":999}}`, cache: true},
		{name: "wrong-case-cache-container", raw: `{"input_tokens":12,"output_tokens":3,"CACHE_CREATION":{"ephemeral_5m_input_tokens":99}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := claudeCompletionUsageFromRaw([]byte(`{"type":"result","model":"exact-terminal","usage":` + tc.raw + `,"total_cost_usd":0.3}`))
			if (err != nil) != tc.invalid || usage.ResolvedModel != "exact-terminal" || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.3 {
				t.Fatalf("token key admission lost independent evidence: %+v, %v", usage, err)
			}
			if tc.invalid {
				if usage.Exactness != runtimeeffects.CompletionUsageUnavailable || usage.InputTokens != nil || usage.OutputTokens != nil || len(usage.Evidence.Problems) != 1 {
					t.Fatalf("wrong-case-only fields fabricated exact counts: %+v", usage)
				}
				return
			}
			if usage.Exactness != runtimeeffects.CompletionUsageExact || usage.InputTokens == nil || *usage.InputTokens != 12 || usage.OutputTokens == nil || *usage.OutputTokens != 3 {
				t.Fatalf("metadata replaced exact protocol counts: %+v", usage)
			}
			if tc.cache {
				if usage.CacheCreation5mInputTokens == nil || *usage.CacheCreation5mInputTokens != 2 || usage.CacheCreation1hInputTokens == nil || *usage.CacheCreation1hInputTokens != 3 {
					t.Fatalf("metadata replaced exact cache counts: %+v", usage)
				}
			} else if usage.CacheCreation5mInputTokens != nil || usage.CacheCreation1hInputTokens != nil {
				t.Fatalf("wrong-case cache fields fabricated observations: %+v", usage)
			}
		})
	}
}

func TestCompletion2270CLIExitRetainsEvidence(t *testing.T) {
	for _, format := range []string{"json", "stream-json"} {
		t.Run(format, func(t *testing.T) {
			script := filepath.Join(t.TempDir(), "provider.sh")
			raw := `{"type":"result","model":"served-on-error","usage":{"input_tokens":10,"output_tokens":2},"total_cost_usd":0.3}`
			if err := os.WriteFile(script, []byte("#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '"+raw+"'\nexit 1\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{}
			cfg.Workspace.DockerBin, cfg.LLM.ClaudeCLI.Command, cfg.LLM.ClaudeCLI.OutputFormat = script, "claude", format
			rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "completion-2270", nil, nil, nil)
			rt.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
			harness, ctx, dispatch := beginClaudeTestCompletion(t, unmanagedLLMTestContext(), "request")
			profile, model := testClaudeProviderSelection(t)
			response, err := rt.runWithPreparedInput(ctx, nil, &workspace.Target{Container: "test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "request", MonitorTurnMeta{}, dispatch, profile, model)
			if err == nil || response != nil {
				t.Fatal("process failure was turned into success")
			}
			target := dispatch.handle.Attempt().Authority.Target
			mutable := claudeFailureUsage(err)
			if len(mutable.Evidence.Models) > 0 {
				mutable.Evidence.Models[0].Model = "mutated-after-failure"
			}
			if mutable.ProviderReportedCostUSD != nil {
				*mutable.ProviderReportedCostUSD = 99
			}
			session := &Session{ID: target.SessionID, AgentID: target.AgentID, Memory: target.Memory, MemoryIdentity: target.AgentIdentity.Normalize()}
			var lease *sessions.Lease
			_ = rt.failClaudeContinuation(harness.CompletionContext("failure"), session, resolvedMemoryExecution{}, &lease, dispatch, target.ID, profile, []byte(`{}`), time.Millisecond, err)
			settlements := harness.CompletionSettlementsForAdapter("claude_cli")
			if len(settlements) != 1 || settlements[0].Settlement.State != runtimeeffects.StateOutcomeUncertain {
				t.Fatalf("original failure/settlement was lost: %+v", settlements)
			}
			usage := settlements[0].Usage
			if usage.ResolvedModel != "served-on-error" || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.3 || usage.InputTokens == nil || *usage.InputTokens != 10 {
				t.Fatalf("owned terminal facts discarded by failure handoff: %+v", usage)
			}
			if session.ProviderSessionID != "" || settlements[0].ProviderHead != nil || settlements[0].Spend.CostUSD != 0.3 || harness.ProjectedSpendCount() != 1 {
				t.Fatalf("metadata repair adopted a successful session or repeated spend: %+v", settlements[0])
			}
		})
	}
}

type completion2270ObservationFaultStore struct {
	*effecttest.Harness
}

func (*completion2270ObservationFaultStore) MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error {
	return errors.New("unacknowledged observation")
}

func TestCompletion2270CLIFailureHandoffBoundaries(t *testing.T) {
	for _, format := range []string{"json", "stream-json"} {
		for _, cut := range []string{"cancellation", "observation-ack"} {
			t.Run(format+"/"+cut, func(t *testing.T) {
				raw := `{"type":"result","model":"served-on-error","usage":{"input_tokens":10,"output_tokens":2},"total_cost_usd":0.3}`
				harness := effecttest.New()
				ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, t.Name()))
				if cut == "observation-ack" {
					ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(&completion2270ObservationFaultStore{harness}, harness, harness, harness))
				}
				profile, model := testClaudeProviderSelection(t)
				ctx = withEffectTestRequest(t, ctx, profile, model)
				handle, err := beginManagedTestCompletion(t, ctx, "claude_cli", []byte("request"))
				if err != nil {
					t.Fatal(err)
				}
				dispatch := newCompletionDispatch(handle, "")
				dispatch.providerModel = model
				ready, script := filepath.Join(t.TempDir(), "ready"), filepath.Join(t.TempDir(), "provider.sh")
				body := "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '" + raw + "'\n"
				if cut == "cancellation" {
					body += fmt.Sprintf("printf ready >%q\nexec sleep 60\n", ready)
				}
				if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
				cfg := &config.Config{}
				cfg.Workspace.DockerBin, cfg.LLM.ClaudeCLI.Command, cfg.LLM.ClaudeCLI.OutputFormat = script, "claude", format
				rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "completion-2270", nil, nil, nil)
				rt.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
				caller, cancel := context.WithCancel(ctx)
				defer cancel()
				joined := make(chan error, 1)
				go func() {
					response, err := rt.runWithPreparedInput(caller, nil, &workspace.Target{Container: "test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "request", MonitorTurnMeta{}, dispatch, profile, model)
					if response != nil || err == nil {
						err = errors.New("failure boundary returned success")
					}
					joined <- err
				}()
				if cut == "cancellation" {
					deadline := time.Now().Add(5 * time.Second)
					for time.Now().Before(deadline) {
						if _, err := os.Stat(ready); err == nil {
							break
						}
						time.Sleep(time.Millisecond)
					}
					cancel()
				}
				select {
				case err = <-joined:
				case <-time.After(5 * time.Second):
					t.Fatal("provider cancellation/observation path did not join")
				}
				target := handle.Attempt().Authority.Target
				session := &Session{ID: target.SessionID, AgentID: target.AgentID, Memory: target.Memory, MemoryIdentity: target.AgentIdentity.Normalize()}
				var lease *sessions.Lease
				_ = rt.failClaudeContinuation(harness.CompletionContext("failure"), session, resolvedMemoryExecution{}, &lease, dispatch, target.ID, profile, []byte(`{}`), time.Millisecond, err)
				settlements := harness.CompletionSettlementsForAdapter("claude_cli")
				if len(settlements) != 1 || settlements[0].Settlement.State != runtimeeffects.StateOutcomeUncertain || settlements[0].Usage.ResolvedModel != "served-on-error" || settlements[0].Usage.ProviderReportedCostUSD == nil || *settlements[0].Usage.ProviderReportedCostUSD != 0.3 || settlements[0].ProviderHead != nil || session.ProviderSessionID != "" {
					t.Fatalf("failure boundary discarded evidence or adopted success: %+v, %v", settlements, err)
				}
			})
		}
	}
}
