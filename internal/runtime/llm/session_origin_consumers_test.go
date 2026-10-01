package llm

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

// Real adapter entrances run against HTTP/CLI/Python provider primitives. The
// effect harness does not earn durable directive-admission or restart credit.
func TestProviderSessionOriginStartAndContinuation(t *testing.T) {
	for _, provider := range []string{"claude", "anthropic", "compatible", "responses", "mock"} {
		for _, memory := range []bool{true, false} {
			for _, origin := range []string{"delivery", "directive", "missing", "malformed", "dual", "dual_malformed", "foreign_agent", "foreign_run", "stale"} {
				t.Run(provider+"/"+origin+"/memory="+map[bool]string{true: "on", false: "off"}[memory], func(t *testing.T) {
					harness := effecttest.New()
					publisher := &eventPublisherStub{markChanged: true}
					runtime, invoked := originTestRuntime(t, provider, harness, publisher)
					ctx := testManagedConversationContext(t, harness, "origin-agent", "support/one", "reader")
					actor, _ := actors.ActorFromContext(ctx)
					plan := agentmemory.Plan{}
					if memory {
						plan = testMemory()
					}
					actor.Memory = plan
					mode := effects.ExecutionModeLive
					if provider == "mock" {
						mode = effects.ExecutionModeMock
						actor.ExecutionMode = mode
						actor.Mock = originTestMock()
						ctx = effects.WithExecutionMode(ctx, mode)
					}
					ctx = actors.WithActor(ctx, actor)
					ctx = agentmemory.WithExecution(ctx, plan, actor.Identity)
					ctx = correlation.WithRunID(ctx, actor.Identity.RunID)
					directive := agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}
					switch origin {
					case "directive", "stale":
						ctx = effects.WithDirectiveCompletionOrigin(deliverylifecycle.WithoutClaim(ctx), directive)
					case "missing":
						ctx = deliverylifecycle.WithoutClaim(ctx)
					case "malformed":
						ctx = effects.WithDirectiveCompletionOrigin(deliverylifecycle.WithoutClaim(ctx), agentcontrol.DirectiveExecutionOrigin{})
					case "dual":
						ctx = effects.WithDirectiveCompletionOrigin(ctx, directive)
					case "dual_malformed":
						ctx = effects.WithDirectiveCompletionOrigin(ctx, agentcontrol.DirectiveExecutionOrigin{})
					case "foreign_agent", "foreign_run":
						agent, run := actor.ID, actor.Identity.RunID
						if origin == "foreign_agent" {
							agent = "other-agent"
						} else {
							run = uuid.NewString()
						}
						claim, err := deliverylifecycle.AdmitPersistedClaim(uuid.NewString(), run, "route", uuid.NewString(), 1, deliverylifecycle.SubscriberAgent, agent)
						if err != nil {
							t.Fatal(err)
						}
						ctx = deliverylifecycle.WithClaim(ctx, claim)
					}
					if origin == "stale" {
						harness.AuthorizeErr = errors.New("test durable authorization refused stale directive")
					}
					conv := newTestManagedConversation(t, actor.ID, actor.Identity.FlowInstance(), "reader", nil, plan, 5, runtime)
					conv.SetToolExecutor(openAIToolExecutor{})
					if err := conv.ensureSession(ctx); err != nil {
						t.Fatal(err)
					}
					startID := conv.Session.ID
					wantMarks := 0
					if origin == "delivery" && memory {
						wantMarks = 1
					}
					if len(publisher.marks) != wantMarks {
						t.Fatalf("start binding=%v want %d", publisher.marks, wantMarks)
					}
					draft := agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEventWithMode(actor.ID, mode)}
					response, err := conv.RunManaged(ctx, draft)
					if origin != "delivery" && origin != "directive" {
						if err == nil || response != nil || invoked() != 0 || len(harness.Attempts) != 0 || len(publisher.marks) != 0 {
							t.Fatalf("invalid origin dispatched/bound: response=%+v err=%v calls=%d attempts=%d marks=%v", response, err, invoked(), len(harness.Attempts), publisher.marks)
						}
						if origin == "stale" && !errors.Is(err, harness.AuthorizeErr) {
							t.Fatalf("lost durable refusal: %v", err)
						}
						if origin != "stale" {
							wantCode := "completion_origin_missing_or_ambiguous"
							if origin == "malformed" {
								wantCode = "completion_origin_invalid"
							}
							if origin == "foreign_agent" || origin == "foreign_run" {
								wantCode = "completion_origin_delivery_claim_mismatch"
							}
							failure, ok := failures.EnvelopeFromError(err)
							if !ok || failure.Detail.Code != wantCode {
								t.Fatalf("refused at wrong gate: %v, want %s", err, wantCode)
							}
						}
						return
					}
					if err != nil || response == nil || (response.Message.Content != "done" && response.Message.Content != "characterized") {
						t.Fatalf("first response=%+v err=%v", response, err)
					}
					response, err = conv.RunManaged(ctx, draft)
					if err != nil || response == nil || invoked() != 2 {
						t.Fatalf("reused response=%+v err=%v calls=%d", response, err, invoked())
					}
					if memory {
						adopted := newTestManagedConversation(t, actor.ID, actor.Identity.FlowInstance(), "reader", nil, plan, 5, runtime)
						adopted.SetToolExecutor(openAIToolExecutor{})
						// A new incoming work item, not a replay of the first root.
						adoptedCtx := effects.WithLogicalOperationIdentity(ctx, "adopted-origin:"+uuid.NewString())
						response, err = adopted.RunManaged(adoptedCtx, draft)
						if err != nil || response == nil || adopted.Session.ID != startID || invoked() != 3 {
							t.Fatalf("adopted session=%+v response=%+v err=%v calls=%d", adopted.Session, response, err, invoked())
						}
					}
					if origin == "directive" || !memory {
						if len(publisher.marks) != 0 {
							t.Fatalf("non-delivery/memory origin bound a delivery: %v", publisher.marks)
						}
					} else if len(publisher.marks) < 4 {
						t.Fatalf("missing fresh/reused/adopted delivery binding: %v", publisher.marks)
					}
					if len(harness.Attempts) != invoked() || harness.CompletionCount() != invoked() {
						t.Fatalf("attempts=%d settlements=%d calls=%d", len(harness.Attempts), harness.CompletionCount(), invoked())
					}
					for _, attempt := range harness.Attempts {
						if origin == "directive" && (attempt.Origin.Kind != effects.CompletionOriginDirective || !attempt.Origin.Directive.Same(directive)) {
							t.Fatalf("changed directive origin %+v", attempt.Origin)
						}
						if origin == "delivery" && attempt.Origin.Kind != effects.CompletionOriginDelivery {
							t.Fatalf("changed delivery origin %+v", attempt.Origin)
						}
						if harness.States[attempt.AttemptID] != effects.StateSettled {
							t.Fatalf("unsettled attempt %+v", attempt)
						}
					}
				})
			}
		}
	}
}

func originTestMock() mockperformance.Performance {
	source := []byte("def handle(input):\n    return {\"text\": \"done\"}\n")
	return mockperformance.Performance{Kind: "python", SourcePath: "mocks/origin.py", Source: source, Digest: pythonSourceDigest(source)}
}

func originTestRuntime(t *testing.T, provider string, harness *effecttest.Harness, publisher *eventPublisherStub) (Runtime, func() int) {
	t.Helper()
	registry := sessions.NewInMemoryRegistry(time.Minute)
	controller := liveTestCompletionController(harness, harness, harness, harness)
	if provider == "claude" {
		root := t.TempDir()
		t.Setenv("CLAUDE_CHARACTERIZATION_ROOT", root)
		t.Setenv("CLAUDE_CHARACTERIZATION_MODE", "success")
		script := filepath.Join(root, "docker")
		if err := os.WriteFile(script, []byte("#!/bin/sh\nCLAUDE_CHARACTERIZATION_HELPER=1 exec "+shellQuote(os.Args[0])+" -test.run=^TestClaudeContinuationSubprocess$ -- \"$@\"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		cfg := &config.Config{}
		cfg.Workspace.DockerBin = script
		cfg.LLM.ClaudeCLI.Command = "claude"
		cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
		runtime := NewClaudeCLIRuntimeWithOptions(cfg, claudeSettledTestRegistry{Registry: registry, effects: harness}, "origin-test", workspaceResolverStub{target: &workspace.Target{Container: "origin-test", Workdir: "/workspace"}}, nil, publisher, ClaudeCLIRuntimeOptions{CompletionController: controller, ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "test-token")})
		return runtime, func() int { files, _ := filepath.Glob(filepath.Join(root, "*.args")); return len(files) }
	}
	if provider == "mock" {
		return NewMockRuntime(&config.Config{}, registry, "origin-test", nil, publisher, controller), func() int {
			count := 0
			for _, state := range harness.States {
				if state == effects.StateSettled || state == effects.StateResponseObserved {
					count++
				}
			}
			return count
		}
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch provider {
		case "anthropic":
			_, _ = w.Write([]byte(`{"model":"test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`))
		case "compatible":
			_, _ = w.Write([]byte(`{"model":"test-model","choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
		case "responses":
			_, _ = w.Write([]byte(`{"id":"resp_1","model":"test-model","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
		}
	}))
	t.Cleanup(server.Close)
	if provider == "anthropic" {
		runtime := NewAnthropicAPIRuntime(&config.Config{}, registry, "origin-test", nil, publisher)
		runtime.apiURL, runtime.apiKey, runtime.httpClient, runtime.completionController = server.URL, "test-key", server.Client(), controller
		return runtime, func() int { return int(calls.Load()) }
	}
	cfg := openAICompatibleTestConfig(server.URL)
	key := "OPENAI_COMPATIBLE_API_KEY"
	if provider == "responses" {
		cfg, key = openAIResponsesTestConfig(server.URL), "OPENAI_API_KEY"
	}
	runtime, err := (RuntimeFactory{Cfg: cfg, Sessions: registry, LiveSessions: newTransientLiveSessionAcquirer(registry), LockOwner: "origin-test", Events: publisher, Credentials: testProviderCredentialResolver(t, key, "test-key").Store, CompletionController: controller}).Build()
	if err != nil {
		t.Fatal(err)
	}
	return runtime, func() int { return int(calls.Load()) }
}
