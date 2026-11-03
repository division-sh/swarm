package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

const memoryConsumedProcessEnv = "SWARM_TEST_MEMORY_CONSUMED_PROCESS"

type memoryConsumedProcessRequest struct {
	Root, Source, ConfigPath, Backend, CutURL string
}

type memoryConsumedStore struct {
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionContinuationStore
	cutURL string
}

func (s *memoryConsumedStore) ConsumeCompletionResponse(ctx context.Context, attempt runtimeeffects.Attempt, successor, terminal *agentframe.ToolContinuation) error {
	if err := s.CompletionContinuationStore.ConsumeCompletionResponse(ctx, attempt, successor, terminal); err != nil {
		return err
	}
	if terminal == nil {
		return nil
	}
	// The real native transaction has committed. The parent kills this child
	// while this request is held, before RunManaged can reach its end step.
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cutURL, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if response != nil {
		response.Body.Close()
	}
	return err
}

type memoryConsumedPublisher struct {
	runtimellm.EventPublisher
	runtimeeffects.CompletionSpendProjector
}
type memoryConsumedRuntime struct {
	*runtimellm.OpenAICompatibleRuntime
}

func runMemoryConsumedProcess(t *testing.T, raw string) {
	t.Helper()
	var request memoryConsumedProcessRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(request.Root); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath, opts.StoreMode = request.Source, request.ConfigPath, request.Backend
	opts.StoreModeSet = true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.Output, opts.ErrorOutput, opts.NoColor = os.Stdout, os.Stderr, true
	opts.ShutdownGrace = 5 * time.Second
	if request.CutURL != "" {
		model := &memoryConsumedRuntime{}
		publisher := &memoryConsumedPublisher{}
		opts.TestLLMRuntime = model
		opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
			publisher.EventPublisher, publisher.CompletionSpendProjector = rt.Bus, rt.Budget
		}
		original := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			stores, err := original(ctx, selection, cfg)
			if err != nil {
				return nil, err
			}
			deps := stores.RuntimeDeps()
			continuations, ok := deps.CompletionStore.(runtimeeffects.CompletionContinuationStore)
			if !ok {
				return nil, fmt.Errorf("native completion owner has no continuation writer")
			}
			observed := &memoryConsumedStore{CompletionStore: deps.CompletionStore, CompletionContinuationStore: continuations, cutURL: request.CutURL}
			credentials, err := cliapp.BuildProviderCredentialStore()
			if err != nil {
				return nil, err
			}
			modelRuntime, err := (runtimellm.RuntimeFactory{
				Cfg: cfg, Sessions: deps.SessionRegistry, LiveSessions: deps.LiveSessionAcquirer, Conversations: deps.ConversationStore,
				Events: publisher, Credentials: credentials,
				CompletionController: runtimeeffects.NewCompletionController(deps.EffectsStore, observed, deps.CompletionHeartbeatStore, publisher).WithExecutionPosture(executionposture.Live),
			}).Build()
			if err != nil {
				return nil, err
			}
			model.OpenAICompatibleRuntime = modelRuntime.(*runtimellm.OpenAICompatibleRuntime)
			return stores, nil
		}
	}
	ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, err := runOwnedLifecycle(ctx, request.Root, request.Root, opts,
		apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "internal-lifecycle-parent"}, executionposture.Live, "")
	if err != nil || code != 0 {
		t.Fatalf("consumed-memory child exit=%d: %v", code, err)
	}
}

func TestConsumedEmissionMemoryCrashRecoveryBothStores(t *testing.T) {
	if raw := os.Getenv(memoryConsumedProcessEnv); raw != "" {
		runMemoryConsumedProcess(t, raw)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			cut := make(chan struct{}, 1)
			barrier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cut <- struct{}{}
				<-r.Context().Done()
			}))
			t.Cleanup(barrier.Close)
			provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "resume-key", func(request llmapi.Request) (llmapi.Reply, error) {
				if request.Number > 2 {
					return llmapi.Reply{}, fmt.Errorf("consumed emission was reissued")
				}
				if request.Number == 2 {
					if _, err := retainedProviderToolResult(request, "consumed-reply-1", "emit_memory_replied"); err != nil {
						return llmapi.Reply{}, err
					}
					if !strings.Contains(string(request.Body), "memory customer first") || !strings.Contains(string(request.Body), "memory customer next") {
						return llmapi.Reply{}, fmt.Errorf("recovery lost a customer input")
					}
				}
				arguments, _ := json.Marshal(map[string]string{"text": fmt.Sprintf("accepted memory reply %d", request.Number)})
				return llmapi.Reply{ID: fmt.Sprintf("memory-consumed-%d", request.Number), Model: "resume-model", InputTokens: 10, OutputTokens: 3, TotalTokens: 13,
					Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("consumed-reply-%d", request.Number), Name: "emit_memory_replied", Arguments: arguments}}}, nil
			})
			source := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
			writeWorkflowValidationFixtureFile(t, filepath.Join(source, "hold", "agents.yaml"), `load-agent:
  role: load_agent
  intent: prompts/load-agent.md
  model: resume-model
  memory: true
  subscriptions: [item.agent_hold]
  emit_events: [memory.replied]
`)
			writeWorkflowValidationFixtureFile(t, filepath.Join(source, "hold", "events.yaml"), "memory.replied:\n  text: text\n")
			root := t.TempDir()
			configPath := writeProviderResumeConfig(t, backend, "openai_compatible", root, provider.URL)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(root, "credentials.json"))
			credentials, err := runtimecredentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(t.Context(), "OPENAI_COMPATIBLE_API_KEY", "resume-key"); err != nil {
				t.Fatal(err)
			}
			request := memoryConsumedProcessRequest{Root: root, Source: source, ConfigPath: configPath, Backend: backend, CutURL: barrier.URL}
			raw, _ := json.Marshal(request)
			first := startServedCrashProcess(t, "TestConsumedEmissionMemoryCrashRecoveryBothStores", []string{memoryConsumedProcessEnv + "=" + string(raw)})
			endpoint := first.endpoint(t) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, source),
				"payload": map[string]any{"item_id": "consumed-memory"}, "idempotency_key": "consumed-memory-seed"})
			business := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.agent_hold", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"note": "memory customer first"}, "idempotency_key": "consumed-memory-first"})
			select {
			case <-cut:
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatalf("native consumed-emission cut not reached:\n%s", first.output.String())
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			request.CutURL = ""
			raw, _ = json.Marshal(request)
			second := startServedCrashProcess(t, "TestConsumedEmissionMemoryCrashRecoveryBothStores", []string{memoryConsumedProcessEnv + "=" + string(raw)})
			endpoint = second.endpoint(t) + "/v1/rpc"
			requireProviderMemoryDelivery(t, endpoint, []string{business.EventID})
			if len(provider.Requests()) != 1 {
				t.Fatalf("recovery reissued provider turn: %d", len(provider.Requests()))
			}
			requireMemoryReplyEvents(t, endpoint, seed.RunID, 1)
			var before operatorread.OperatorConversationListResult
			requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": seed.RunID}, &before)
			next := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.agent_hold", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"note": "memory customer next"}, "idempotency_key": "consumed-memory-next"})
			requireProviderMemoryDelivery(t, endpoint, []string{next.EventID})
			requireMemoryReplyEvents(t, endpoint, seed.RunID, 2)
			var after operatorread.OperatorConversationListResult
			requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": seed.RunID}, &after)
			if len(before.Conversations) != 1 || len(after.Conversations) != 1 || before.Conversations[0].SessionID != after.Conversations[0].SessionID || len(provider.Requests()) != 2 {
				t.Fatalf("recovered memory rotated/reissued: before=%+v after=%+v requests=%d", before, after, len(provider.Requests()))
			}
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requireMemoryReplyEvents(t *testing.T, endpoint, runID string, want int) {
	t.Helper()
	var page operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "memory.replied"}, "limit": 100}, &page)
	if len(page.Events) != want {
		t.Fatalf("accepted memory replies=%d, want %d", len(page.Events), want)
	}
}
