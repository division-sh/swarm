package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type sessionCleanupRecoveryProbe struct {
	*effecttest.Harness
	attempt    runtimeeffects.Attempt
	recoveries int
}

func (p *sessionCleanupRecoveryProbe) RecoverCompletionContinuation(_ context.Context, request runtimeeffects.CompletionContinuationRequest) (runtimeeffects.Attempt, bool, error) {
	if p.attempt.AttemptID == "" {
		return runtimeeffects.Attempt{}, false, nil
	}
	if request.SessionID != p.attempt.Authority.Target.SessionID || request.Origin != p.attempt.Origin {
		return runtimeeffects.Attempt{}, false, errors.New("recovery changed exact completion identity")
	}
	p.recoveries++
	return p.attempt, true, nil
}

// The real HTTP adapter and conversation consume the existing effect-owner
// recovery contract. The effect store is a harness, not a crash/durability proof.
func TestSessionCleanupErrorRetainsResponseAndDoesNotReplayProvider(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "cleanup_error"
		if canceled {
			name = "canceled_caller_cleanup_error"
		}
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("content-type", "application/json")
				_, _ = w.Write([]byte(`{"id":"resp_1","model":"test-model","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`))
			}))
			defer server.Close()
			probe := &sessionCleanupRecoveryProbe{Harness: effecttest.New()}
			registry := &rotationOutcomeRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute)}
			runtime, err := (RuntimeFactory{
				Cfg: openAIResponsesTestConfig(server.URL), Sessions: registry,
				LiveSessions: newTransientLiveSessionAcquirer(registry), LockOwner: "worker-1",
				Credentials:          testProviderCredentialResolver(t, "OPENAI_API_KEY", "test-key").Store,
				CompletionController: liveTestCompletionController(probe, probe, probe, probe),
			}).Build()
			if err != nil {
				t.Fatal(err)
			}
			base := testManagedConversationContext(t, probe.Harness, "agent-1", "support/inst-1", "support")
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			conversation := newTestManagedConversation(t, "agent-1", "support/inst-1", "support", nil, testMemory(), 5, runtime)
			conversation.SetToolExecutor(openAIToolExecutor{})
			if err := conversation.ensureSession(ctx); err != nil {
				t.Fatal(err)
			}
			acquires, releases := registry.acquires, registry.releases
			cleanup := errors.New("independent session release failure")
			registry.releaseErr = cleanup
			if canceled {
				registry.releaseCancel = cancel
			}
			draft := agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("agent-1")}
			response, err := conversation.RunManaged(ctx, draft)
			if !errors.Is(err, cleanup) || response == nil || response.Message.Content != "done" || response.completionHandle == nil {
				t.Fatalf("lost settled response or cleanup error: response=%+v err=%v", response, err)
			}
			if requests.Load() != 1 || registry.acquires != acquires+1 || registry.releases != releases+1 {
				t.Fatalf("replayed provider/lease operation: requests=%d acquire=%d release=%d", requests.Load(), registry.acquires-acquires, registry.releases-releases)
			}
			if err := probe.RequireState("openai_responses", runtimeeffects.StateSettled); err != nil {
				t.Fatal(err)
			}
			if len(probe.CompletionSettlementsForAdapter("openai_responses")) != 1 {
				t.Fatal("completion settled more than once")
			}
			probe.attempt = response.completionHandle.Attempt()
			if _, ok := probe.attempt.CompletionContinuation(); !ok {
				t.Fatal("response lost committed continuation evidence")
			}
			registry.releaseErr, registry.releaseCancel = nil, nil
			current, err := registry.Registry.Acquire(base, conversation.Session.MemoryIdentity, "worker-2")
			if err != nil {
				t.Fatalf("exact lease cleanup failed: %v", err)
			}
			if current.SessionID != conversation.Session.ID {
				t.Fatalf("wrong lease released: %+v", current)
			}
			if err := registry.Registry.Release(base, current); err != nil {
				t.Fatal(err)
			}
			recovered, err := conversation.RunManaged(base, draft)
			if err != nil || recovered == nil || recovered.Message.Content != "done" {
				t.Fatalf("settled recovery failed: %+v err=%v", recovered, err)
			}
			if requests.Load() != 1 || probe.recoveries != 1 || registry.acquires != acquires+1 || len(probe.CompletionSettlementsForAdapter("openai_responses")) != 1 {
				t.Fatalf("cleanup error authorized replay: requests=%d recoveries=%d acquires=%d", requests.Load(), probe.recoveries, registry.acquires-acquires)
			}
		})
	}
}
