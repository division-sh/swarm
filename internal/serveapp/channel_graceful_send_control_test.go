package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestGracefulStopKeepsAlreadyLaunchedChannelSendUntilAcknowledgement(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, hash := startChannelAnchorPublicJourney(t, backend, "synthetic-drain-token", false, 0, nil)
			reader := openChannelAnchorObservation(t, h)
			runID := seedServedPublicationRun(t, h.runtime)
			arrived, release := h.provider.PauseDeliveryResponseMatching(func(message map[string]any) bool {
				text := fmt.Sprint(message["text"])
				return strings.Contains(text, "Run: "+runID[:8]) && strings.Contains(text, "Flow instance: chat/reviews") && strings.Contains(text, "Decision: pending")
			})
			defer release()
			seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "work.requested", "bundle_hash": hash, "run_id": runID,
				"payload": map[string]any{"seed": true}, "idempotency_key": "held-channel-stop",
			})
			cardID := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
			var heldMessage int
			select {
			case heldMessage = <-arrived:
			case <-time.After(15 * time.Second):
				t.Fatal("accepted channel send did not reach the held provider response")
			}
			process := h.process
			process.cancel()
			release()
			code := process.stop()
			h.process = nil
			receipt, err := reader.HistoricalReceipt(context.Background(), cardID)
			if code != 0 || err != nil || receipt == "" {
				t.Fatalf("graceful stop lost the already-launched channel acknowledgement: run=%s card=%s held_message=%d payload=%+v exit=%d receipt=%q error=%v\n%s", runID, cardID, heldMessage, h.provider.Delivery(heldMessage-1), code, receipt, err, process.outputString())
			}
		})
	}
}

type lateChannelNoticeRuntime struct {
	channelAnchorLLMRuntime
	entered, resume chan struct{}
	completed       chan string
	once            sync.Once
}

func (r *lateChannelNoticeRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ManagedCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(call.Frame().Turn.Event.Type, "notice.requested") {
		if message.Role == "tool" {
			var result struct {
				ID     string `json:"mailbox_id"`
				Status string `json:"status"`
			}
			if err := json.Unmarshal([]byte(message.Content), &result); err != nil || result.ID == "" || result.Status != "queued" {
				return nil, fmt.Errorf("late notice tool result lacks its committed mailbox identity: %v", err)
			}
			select {
			case r.completed <- result.ID:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			r.once.Do(func() { close(r.entered) })
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-r.resume:
			}
		}
	}
	return r.channelAnchorLLMRuntime.ContinueManagedSession(ctx, session, call)
}

func TestGracefulStopSendsNoticeCreatedByAcceptedTurnAfterStoredIntakeBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			model := &lateChannelNoticeRuntime{entered: make(chan struct{}), resume: make(chan struct{}), completed: make(chan string, 1)}
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(model.resume) }) }
			defer resume()
			managers := make(chan *runtime.RuntimeContextManager, 1)
			h := prepareChannelAnchorPublicJourney(t, backend, "late-notice-token", true, 0, func(opts *cliapp.ServeOptions) {
				opts.TestLLMRuntime = model
				opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) { managers <- m }
			})
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			manager := <-managers
			h.registration = connectDeploymentChannelFixtureOperator(t, h, "late-notice-token", 901119, 1001, 7000, "private")
			reader := openChannelAnchorObservation(t, h)
			waitRegisteredSourceQualification(t, h, reader, h.runtime.Options.ChannelOnboardingStore.(channelnative.Store).ReadNativeInboxQualificationParent)
			hash := channelAnchorBundleHash(t, h.rpcEndpoint())
			accepted, lookup, err := manager.AcquireBundleHash(t.Context(), hash)
			if err != nil || accepted == nil || !lookup.Loaded() {
				t.Fatalf("hold accepted source: loaded=%t err=%v", lookup.Loaded(), err)
			}
			defer func() { _ = accepted.Done() }()
			targets, err := h.runtime.PlanStandingTargets()
			if err != nil || len(targets) != 1 {
				t.Fatalf("late-notice ingress target: count=%d err=%v", len(targets), err)
			}
			requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "notice.requested", "bundle_hash": hash, "run_id": seedServedPublicationRun(t, h.runtime),
				"payload": map[string]any{"seed": true}, "idempotency_key": "late-accepted-notice",
			})
			select {
			case <-model.entered:
			case <-time.After(15 * time.Second):
				t.Fatal("accepted notice turn did not reach its provider request")
			}
			h.process.cancel()
			cut, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for {
				request, err := http.NewRequestWithContext(cut, http.MethodGet, h.endpoint+"/readyz", nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode == http.StatusServiceUnavailable {
					break
				}
				if response.StatusCode != http.StatusOK {
					t.Fatalf("stop readiness status=%d", response.StatusCode)
				}
			}
			publication, target, err := manager.AcquireInboundPublication(cut, targets[0].Alias, targets[0].Provider)
			if err != nil || publication == nil || !target.Loaded() {
				t.Fatalf("actual stored-intake boundary: loaded=%t err=%v", target.Loaded(), err)
			}
			if err := publication.Done(); err != nil {
				t.Fatal(err)
			}
			resume()
			var notice string
			select {
			case notice = <-model.completed:
			case <-cut.Done():
				t.Fatal("accepted turn did not commit its notice after stored intake opened")
			}
			if err := accepted.Done(); err != nil {
				t.Fatal(err)
			}
			process := h.process
			code := process.stop()
			h.process = nil
			receipt, err := reader.HistoricalReceipt(t.Context(), notice)
			if code != 0 || err != nil || receipt == "" {
				t.Fatalf("accepted late notice lacks sent receipt at stop: exit=%d receipt=%q err=%v\n%s", code, receipt, err, process.outputString())
			}
		})
	}
}
