package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	completed       chan lateChannelNoticeObservation
	once            sync.Once
	mu              sync.Mutex
	expectedCall    string
	overflow        atomic.Bool
}

type lateChannelNoticeObservation struct {
	id  string
	err error
}

func (r *lateChannelNoticeRuntime) observe(result lateChannelNoticeObservation) {
	select {
	case r.completed <- result:
	default:
		r.overflow.Store(true)
	}
}

func (r *lateChannelNoticeRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ManagedCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(call.Frame().Turn.Event.Type, "notice.requested") {
		if message.Role == "tool" {
			var batch []struct {
				Name   string `json:"name"`
				CallID string `json:"tool_call_id"`
				OK     bool   `json:"ok"`
				Result struct {
					ID     string `json:"mailbox_id"`
					Status string `json:"status"`
				} `json:"result"`
			}
			r.mu.Lock()
			expected := r.expectedCall
			r.mu.Unlock()
			if err := json.Unmarshal([]byte(message.Content), &batch); err != nil || len(batch) != 1 || expected == "" ||
				batch[0].Name != "notify_human" || batch[0].CallID != expected || !batch[0].OK ||
				batch[0].Result.ID == "" || batch[0].Result.Status != "queued" {
				r.observe(lateChannelNoticeObservation{err: fmt.Errorf("invalid successful notify_human result batch: %v", err)})
			} else {
				r.observe(lateChannelNoticeObservation{id: batch[0].Result.ID})
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
	response, err := r.channelAnchorLLMRuntime.ContinueManagedSession(ctx, session, call)
	if strings.HasSuffix(call.Frame().Turn.Event.Type, "notice.requested") && message.Role != "tool" {
		r.mu.Lock()
		if err != nil || response == nil || len(response.ToolCalls) != 1 || response.ToolCalls[0].Name != "notify_human" || response.ToolCalls[0].ID == "" || r.expectedCall != "" {
			r.observe(lateChannelNoticeObservation{err: fmt.Errorf("invalid or duplicate emitted notice call: %v", err)})
		} else {
			r.expectedCall = response.ToolCalls[0].ID
		}
		r.mu.Unlock()
	}
	return response, err
}

func TestGracefulStopSendsNoticeCreatedByAcceptedTurnAfterStoredIntakeBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			model := &lateChannelNoticeRuntime{entered: make(chan struct{}), resume: make(chan struct{}), completed: make(chan lateChannelNoticeObservation, 2)}
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
			var observation lateChannelNoticeObservation
			select {
			case observation = <-model.completed:
			case <-cut.Done():
				t.Fatal("accepted notice tool continuation was not observed after stored intake opened")
			}
			if observation.err != nil {
				t.Fatal(observation.err)
			}
			notice := observation.id
			if err := accepted.Done(); err != nil {
				t.Fatal(err)
			}
			process := h.process
			code := process.stop()
			h.process = nil
			select {
			case <-model.completed:
				t.Error("duplicate late-notice model observation")
			default:
			}
			if model.overflow.Load() {
				t.Error("late-notice observation channel overflowed")
			}
			receipt, err := reader.HistoricalReceipt(t.Context(), notice)
			if code != 0 || err != nil || receipt == "" {
				t.Fatalf("accepted late notice lacks sent receipt at stop: exit=%d receipt=%q err=%v\n%s", code, receipt, err, process.outputString())
			}
		})
	}
}
