package serveapp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/servedparity"
)

func claimServedPausedPublication(ctx context.Context, owner pipelineobligation.Store, eventID string) (pipelineobligation.ClaimedWork, error) {
	for {
		if err := ctx.Err(); err != nil {
			return pipelineobligation.ClaimedWork{}, err
		}
		work, err := owner.ClaimEvent(ctx, eventID, pipelineobligation.PurposePublication)
		if !errors.Is(err, pipelineobligation.ErrBusy) {
			return work, err
		}
		select {
		case <-ctx.Done():
			return pipelineobligation.ClaimedWork{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type servedAgentStartDiagnosticHold struct {
	manager.AgentLifecycleDiagnosticPersistence
	armed   atomic.Bool
	entered chan diaglog.LifecycleDiagnostic
	release <-chan struct{}
}

func (h *servedAgentStartDiagnosticHold) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error) {
	items, err := h.AgentLifecycleDiagnosticPersistence.ListPendingAgentLifecycleDiagnostics(ctx, limit)
	if err == nil {
		for _, item := range items {
			if item.AgentID == "fork-source-agent" && item.Payload["phase"] == string(manager.AgentLifecycleRegistered) && h.armed.CompareAndSwap(true, false) {
				h.entered <- item
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-h.release:
				}
			}
		}
	}
	return items, err
}

func TestServedParityConversationSourceActivationBoundaryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			hold := &servedAgentStartDiagnosticHold{entered: make(chan diaglog.LifecycleDiagnostic, 1), release: release}
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projected := previous(owner)
				hold.AgentLifecycleDiagnosticPersistence = projected.deps.ManagerPersistenceRoles.LifecycleDiagnostics
				projected.deps.ManagerPersistenceRoles.LifecycleDiagnostics = hold
				return projected
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			rt := startServedConversationForkProofRuntime(t, backend)
			hold.armed.Store(true)
			initial := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "external.observed", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{}, "idempotency_key": "held-source-run",
			})
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			select {
			case diagnostic := <-hold.entered:
				if diagnostic.Identity.RunID != initial.RunID || diagnostic.AgentID != "fork-source-agent" {
					t.Fatalf("held a different registered agent: %+v", diagnostic)
				}
				t.Logf("exact source agent before start: run=%s generation=%v phase=%v", diagnostic.Identity.RunID, diagnostic.Payload["generation"], diagnostic.Payload["phase"])
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// The old gate returns even though the real source owner cannot start yet.
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, initial.RunID)
			canceled, cancelWait := context.WithCancel(ctx)
			cancelWait()
			if _, err := rt.Probe.Raw().Wait(canceled, lifecycleprobe.Signal{Kind: lifecycleprobe.EventPersisted, EventID: initial.EventID}); !errors.Is(err, context.Canceled) {
				t.Fatalf("empty delivery queues advertised completed source activation: %v", err)
			}
			unblock()
			rt.Probe.RequireEventPersisted(initial.EventID)
			ready := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "fork.source_message", "run_id": initial.RunID, "source_event_id": initial.EventID,
				"payload": map[string]any{"note": "after activation"}, "idempotency_key": "held-source-after",
			})
			waitServedConversationForkSourceAgentReady(t, rt, initial.RunID, ready.EventID)
			t.Logf("empty queues preceded native start; exact finalized source %s admitted %s", initial.EventID, ready.EventID)
		})
	}
}

type servedPublicationReleaseHold struct {
	pipelineobligation.Store
	pipelineobligation.ResumeAdmissionPreflighter
	mu      sync.Mutex
	armed   bool
	claim   pipelineobligation.Claim
	entered chan pipelineobligation.Claim
	release <-chan struct{}
}

func (h *servedPublicationReleaseHold) ClaimPublication(ctx context.Context, eventID string) (pipelineobligation.Claim, error) {
	claim, err := h.Store.ClaimPublication(ctx, eventID)
	if err == nil {
		h.mu.Lock()
		if h.armed {
			h.armed = false
			h.claim = claim
		}
		h.mu.Unlock()
	}
	return claim, err
}

func (h *servedPublicationReleaseHold) Release(ctx context.Context, claim pipelineobligation.Claim) error {
	h.mu.Lock()
	held := h.claim == claim
	h.mu.Unlock()
	if held {
		h.entered <- claim
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.release:
		}
	}
	return h.Store.Release(ctx, claim)
}

func TestServedParityPausedPublicationClaimBoundaryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			hold := &servedPublicationReleaseHold{entered: make(chan pipelineobligation.Claim, 1), release: release}
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projected := previous(owner)
				hold.Store = projected.deps.PipelineObligations
				var ok bool
				hold.ResumeAdmissionPreflighter, ok = hold.Store.(pipelineobligation.ResumeAdmissionPreflighter)
				if !ok {
					panic("native pipeline resume preflight is required")
				}
				projected.deps.PipelineObligations = hold
				return projected
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			rt := startServedControlProofRuntime(t, backend)
			runID, initialID, _ := createServedControlWaitingRun(t, rt, "held-publication")
			rt.Probe.RequirePostCommitDispatchCompleted(initialID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "held-pause"})
			hold.mu.Lock()
			hold.armed = true
			hold.mu.Unlock()
			queued := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "item.processed", "run_id": runID, "source_event_id": initialID,
				"payload": map[string]any{"item_id": "review"}, "idempotency_key": "held-queued",
			})
			ctx, cancel := context.WithTimeout(servedControlProofAuthorActivityContext(t, rt), 5*time.Second)
			defer cancel()
			select {
			case claim := <-hold.entered:
				if claim.EventID() != queued.EventID || claim.Purpose() != pipelineobligation.PurposePublication {
					t.Fatalf("held a different foreground publication: %s/%s", claim.EventID(), claim.Purpose())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, queued.EventID, "node", identitytest.RootNode(t, "item-observer").Key(), "pending", 1)
			if _, err := hold.Store.ClaimEvent(ctx, queued.EventID, pipelineobligation.PurposePublication); !errors.Is(err, pipelineobligation.ErrBusy) {
				t.Fatalf("original boundary missed its exact live foreground claim: %v", err)
			}
			canceled, cancelWait := context.WithCancel(ctx)
			cancelWait()
			if _, err := claimServedPausedPublication(canceled, hold.Store, queued.EventID); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled claim wait = %v", err)
			}
			unblock()
			work, err := claimServedPausedPublication(ctx, hold.Store, queued.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if work.Claim.EventID() != queued.EventID || work.Claim.Purpose() != pipelineobligation.PurposePublication {
				t.Fatal("paused publication wait borrowed a different obligation")
			}
			if _, err := hold.Store.Settle(ctx, work.Claim, pipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
				t.Fatal(err)
			}
			requireServedDirectivePipelineReceiptCount(t, rt, queued.EventID, 1)
			t.Logf("exact foreground claim %s: pending delivery preceded native Release; BUSY -> owned claim -> one receipt", queued.EventID)
		})
	}
}
