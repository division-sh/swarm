package serveapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type exactOriginDiagnosticHold struct {
	logger  *runtimepkg.RuntimeLogger
	entered chan string
	release <-chan struct{}
}

type exactOriginHandoffHold struct {
	pipelineobligation.Store
	pipelineobligation.ResumeAdmissionPreflighter
	entered chan string
	release <-chan struct{}
	once    sync.Once
	probe   *lifecycleprobe.Probe
	mu      sync.Mutex
	eventID string
}

func (h *exactOriginHandoffHold) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.EventPersisted && signal.EventType == "parent.seeded" {
		h.mu.Lock()
		h.eventID = signal.EventID
		h.mu.Unlock()
	}
	h.probe.NotifyLifecycle(ctx, signal)
}

func (h *exactOriginHandoffHold) Settle(ctx context.Context, claim pipelineobligation.Claim, disposition pipelineobligation.Disposition) (pipelineobligation.SettlementOutcome, error) {
	h.mu.Lock()
	hold := h.eventID != "" && h.eventID == claim.EventID()
	h.mu.Unlock()
	if hold {
		h.once.Do(func() { h.entered <- claim.EventID() })
		select {
		case <-h.release:
		case <-ctx.Done():
			return pipelineobligation.SettlementOutcome{}, ctx.Err()
		}
	}
	return h.Store.Settle(ctx, claim, disposition)
}

func (h *exactOriginDiagnosticHold) ProjectLifecycleDiagnostic(ctx context.Context, diagnostic diaglog.LifecycleDiagnostic) error {
	return h.logger.ProjectLifecycleDiagnostic(ctx, diagnostic)
}

func (h *exactOriginDiagnosticHold) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *failures.Envelope, durationUS int) error {
	if component == "eventbus" && action == "published" && eventType == "parent.seeded" {
		h.entered <- eventID
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return h.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID,
		SessionID: sessionID, Correlation: correlation, Detail: detail,
		Failure: failures.CloneEnvelope(failure), DurationUS: durationUS,
	})
}

func newExactOriginPublicationProbe(t *testing.T) *lifecycleprobe.Probe {
	t.Helper()
	probe := lifecycleprobe.New()
	configureOwnedMockLifecycleProbe(t, probe)
	return probe
}

func waitExactOriginPublications(ctx context.Context, probe *lifecycleprobe.Probe, eventIDs ...string) error {
	if probe == nil || len(eventIDs) == 0 {
		return errors.New("exact originating publication probe and event IDs are required")
	}
	ctx, cancel := context.WithTimeout(ctx, servedEventPublishLifecycleProbeWaitTimeout)
	defer cancel()
	for _, eventID := range eventIDs {
		if eventID == "" {
			return errors.New("exact originating publication event ID is required")
		}
		var cursor lifecycleprobe.Cursor
		for _, kind := range []lifecycleprobe.Kind{lifecycleprobe.PostCommitDispatchStarted, lifecycleprobe.PostCommitDispatchCompleted} {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, next, err := probe.WaitAfter(ctx, cursor, lifecycleprobe.Signal{Kind: kind, EventID: eventID})
			if err != nil {
				return fmt.Errorf("originating publication %s: %w", eventID, err)
			}
			cursor = next
		}
	}
	return ctx.Err()
}

func TestExactOriginPublicationWaitRejectsMissingAndLateEvidence(t *testing.T) {
	probe := lifecycleprobe.New()
	probe.NotifyLifecycle(t.Context(), lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchStarted, EventID: "origin"})
	probe.NotifyLifecycle(t.Context(), lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchCompleted, EventID: "unrelated"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitExactOriginPublications(ctx, probe, "origin"); !errors.Is(err, context.Canceled) {
		t.Fatalf("missing exact completion accepted: %v", err)
	}
	probe.NotifyLifecycle(t.Context(), lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchCompleted, EventID: "origin"})
	if err := waitExactOriginPublications(ctx, probe, "origin"); !errors.Is(err, context.Canceled) {
		t.Fatalf("late completion revived an expired observation: %v", err)
	}
	if err := waitExactOriginPublications(t.Context(), probe, "origin"); err != nil {
		t.Fatal(err)
	}
	if err := waitExactOriginPublications(t.Context(), probe, ""); err == nil {
		t.Fatal("empty originating identity accepted")
	}
}

func TestExactOriginPublicationWaitHeldDiagnosticBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := lifecycleprobe.New()
			handoffRelease := make(chan struct{})
			handoffEntered := make(chan string, 1)
			handoff := &exactOriginHandoffHold{probe: probe, entered: handoffEntered, release: handoffRelease}
			configureOwnedMockLifecycleProbe(t, handoff)
			releaseHandoff := sync.OnceFunc(func() { close(handoffRelease) })
			defer releaseHandoff()
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projected := previous(owner)
				preflight, ok := projected.deps.PipelineObligations.(pipelineobligation.ResumeAdmissionPreflighter)
				if !ok {
					panic("native pipeline resume preflight owner is required")
				}
				handoff.Store, handoff.ResumeAdmissionPreflighter = projected.deps.PipelineObligations, preflight
				projected.deps.PipelineObligations = handoff
				return projected
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			rt := startWorkspaceGatewayProofRuntime(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t), "", nil, "127.0.0.1:0")
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			t.Cleanup(unblock)
			entered := make(chan string, 1)
			rt.Runtime.Bus.SetLoggerHook(&exactOriginDiagnosticHold{logger: rt.Runtime.Logger, entered: entered, release: release})
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "parent.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"work_id": "exact-origin"}, "idempotency_key": "exact-origin-seed"})
			cut, cancelCut := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancelCut()
			select {
			case id := <-handoffEntered:
				if id != seed.EventID {
					t.Fatalf("handoff hold borrowed event %s", id)
				}
			case <-cut.Done():
				t.Fatal("exact publication did not reach native handoff")
			}
			beforeHandoff := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), rt.Events, seed.RunID, seed.EventID)
			if !beforeHandoff.RecordFound || beforeHandoff.PipelineReceiptCount != 0 {
				t.Fatalf("handoff hold missed its native predecessor: %+v", beforeHandoff)
			}
			if len(beforeHandoff.DeliveryStatuses) != 1 || beforeHandoff.SettledDeliveryAttemptCount != 0 {
				t.Fatalf("handoff hold did not retain the exact pending node: %+v", beforeHandoff)
			}
			for _, status := range beforeHandoff.DeliveryStatuses {
				if status != "pending" {
					t.Fatalf("node executed before native handoff: %s", status)
				}
			}
			t.Logf("HELD HANDOFF: exact event=%s pipeline receipts=%d pending=%d", seed.EventID, beforeHandoff.PipelineReceiptCount, len(beforeHandoff.DeliveryStatuses))
			releaseHandoff()
			select {
			case id := <-entered:
				if id != seed.EventID {
					t.Fatalf("diagnostic hold borrowed event %s", id)
				}
			case <-cut.Done():
				t.Fatal("exact publication did not reach diagnostic tail")
			}
			waitWorkspaceProofPipelineHandoff(t, rt, seed.RunID)
			evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), rt.Events, seed.RunID, seed.EventID)
			if !evidence.RecordFound || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != "success" {
				t.Fatalf("held diagnostic lacks native pipeline settlement: %+v", evidence)
			}
			early, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.Events, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			probe.NotifyLifecycle(t.Context(), lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchCompleted, EventID: "unrelated"})
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := waitExactOriginPublications(ctx, probe, seed.EventID); !errors.Is(err, context.Canceled) {
				t.Fatalf("pipeline/unrelated completion released the source gate: %v", err)
			}
			// Probe history is consulted before its canceled-context arm. A success
			// here would prove that this held writer already advertised completion.
			if _, err := probe.Wait(ctx, lifecycleprobe.Signal{Kind: lifecycleprobe.PostCommitDispatchCompleted, EventID: seed.EventID}); err == nil {
				t.Fatal("held exact publication advertised completion")
			}
			unblock()
			if err := waitExactOriginPublications(t.Context(), probe, seed.EventID); err != nil {
				t.Fatal(err)
			}
			waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)
			settled, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.Events, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(early["events"], settled["events"]) {
				t.Fatal("held diagnostic did not expose the delivery-only snapshot counterexample")
			}
			t.Logf("HELD PUBLICATION: pipeline receipts=%d->%d events=%d->%d; completion, not handoff, joins the diagnostic writer", beforeHandoff.PipelineReceiptCount, evidence.PipelineReceiptCount, len(early["events"].Rows), len(settled["events"].Rows))
		})
	}
}
