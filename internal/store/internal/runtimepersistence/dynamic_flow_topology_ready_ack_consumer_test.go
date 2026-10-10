package runtimepersistence

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type readinessPostcommitFaultWorkflow struct {
	*pipeline.PipelineCoordinator
	fault error
	marks atomic.Int32
}

func (w *readinessPostcommitFaultWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	if previous != pipeline.FlowAttachmentTimersArmed {
		return result, nil
	}
	w.marks.Add(1)
	return result, w.fault
}

func TestDynamicFlowTopologyReadyAcknowledgedFaultCompletesCreationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithOptions(t, backend, true, true)
			f.constructKeylessRoot(t)
			source := semanticview.Wrap(f.bundle)
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := authoractivity.ScopeFromContext(f.ctx)
			lease, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lease.Release)
			publisher := f.newRuntimeEventBus(t, bus.EventBusOptions{})
			req := f.request("acknowledged-ready", "ti-acknowledged", "committed")
			supplied := receiverSuppliedPayload(t, req)
			supplied["nested"] = []any{int64(7), float64(7)}
			payload, err := canonicaljson.MarshalPreservingNumberKinds(map[string]any{
				"request_id": req.ResolvedKey, "label": supplied["label"], "nested": supplied["nested"],
			})
			if err != nil {
				t.Fatal(err)
			}
			req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "request.started", "operator", "", payload, 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
			if err := publisher.Publish(f.ctx, req.TriggerEvent); err != nil {
				t.Fatal(err)
			}
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created || plan.Readiness.CreationEvent == nil {
				t.Fatalf("initial activation = %+v err=%v", committed, err)
			}
			f.requireCreationOccurrence(t, plan, 0)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected acknowledged topology readiness cleanup failure")
			workflow := &readinessPostcommitFaultWorkflow{PipelineCoordinator: f.workflows, fault: fault}
			restarted := f.restartedReceiverConfigManager(t, publisher, workflow)
			finalizeErr := restarted.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed)
			if !errors.Is(finalizeErr, fault) || workflow.marks.Load() != 1 {
				t.Fatalf("finalization error=%v marks=%d, want acknowledged fault after one mark", finalizeErr, workflow.marks.Load())
			}
			readiness, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, plan.Readiness.RunID, plan.Identity.Route())
			if err != nil || !found || (readiness.Phase != pipeline.FlowAttachmentReady) || readiness.CreationEventEmittedAt.IsZero() {
				t.Fatalf("committed readiness = %+v found=%v loadErr=%v finalizeErr=%v", readiness, found, err, finalizeErr)
			}
			restarted.requireCreationOccurrence(t, plan, 1)
			if len(plan.Readiness.Agents) != 1 {
				t.Fatalf("acknowledged readiness requires one actual agent: %+v", plan.Readiness.Agents)
			}
			for _, expected := range plan.Readiness.Agents {
				state, found, err := f.store.(manager.AgentLifecycleStateReader).LoadAgentLifecycleState(f.ctx, expected.Identity)
				owned := state.Topology.Authority.Readiness
				if err != nil || !found || state.Phase != manager.AgentLifecycleRegistered || state.RunMode != manager.AgentRunModeStopped ||
					state.ConfigRevision != expected.ConfigRevision || owned == nil || owned.RunID != plan.Readiness.RunID ||
					owned.InstancePath != plan.Identity.InstancePath || owned.PlanFingerprint != readiness.PlanHash || owned.AttemptID == "" {
					t.Fatalf("acknowledged readiness lost its exact prepared receiver: %+v readiness=%+v found=%t err=%v", state, owned, found, err)
				}
			}
			if err := restarted.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
