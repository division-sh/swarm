package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestRunStartOriginalPointPreservesCreatingInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			planner := fixture.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			ctx := testAuthorActivityContextForBundle(runLifecycleCandidateParityBundleHash)
			runID := uuid.NewString()
			at := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
			initial := eventtest.RunCreatingRootIngress(uuid.NewString(), "origin.trigger", "ingress", "", json.RawMessage(`{}`), 0,
				runID, "", events.EventEnvelope{}, at)
			binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
				BundleHash: runLifecycleCandidateParityBundleHash, FlowID: ".", EventKey: string(initial.Type()),
				SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
				SchemaClass:  events.PayloadSchemaSchemaLess,
			})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := events.NewPayloadAdmission(initial.Payload(), binding)
			if err != nil {
				t.Fatal(err)
			}
			initial, err = events.ApplyPayloadAdmission(initial, payload)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPublish(initial, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, fixture.store.(semanticEventFixtureStore), admitted, nil, pipelineobligation.ScopeDirect); err != nil {
				t.Fatal(err)
			}
			assertStart := func(point *runfork.RunForkPoint) runfork.RunForkPoint {
				t.Helper()
				plan, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true, ResolvedPoint: point})
				if err != nil {
					t.Fatal(err)
				}
				if plan.ForkPoint.Kind != runfork.RunForkPointRunStart || plan.ForkPoint.Revision != 1 || plan.ForkPoint.EventID != "" ||
					plan.EventCountAtFork != 0 || len(plan.PendingWork) != 0 || plan.StartFirstTurn == nil {
					t.Fatalf("start is not exclusive original creation: %+v", plan)
				}
				event, present := plan.StartFirstTurn.Event()
				if !present || event.ID() != initial.ID() || event.RunID() != runID || string(event.Type()) != string(initial.Type()) {
					t.Fatalf("first turn differs from immutable origin: %+v", plan.StartFirstTurn.Coordinates())
				}
				return plan.ForkPoint
			}
			point := assertStart(nil)
			later := eventtest.ExistingRunRootIngress(uuid.NewString(), "origin.later", "ingress", "", json.RawMessage(`{}`), 0,
				runID, events.EventEnvelope{}, at.Add(time.Second))
			if err := commitSemanticEventFixture(ctx, fixture.store, later); err != nil {
				t.Fatal(err)
			}
			assertStart(&point)
			inclusive, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: initial.ID()})
			if err != nil || inclusive.ForkPoint.Kind != runfork.RunForkPointEvent || inclusive.EventCountAtFork != 1 || inclusive.StartFirstTurn != nil {
				t.Fatalf("inclusive first event was confused with start: %+v err=%v", inclusive, err)
			}
		})
	}
}

func TestEventlessRunHasOriginalStartWithoutInventedIntakeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			runID := uuid.NewString()
			ctx := testAuthorActivityContextForBundle(runLifecycleCandidateParityBundleHash)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC))
			planner := fixture.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			plan, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true})
			if err != nil || plan.ForkPoint.Kind != runfork.RunForkPointRunStart || plan.ForkPoint.Revision != 1 ||
				plan.StartFirstTurn != nil || plan.EventCountAtFork != 0 || plan.PendingWorkCount != 0 {
				t.Fatalf("eventless start invented work or required an event: %+v err=%v", plan, err)
			}
			if _, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID}); err == nil {
				t.Fatal("default event selection silently changed to original start")
			}
		})
	}
}
