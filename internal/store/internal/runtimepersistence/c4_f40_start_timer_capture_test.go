package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// Unlike the private materializer's admitted component fixture, this timer is
// captured from a real deployment creation commit by the public fixed planner.
// It proves source capture, not selected start-cut child execution.
func TestC4F40RealCreationStartTimerCaptureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := newP16RaceStore(t, backend)
			ctx, command := c4DeploymentStartCommand(t, selected, true)
			if len(command.Root.Plan.Lifecycle.Timers) != 1 {
				t.Fatalf("real root plan omitted its initial timer: %+v", command.Root.Plan.Lifecycle)
			}
			created, err := selected.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, command)
			if err != nil || !created.Acknowledged || created.Replay {
				t.Fatalf("real deployment creation: %+v err=%v", created, err)
			}
			runID := command.RunCreation.RunID
			plan := c4RequireExclusiveStart(t, ctx, selected, runID, command.RunCreation.BundleHash, "")
			if len(plan.WorkflowTimers) != 1 {
				t.Fatalf("original start omitted initial timer: %+v", plan.WorkflowTimers)
			}
			initial := command.Root.Plan.Lifecycle.Timers[0].Activation.Canonical()
			got, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(plan.WorkflowTimers[0])
			if err != nil || !reflect.DeepEqual(got.Canonical(), initial) {
				t.Fatalf("start timer changed original arm/due/schema codec: got=%+v want=%+v err=%v", got, initial, err)
			}
			_, projection, err := c4LoadStartProjection(t, ctx, selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			members := 0
			for _, fact := range projection.Facts {
				if fact.Family == runforkrevision.FamilyTimers {
					if fact.Key != initial.Ref.ActivationID {
						t.Fatalf("foreign timer in original creation: %+v", fact)
					}
					members++
				}
			}
			if members != 1 {
				t.Fatalf("exact initial timer membership=%d", members)
			}
			fingerprint := func(plan runfork.RunForkPlan) string {
				t.Helper()
				hash, err := runfork.SelectedPreparationPlanFingerprint(plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "c4-original-timer")
				if err != nil {
					t.Fatal(err)
				}
				return hash
			}
			before := fingerprint(plan)
			missing := plan
			missing.WorkflowTimers = nil
			if fingerprint(missing) == before {
				t.Fatal("preparation hash omitted non-JSON initial timer evidence")
			}
			later := eventtest.ExistingRunRootIngress(uuid.NewString(), "c4.later", "ingress", "", json.RawMessage(`{}`), 0,
				runID, events.EventEnvelope{}, initial.CreatedAt.Add(time.Second))
			binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
				BundleHash: command.RunCreation.BundleHash, FlowID: ".", EventKey: string(later.Type()),
				SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000", SchemaClass: events.PayloadSchemaSchemaLess,
			})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := events.NewPayloadAdmission(later.Payload(), binding)
			if err != nil {
				t.Fatal(err)
			}
			later, err = events.ApplyPayloadAdmission(later, payload)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPublish(later, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, selected, admitted, nil, pipelineobligation.ScopeDirect); err != nil {
				t.Fatal(err)
			}
			fixed, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true, ResolvedPoint: &plan.ForkPoint})
			if err != nil || !reflect.DeepEqual(fixed.WorkflowTimers, plan.WorkflowTimers) || fingerprint(fixed) != before {
				t.Fatalf("later ingress changed fixed initial timer evidence: err=%v", err)
			}
			wrong := plan.ForkPoint
			wrong.Revision++
			if _, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true, ResolvedPoint: &wrong}); err == nil {
				t.Fatal("start timer accepted a non-creation revision")
			}
			inclusive, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: later.ID()})
			if err != nil || inclusive.ForkPoint.Kind != runfork.RunForkPointEvent || inclusive.EventCountAtFork != 1 || len(inclusive.WorkflowTimers) != 1 {
				t.Fatalf("event-cut timer control lost: %+v err=%v", inclusive, err)
			}
			if fingerprint(inclusive) == before {
				t.Fatal("preparation hash confused event and original start cuts")
			}
		})
	}
}
