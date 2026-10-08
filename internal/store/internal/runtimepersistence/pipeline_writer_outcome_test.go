package runtimepersistence

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

// The sink error is injected after real state/delivery COMMIT. It is not a
// transport-loss simulation; a stale claim is the uncommitted control.
func TestWorkflowEngineMutationRetainsCommittedResultAfterHandoffFailure(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "handoff_failure", "stale_claim"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				flowID := "engine-outcome-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture, record := constructWorkflowMutationFixture(t, backend, flowID, createdAt)
				selected, db, ctx, runID := fixture.store.(stateOnlyAcquisitionStore), fixture.db, fixture.ctx, correlation.RunIDFromContext(fixture.ctx)
				owner, entityID := fixture.store.(runtimepipeline.WorkflowEngineMutationOwner), record.EntityID
				node := mustPersistenceNode(flowID, "engine-outcome")
				route := events.DeliveryRoute{
					Recipient: events.MustNodeDeliveryRecipient(node),
					Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: flowID, FlowInstance: instancePath, EntityID: entityID,
					}),
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "engine.outcome.requested", "fixture", "", []byte(`{}`), 0, runID,
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath), createdAt)
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
				if phase == "stale_claim" {
					if _, err := selected.SettleSuccess(ctx, claimed.Claim, []string{"pre_settled"}, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				query := "SELECT bundle_hash FROM runs WHERE run_id=?"
				if backend == "postgres" {
					query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
				}
				var bundleHash string
				if err := db.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected workflow engine postcommit handoff failure")
				submits := 0
				sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
					submits++
					if candidate.RunID != runID {
						t.Errorf("foreign candidate: %+v", candidate)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 2, 1)
					if phase == "handoff_failure" {
						return injected
					}
					return nil
				}}
				registration, err := selected.(runtimerunlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				result, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
					State: record,
					DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
						Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
						RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
					},
				})
				if phase == "stale_claim" {
					if err == nil || !reflect.DeepEqual(result, runtimepipeline.CommittedWorkflowEngineMutation{}) || submits != 0 {
						t.Fatalf("uncommitted result=%+v error=%v submits=%d", result, err, submits)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "active", 1, 1)
					return
				}
				if phase == "handoff_failure" && !errors.Is(err, injected) || phase == "healthy" && err != nil {
					t.Fatalf("unexpected result error: %v", err)
				}
				if result.DeliverySuccess == nil || !result.DeliverySuccess.Same(claimed.Claim) || submits != 1 {
					t.Fatalf("lost committed delivery result=%+v error=%v submits=%d", result, err, submits)
				}
				wantStage, stageErr := runtimepipeline.CommittedWorkflowStage(record)
				if stageErr != nil || result.Stage != wantStage {
					t.Fatalf("handoff error lost acknowledged stage: %+v want=%+v err=%v", result.Stage, wantStage, stageErr)
				}
				snapshot, readErr := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
				if readErr != nil || snapshot.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("durable delivery=%+v error=%v", snapshot, readErr)
				}
				page, readErr := selected.(runtimerunlifecycle.CandidateStore).ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, runtimerunlifecycle.CandidateCursor{}, 100)
				if readErr != nil {
					t.Fatal(readErr)
				}
				found := false
				for _, candidate := range page.Candidates {
					found = found || candidate.RunID == runID
				}
				if !found {
					t.Fatal("committed completion candidate missing from recovery reader")
				}
			})
		}
	}
}
