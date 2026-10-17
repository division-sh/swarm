package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// This native-owner diagnostic isolates post-cutover mutation admission. It
// does not simulate PostgreSQL session loss or the serve shutdown watcher.
func TestSystemNodeMutationRejectsPredecessorAfterProcessCutoverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			flowID := "system-node-process-fence"
			f, record := constructWorkflowMutationFixture(t, backend, flowID, at)
			selected := f.store.(stateOnlyAcquisitionStore)
			node := mustPersistenceNode(flowID, "predecessor-handler")
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: flowID, FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID,
				}),
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "handler.requested", "fixture", "", []byte(`{}`), 0,
				correlation.RunIDFromContext(f.ctx),
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, record.EntityID), record.Identity.Route.InstancePath), at)
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			claimed, err := claimDeliveryFixture(f.ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim predecessor system-node work: %+v err=%v", claimed, err)
			}
			record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
			before, found, err := f.workflows.Load(f.ctx, record.Identity)
			if err != nil || !found {
				t.Fatalf("load predecessor state: found=%v err=%v", found, err)
			}
			process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			old, err := process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if err := process.Release(f.ctx); err != nil {
				t.Fatalf("release predecessor possession: %v", err)
			}
			successor, err := f.store.(startupownership.Store).AcquireProcessCapability(f.ctx, testStartupAcquireRequest("system-node-successor"))
			if err != nil {
				t.Fatalf("acquire successor: %v", err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			next, err := successor.Evidence()
			if err != nil || next.AuthorityGeneration != old.AuthorityGeneration+1 {
				t.Fatalf("prove durable process cutover: before=%+v after=%+v err=%v", old, next, err)
			}
			committed, commitErr := f.store.(runtimepipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx,
				runtimepipeline.WorkflowEngineMutationCommand{State: record, DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
					Claim: claimed.Claim, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
				}})
			after, found, readErr := f.workflows.Load(f.ctx, record.Identity)
			t.Logf("process_generation=%d->%d acknowledged=%v refusal=%v state_conserved=%v found=%v read_err=%v",
				old.AuthorityGeneration, next.AuthorityGeneration, committed.Committed, commitErr, reflect.DeepEqual(before, after), found, readErr)
			if commitErr == nil || committed.Committed || readErr != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("predecessor system-node mutation must refuse without changing state after successor cutover")
			}
		})
	}
}
