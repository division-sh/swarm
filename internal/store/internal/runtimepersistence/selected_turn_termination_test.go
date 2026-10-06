package runtimepersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestSelectedTurnTerminationPreservesExactOriginBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(fixture.request.DeclarationPlan.BundleHash)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-terminate", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			plan := admitSelectedProviderFixture(t, ctx, fixture, issued, authority)
			authority.Target = selectedProviderTarget(fixture)
			ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
			ctx = selectedProviderClaimContext(t, ctx, fixture, authority)
			ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "selected-terminate")

			cause := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, fixture.forkRun, events.EventEnvelope{}, time.Now().UTC())
			node, err := identity.AdmitExecutableNodeDeclaration("global", "router")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID: "global", FlowInstance: plan.Identity.Route().InstancePath, EntityID: plan.Instance.EntityID,
			})}
			cause = eventtest.TargetRouted(cause, route.Target.Route())
			actual := selected.(deliverylifecycle.Store)
			origin, err := actual.Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			commitSelectedReceiverClaimEvent(t, ctx, selected.(agentFixtureFlowStore), cause, route, origin.Authority)
			claimed, err := actual.ClaimDelivery(ctx, origin.Authority, cause, route)
			work, acquired := claimed.Acquired()
			if err != nil || !acquired {
				t.Fatalf("claim selected transition: %+v %v", claimed, err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			command := turnTerminationCommandForClaim(t, record.State, cause, work.Claim, cause.CreatedAt())
			entry, enters, err := command.Lifecycle.TurnTermination.Cause().StageEntry(command.State.Identity)
			if err != nil || !enters {
				t.Fatalf("selected authored stage entry: %t %v", enters, err)
			}
			var bookkeeping map[string]any
			if err := json.Unmarshal(command.State.Bookkeeping, &bookkeeping); err != nil {
				t.Fatal(err)
			}
			if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
				t.Fatal(err)
			}
			command.State.Bookkeeping, err = json.Marshal(bookkeeping)
			if err != nil {
				t.Fatal(err)
			}
			command.Lifecycle.StageEntry = &entry
			committed, err := selected.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
			if err != nil || !committed.Committed || len(committed.Lifecycle.TurnCancellations) != 1 ||
				!committed.Lifecycle.TurnCancellations[0].Origin.Same(handle.Attempt().Origin) {
				t.Fatalf("selected guarded termination lost its actual origin: %+v %v", committed, err)
			}
			before, err := actual.Snapshot(ctx, origin.DeliveryID)
			if err != nil || before.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("intent settled unjoined provider work: %+v %v", before, err)
			}
			settleSelectedCompletionForTest(t, ctx, handle, authority.Target, time.Now().UTC())
			canceled, err := selected.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil))
			if err != nil || canceled.Validate() != nil || canceled.Delivery.Status != deliverylifecycle.StatusCanceled || canceled.Delivery.ReasonCode != "terminate" ||
				!canceled.Delivery.MatchesSettlementClaim(handle.Attempt().Origin.Delivery) || canceled.Publication != nil {
				t.Fatalf("selected joined origin did not settle canceled: %+v %v", canceled, err)
			}
			repeated, err := selected.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil))
			if err != nil || repeated.Validate() != nil || !repeated.Delivery.SettledAt.Equal(canceled.Delivery.SettledAt) {
				t.Fatalf("repeat selected cancellation changed evidence: %+v %v", repeated, err)
			}
		})
	}
}
