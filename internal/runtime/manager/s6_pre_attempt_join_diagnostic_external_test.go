package manager_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// This is a native row/claim counterexample, not a served reconfiguration proof.
func TestS6PreAttemptCapturedBucketJoinDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			selected := fixture.Store.(storetest.AgentFixtureStore)
			ctx := fixture.Context
			record := manager.ReconfigureMemoryNativeFixture(t, false)
			storetest.RequireStaticAgentFixture(t, ctx, selected, record)
			before, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found || before.Phase != manager.AgentLifecycleRunning {
				t.Fatalf("running predecessor: row=%+v found=%t err=%v", before, found, err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "s6.pre_attempt.requested", "source", "", nil, 0, record.Config.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(record.Config.ID), AgentIdentity: record.Config.Identity}
			fixture.Publish(t, ctx, event, []events.DeliveryRoute{route}, fixture.Authority)
			result, err := fixture.ClaimDelivery(ctx, fixture.Authority, event, route)
			claimed, ok := result.Acquired()
			if err != nil || !ok || !result.Acknowledged {
				t.Fatalf("native pre-attempt claim: %+v err=%v", result, err)
			}
			claimBefore, err := fixture.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || claimBefore.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("pre-attempt claim readback: %+v err=%v", claimBefore, err)
			}
			attempts, err := storetest.ReadExternalAttemptStorage(ctx, selected)
			if err != nil || len(attempts) != 0 {
				t.Fatalf("cut has external attempts: count=%d err=%v", len(attempts), err)
			}
			candidate := record
			candidate.Topology = agenttopology.Admission{}
			candidate.Config.Model = llmselection.ModelAliasCheap
			profile, err := llmselection.ResolveActiveBackend(record.Config.ResolvedLLMBackend)
			if err != nil {
				t.Fatal(err)
			}
			model, err := llmselection.ResolveModel(profile, llmselection.ModelResolution{Model: candidate.Config.Model})
			if err != nil {
				t.Fatal(err)
			}
			candidate.Config.ResolvedModel = model.ConcreteModel
			if err := storetest.UpsertStaticAgentFixture(t, ctx, selected, candidate); err != nil {
				t.Fatalf("full source-owned native replacement: %v", err)
			}
			after, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found || after.Generation != before.Generation+1 || after.ConfigRevision == before.ConfigRevision {
				t.Fatalf("replacement did not commit: before=%+v after=%+v found=%t err=%v", before, after, found, err)
			}
			rows, err := selected.LoadAgents(ctx)
			if err != nil || len(rows) != 1 || rows[0].Config.Model != candidate.Config.Model || rows[0].Config.ResolvedModel != candidate.Config.ResolvedModel {
				t.Fatalf("current agent catalog: %+v err=%v", rows, err)
			}
			claimAfter, err := fixture.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || !reflect.DeepEqual(claimBefore, claimAfter) {
				t.Fatalf("replacement changed the old claim: before=%+v after=%+v err=%v", claimBefore, claimAfter, err)
			}
			attempts, err = storetest.ReadExternalAttemptStorage(ctx, selected)
			if err != nil || len(attempts) != 0 {
				t.Fatalf("replacement fabricated captured provider evidence: count=%d err=%v", len(attempts), err)
			}
			if _, err := fixture.SettleSuccess(ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
				t.Fatal(err)
			}
			t.Logf("full typed in-progress claim snapshot unchanged with no external attempt; current agent model changed %s/%s -> %s/%s, generation %d -> %d; claim has no agent ConfigRevision join", record.Config.Model, record.Config.ResolvedModel, candidate.Config.Model, candidate.Config.ResolvedModel, before.Generation, after.Generation)
		})
	}
}
