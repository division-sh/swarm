package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// The source session is an explicit fixture; all five domain histories below
// are written by the ordinary engine mutation owner, not inserted audit rows.
func TestMutationDomainsReachBothForkConsumersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newForkChatCompletionAuthorityFixture(t, backend == "sqlite")
			runID, entityID := f.source.runID, uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContext(), runID)
			captureFanOutBarrierForkRevision(t, ctx, f.db, runID, backend == "postgres")
			owner := f.store.(pipeline.WorkflowEngineMutationOwner)
			created := f.source.turn1At.Add(-90 * time.Second)
			record := stateOnlyWorkflowEngineMutationRecord(t, runID, "flow/forkchat", "flow/forkchat", entityID, "", 0, created)
			record.Transition = pipeline.WorkflowEngineStateTransitionCreateStateAndCompanion
			var wantFields, wantAtomic map[string]any
			for step := 0; step < 3; step++ {
				fields := map[string]any{"a": map[string]any{"b": "authored", "c": "neighbor"}}
				atomic := map[string]any{"a.b": false, "a": true, "removed.key": true}
				if step >= 1 {
					atomic["a.b"] = true
				}
				if step == 2 {
					delete(atomic, "removed.key")
				}
				record.CurrentState = []string{"draft", "review", "ready"}[step]
				record.UpdatedAt = time.Now().UTC()
				record.EnteredStageAt = record.UpdatedAt
				record.Fields = json.RawMessage(forkTestJSON(t, fields))
				record.Bookkeeping, record.Gates, record.Accumulator = json.RawMessage(forkTestJSON(t, atomic)), json.RawMessage(forkTestJSON(t, atomic)), json.RawMessage(forkTestJSON(t, atomic))
				if _, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
					t.Fatalf("ordinary domain writer step %d: %v", step, err)
				}
				record.ExpectedState, record.ExpectedRevision = record.CurrentState, int64(step+1)
				record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
				wantFields, wantAtomic = fields, atomic
			}
			turnID, turnAt := uuid.NewString(), time.Now().UTC()
			if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, ctx, f.store.(completionSettlementTestStore), runtimellm.AgentTurnRecord{
				AgentID: f.source.agentID, Identity: testAgentMemoryIdentity(t, runID, f.source.agentID, conversationForkSourceFlowInstance),
				Memory: agentmemory.Authored(true), SessionID: f.source.sessionID, RunID: runID,
				FlowInstance: conversationForkSourceFlowInstance, TriggerEventID: uuid.NewString(), TriggerEventType: "domain.snapshot", ParseOK: true,
			}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: turnAt}); err != nil {
				t.Fatal(err)
			}
			var err error
			f.fork, err = f.store.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
				SourceSessionID: f.source.sessionID, ForkPoint: runfork.ConversationForkPointSelector{Kind: "turn", TurnID: turnID}, CreatedBy: "actor-token", Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			cut := eventtest.ExistingRunRootIngress(uuid.NewString(), "domain.snapshot", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(ctx, f.store.(storeTestDurableEventBusStore), cut); err != nil {
				t.Fatal(err)
			}
			// A later ordinary clear must not leak backward into either fixed cut.
			record.CurrentState, record.UpdatedAt = "cleared", time.Now().UTC()
			record.EnteredStageAt = record.UpdatedAt
			record.Fields, record.Bookkeeping, record.Gates, record.Accumulator = []byte(`{}`), []byte(`{}`), []byte(`{}`), []byte(`{}`)
			if _, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
				t.Fatal(err)
			}
			foreignRun := uuid.NewString()
			foreignCtx := correlation.WithRunID(testAuthorActivityContext(), foreignRun)
			requireRunFixtureForTest(t, foreignCtx, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: foreignRun, StartedAt: created, BundleHash: f.source.bundleHash})
			foreign := record
			foreign.Identity.RunID = foreignRun
			foreign.ExpectedState, foreign.ExpectedRevision, foreign.CurrentState = "", 0, "foreign"
			foreign.Transition = pipeline.WorkflowEngineStateTransitionCreateStateAndCompanion
			if _, err := owner.CommitWorkflowEngineMutation(foreignCtx, pipeline.WorkflowEngineMutationCommand{State: foreign}); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			planner := f.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			plan, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: cut.ID()})
			if err != nil || len(plan.Entities) != 1 {
				t.Fatalf("fixed-revision reconstruction: %+v %v", plan.Entities, err)
			}
			entity := plan.Entities[0]
			assertForkDomainSnapshot(t, entity.EntityID, entity.CurrentState, entity.Fields, entity.Bookkeeping, entity.Gates, entity.Accumulator, entityID, wantFields, wantAtomic)
			if forkTestJSON(t, before) != forkTestJSON(t, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("planning mutated source history")
			}
			prepared := prepareForkChatCompletionGroup(t, f, "domain-snapshot", "inspect atomic keys")
			if len(prepared.Snapshot.EntitySnapshot) != 1 {
				t.Fatalf("conversation snapshot: %#v", prepared.Snapshot.EntitySnapshot)
			}
			chat := prepared.Snapshot.EntitySnapshot[0]
			assertForkDomainSnapshot(t, chat.EntityID, chat.CurrentState, chat.Fields, chat.Bookkeeping, chat.Gates, chat.Accumulator, entityID, wantFields, wantAtomic)
			after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			for _, table := range []string{"entity_state", "entity_mutations"} {
				if forkTestJSON(t, before[table]) != forkTestJSON(t, after[table]) {
					t.Fatalf("conversation preparation mutated %s", table)
				}
			}
		})
	}
}

func assertForkDomainSnapshot(t *testing.T, entityID, stage string, fields, bookkeeping, gates, accumulator map[string]any, wantID string, wantFields, wantAtomic map[string]any) {
	t.Helper()
	if entityID != wantID || stage != "ready" || forkTestJSON(t, fields) != forkTestJSON(t, wantFields) {
		t.Fatalf("snapshot scalar/fields: %s %s %#v", entityID, stage, fields)
	}
	for _, values := range []map[string]any{bookkeeping, gates, accumulator} {
		if forkTestJSON(t, values) != forkTestJSON(t, wantAtomic) {
			t.Fatalf("opaque domain disagrees with writer: got=%#v want=%#v", values, wantAtomic)
		}
	}
}
