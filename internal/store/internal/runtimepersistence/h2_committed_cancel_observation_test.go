package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestH2CommittedCancellationHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithTimer(t, backend, false, false, true, false)
			f.constructKeylessRoot(t)
			req := f.request("h2-capture", "h2-capture-instance", "native")
			req.OccurredAt = time.Now().UTC().Truncate(time.Microsecond)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil || len(plan.Lifecycle.Timers) != 1 {
				t.Fatalf("compiled timer: %v", err)
			}
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			if result, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged || !result.Created {
				t.Fatalf("construction: %+v %v", result, err)
			}
			initial := plan.Lifecycle.Timers[0].Activation
			read := func() H2CommittedHistoryEvidence {
				t.Helper()
				got, err := ObserveH2CommittedHistoryForTest(f.ctx, f.store, initial.RunID)
				if err != nil || !got.Complete || got.Truncated || len(got.Facts) == 0 {
					t.Fatalf("committed history: %+v %v", got, err)
				}
				return got
			}
			before := read()
			binding, err := f.grant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := f.workflows.BeginDynamicFlowRuntimeActivation(f.ctx, pipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding))
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("activation: %+v %v", admitted, err)
			}
			t.Cleanup(func() {
				if err := f.workflows.RetireDynamicFlowRuntimeActivationAttempt(context.WithoutCancel(f.ctx), admitted.Attempt); err != nil {
					t.Error(err)
				}
			})
			command := pipeline.WorkflowTimerReconciliationCommand{RunID: initial.RunID, Route: initial.Route, EntityID: initial.EntityID, ActivationAttempt: &admitted.Attempt,
				Plan: pipeline.WorkflowLifecycleMutationPlan{Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationCancel, Activation: initial}}}}
			owner := f.store.(pipeline.WorkflowTimerActivationPersistence)
			// The first real cancellation executes, then the second mutation refuses;
			// neither the timer nor its committed fact may survive the rollback.
			refused := command
			absent := initial
			absent.Ref.ActivationID = uuid.NewString()
			refused.Plan.Timers = append(append([]pipeline.WorkflowTimerMutation(nil), command.Plan.Timers...), pipeline.WorkflowTimerMutation{Kind: pipeline.WorkflowTimerMutationCancel, Activation: absent})
			if err := refused.Validate(); err != nil {
				t.Fatalf("rollback control refused before native mutation: %v", err)
			}
			if result, err := owner.CommitWorkflowTimerReconciliation(f.ctx, refused); err == nil || result.Committed || !strings.Contains(err.Error(), "timer mutation 1") {
				t.Fatalf("invalid suffix accepted: %+v %v", result, err)
			}
			if got := read(); !reflect.DeepEqual(got, before) {
				t.Fatal("rolled-back cancellation exposed committed revision facts")
			}
			if result, err := owner.CommitWorkflowTimerReconciliation(f.ctx, command); err != nil || !result.Committed {
				t.Fatalf("committed cancellation: %+v %v", result, err)
			}
			after := read()
			active, cancelled := int64(0), int64(0)
			for _, fact := range after.Facts {
				if fact.Family != "timers" || fact.Key != initial.Ref.ActivationID || !fact.Present {
					continue
				}
				var timer struct {
					Status string `json:"status"`
				}
				if err := json.Unmarshal(fact.Fact, &timer); err != nil {
					t.Fatal(err)
				}
				if timer.Status == "active" {
					active = fact.Revision
				}
				if timer.Status == "cancelled" {
					cancelled = fact.Revision
				}
			}
			if active <= 0 || cancelled <= active {
				t.Fatalf("no distinct committed active/cancelled cuts: %d %d", active, cancelled)
			}
			if result, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged || result.Created {
				t.Fatalf("later construction replay: %+v %v", result, err)
			}
			if got := read(); !reflect.DeepEqual(after, got) {
				t.Fatal("later replay changed retained committed cancellation facts")
			}
			state := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			_ = read()
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != 1 || got.Total.WriteCommits != 0 {
				t.Fatal("capture bypassed native read boundary")
			}
			restore()
			if !reflect.DeepEqual(state, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("capture changed durable rows")
			}
			foreign, err := ObserveH2CommittedHistoryForTest(f.ctx, f.store, uuid.NewString())
			if err != nil || !foreign.Complete || len(foreign.Facts) != 0 {
				t.Fatal("foreign run borrowed history")
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ObserveH2CommittedHistoryForTest(ctx, f.store, initial.RunID); err == nil || got.Complete {
				t.Fatal("cancelled read claimed completeness")
			}
		})
	}
}
