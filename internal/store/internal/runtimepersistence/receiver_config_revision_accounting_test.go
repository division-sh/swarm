package runtimepersistence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestReceiverConfigActivationRollbackLeavesNoDurableResidueBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			barrier := newForkContentionBarrier(t, backend, true)
			f := newReceiverConfigActivationFixture(t, backend)
			ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
			defer cancel()
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, f.request("business-key", "ti-rollback", "uncommitted"))
			if err != nil {
				t.Fatal(err)
			}
			before := f.revisionCounts(t)
			barrier.install(t, f.db, correlation.RunIDFromContext(ctx), "", "writer")
			done := make(chan forkContentionResult, 1)
			acknowledged := make(chan bool, 1)
			finished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				barrier.release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("rollback writer did not join")
				}
			})
			go func() {
				defer close(finished)
				committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
				acknowledged <- committed.Acknowledged
				done <- forkContentionResult{err: err}
			}()
			barrier.awaitWinner(t, ctx, f.db, done)
			barrier.release()
			result := awaitForkContentionResult(t, ctx, done)
			if result.err == nil || !strings.Contains(result.err.Error(), "h18_requested_rollback") {
				t.Fatalf("activation did not roll back at finalization: %v", result.err)
			}
			if <-acknowledged {
				t.Fatal("rolled-back activation reported an acknowledged result")
			}
			f.requireCounts(t, 0)
			if after := f.revisionCounts(t); after != before {
				t.Fatalf("rollback left revision residue: before=%v after=%v", before, after)
			}
			if agents, err := f.store.LoadAgents(ctx); err != nil || len(agents) != 0 || len(f.bus.routePaths()) != 0 {
				t.Fatalf("rollback leaked agents/topology: %#v %v", agents, err)
			}
			retried, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
			if err != nil || !retried.Created || !retried.Acknowledged {
				t.Fatalf("rollback poisoned clean retry: created=%v acknowledged=%v err=%v", retried.Created, retried.Acknowledged, err)
			}
			f.requireCounts(t, 1)
			f.requireConfig(t, plan)
		})
	}
}

func (f receiverConfigActivationFixture) revisionCounts(t *testing.T) [3]int64 {
	t.Helper()
	var counts [3]int64
	runID := correlation.RunIDFromContext(f.ctx)
	for i, query := range []string{
		`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`,
		`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`,
		`SELECT COALESCE(MAX(last_revision),0) FROM run_fork_revision_heads WHERE run_id=$1`,
	} {
		if err := f.db.QueryRowContext(f.ctx, query, runID).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func TestReceiverConfigOnlyWorkflowMutationRegistersEntityMetadataBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixture(t, backend)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, f.request("business-key", "ti-config-only", "committed"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			state := record.State
			state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
			state.ExpectedState = state.CurrentState
			owner := f.store.(pipeline.WorkflowEngineMutationOwner)
			for i, change := range []string{"workflow_version_control", "config_only_number_kind"} {
				before := f.revisionCounts(t)
				var envelope map[string]any
				if err := canonicaljson.DecodePreservingNumberLexemes(state.Config, &envelope); err != nil {
					t.Fatal(err)
				}
				if change == "workflow_version_control" {
					envelope["workflow_version"] = "revised-source-control"
				} else {
					// This probes accounting at the closed persistence boundary,
					// not permission to update immutable receiver config on reuse.
					envelope["config"].(map[string]any)["nested"].([]any)[1] = int64(7)
				}
				state.Config, err = canonicaljson.MarshalPreservingNumberKinds(envelope)
				if err != nil {
					t.Fatal(err)
				}
				state.ExpectedRevision = int64(i + 1)
				state.UpdatedAt = state.CreatedAt.Add(time.Duration(i+1) * time.Second)
				if _, err := owner.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
					t.Fatal(err)
				}
				after := f.revisionCounts(t)
				if after[2] != before[2]+1 {
					t.Fatalf("%s did not advance recorded config: before=%v after=%v", change, before, after)
				}
				var raw []byte
				if err := f.db.QueryRowContext(f.ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 AND family='entity_metadata' AND fact_key=$3`, state.Identity.RunID, after[2], state.EntityID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var fact struct {
					Config json.RawMessage `json:"flow_config"`
				}
				if err := canonicaljson.DecodePreservingNumberLexemes(raw, &fact); err != nil {
					t.Fatal(err)
				}
				var captured any
				if err := canonicaljson.DecodePreservingNumberLexemes(fact.Config, &captured); err != nil {
					t.Fatal(err)
				}
				got, err := canonicaljson.MarshalPreservingNumberKinds(captured)
				if err != nil || string(got) != string(state.Config) {
					t.Fatalf("%s config missing from exact entity revision: got=%s want=%s err=%v", change, got, state.Config, err)
				}
				var mutations int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2`, state.Identity.RunID, state.EntityID).Scan(&mutations); err != nil {
					t.Fatal(err)
				}
				// Config is carried by entity_metadata even when no authored field
				// changed; it must not rely on a new entity_mutations fact.
				var mutationFacts int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 AND family='entity_mutations'`, state.Identity.RunID, after[2]).Scan(&mutationFacts); err != nil || mutationFacts != 0 {
					t.Fatalf("config-only change relied on field history: %d %v (total mutations %d)", mutationFacts, err, mutations)
				}
			}
		})
	}
}
