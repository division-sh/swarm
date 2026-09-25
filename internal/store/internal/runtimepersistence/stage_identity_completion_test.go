package runtimepersistence

import (
	"context"
	"testing"

	runtimebudget "github.com/division-sh/swarm/internal/runtime/budgetspend"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	run "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
)

type stageIdentityExactCatalog struct{}

func (stageIdentityExactCatalog) Terminal(_, _, stage string) (bool, bool) {
	switch stage {
	case "ready":
		return false, true
	case "Ready":
		return true, true
	default:
		return false, false
	}
}

func TestSelectedCompletionCatalogPreservesCaseAndRejectsUnknown(t *testing.T) {
	c := stageIdentityCompiledCatalog(t, "child")
	for _, tc := range []struct {
		stage           string
		terminal, known bool
	}{{"ready", false, true}, {"Ready", true, true}, {"unknown", false, false}} {
		t.Run(tc.stage, func(t *testing.T) {
			terminal, known := c.Terminal("child", "child", tc.stage)
			if terminal != tc.terminal || known != tc.known {
				t.Fatalf("terminal=%v known=%v; want %v/%v", terminal, known, tc.terminal, tc.known)
			}
		})
	}
}

func TestSelectedCompletionStageReadersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			set := func(runID, stage string) {
				query := "UPDATE entity_state SET current_state=? WHERE run_id=?"
				if f.postgres {
					query = "UPDATE entity_state SET current_state=$1 WHERE run_id=$2::uuid"
				}
				if _, err := f.db.ExecContext(ctx, query, stage, runID); err != nil {
					t.Fatal(err)
				}
			}
			t.Run("raw_sql_must_preserve_Ready", func(t *testing.T) {
				id := seedCompletionBlockerRun(t, f, ctx)
				set(id, "Ready")
				dialect := entitystore.SummaryDialectSQLite
				if f.postgres {
					dialect = entitystore.SummaryDialectPostgres
				}
				summary, err := entitystore.ReadRunSummary(ctx, f.db, dialect, id, stageIdentityExactCatalog{})
				if err != nil {
					t.Fatal(err)
				}
				if summary.Terminal != 1 {
					t.Fatalf("exact SQL/catalog projection lost case: %+v", summary)
				}
			})
			for _, stage := range []string{"ready", "Ready"} {
				t.Run("candidate/"+stage, func(t *testing.T) {
					id := seedCompletionBlockerRun(t, f, ctx)
					set(id, stage)
					request := requestRunLifecycleCandidateParity(t, f, ctx, id)
					out, err := f.store.ExecuteCompletionCandidate(ctx, request.Candidate, stageIdentityCompiledCatalog(t, semanticRunFixtureFlow))
					if err != nil {
						t.Fatal(err)
					}
					snapshot, err := f.store.LoadRunLifecycleSnapshot(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if stage == "ready" && snapshot.Status != string(run.StateRunning) {
						t.Fatalf("nonterminal ready completed: outcome=%s status=%s", out.Outcome, snapshot.Status)
					}
					if stage == "Ready" && snapshot.Status != string(run.StateCompleted) {
						t.Fatalf("terminal Ready did not complete: outcome=%s status=%s", out.Outcome, snapshot.Status)
					}
				})
			}
			for _, tc := range []struct {
				name, stage, catalogFlow string
			}{
				{"undeclared_stage", "unknown", semanticRunFixtureFlow},
				{"foreign_flow", "ready", "other-flow"},
			} {
				t.Run("candidate/"+tc.name, func(t *testing.T) {
					id := seedCompletionBlockerRun(t, f, ctx)
					set(id, tc.stage)
					catalog := stageIdentityCompiledCatalog(t, tc.catalogFlow)
					dialect := entitystore.SummaryDialectSQLite
					if f.postgres {
						dialect = entitystore.SummaryDialectPostgres
					}
					summary, err := entitystore.ReadRunSummary(ctx, f.db, dialect, id, catalog)
					if err != nil || summary.Malformed != 1 || summary.Nonterminal != 0 || summary.Terminal != 0 {
						t.Fatalf("malformed entity stage was classified as known: summary=%#v err=%v", summary, err)
					}
					request := requestRunLifecycleCandidateParity(t, f, ctx, id)
					outcome, err := f.store.ExecuteCompletionCandidate(ctx, request.Candidate, catalog)
					if err != nil || outcome.Outcome != run.OutcomeAwaitMutation {
						t.Fatalf("malformed stage/flow did not block completion: outcome=%#v err=%v", outcome, err)
					}
					snapshot, err := f.store.LoadRunLifecycleSnapshot(ctx, id)
					if err != nil || snapshot.Status != string(run.StateRunning) {
						t.Fatalf("malformed candidate changed run: snapshot=%#v err=%v", snapshot, err)
					}
				})
			}
			t.Run("budget_child_nonterminal_must_not_borrow_root_terminal", func(t *testing.T) {
				id := seedCompletionBlockerRun(t, f, ctx)
				set(id, "completed")
				child := contracts.BuildWorkflowStageTopology(semanticRunFixtureFlow, "completed", []string{"completed", "Ready"}, []string{"Ready"}, nil, nil, nil)
				if len(child.TerminalStages) != 1 || child.TerminalStages[0] != "Ready" {
					t.Fatal("bad child graph")
				}
				reader := f.store.(interface {
					ListBudgetProjectionTargets(context.Context) ([]runtimebudget.ProjectionTarget, error)
				})
				targets, err := reader.ListBudgetProjectionTargets(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, target := range targets {
					if target.RunID == id {
						return
					}
				}
				t.Fatal("child nonterminal entity omitted by root terminal-name projection")
			})
		})
	}
}

func stageIdentityCompiledCatalog(t *testing.T, flowID string) run.TerminalCatalog {
	t.Helper()
	root := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	child := contracts.BuildWorkflowStageTopology(flowID, "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	owner, err := contracts.NewWorkflowStageClassifier(root, map[string]contracts.WorkflowStageTopology{flowID: child})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := run.NewCompiledTerminalCatalog(owner)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
