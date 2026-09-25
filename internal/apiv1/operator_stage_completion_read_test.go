package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type stageCompletionReadStore interface {
	RunReadStore
	runtimerunlifecycle.CandidateStore
	RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error)
}

func TestExactStageCompletionPublicReadbackBothStores(t *testing.T) {
	artifact := sourceartifactfixture.New("schema.yaml", []byte("name: exact-stage-completion\n"))
	root := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	classifier, err := runtimecontracts.NewWorkflowStageClassifier(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := runtimerunlifecycle.NewCompiledTerminalCatalog(classifier)
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected stageCompletionReadStore
			var db *sql.DB
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, db = s, storetest.DatabaseForTest(s)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, db = storetest.AdmitPostgresRuntimeStore(t, opened), opened
			}
			fact := sourceartifactfixture.FactFor(artifact)
			ctx := testAuthorActivityContextForSource(context.Background(), fact)
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Runs: selected})})
			for _, tc := range []struct {
				stage, status string
			}{{"ready", "running"}, {"Ready", "completed"}} {
				t.Run(tc.stage, func(t *testing.T) {
					runID := uuid.NewString()
					fixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: artifact, StartedAt: time.Now().UTC()}
					if backend == "sqlite" {
						storetest.RequireSQLiteRun(t, ctx, db, fixture)
					} else {
						storetest.RequirePostgresRun(t, ctx, db, fixture)
					}
					query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES (?, ?, '', 'stage-proof', ?)`
					if backend == "postgres" {
						query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1::uuid, $2::uuid, '', 'stage-proof', $3)`
					}
					if _, err := db.ExecContext(ctx, query, runID, uuid.NewString(), tc.stage); err != nil {
						t.Fatalf("seed exact entity stage: %v", err)
					}
					if _, err := selected.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil {
						t.Fatalf("request completion candidate: %v", err)
					}
					outcome, err := storetest.ExecuteRunCompletionCandidate(ctx, selected, artifact.BundleHash(), runID, catalog)
					if err != nil {
						t.Fatalf("execute completion candidate: %v", err)
					}
					if tc.stage == "ready" && outcome.Outcome != runtimerunlifecycle.OutcomeAwaitMutation {
						t.Fatalf("nonterminal ready completion outcome = %s", outcome.Outcome)
					}
					if tc.stage == "Ready" && outcome.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
						t.Fatalf("terminal Ready completion outcome = %s", outcome.Outcome)
					}
					for _, method := range []string{"run.get", "run.diagnose"} {
						resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":{"run_id":%q}}`, method, method, runID))
						if resp.Error != nil {
							t.Fatalf("%s error = %#v", method, resp.Error)
						}
						result := asMap(t, resp.Result)
						run := asMap(t, result["run"])
						if run["status"] != tc.status {
							t.Fatalf("%s status = %#v, want %s", method, run["status"], tc.status)
						}
					}
				})
			}
		})
	}
}
