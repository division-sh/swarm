package apiv1

import (
	"context"
	"fmt"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type stageCompletionReadStore interface {
	storetest.RunFixtureStore
	RunReadStore
}

func TestExactStageCompletionPublicReadbackBothStores(t *testing.T) {
	root := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	classifier, err := runtimecontracts.NewWorkflowStageClassifier(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := runtimerunlifecycle.NewCompiledFinalCatalog(classifier)
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected stageCompletionReadStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Runs: selected})})
			for _, tc := range []struct {
				stage, status string
			}{{"ready", "running"}, {"Ready", "completed"}} {
				t.Run(tc.stage, func(t *testing.T) {
					stages := "  ready: {}\n  Ready: {final: true}\n"
					if tc.stage == "Ready" {
						stages = "  Ready: {final: true}\n  ready: {}\n"
					}
					artifact := sourceartifactfixture.New("schema.yaml", []byte("name: exact-stage-completion\nstages:\n"+stages))
					fact := sourceartifactfixture.FactFor(artifact)
					ctx := testAuthorActivityContextForSource(context.Background(), fact)
					repo := runCompletionRepoRoot(t)
					bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
					if err != nil {
						t.Fatal(err)
					}
					source := semanticview.Wrap(bundle)
					runID := uuid.NewString()
					fixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: artifact, StartedAt: time.Now().UTC()}
					storetest.RequireRun(t, ctx, selected, fixture)
					bus, err := newScopedAPITestEventBus(t, selected.(runtimebus.EventStore), runStartTestEventBusOptions(source))
					if err != nil {
						t.Fatal(err)
					}
					constructor, err := newAPITestFlowConstructor(t, selected.(runtimebus.EventStore), bus, runStartTestEventBusOptions(source))
					if err != nil {
						t.Fatal(err)
					}
					ctx = runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(ctx, runID), executionmode.Live)
					plan, err := constructor.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
						ContractBundle: source, Instance: runtimeflowidentity.Stored(source, ".", runID, runID, "", ""), OccurredAt: fixture.StartedAt,
					})
					if err != nil {
						t.Fatal(err)
					}
					committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
					if err != nil || !committed.Acknowledged || !committed.Created {
						t.Fatalf("construct exact fieldless stage: %+v %v", committed, err)
					}
					physical, err := storetest.ReadStageCompletionFieldlessStorage(ctx, selected, runID, tc.stage)
					headers, fields := physical.Headers, physical.Fields
					if err != nil || headers != 1 || fields != 0 {
						t.Fatalf("fieldless constructor headers=%d fields=%d err=%v", headers, fields, err)
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
