package conformance

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestIssue2655EventSelectorsKeepPreparedRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, explicit := range []bool{false, true} {
			t.Run(backend+"/"+map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
				root := canonicalrouting.CopySelectedDeploymentResource(t, "root", true)
				schemaPath := filepath.Join(root, "schema.yaml")
				schema, err := os.ReadFile(schemaPath)
				if err != nil || strings.Count(string(schema), "inputs: [fixture.close.requested]") != 1 {
					t.Fatalf("event-origin control requires the exact root fixture: %v", err)
				}
				schema = []byte(strings.Replace(string(schema), "inputs: [fixture.close.requested]", "inputs: [root.ready, fixture.close.requested]", 1))
				if err := os.WriteFile(schemaPath, schema, 0o600); err != nil {
					t.Fatal(err)
				}
				repo := conformanceRepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
				f.runtime.fanOutServing.Close()
				server := f.operatorServer(t)
				sourceRun := uuid.NewString()
				payloadPath := filepath.Join(t.TempDir(), "event.json")
				if err := os.WriteFile(payloadPath, []byte(`{"account_id":"event-source","document":{}}`), 0o600); err != nil {
					t.Fatal(err)
				}
				var stdout, stderr bytes.Buffer
				args := []string{"run", "start", "--connect", server.URL, "--run-id", sourceRun, "--event", selectedDeploymentEvent, "--payload", payloadPath, "--no-follow"}
				if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 {
					t.Fatalf("create the real event-origin source: code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
				}
				page := deploymentResourcePublicEventsByName(t, f.ctx, server, sourceRun, selectedDeploymentEvent, "")
				if len(page.Events) != 1 {
					t.Fatalf("initial source event count=%d want one", len(page.Events))
				}
				eventID := page.Events[0].EventID
				owner, recovered := deploymentForkOwner(t, f)
				if len(recovered) != 0 {
					t.Fatal("fresh event source unexpectedly requires selected recovery")
				}
				ctx := worklifetime.WithProcess(f.ctx, conformanceTestProcessOwner(t))
				at := ""
				if explicit {
					at = eventID
				}
				prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: sourceRun, At: at, ExpectedBundleHash: f.runtime.sourceArtifactFact.BundleHash(),
					ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
					SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)},
					AgentRuntime:      runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: f.topology.capability},
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := prepared.Close(); err != nil {
						t.Error(err)
					}
				})
				request, err := prepared.MaterializationRequest()
				point := request.Preparation.ForkPoint
				if err != nil || request.ForkOperation != nil || request.At != at || point.Validate() != nil || point.Kind != runfork.RunForkPointEvent || point.Input != at || (explicit && point.EventID != eventID) {
					t.Fatalf("event selector lost its admitted meaning: at=%q point=%+v err=%v", request.At, point, err)
				}
				fork := f.selected.(runforkexecution.SelectedContractForkLifecycle)
				fixed, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: at, ResolvedPoint: &point})
				if err != nil || fixed.ForkPoint != point {
					t.Fatalf("prepared event is not its exact committed point: fixed=%+v expected=%+v err=%v", fixed.ForkPoint, point, err)
				}
				dialect := authoractivityfixture.DialectSQLite
				if backend == "postgres" {
					dialect = authoractivityfixture.DialectPostgres
				}
				laterEvent := uuid.NewString()
				storetest.InsertExistingRunRootEventRecord(t, ctx, f.db, dialect, laterEvent, sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "later-event-proof"), []byte(`{"account_id":"later-event","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
				current, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun})
				if err != nil || current.ForkPoint.EventID != laterEvent || current.ForkPoint.Revision <= request.Preparation.ForkPoint.Revision {
					t.Fatalf("source did not acquire a newer event point: current=%+v err=%v", current.ForkPoint, err)
				}
				materialized, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
				bindingPoint := runfork.RunForkPoint{Kind: point.Kind, Revision: point.Revision, EventID: point.EventID}
				if err != nil || materialized.ForkPoint != point || materialized.SelectedContractBinding == nil || materialized.SelectedContractBinding.ForkPoint != bindingPoint {
					t.Fatalf("event materialization moved to the new event/selector: point=%+v expected=%+v binding=%+v err=%v", materialized.ForkPoint, point, materialized.SelectedContractBinding, err)
				}
				if history, known := current.HistoricalEventIDs(request.Preparation.ForkPoint.Revision); known || len(history) != 0 {
					t.Fatal("newer plan incorrectly carries the old revision certificate")
				}
			})
		}
	}
}
