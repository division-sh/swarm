package conformance

import (
	"bytes"
	"os"
	"path/filepath"
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
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestIssue2655EventSelectorsKeepPreparedRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, explicit := range []bool{false, true} {
			t.Run(backend+"/"+map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
				f := selectedDeploymentResourceFixture(t, backend, "root")
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
				repo := conformanceRepoRoot(t)
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
				if err != nil || request.ForkOperation != nil || request.At != at || request.Preparation.ForkPoint.Kind != runfork.RunForkPointEvent || request.Preparation.ForkPoint.Input != at || request.Preparation.ForkPoint.EventID != eventID {
					t.Fatalf("event selector lost its admitted meaning: request=%+v err=%v", request, err)
				}
				dialect := authoractivityfixture.DialectSQLite
				if backend == "postgres" {
					dialect = authoractivityfixture.DialectPostgres
				}
				laterEvent := uuid.NewString()
				storetest.InsertExistingRunRootEventRecord(t, ctx, f.db, dialect, laterEvent, sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "later-event-proof"), []byte(`{"account_id":"later-event","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
				fork := f.selected.(runforkexecution.SelectedContractForkLifecycle)
				current, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun})
				if err != nil || current.ForkPoint.EventID != laterEvent || current.ForkPoint.Revision <= request.Preparation.ForkPoint.Revision {
					t.Fatalf("source did not acquire a newer event point: current=%+v err=%v", current.ForkPoint, err)
				}
				materialized, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
				if err != nil || materialized.ForkPoint != request.Preparation.ForkPoint || materialized.SelectedContractBinding == nil || materialized.SelectedContractBinding.ForkPoint != request.Preparation.ForkPoint {
					t.Fatalf("event materialization moved to the new event/selector: result=%+v err=%v", materialized, err)
				}
				if history, known := current.HistoricalEventIDs(request.Preparation.ForkPoint.Revision); known || len(history) != 0 {
					t.Fatal("newer plan incorrectly carries the old revision certificate")
				}
			})
		}
	}
}
