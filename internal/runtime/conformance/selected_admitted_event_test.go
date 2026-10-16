package conformance

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
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
	"github.com/google/uuid"
)

func TestIssue2655EventSelectorsKeepPreparedRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, explicit := range []bool{false, true} {
			t.Run(backend+"/"+map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
				f, sourceRun, eventID := issue2655EventSource(t, backend)
				repo := conformanceRepoRoot(t)
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
					SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)},
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
				laterEvent := uuid.NewString()
				storetest.InsertRevisionedExistingRunRootEventRecord(t, ctx, f.selected, laterEvent, sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "later-event-proof"), []byte(`{"account_id":"later-event","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
				current, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun})
				if err != nil || current.ForkPoint.EventID != laterEvent || current.ForkPoint.Revision <= request.Preparation.ForkPoint.Revision {
					t.Fatalf("source did not acquire a newer event point: current=%+v err=%v", current.ForkPoint, err)
				}
				materialized, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
				bindingPoint := runfork.RunForkPoint{Kind: point.Kind, Revision: point.Revision, EventID: point.EventID}
				if err != nil || materialized.ForkPoint != point || materialized.SelectedContractBinding == nil || materialized.SelectedContractBinding.ForkPoint != bindingPoint {
					t.Fatalf("event materialization moved to the new event/selector: point=%+v expected=%+v binding=%+v err=%v", materialized.ForkPoint, point, materialized.SelectedContractBinding, err)
				}
				assertIssue2655ChildCutContents(t, ctx, f, sourceRun, materialized.ForkRunID, point, laterEvent, "")
			})
		}
	}
}

func issue2655EventSource(t *testing.T, backend string) (*deploymentResourceFixture, string, string) {
	t.Helper()
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
	issue2655WaitEventSource(t, f, sourceRun)
	page := deploymentResourcePublicEventsByName(t, f.ctx, server, sourceRun, selectedDeploymentEvent, "")
	if len(page.Events) != 1 {
		t.Fatalf("initial source event count=%d want one", len(page.Events))
	}
	// The latest cut must follow initial delivery settlement, not select the
	// runtime log emitted while that node was still executing. Explicit cuts
	// continue to use the original creating event returned below.
	storetest.InsertRevisionedExistingRunRootEventRecord(t, f.ctx, f.selected, uuid.NewString(), sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "supported-at-cut-anchor"), []byte(`{"account_id":"at-cut-anchor","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
	return f, sourceRun, page.Events[0].EventID
}

func issue2655WaitEventSource(t *testing.T, f *deploymentResourceFixture, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := f.runtime.bus.WaitForQuiescence(ctx); err != nil {
			t.Fatal(err)
		}
		summary, err := storetest.ReadServedRunDeliverySummary(ctx, f.selected, runID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Settled() {
			if summary.Total == 0 || summary.Delivered != summary.Total {
				t.Fatalf("initial event did not complete its ordinary deliveries: %+v", summary)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("initial event delivery did not settle: %+v cause=%v", summary, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestIssue2655EventOperationSelectorCorrespondenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, explicit := range []bool{false, true} {
			t.Run(backend+"/"+map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
				f, sourceRun, eventID := issue2655EventSource(t, backend)
				owner, _ := deploymentForkOwner(t, f)
				ctx := worklifetime.WithProcess(f.ctx, conformanceTestProcessOwner(t))
				at := ""
				if explicit {
					at = eventID
				}
				selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
				operation := runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "original-selector-proof", IdempotencyKey: uuid.NewString(), TransportHash: "event-selector",
					SourceRunID: sourceRun, ForkEventID: at, TargetBundleHash: f.runtime.sourceArtifactFact.BundleHash(), ContractSelection: selection,
				}
				_, hash, err := operation.Canonical()
				if err != nil {
					t.Fatal(err)
				}
				repo := conformanceRepoRoot(t)
				prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
					ForkOperation: &operation, SourceRunID: sourceRun, At: at, ExpectedBundleHash: operation.TargetBundleHash, ContractSelection: selection,
					SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)},
					AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: f.topology.capability},
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
				if err != nil {
					t.Fatal(err)
				}
				fork := f.selected.(runforkexecution.SelectedContractForkLifecycle)
				postCutEvent := uuid.NewString()
				storetest.InsertRevisionedExistingRunRootEventRecord(t, ctx, f.selected, postCutEvent, sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "selector-post-cut"), []byte(`{"account_id":"selector-post-cut","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
				advanced, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun})
				if err != nil || advanced.ForkPoint.Revision <= request.Preparation.ForkPoint.Revision || advanced.ForkPoint.EventID != postCutEvent {
					t.Fatalf("selector source did not advance canonically: point=%+v err=%v", advanced.ForkPoint, err)
				}
				bad, err := prepared.MaterializationRequest()
				if err != nil {
					t.Fatal(err)
				}
				bad.ForkOperation.ResolvedPoint = nil
				if explicit {
					bad.ForkOperation.ForkEventID = ""
				} else {
					bad.ForkOperation.ForkEventID = request.Preparation.ForkPoint.EventID
				}
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, f.selected)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, bad); err == nil {
					t.Fatal("same event admitted a different original operation selector")
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, f.selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("selector refusal changed child/operation/history storage: err=%v", err)
				}
				materialized, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
				if err != nil || materialized.ForkPoint != request.Preparation.ForkPoint {
					t.Fatalf("matched original operation selector refused: result=%+v err=%v", materialized, err)
				}
				assertIssue2655ChildCutContents(t, ctx, f, sourceRun, materialized.ForkRunID, request.Preparation.ForkPoint, postCutEvent, "")
				record, found, err := f.selected.(apiv1.RunForkOperationReader).LoadForkOperationByID(ctx, operation.OperationID)
				if err != nil || !found || record.Validate() != nil || record.SemanticHash != hash || record.Request.ForkEventID != at || record.Request.ResolvedPoint == nil || *record.Request.ResolvedPoint != request.Preparation.ForkPoint {
					t.Fatalf("matched durable operation changed selector/hash/point: record=%+v found=%v err=%v", record, found, err)
				}
			})
		}
	}
}
