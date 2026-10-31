package runtimepersistence

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue642SettledFanOutSelectedRemovalAndReforkBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, variant := range []string{"changed", "removed"} {
				t.Run(variant, func(t *testing.T) {
					opened := backend.open(t)
					// This fixture otherwise pins its receipt clock to July. The
					// ordinary producer uses wall-clock admission for queue priority.
					if sqlite, ok := opened.store.(*SQLiteRuntimeStore); ok {
						sqlite.nowFn = time.Now
					}
					forks := opened.store.(selectedActivityProjectionStore)
					pump := opened.store.(selectedFanOutOwner)
					at := time.Now().UTC().Truncate(time.Microsecond)
					ctx, fixture := seedDeclaredForkFanOutFixture(t, backend.name, opened, 2, at)
					at = time.Now().UTC().Truncate(time.Microsecond)
					_, claim, found, err := pump.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "settled-source", BundleHash: fixture.bundleHash, Now: at.Add(time.Second), Lease: time.Minute})
					if err != nil || !found {
						t.Fatalf("claim original source: %v %t", err, found)
					}
					if _, err := pump.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 2, at.Add(2*time.Second))); err != nil {
						t.Fatal(err)
					}
					cut := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.settled-cut", "operator", "", []byte(`{}`), 0, fixture.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(fixture.runID), at.Add(3*time.Second))
					if err := insertCanonicalEventRecordFixture(ctx, opened.store, cut); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, opened.db, fixture.runID, backend.name == "postgres")
					root := canonicalrouting.CopyForkFanOutCarrier(t, false, false)
					nodesPath := filepath.Join(root, "nodes.yaml")
					nodes, err := os.ReadFile(nodesPath)
					if err != nil {
						t.Fatal(err)
					}
					if variant == "changed" {
						nodes = []byte(strings.Replace(string(nodes), "value: entry", "value: entry + '-selected'", 1))
					} else {
						nodes = []byte("fan-out-source:\n  execution_type: system_node\n  subscribes_to: [items.ready]\n  event_handlers:\n    items.ready:\n      emit:\n        event: items.child\n        fields:\n          value: \"'new-ordinary-handler'\"\n")
					}
					if err := os.WriteFile(nodesPath, nodes, 0o644); err != nil {
						t.Fatal(err)
					}
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
					if err != nil {
						t.Fatal(err)
					}
					selected := semanticview.Wrap(bundle)
					if (len(selected.FanOutPlans()) == 0) != (variant == "removed") {
						t.Fatal("fixture did not change/remove the selected fan-out declaration")
					}
					rooted, err := bundle.RootedSource()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := opened.store.(selectedSourceArtifactStore).EnsureRootedSource(ctx, rooted); err != nil {
						t.Fatal(err)
					}
					target, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
					if err != nil {
						t.Fatal(err)
					}
					if target.BundleHash() == fixture.bundleHash {
						t.Fatal("selected fan-out variant must change the captured source")
					}
					selectedCtx := correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, target), rooted.Admission())
					scope, err := authoractivity.BundleScopeForTarget(selectedCtx, target.BundleHash())
					if err != nil {
						t.Fatal(err)
					}
					selectedCtx = authoractivity.WithScope(selectedCtx, scope)
					selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: target.BundleHash()}
					storeTestWorkOwner(t)
					value, _ := storeTestWorkFixtures.Load(t)
					work := value.(*storeTestWorkFixture)
					selectedCtx = worklifetime.WithProcess(selectedCtx, work.process)
					capability := selectedMaterializationProcessForTest(t, work, opened.store)
					// Startup is performed once, before the live child and its refork.
					preparationOwner := selectedStorePreparationOwnerForTest(t, opened.store)
					if err := preparationOwner.BindSelectedProcess(selectedCtx, work.process, capability); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := preparationOwner.RetireSelectedContexts(context.Background()); err != nil {
							t.Error(err)
						}
					})
					if _, err := preparationOwner.RecoverSelectedForkContexts(selectedCtx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil {
						t.Fatal(err)
					}
					preparationRequest := runforkexecution.SelectedContractExecutionRequest{
						SourceRunID: fixture.runID, At: cut.ID(), ContractSelection: selection, ExpectedBundleHash: target.BundleHash(),
						SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: opened.store.(runforkexecution.SourceArtifactSelectedContractSourceStore)},
						AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
							ProcessCapability: capability, ExecutionPosture: executionposture.Live,
							Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
						},
					}
					prepared, err := preparationOwner.Prepare(selectedCtx, preparationRequest)
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
					request.ForkOperation = &runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "bearer:settled-fan-out-proof", IdempotencyKey: uuid.NewString(),
						TransportHash: "sha256:settled-fan-out-proof", SourceRunID: fixture.runID, ForkEventID: cut.ID(),
						TargetBundleHash: target.BundleHash(), TargetRootReference: rooted.Admission().Reference(),
						ContractSelection: selection,
					}
					child, err := forks.MaterializeRunForkForSelectedContractExecution(selectedCtx, request)
					if err != nil {
						t.Fatalf("settled source required an executable selected declaration: %v", err)
					}
					readback := request
					readback.ForkOperation = nil
					if reused, err := forks.RequireMaterializedRunForkForSelectedContractExecution(selectedCtx, child.ForkRunID, readback); err != nil || reused.ForkRunID != child.ForkRunID {
						t.Fatalf("exact settled-history readback: %+v %v", reused, err)
					}
					requireSettledFanOutHistoryForTest(t, selectedCtx, opened.store, child.ForkRunID, fixture.bundleHash)
					childCtx, selectedOwner, _ := admitSelectedForkFanOutForTest(t, selectedCtx, opened, selected, child, request)
					candidate := claim.Key
					candidate.RunID = child.ForkRunID
					if _, _, found, err := selectedOwner.ClaimFanOutIntent(childCtx, pipeline.FanOutClaimRequest{Owner: "must-not-reissue", BundleHash: target.BundleHash(), Candidate: &candidate, Now: time.Now().UTC(), Lease: time.Minute}); err != nil || found {
						t.Fatalf("settled child reissued work: %t %v", found, err)
					}
					checkpoint := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.refork-cut", "operator", "", []byte(`{}`), 0, child.ForkRunID, events.EventEnvelope{}, eventtest.RootRoutingSource(child.ForkRunID), at.Add(4*time.Second))
					if err := insertCanonicalEventRecordFixture(childCtx, opened.store, checkpoint); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, childCtx, opened.db, child.ForkRunID, backend.name == "postgres")
					preparationRequest.SourceRunID, preparationRequest.At = child.ForkRunID, checkpoint.ID()
					secondPrepared, err := preparationOwner.Prepare(childCtx, preparationRequest)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := secondPrepared.Close(); err != nil {
							t.Error(err)
						}
					})
					secondRequest, err := secondPrepared.MaterializationRequest()
					if err != nil {
						t.Fatal(err)
					}
					secondRequest.ForkOperation = &runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "bearer:settled-fan-out-proof", IdempotencyKey: uuid.NewString(),
						TransportHash: "sha256:settled-fan-out-refork-proof", SourceRunID: child.ForkRunID, ForkEventID: checkpoint.ID(),
						TargetBundleHash: target.BundleHash(), TargetRootReference: rooted.Admission().Reference(),
						ContractSelection: selection,
					}
					second, err := forks.MaterializeRunForkForSelectedContractExecution(childCtx, secondRequest)
					if err != nil {
						t.Fatalf("refork settled history with exact archived evidence: %v", err)
					}
					requireSettledFanOutHistoryForTest(t, childCtx, opened.store, second.ForkRunID, fixture.bundleHash)
				})
			}
		})
	}
}

func requireSettledFanOutHistoryForTest(t *testing.T, ctx context.Context, selected any, runID, originalHash string) {
	t.Helper()
	reader := selected.(interface {
		ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
	})
	page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: runID})
	if err != nil || len(page.Intents) != 1 {
		t.Fatalf("read retained settled intent: %+v %v", page, err)
	}
	row := page.Intents[0]
	if row.BundleHash != originalHash || row.Status != fanoutobligation.StatusClosed || row.Cursor != 2 || row.Cardinality != 2 || row.Owed != 0 || row.ClaimOwner != "" {
		t.Fatalf("settled evidence was reinterpreted or rearmed: %+v", row)
	}
}
