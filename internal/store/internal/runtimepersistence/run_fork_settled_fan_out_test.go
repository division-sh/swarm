package runtimepersistence

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
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
					forks := opened.store.(runForkSelectedLifecycleStore)
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
					if _, err := opened.store.(selectedSourceArtifactStore).EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
						t.Fatal(err)
					}
					target, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
					if err != nil {
						t.Fatal(err)
					}
					original := originalCarriageForRun(t, opened.store, fixture.runID)
					request := runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: cut.ID(), SourceArtifactFact: target, OriginalLoopCarriage: original}
					child, err := forks.MaterializeRunFork(ctx, request)
					if err != nil {
						t.Fatalf("settled source required an executable selected declaration: %v", err)
					}
					if reused, err := forks.MaterializeRunFork(ctx, request); err != nil || reused.ForkRunID != child.ForkRunID {
						t.Fatalf("exact settled-history readback: %+v %v", reused, err)
					}
					requireSettledFanOutHistoryForTest(t, ctx, opened.store, child.ForkRunID, fixture.bundleHash)
					activation, err := forks.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID, AllowSourceFreeze: true,
						OriginalLoopCarriage: original, HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{}})
					if err != nil || !activation.Activated {
						t.Fatalf("activate settled history: %+v %v", activation, err)
					}
					if _, _, found, err := pump.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "must-not-reissue", BundleHash: target.BundleHash(), Now: time.Now().UTC(), Lease: time.Minute}); err != nil || found {
						t.Fatalf("settled child reissued work: %t %v", found, err)
					}
					childCtx := correlation.WithSourceArtifactFact(ctx, target)
					scope, err := authoractivity.BundleScopeForTarget(childCtx, target.BundleHash())
					if err != nil {
						t.Fatal(err)
					}
					childCtx = authoractivity.WithScope(childCtx, scope)
					checkpoint := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.refork-cut", "operator", "", []byte(`{}`), 0, child.ForkRunID, events.EventEnvelope{}, eventtest.RootRoutingSource(child.ForkRunID), at.Add(4*time.Second))
					if err := insertCanonicalEventRecordFixture(childCtx, opened.store, checkpoint); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, childCtx, opened.db, child.ForkRunID, backend.name == "postgres")
					primary := originalCarriageForRun(t, opened.store, child.ForkRunID)
					secondRequest := runfork.RunForkMaterializeRequest{SourceRunID: child.ForkRunID, At: checkpoint.ID(), OriginalLoopCarriage: primary}
					if _, err := forks.MaterializeRunFork(childCtx, secondRequest); err == nil {
						t.Fatal("refork inferred archived meaning from the current selected artifact")
					}
					loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: opened.store.(runforkexecution.SourceArtifactSelectedContractSourceStore)}
					archive, err := loader.LoadRunForkSelectedContractSourceForRequest(childCtx, runforkexecution.SelectedContractSourceLoadRequest{
						BundleHash: fixture.bundleHash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: fixture.bundleHash}})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := archive.Cleanup(); err != nil {
							t.Error(err)
						}
					})
					secondRequest.OriginalLoopCarriage, err = primary.WithFanOutHistory(archive.Source)
					if err != nil {
						t.Fatal(err)
					}
					second, err := forks.MaterializeRunFork(childCtx, secondRequest)
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
