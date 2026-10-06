package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestRunForkSelectedSourceAdvancedFanOutConsumersBothStores(t *testing.T) {
	const code = "source_fan_out_obligations_advanced_after_fork_point"
	for _, backend := range eventRecordContractBackends() {
		liveBackend := eventRecordContractBackend{name: backend.name, open: func(t *testing.T) authorActivityReceiptFixture {
			opened := backend.open(t)
			if store, ok := opened.store.(*SQLiteRuntimeStore); ok {
				store.nowFn = time.Now
			}
			return opened
		}}
		for _, position := range []string{"before_frontier", "after_frontier"} {
			t.Run(backend.name+"/"+position, func(t *testing.T) {
				f, construction := newBranchPointDeploymentFixture(t, liveBackend, []byte("{\"body\":\"source row\"}\n"))
				hash := construction.bundle.SourceArtifact.BundleHash()
				owner, candidate := captureBranchPointFanOutOwner(t, f, construction)
				advance := func() {
					intent, claim, found, err := owner.ClaimFanOutIntent(f.ctx, pipeline.FanOutClaimRequest{
						Owner: "source-advanced-proof", BundleHash: hash, Candidate: &candidate.key,
						Now: time.Now().UTC(), Lease: time.Minute,
					})
					if err != nil || !found || intent.Cursor != 0 || intent.Request.Cardinality != 1 {
						t.Fatalf("claim real deployment feed: %+v found=%t err=%v", intent, found, err)
					}
					// A schema rejection is a lawful, revisioned ordinal outcome,
					// without unrelated publication or receiver mutations.
					result, err := owner.CommitFanOutChunk(f.ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC()))
					if err != nil || result.Intent.Cursor != 1 || result.PostCommitFailure != nil {
						t.Fatalf("commit real fan-out advancement: %+v err=%v", result, err)
					}
				}
				if position == "before_frontier" {
					advance()
				}
				// An explicit empty-version override leaves no child issuance
				// to execute. Source progress must remain lineage, not be copied.
				ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
				if err != nil {
					t.Fatal(err)
				}
				pinned, err := f.store.(interface {
					LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
				}).LoadPinnedSource(f.ctx, f.runID, hash, ref)
				if err != nil {
					t.Fatal(err)
				}
				imported, err := f.store.(interface {
					ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				}).ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: hash,
					Declaration: ref, ExpectedHead: durabledata.VersionHead(pinned.VersionID), InputFormat: "jsonl",
				})
				if err != nil {
					t.Fatal(err)
				}
				pins := []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}
				staged, request := stageForkContentionFixtureAt(t, f, true, "", true, pins...)
				if position == "after_frontier" {
					advance()
				}
				observedAt := time.Now().UTC()
				before, err := owner.FanOutRunSummary(f.ctx, f.runID, observedAt)
				if err != nil || before.Intents != 1 || before.Cursor != 1 || before.SemanticRejected != 1 || before.Owed != 0 {
					t.Fatalf("lawful source outcome: %+v err=%v", before, err)
				}
				activation, err := f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(f.ctx, request)
				if err != nil || !activation.Activated {
					t.Fatalf("supported activation: %+v err=%v", activation, err)
				}
				if position == "after_frontier" {
					if !activation.SourceAdvancedAfterFork || activation.SourceFrozen || activation.BranchDivergence == nil ||
						!reflect.DeepEqual(activation.BranchDivergence.SourceAdvancedFacts, []string{code}) ||
						activation.BranchDivergence.Policy != runfork.RunForkSelectedContractSourceAdvancedBranchPolicy {
						t.Fatalf("selected advancement must be exact branch lineage: %+v", activation)
					}
				} else if activation.SourceAdvancedAfterFork || activation.BranchDivergence != nil {
					t.Fatalf("at/before-frontier fan-out must not count as source advancement: %+v", activation)
				}
				child, err := owner.FanOutRunSummary(f.ctx, staged.ForkRunID, observedAt)
				if err != nil || child.Cardinality != 0 || child.Cursor != 0 || child.SemanticRejected != 0 || child.Owed != 0 {
					t.Fatalf("post-frontier source outcome leaked into child execution: %+v err=%v", child, err)
				}
				after, err := owner.FanOutRunSummary(f.ctx, f.runID, observedAt)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("activation rewrote source fan-out progress: before=%+v after=%+v err=%v", before, after, err)
				}
			})
		}
	}
}

func TestRunForkGenericSourceAdvancedFanOutConsumersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, position := range []string{"before_frontier", "after_frontier"} {
			t.Run(backend.name+"/"+position, func(t *testing.T) {
				opened := backend.open(t)
				frontier := uuid.NewString()
				ctx, fixture, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend.name, opened, 0, time.Now().UTC(), false, false,
					canonicalrouting.CopyForkFanOutCarrier(t, false, false), nil, nil, frontier)
				owner := opened.store.(snapshotOwnershipStore)
				if position == "before_frontier" {
					// The empty intent was closed by the real handler commit. A
					// later lawful ingress puts those facts before the chosen point.
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "fan-out-test", "", []byte(`{"items":[]}`), 0,
						fixture.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(fixture.runID), time.Now().UTC())
					if err := commitSemanticEventFixture(ctx, opened.store.(storeTestDurableEventBusStore), event); err != nil {
						t.Fatal(err)
					}
					frontier = event.ID()
				}
				original := originalCarriageForRun(t, owner, fixture.runID)
				fork, err := owner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: frontier, OriginalLoopCarriage: original})
				if err != nil {
					t.Fatal(err)
				}
				wantInherited := 1
				if position == "after_frontier" {
					wantInherited = 0
				}
				if fork.MaterializedFanOutCount != wantInherited {
					t.Fatalf("fork copied fan-out beyond its exact frontier: count=%d want=%d", fork.MaterializedFanOutCount, wantInherited)
				}
				activation, err := owner.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: fork.ForkRunID, AllowSourceFreeze: true, OriginalLoopCarriage: original,
					HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{}})
				if position == "after_frontier" {
					_, fact, ok := runForkReplayResumeBlockerFromError(err)
					if !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced || activation.Activated || !activation.SourceAdvancedAfterFork {
						t.Fatalf("generic activation did not refuse revisioned fan-out advancement: %+v err=%v", activation, err)
					}
				} else if err != nil || !activation.Activated || activation.SourceAdvancedAfterFork {
					t.Fatalf("at/before-frontier closed intent must not be advancement: %+v err=%v", activation, err)
				}
			})
		}
	}
}

func captureBranchPointFanOutOwner(t *testing.T, f forkContentionFixture, construction receiverConfigActivationFixture) (pipeline.FanOutObligationOwner, capturedFanOutTurn) {
	t.Helper()
	evidence, err := construction.grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	work := worklifetime.NewProcess()
	t.Cleanup(func() {
		work.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	occurrence, err := work.NewRuntime(f.ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: evidence.RuntimeInstanceID, BundleHash: evidence.BundleHash})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := occurrence.RetireAndWait(ctx); err != nil {
			t.Error(err)
		}
	})
	executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
	workers := 1
	registration, err := startupownership.StartFanOutServing(f.ctx, construction.grant, occurrence, &workers, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	select {
	case candidate := <-executor.turns:
		registration.Close()
		<-executor.done
		if candidate.key.RunID != f.runID || candidate.key.DeploymentFeedID == "" {
			t.Fatalf("wrong admitted deployment candidate: %+v", candidate.key)
		}
		return candidate.owner, candidate
	case err := <-executor.errors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("admitted deployment candidate was not selected")
	}
	return nil, capturedFanOutTurn{}
}
