package runforkexecution

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type liveWinnerOperationReader interface {
	LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
}

type liveWinnerForkProbe struct {
	SelectedContractForkLifecycle
	reader       liveWinnerOperationReader
	materialized chan struct{}
	allowBinding chan struct{}
	secondCreate chan struct{}
	observed     chan struct{}
	observedOnce sync.Once
	secondOnce   sync.Once
	creates      atomic.Int32
}

func (p *liveWinnerForkProbe) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	if p.creates.Add(1) != 1 {
		p.secondOnce.Do(func() { close(p.secondCreate) })
		<-ctx.Done()
		return runfork.RunForkMaterialization{}, ctx.Err()
	}
	result, err := p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		return result, err
	}
	close(p.materialized)
	<-p.allowBinding
	return result, nil
}

func (p *liveWinnerForkProbe) LoadForkOperation(ctx context.Context, actor, key, hash string) (runfork.ForkOperationRecord, bool, error) {
	record, found, err := p.reader.LoadForkOperation(ctx, actor, key, hash)
	if err == nil && found {
		p.observedOnce.Do(func() { close(p.observed) })
	}
	return record, found, err
}

type liveWinnerExecutionProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	entered chan struct{}
	allow   chan struct{}
	issues  atomic.Int32
}

func (p *liveWinnerExecutionProbe) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	if p.issues.Add(1) == 1 {
		close(p.entered)
		select {
		case <-p.allow:
		case <-ctx.Done():
			return runfork.SelectedContractRuntimeExecution{}, ctx.Err()
		}
	}
	return p.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, req)
}

func TestSelectedForkSameKeyJoinsBoundOwnerBeforeExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx, cancel := context.WithTimeout(runForkTestContext(t), 20*time.Second)
			defer cancel()
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			repo := runForkExecutionRepoRoot(t)
			fixtureLoader := admittedFixtureSelectedContractSourceLoader{
				RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"),
				PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo),
			}
			loaded, err := fixtureLoader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			sourceID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Unix(1700002200, 0).UTC()
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
				RunID: sourceID, Origin: storetest.ScenarioSetupOrigin(), BundleHash: loaded.SourceArtifactFact.BundleHash(),
				Artifact: selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash()), StartedAt: at.Add(-time.Minute),
			})
			event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "item.received", "source-runtime", "", []byte(`{}`), 0,
				sourceID, events.EventEnvelope{}, events.NoRoutingSource(), at, executionmode.Mock)
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event,
				[]events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("source-only-node")}, pipelineobligation.ScopeSubscribed)
			commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)
			storetest.CaptureRunForkSnapshot(t, ctx, selected, sourceID)
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "bound-live-winner", TransportHash: "sha256:same-request",
				SourceRunID: sourceID, ForkEventID: eventID, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(),
				AllowSourceFreeze: true, ContractSelection: selection,
			}
			request := SelectedContractExecutionRequest{
				Owner: owner, ForkOperation: &operation, SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true,
				ExpectedBundleHash: loaded.SourceArtifactFact.BundleHash(), ContractSelection: selection,
				SourceLoader: SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo),
					Store: selected.(SourceArtifactSelectedContractSourceStore)},
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
			}
			probe := &liveWinnerForkProbe{SelectedContractForkLifecycle: owner.ports.fork, reader: selected.(liveWinnerOperationReader),
				materialized: make(chan struct{}), allowBinding: make(chan struct{}), secondCreate: make(chan struct{}), observed: make(chan struct{})}
			execution := &liveWinnerExecutionProbe{SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution,
				entered: make(chan struct{}), allow: make(chan struct{})}
			owner.ports.fork, owner.ports.runtimeExecution = probe, execution
			type outcome struct {
				result SelectedContractExecutionResult
				err    error
			}
			var winner, duplicate outcome
			winnerDone, duplicateDone := make(chan struct{}), make(chan struct{})
			var releaseBinding, releaseExecution sync.Once
			duplicateStarted := false
			duplicateCtx, cancelDuplicate := context.WithCancel(ctx)
			defer func() {
				releaseBinding.Do(func() { close(probe.allowBinding) })
				releaseExecution.Do(func() { close(execution.allow) })
				cancelDuplicate()
				cancel()
				<-winnerDone
				if duplicateStarted {
					<-duplicateDone
				}
			}()
			go func() {
				defer close(winnerDone)
				winner.result, winner.err = ExecuteSelectedContractRunFork(ctx, request)
			}()
			select {
			case <-probe.materialized:
			case <-winnerDone:
				t.Fatalf("winner failed before native commit: %v", winner.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			duplicateRequest := request
			duplicateOperation := operation
			duplicateOperation.OperationID = uuid.NewString()
			duplicateRequest.ForkOperation = &duplicateOperation
			duplicateStarted = true
			go func() {
				defer close(duplicateDone)
				duplicate.result, duplicate.err = ExecuteSelectedContractRunFork(duplicateCtx, duplicateRequest)
			}()
			releaseBinding.Do(func() { close(probe.allowBinding) })
			select {
			case <-execution.entered:
			case <-winnerDone:
				t.Fatalf("winner failed before execution issue: %v", winner.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-probe.observed:
			case <-probe.secondCreate:
				t.Fatal("duplicate entered materialization while the durable winner had a bound live preparation and no execution row")
			case <-duplicateDone:
				t.Fatalf("duplicate did not join the winning outcome: %+v err=%v", duplicate.result, duplicate.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			releaseExecution.Do(func() { close(execution.allow) })
			for _, done := range []chan struct{}{winnerDone, duplicateDone} {
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("operation outcome waited for retained-runtime retirement")
				}
			}
			if winner.err != nil || duplicate.err != nil || !winner.result.Activation.Activated || !duplicate.result.Activation.Activated ||
				winner.result.Materialization.ForkRunID == "" || winner.result.Materialization.ForkRunID != duplicate.result.Materialization.ForkRunID ||
				probe.creates.Load() != 1 || execution.issues.Load() != 1 {
				t.Fatalf("keyed join changed child/execution: winner=%+v/%v duplicate=%+v/%v materializations=%d issues=%d",
					winner.result, winner.err, duplicate.result, duplicate.err, probe.creates.Load(), execution.issues.Load())
			}
		})
	}
}
