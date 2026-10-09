package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type retainedExecutionOrderProbe struct {
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	mu        sync.Mutex
	activated bool
	violation bool
	started   chan struct{}
	release   chan struct{}
	cleanup   error
}

func (p *retainedExecutionOrderProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	activation, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
	if activation.Activated {
		p.mu.Lock()
		p.activated = true
		p.mu.Unlock()
		return activation, errors.Join(err, p.cleanup)
	}
	return activation, err
}

func (p *retainedExecutionOrderProbe) CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error) {
	p.mu.Lock()
	p.violation = !p.activated
	close(p.started)
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return bus.CommittedSelectedForkEvent{}, ctx.Err()
	case <-p.release:
		return p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	}
}

func TestSelectedForkAcknowledgmentPrecedesBusinessDrainBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected SelectedContractForkLifecycle
			var owner SelectedContractExecutionOwner
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				pg := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = pg, selectedContractExecutionOwnerForTest(t, pg)
			} else {
				sqlite := storetest.StartSQLiteRuntimeStore(t)
				db = storetest.DatabaseForTest(sqlite)
				selected, owner = sqlite, selectedRuntimeOutcomeSQLiteOwner(t, sqlite)
			}
			ctx := runForkTestContext(t)
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts})
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
			scope, err := authoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx = authoractivity.WithScope(ctx, scope)
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			sourceID, eventID := uuid.NewString(), uuid.NewString()
			seedSelectedRuntimeOutcomeSource(t, ctx, backend, db, selected, loaded, sourceID, eventID, selectedExecutionEntitylessNodeRoute("source-only-node"), time.Unix(1700002200, 0).UTC())
			probe := &retainedExecutionOrderProbe{
				SelectedContractForkLifecycle: owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
				started: make(chan struct{}), release: make(chan struct{}), cleanup: errors.New("activation cleanup diagnostic"),
			}
			defer close(probe.release)
			owner.ports.fork, owner.ports.replay = probe, probe
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "retention-proof", IdempotencyKey: "fixed-cut",
				TransportHash: "retention-proof-transport", SourceRunID: sourceID, ForkEventID: eventID,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection,
			}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			if err != nil || !result.Activation.Activated || result.ExecutedEventCount != 0 {
				t.Fatalf("activation was confused with drain or cleanup: result=%+v error=%v", result, err)
			}
			operations, ok := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			if !ok {
				t.Fatal("selected store lacks its permanent operation reader")
			}
			record, found, err := operations.LoadForkOperation(context.Background(), operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || record.Status != runfork.ForkOperationActivated || record.ForkRunID != result.Materialization.ForkRunID || record.Result == nil || record.Result.ExecutedEventCount != 0 {
				t.Fatalf("activation lacked durable acknowledgment before drain: record=%+v found=%v error=%v", record, found, err)
			}
			select {
			case <-probe.started:
			case <-time.After(5 * time.Second):
				t.Fatal("retained executor never began ordinary publication")
			}
			probe.mu.Lock()
			violation := probe.violation
			probe.mu.Unlock()
			if violation {
				t.Fatal("business publication preceded acknowledged activation")
			}
			storage, err := storetest.ReadSelectedExecutionStorage(context.Background(), selected, result.Materialization.ForkRunID)
			if err != nil || storage.State != "running" || storage.RunStatus != "running" || storage.Occurrences != 1 {
				t.Fatalf("acknowledgment did not retain one current executor: %+v %v", storage, err)
			}
		})
	}
}
