package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type preparedCommitConnector struct {
	*pipelineCrashConnector
	armed                   atomic.Bool
	entered, resume, joined chan struct{}
	afterCommit             bool
}

func TestPreparedProcessReleaseFencesSelectedForkCommandsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			unissued := newSelectedCompletionFixture(t, store, db, sqlite)
			prepared := newSelectedCompletionFixtureWithProcess(t, store, db, sqlite, unissued.process)
			ctx := testAuthorActivityContext()
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, prepared.request)
			if err != nil {
				t.Fatal(err)
			}
			control := store.(interface {
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
				StopSelectedFork(context.Context, runcontrol.SelectedStopRequest) (runcontrol.State, error)
			})
			binding, err := control.RequireRunForkSelectedContractBinding(ctx, unissued.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			process, err := unissued.process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if err := unissued.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			beforeUnissued := loadSelectedForkDiscardProof(t, ctx, db, sqlite, unissued.forkRun, uuid.Nil.String())
			beforePrepared := loadSelectedForkDiscardProof(t, ctx, db, sqlite, prepared.forkRun, issued.ExecutionID)
			if _, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, unissued.request); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
				t.Errorf("released process issued a new executable preparation: %v", err)
			}
			if _, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "released-claim", time.Minute); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
				t.Errorf("released process claimed accepted preparation: %v", err)
			}
			if _, err := control.StopSelectedFork(ctx, runcontrol.SelectedStopRequest{Transition: runcontrol.TransitionRequest{RunID: unissued.forkRun}, Binding: binding, Process: process.LeaseStamp()}); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
				t.Errorf("released process authorized a new operator stop: %v", err)
			}
			if after := loadSelectedForkDiscardProof(t, ctx, db, sqlite, unissued.forkRun, uuid.Nil.String()); !reflect.DeepEqual(after, beforeUnissued) {
				t.Errorf("refused issue/stop changed durable evidence: before=%+v after=%+v", beforeUnissued, after)
			}
			if after := loadSelectedForkDiscardProof(t, ctx, db, sqlite, prepared.forkRun, issued.ExecutionID); !reflect.DeepEqual(after, beforePrepared) {
				t.Errorf("refused claim changed durable evidence: before=%+v after=%+v", beforePrepared, after)
			}
		})
	}
}

func TestPreparedProcessReleaseFencesMaterializationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, healthy := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			refusedCtx, refused := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			store := fixture.store.(forkWorkflowOwnershipStore)
			materialized, err := store.MaterializeRunForkForSelectedContractExecution(ctx, healthy)
			if err != nil {
				t.Fatalf("live otherwise-equivalent materialization refused: %v", err)
			}
			requireForkWorkflowOwnershipPreserved(t, fixture.db, healthy.SourceRunID, materialized.ForkRunID)
			value, ok := storeTestWorkFixtures.Load(t)
			if !ok {
				t.Fatal("original preparation work owner missing")
			}
			process := selectedMaterializationProcessForTest(t, value.(*storeTestWorkFixture), fixture.store)
			stamp, err := process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if err := process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			inspection, err := fixture.store.(startupAuthorityParityStore).InspectAuthority(ctx)
			if err != nil || inspection.Authority == nil || *inspection.Authority != stamp {
				t.Fatalf("Release changed last-owner evidence: %+v %v", inspection, err)
			}
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := store.MaterializeRunForkForSelectedContractExecution(refusedCtx, refused); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
				t.Fatalf("released process materialization refusal: %v", err)
			}
			if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
				t.Fatal("released materialization changed durable evidence")
			}
		})
	}
}

func TestPreparedProcessReleaseFencesMutatingRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			healthy := newSelectedProviderCompletionFixture(t, store, db, sqlite)
			refused := newSelectedProviderCompletionFixtureWithProcess(t, store, db, sqlite, healthy.process)
			ctx := testAuthorActivityContextForBundle(healthy.request.DeclarationPlan.BundleHash)
			attempts := map[string]*effects.Handle{}
			for _, fixture := range []selectedCompletionFixture{healthy, refused} {
				issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "prepared-release-recovery", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				admitSelectedProviderFixture(t, ctx, fixture, issued, authority)
				authority.Target = selectedProviderTarget(fixture)
				attemptCtx := effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(store)), authority)
				attemptCtx = selectedProviderClaimContext(t, attemptCtx, fixture, authority)
				attemptCtx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, attemptCtx, authority), authority, "openai_responses")
				handle, err := beginManagedCompletionForTest(t, attemptCtx, "openai_responses", []byte("prepared-release-recovery"))
				if err != nil {
					t.Fatal(err)
				}
				if handle.Attempt().Authority.ID != issued.ExecutionID || handle.Attempt().Authority.SelectedFork.ForkRunID != fixture.forkRun {
					t.Fatalf("prelaunch attempt escaped its predecessor: %+v", handle.Attempt())
				}
				requirePreparedRecoveryAttemptPhase(t, store.(effects.OutcomeStore), handle.Attempt(), effects.StateAuthorized)
				attempts[fixture.forkRun] = handle
			}
			if err := healthy.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			process := selectedPreparationProcessForTest(t, store)
			stamp, err := process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			recovery := store.(interface {
				ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
				InspectSelectedForkRecovery(context.Context, runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error)
				RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
			})
			entries, err := recovery.ListSelectedForkRecoveryEntries(ctx)
			if err != nil || len(entries) != 2 {
				t.Fatalf("exact predecessor inventory: %+v %v", entries, err)
			}
			requests := map[string]runcontrol.SelectedForkRecoveryRequest{}
			for _, entry := range entries {
				request := runcontrol.SelectedForkRecoveryRequest{Entry: entry, Process: stamp.LeaseStamp(), Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
				if err := request.Validate(); err != nil {
					t.Fatalf("live admitted recovery request: %v", err)
				}
				requests[entry.Binding.ForkRunID] = request
				attempt := attempts[entry.Binding.ForkRunID].Attempt()
				inspection, err := recovery.InspectSelectedForkRecovery(ctx, entry)
				if err != nil || attempt.Authority.ID != inspection.Plan.ExecutionID || inspection.Plan.Disposition != runfork.SelectedForkRecoveryFailed {
					t.Fatalf("recovery inventory substituted prelaunch execution: %+v %v", inspection, err)
				}
				requirePreparedRecoveryAttemptPhase(t, store.(effects.OutcomeStore), attempt, effects.StateAuthorized)
			}
			beforeHealthy := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
			result, err := recovery.RecoverSelectedFork(ctx, requests[healthy.forkRun])
			if err != nil || result.RunID != healthy.forkRun || result.Disposition != runfork.SelectedForkRecoveryFailed {
				t.Fatalf("live mutating recovery: %+v %v", result, err)
			}
			if result.Effects.PrelaunchTerminal != 1 || result.Effects.OutcomeUncertain != 0 {
				t.Fatalf("mutating recovery did not settle its one authorized prelaunch attempt: %+v", result.Effects)
			}
			requirePreparedRecoveryAttemptPhase(t, store.(effects.OutcomeStore), attempts[healthy.forkRun].Attempt(), effects.StateTerminalFailure)
			requirePreparedRecoveryAttemptPhase(t, store.(effects.OutcomeStore), attempts[refused.forkRun].Attempt(), effects.StateAuthorized)
			if reflect.DeepEqual(beforeHealthy, snapshotForkHistoricalExecutionTables(t, db, !sqlite)) {
				t.Fatal("healthy recovery did not exercise its mutation owner")
			}
			if err := process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			inspection, err := store.(startupAuthorityParityStore).InspectAuthority(ctx)
			if err != nil || inspection.Authority == nil || *inspection.Authority != stamp {
				t.Fatalf("Release changed last-owner evidence: %+v %v", inspection, err)
			}
			before := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
			if _, err := recovery.RecoverSelectedFork(ctx, requests[refused.forkRun]); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
				t.Fatalf("released process mutating recovery refusal: %v", err)
			}
			if after := snapshotForkHistoricalExecutionTables(t, db, !sqlite); !reflect.DeepEqual(before, after) {
				t.Fatal("released mutating recovery changed durable evidence")
			}
		})
	}
}

func requirePreparedRecoveryAttemptPhase(t *testing.T, reader effects.OutcomeStore, attempt effects.Attempt, want effects.State) {
	t.Helper()
	outcome, found, err := reader.GetExternalEffectOutcome(t.Context(), attempt.OperationID)
	if err != nil || !found || outcome.OperationID != attempt.OperationID || outcome.AuthorityID != attempt.Authority.ID ||
		outcome.AuthorityKind != attempt.Authority.Kind || outcome.State != want || outcome.AttemptState != want {
		t.Fatalf("prelaunch recovery effect differs from its durable owner: %+v found=%t err=%v want=%s", outcome, found, err, want)
	}
}

func (c *preparedCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.pipelineCrashConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &preparedCommitConn{pipelineCrashConn: conn.(*pipelineCrashConn), owner: c}, nil
}

type preparedCommitConn struct {
	*pipelineCrashConn
	owner *preparedCommitConnector
}

func (c *preparedCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.pipelineCrashConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &preparedCommitTx{pipelineCrashTx: tx.(*pipelineCrashTx), owner: c.owner, readOnly: opts.ReadOnly}, nil
}

type preparedCommitTx struct {
	*pipelineCrashTx
	owner    *preparedCommitConnector
	readOnly bool
}

func (tx *preparedCommitTx) Commit() error {
	if tx.readOnly || !tx.owner.armed.Swap(false) {
		return tx.pipelineCrashTx.Commit()
	}
	var err error
	if tx.owner.afterCommit {
		err = tx.pipelineCrashTx.Commit()
	}
	close(tx.owner.entered)
	<-tx.owner.resume
	if !tx.owner.afterCommit {
		err = tx.pipelineCrashTx.Commit()
	}
	close(tx.owner.joined)
	return err
}

func TestPreparedProcessOperationReleaseJoinsNativeOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, afterCommit := range []bool{false, true} {
			t.Run(backend+map[bool]string{false: "/before_commit", true: "/after_commit_before_ack"}[afterCommit], func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "prepared.db")
				var native driver.Connector = pipelineCrashSQLiteConnector{path: location}
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
					var err error
					native, err = pq.NewConnector(location)
					if err != nil {
						t.Fatal(err)
					}
				}
				gate := &preparedCommitConnector{pipelineCrashConnector: &pipelineCrashConnector{Connector: native},
					entered: make(chan struct{}), resume: make(chan struct{}), joined: make(chan struct{}), afterCommit: afterCommit}
				fixture, _ := openPipelineCrashStore(t, backend, location, gate)
				store := fixture.store.(startupAuthorityParityStore)
				ctx := testAuthorActivityContext()
				process, _, evidence := admitRegistrationTestGeneration(t, ctx, store, "prepared-operation-owner")
				var resumeOnce sync.Once
				resume := func() { resumeOnce.Do(func() { close(gate.resume) }) }
				t.Cleanup(func() { resume(); _ = process.Release(context.Background()) })
				authority := testServeRegistrationAuthority(evidence)
				registration, ok := effects.RegistrationFor("provider_registration")
				if !ok {
					t.Fatal("provider registration adapter missing")
				}
				request := effects.AuthorizeRequest{OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
					Adapter: registration.Adapter, Transport: registration.Transport, RequestFingerprint: "native-release-proof",
					Lineage: map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC()}
				gate.armed.Store(true)
				type authorized struct {
					attempt effects.Attempt
					err     error
				}
				completed := make(chan authorized, 1)
				go func() {
					attempt, err := store.AuthorizeExternalAttempt(ctx, authority, request)
					completed <- authorized{attempt, err}
				}()
				select {
				case <-gate.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("authorization did not reach real native commit cut")
				}
				releasing, released := make(chan struct{}), make(chan error, 1)
				go func() { close(releasing); released <- process.Release(ctx) }()
				<-releasing
				select {
				case err := <-released:
					t.Fatalf("Release returned before actual outcome joined: %v", err)
				default:
				}
				observer := fixture.store.(interface {
					ProbePossession(context.Context) (startupownership.PossessionObservation, error)
				})
				observeCtx, stopObserve := context.WithTimeout(ctx, 5*time.Second)
				observation, err := observer.ProbePossession(observeCtx)
				stopObserve()
				if err != nil || observation.Available {
					t.Fatalf("operation still owns native outcome but lease was released: %+v %v", observation, err)
				}
				resume()
				var result authorized
				select {
				case result = <-completed:
				case <-time.After(5 * time.Second):
					t.Fatal("native operation did not join")
				}
				if result.err != nil || result.attempt.OperationID != request.OperationID {
					t.Fatalf("acknowledged authorization lost: %+v %v", result.attempt, result.err)
				}
				select {
				case <-gate.joined:
				default:
					t.Fatal("operation returned before native commit return")
				}
				select {
				case err := <-released:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Release did not join after operation")
				}
				if _, err := store.MarkExternalAttemptLaunched(ctx, result.attempt, time.Now().UTC()); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
					t.Fatalf("Release winner permitted a launch: %v", err)
				}
				request.OperationID, request.AttemptID = uuid.NewString(), uuid.NewString()
				if _, err := store.AuthorizeExternalAttempt(ctx, authority, request); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
					t.Fatalf("Release winner permitted new authorization: %v", err)
				}
			})
		}
	}
}
