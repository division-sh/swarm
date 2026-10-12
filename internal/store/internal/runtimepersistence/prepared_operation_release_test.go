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
