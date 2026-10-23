package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	obligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"

	startupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storeownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"

	"github.com/google/uuid"
	"github.com/lib/pq"
	modernc "modernc.org/sqlite"
)

// These are process-death proofs at the selected-store/runtime recovery surface,
// not serve-startup tests. Only driver transaction exit is instrumented. SQL,
// ownership acquisition, pipeline recovery and candidate enumeration are real.
type pipelineCrashConnector struct {
	driver.Connector
	armed   atomic.Bool
	cut     string
	barrier func()
}

func (c *pipelineCrashConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &pipelineCrashConn{Conn: conn, owner: c}, nil
}

type pipelineCrashSQLiteConnector struct{ path string }

func (c pipelineCrashSQLiteConnector) Driver() driver.Driver { return &modernc.Driver{} }
func (c pipelineCrashSQLiteConnector) Connect(context.Context) (driver.Conn, error) {
	return c.Driver().Open("file:" + c.path + "?_pragma=foreign_keys(ON)&_pragma=busy_timeout(50)")
}

type pipelineCrashConn struct {
	driver.Conn
	owner *pipelineCrashConnector
}

func (c *pipelineCrashConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pipelineCrashTx{Tx: tx, owner: c.owner}, nil
}
func (c *pipelineCrashConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}
func (c *pipelineCrashConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c *pipelineCrashConn) CheckNamedValue(v *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(v)
	}
	return driver.ErrSkip
}
func (c *pipelineCrashConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}
func (c *pipelineCrashConn) IsValid() bool {
	if validator, ok := c.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}
func (c *pipelineCrashConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

type pipelineCrashTx struct {
	driver.Tx
	owner *pipelineCrashConnector
}

func (tx *pipelineCrashTx) Commit() error {
	armed := tx.owner.armed.Swap(false)
	if armed && tx.owner.cut == "before_commit" {
		tx.owner.barrier()
	}
	err := tx.Tx.Commit()
	if armed && err == nil && tx.owner.cut == "after_commit_before_ack" {
		tx.owner.barrier()
	}
	return err
}

func openPipelineCrashStore(t *testing.T, backend, location string, instrumentation ...driver.Connector) (authorActivityReceiptFixture, *pipelineCrashConnector) {
	t.Helper()
	var native driver.Connector
	if backend == "postgres" {
		var err error
		native, err = pq.NewConnector(location)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		native = pipelineCrashSQLiteConnector{path: location}
	}
	if len(instrumentation) > 1 || len(instrumentation) == 1 && instrumentation[0] == nil {
		t.Fatal("native process fixture accepts one nonnil transaction observer")
	}
	if len(instrumentation) == 1 {
		native = instrumentation[0]
	}
	var identity *storeownership.SQLiteBackendIdentity
	if backend == "sqlite" && len(instrumentation) == 1 {
		guard, err := storeownership.AcquireSQLiteConstructionGuard(location)
		if err != nil {
			t.Fatal(err)
		}
		identity, err = guard.BackendIdentity(context.Background())
		if err != nil {
			_ = guard.Release()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = identity.ReleaseConstructionPossession() })
	}
	control := &pipelineCrashConnector{Connector: native}
	db := sql.OpenDB(control)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if backend == "postgres" {
		var inspection []pq.Config
		if len(instrumentation) != 0 {
			cfg, err := pq.NewConfig(location)
			if err != nil {
				t.Fatal(err)
			}
			inspection = append(inspection, cfg)
		}
		store := admitTestPostgresStore(t, db, inspection...)
		registerTestAuthorActivityCatalog(t, store)
		return authorActivityReceiptFixture{store: store, db: db, dialect: authoractivityfixture.DialectPostgres}, control
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	b, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := storeschema.NewSQLiteWithBackend(b, location)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSQLiteStoreComposition(schema, b, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(testAuthorActivityContext(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatal(err)
	}
	store.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
	registerTestAuthorActivityCatalog(t, store)
	return authorActivityReceiptFixture{store: store, db: db, dialect: authoractivityfixture.DialectSQLite}, control
}

type pipelineCrashEvidence struct {
	Authority startupownership.Authority
	Recovered bool
}

func TestPipelineProcessSIGKILLRecovery(t *testing.T) {
	if mode := os.Getenv("SWARM_PIPELINE_CRASH_CHILD"); mode != "" {
		runPipelineCrashChild(t, mode)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"before_commit", "after_commit_before_ack"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "pipeline.db")
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
				}
				fixture, _ := openPipelineCrashStore(t, backend, location)
				ctx := storeTestOriginalProcessContext(t, testAuthorActivityContext(), fixture.store)
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				eventID := commitPipelineParityEvent(t, ctx, fixture.store.(pipelineObligationParityStore), runID, time.Now().UTC())
				work, _ := storeTestWorkFixtures.Load(t)
				if err := selectedMaterializationProcessForTest(t, work.(*storeTestWorkFixture), fixture.store).Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				env := []string{"SWARM_PIPELINE_CRASH_BACKEND=" + backend, "SWARM_PIPELINE_CRASH_LOCATION=" + location,
					"SWARM_PIPELINE_CRASH_CUT=" + cut, "SWARM_PIPELINE_CRASH_EVENT=" + eventID, "SWARM_PIPELINE_CRASH_RUN=" + runID}
				child, receipt, wait := startPipelineCrashChild(t, "predecessor", env)
				old := awaitPipelineCrashEvidence(t, receipt)
				// A live predecessor, not a stale row or local socket assumption,
				// must prevent another process authority from being granted.
				attemptCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				type contenderResult struct {
					admitted bool
					err      error
				}
				contended := make(chan contenderResult, 1)
				contenderDone := make(chan struct{})
				go func() {
					defer close(contenderDone)
					contender, err := fixture.store.(startupownership.Store).AcquireProcessCapability(attemptCtx, testStartupAcquireRequest("crash-contender"))
					if err == nil {
						err = contender.Release(context.WithoutCancel(attemptCtx))
						contended <- contenderResult{admitted: true, err: err}
						return
					}
					contended <- contenderResult{err: err}
				}()
				t.Cleanup(func() {
					cancel()
					_ = child.Process.Kill()
					select {
					case <-contenderDone:
					case <-time.After(10 * time.Second):
						t.Error("contender cleanup did not join after process death")
					}
				})
				<-attemptCtx.Done()
				cancel()
				// SQLite's real flock wait joins after physical release; a canceled
				// caller cannot stop us from delivering the promised process death.
				if err := child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-wait:
					var exit *exec.ExitError
					if !errors.As(err, &exit) {
						t.Fatalf("kill exit: %v", err)
					}
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
						t.Fatalf("not SIGKILL: %v", exit)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("SIGKILL child did not exit")
				}
				select {
				case result := <-contended:
					if result.admitted || !errors.Is(result.err, context.DeadlineExceeded) {
						t.Fatalf("live predecessor exclusion or canceled wait changed: admitted=%t err=%v", result.admitted, result.err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("canceled contender did not join after SIGKILL")
				}
				encoded, err := json.Marshal(old.Authority)
				if err != nil {
					t.Fatal(err)
				}
				env = append(env, "SWARM_PIPELINE_CRASH_PREDECESSOR="+string(encoded))
				_, recoveryReceipt, recovered := startPipelineCrashChild(t, "successor", env)
				next := awaitPipelineCrashEvidence(t, recoveryReceipt)
				if !next.Recovered {
					t.Fatal("successor did not prove recovery")
				}
				select {
				case err := <-recovered:
					if err != nil {
						t.Fatalf("successor: %v", err)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("successor did not settle")
				}
				t.Logf("SIGKILL %s: exact authority %s -> %s; real bus recovery, one receipt, durable candidate, repeat sweep unchanged", cut, old.Authority.AuthorityID, next.Authority.AuthorityID)
			})
		}
	}
}

func startPipelineCrashChild(t *testing.T, mode string, env []string) (*exec.Cmd, <-chan pipelineCrashEvidence, <-chan error) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPipelineProcessSIGKILLRecovery$", "-test.count=1", "-test.timeout=45s")
	cmd.Env = append(append(os.Environ(), env...), "SWARM_PIPELINE_CRASH_CHILD="+mode)
	cmd.ExtraFiles = []*os.File{write}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		_ = read.Close()
		_ = write.Close()
		t.Fatal(err)
	}
	_ = write.Close()
	wait := make(chan error, 1)
	done := make(chan struct{})
	go func() { wait <- cmd.Wait(); close(done) }()
	evidence := make(chan pipelineCrashEvidence, 1)
	go func() {
		defer close(evidence)
		defer read.Close()
		var record pipelineCrashEvidence
		if json.NewDecoder(read).Decode(&record) == nil {
			evidence <- record
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = read.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("child cleanup did not join process exit")
		}
	})
	return cmd, evidence, wait
}

func awaitPipelineCrashEvidence(t *testing.T, evidence <-chan pipelineCrashEvidence) pipelineCrashEvidence {
	t.Helper()
	select {
	case record, ok := <-evidence:
		if !ok {
			t.Fatal("child closed evidence pipe before proof boundary")
		}
		return record
	case <-time.After(30 * time.Second):
		t.Fatal("child did not reach proof boundary")
	}
	return pipelineCrashEvidence{}
}

func runPipelineCrashChild(t *testing.T, mode string) {
	backend, location := os.Getenv("SWARM_PIPELINE_CRASH_BACKEND"), os.Getenv("SWARM_PIPELINE_CRASH_LOCATION")
	fixture, control := openPipelineCrashStore(t, backend, location)
	ctx := testAuthorActivityContext()
	selected := fixture.store.(pipelineObligationParityStore)
	request := testStartupAcquireRequest("pipeline-crash-" + mode)
	capability, err := fixture.store.(startupownership.Store).AcquireProcessCapability(ctx, request)
	if err != nil {
		t.Fatalf("acquire exact process authority: %v", err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	authority, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	if err := work.process.BindOriginalProcess(authority.LeaseStamp()); err != nil {
		t.Fatal(err)
	}
	work.capabilities = map[any]startupownership.ProcessCapability{fixture.store: capability}
	ctx = worklifetime.WithProcess(ctx, work.process)
	report := os.NewFile(3, "pipeline-crash-evidence")
	defer report.Close()
	cut, eventID := os.Getenv("SWARM_PIPELINE_CRASH_CUT"), os.Getenv("SWARM_PIPELINE_CRASH_EVENT")
	if mode == "predecessor" {
		work, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery)
		if err != nil {
			t.Fatal(err)
		}
		control.cut = cut
		control.barrier = func() {
			if err := json.NewEncoder(report).Encode(pipelineCrashEvidence{Authority: authority}); err != nil {
				t.Fatal(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		control.armed.Store(true)
		_, err = selected.PipelineObligations().Settle(ctx, work.Claim, obligation.Acknowledged("processed"))
		t.Fatalf("settlement escaped crash barrier: %v", err)
	}
	var old startupownership.Authority
	if err := json.Unmarshal([]byte(os.Getenv("SWARM_PIPELINE_CRASH_PREDECESSOR")), &old); err != nil {
		t.Fatal(err)
	}
	if authority.Validate() != nil || authority.AuthorityID == old.AuthorityID || authority.AuthorityGeneration != old.AuthorityGeneration+1 || authority.Backend != old.Backend || authority.OwnerID != request.OwnerID || authority.BootID != request.BootID || authority.RuntimeInstanceID != request.RuntimeInstanceID {
		t.Fatalf("not exact crash takeover: old=%#v next=%#v", old, authority)
	}
	count, _, _ := readExactPipelineReceipt(t, ctx, fixture, eventID)
	wantBefore := 0
	if cut == "after_commit_before_ack" {
		wantBefore = 1
	}
	if count != wantBefore {
		t.Fatalf("receipt at restart=%d want %d", count, wantBefore)
	}
	bus, err := newRunConvergenceEventBus(t, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	recovery := runtimepipeline.NewRecoveryManagerWith(bus)
	if err := recovery.RecoverToExhaustion(ctx); err != nil {
		t.Fatalf("real bus startup recovery: %v", err)
	}
	count, receiptOutcome, receiptReason := readExactPipelineReceipt(t, ctx, fixture, eventID)
	wantReason := "pipeline_persisted"
	if wantBefore == 1 {
		wantReason = "processed"
	}
	if count != 1 || receiptOutcome != "success" || receiptReason != wantReason {
		t.Fatalf("recovery receipt=%d/%s/%s want 1/success/%s", count, receiptOutcome, receiptReason, wantReason)
	}
	beforeRecovery := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	if err := recovery.RecoverToExhaustion(ctx); err != nil {
		t.Fatal(err)
	}
	count, receiptOutcome, receiptReason = readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 1 || receiptOutcome != "success" || receiptReason != wantReason || !reflect.DeepEqual(beforeRecovery, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatalf("duplicate durable processing: receipt=%d/%s/%s or changed execution history", count, receiptOutcome, receiptReason)
	}
	if _, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery); !errors.Is(err, obligation.ErrIneligible) {
		t.Fatalf("settled event remains replayable: %v", err)
	}
	if err := json.NewEncoder(report).Encode(pipelineCrashEvidence{Authority: authority, Recovered: true}); err != nil {
		t.Fatal(err)
	}
}
