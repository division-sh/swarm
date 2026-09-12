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
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	obligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	startupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
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

func openPipelineCrashStore(t *testing.T, backend, location string) (authorActivityReceiptFixture, *pipelineCrashConnector) {
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
		store := admitTestPostgresStore(t, db)
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
	store, err := newSQLiteStoreComposition(schema, b, nil)
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
				ctx := testAuthorActivityContext()
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				eventID := commitPipelineParityEvent(t, ctx, fixture.store.(pipelineObligationParityStore), runID, time.Now().UTC())
				// Drain any setup notification through its real owner so only the
				// killed settlement can authorize the candidate tested at restart.
				candidates := fixture.store.(runlifecycle.CandidateStore)
				if pending := pipelineCrashCandidate(t, ctx, candidates, runID); pending.RunID != "" {
					result, err := candidates.ExecuteCompletionCandidate(ctx, pending, runlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}))
					if err != nil || result.Outcome != runlifecycle.OutcomeAwaitMutation {
						t.Fatalf("drain setup candidate: %#v %v", result, err)
					}
				}
				if pending := pipelineCrashCandidate(t, ctx, candidates, runID); pending.RunID != "" {
					t.Fatal("setup left a candidate masking lost settlement handoff")
				}
				env := []string{"SWARM_PIPELINE_CRASH_BACKEND=" + backend, "SWARM_PIPELINE_CRASH_LOCATION=" + location,
					"SWARM_PIPELINE_CRASH_CUT=" + cut, "SWARM_PIPELINE_CRASH_EVENT=" + eventID, "SWARM_PIPELINE_CRASH_RUN=" + runID}
				child, receipt, wait := startPipelineCrashChild(t, "predecessor", env)
				old := awaitPipelineCrashEvidence(t, receipt)
				// A live predecessor, not a stale row or local socket assumption,
				// must prevent another process authority from being granted.
				attemptCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				contender, err := fixture.store.(startupownership.Store).AcquireProcessCapability(attemptCtx, testStartupAcquireRequest("crash-contender"))
				cancel()
				if err == nil {
					_ = contender.Release(ctx)
					t.Fatal("live predecessor permitted another process capability")
				}
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
	capability, err := fixture.store.(startupownership.Store).AcquireProcessCapability(ctx, testStartupAcquireRequest("pipeline-crash-"+mode))
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
	report := os.NewFile(3, "pipeline-crash-evidence")
	defer report.Close()
	cut, eventID, runID := os.Getenv("SWARM_PIPELINE_CRASH_CUT"), os.Getenv("SWARM_PIPELINE_CRASH_EVENT"), os.Getenv("SWARM_PIPELINE_CRASH_RUN")
	if mode == "predecessor" {
		work, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery)
		if err != nil {
			t.Fatal(err)
		}
		process := worklifetime.NewProcess()
		occurrence := newRunLifecycleExecutorOccurrence(t, process)
		ctx = worklifetime.WithRuntimeOccurrence(ctx, occurrence)
		sink := &pipelineGracefulSink{}
		registration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: authorActivityTestBundleHash}, sink)
		if err != nil {
			t.Fatal(err)
		}
		defer registration.Release()
		control.cut = cut
		control.barrier = func() {
			if sink.submits.Load() != 0 {
				t.Fatal("handoff ran before crash barrier")
			}
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
	if authority.AuthorityID == old.AuthorityID || authority.PredecessorAuthorityID != old.AuthorityID || authority.AuthorityGeneration != old.AuthorityGeneration+1 || authority.AcquisitionKind != startupownership.AcquisitionCrashTakeover {
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
	candidates := fixture.store.(runlifecycle.CandidateStore)
	before := pipelineCrashCandidate(t, ctx, candidates, runID)
	if (before.RunID != "") != (wantBefore == 1) {
		t.Fatalf("candidate at restart=%#v, committed settlement=%v", before, wantBefore == 1)
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
	after := pipelineCrashCandidate(t, ctx, candidates, runID)
	if after.RunID != runID {
		t.Fatal("committed follow-up candidate lost")
	}
	if wantBefore == 1 && after.Revision != before.Revision {
		t.Fatalf("committed settlement replayed: candidate revision %d -> %d", before.Revision, after.Revision)
	}
	if err := recovery.RecoverToExhaustion(ctx); err != nil {
		t.Fatal(err)
	}
	again := pipelineCrashCandidate(t, ctx, candidates, runID)
	count, receiptOutcome, receiptReason = readExactPipelineReceipt(t, ctx, fixture, eventID)
	if count != 1 || receiptOutcome != "success" || receiptReason != wantReason || again.Revision != after.Revision {
		t.Fatalf("duplicate durable processing: receipt=%d candidate=%d -> %d", count, after.Revision, again.Revision)
	}
	if _, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery); !errors.Is(err, obligation.ErrIneligible) {
		t.Fatalf("settled event remains replayable: %v", err)
	}
	// The real startup executor must discover the durable candidate without any
	// predecessor notification. Observe its actual selected-store execution, not
	// a replacement recovery implementation or direct SQL interpretation.
	process := worklifetime.NewProcess()
	occurrence := newRunLifecycleExecutorOccurrence(t, process)
	t.Cleanup(func() {
		retireRunLifecycleExecutorOccurrence(t, occurrence)
		retireRunLifecycleProcess(t, process)
	})
	observed := &pipelineCrashCandidateObserver{CandidateStore: candidates, results: make(chan pipelineCrashCandidateResult, 8)}
	executor, err := runlifecycle.NewExecutor(observed, runlifecycle.CandidateScope{BundleHash: authorActivityTestBundleHash},
		runlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}), occurrence, runlifecycle.ExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := executor.Start(ctx); err != nil {
		t.Fatalf("real lifecycle startup executor: %v", err)
	}
	select {
	case result := <-observed.results:
		if result.err != nil || result.candidate.RunID != runID || result.candidate.Revision != after.Revision || result.outcome.Outcome != runlifecycle.OutcomeAwaitMutation {
			t.Fatalf("recovered lifecycle execution=%#v err=%v", result, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup executor did not discover committed candidate")
	}
	if err := executor.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if observed.executions.Load() != 1 {
		t.Fatalf("startup candidate executions=%d want 1", observed.executions.Load())
	}
	if pending := pipelineCrashCandidate(t, ctx, candidates, runID); pending.RunID != "" {
		t.Fatalf("settled await-mutation candidate remains pending: %#v", pending)
	}
	if err := json.NewEncoder(report).Encode(pipelineCrashEvidence{Authority: authority, Recovered: true}); err != nil {
		t.Fatal(err)
	}
}

type pipelineCrashCandidateResult struct {
	candidate runlifecycle.Candidate
	outcome   runlifecycle.CompletionResult
	err       error
}

type pipelineCrashCandidateObserver struct {
	runlifecycle.CandidateStore
	executions atomic.Int32
	results    chan pipelineCrashCandidateResult
}

func (s *pipelineCrashCandidateObserver) ExecuteCompletionCandidate(ctx context.Context, candidate runlifecycle.Candidate, catalog runlifecycle.TerminalCatalog) (runlifecycle.CompletionResult, error) {
	s.executions.Add(1)
	outcome, err := s.CandidateStore.ExecuteCompletionCandidate(ctx, candidate, catalog)
	select {
	case s.results <- pipelineCrashCandidateResult{candidate: candidate, outcome: outcome, err: err}:
	default:
	}
	return outcome, err
}

func pipelineCrashCandidate(t *testing.T, ctx context.Context, store runlifecycle.CandidateStore, runID string) runlifecycle.Candidate {
	t.Helper()
	page, err := store.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: authorActivityTestBundleHash}, runlifecycle.CandidateCursor{}, 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range page.Candidates {
		if candidate.RunID == runID {
			return candidate
		}
	}
	return runlifecycle.Candidate{}
}
