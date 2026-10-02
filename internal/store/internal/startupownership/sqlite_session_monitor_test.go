//go:build darwin || linux

package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type cancellingSQLitePossession struct {
	delegate sqlitePossession
	entered  chan struct{}
	resume   chan struct{}
	phase    string
	once     sync.Once
}

func (p *cancellingSQLitePossession) ProveCurrent(ctx context.Context) error {
	if p.phase == "os proof" {
		p.once.Do(func() { close(p.entered) })
		<-ctx.Done()
		return p.delegate.ProveCurrent(ctx)
	}
	if err := p.delegate.ProveCurrent(ctx); err != nil {
		return err
	}
	p.once.Do(func() { close(p.entered) })
	<-p.resume
	return nil
}

func (p *cancellingSQLitePossession) Release() error {
	return p.delegate.Release()
}

type sqliteSessionTerminalProbe struct {
	results chan runtimestartupownership.TerminalResult
}

func (p *sqliteSessionTerminalProbe) SelectedStoreSessionTerminal(result runtimestartupownership.TerminalResult) {
	p.results <- result
}

func TestTerminalAuthorityReadbackUsesBoundedDeadline(t *testing.T) {
	const deadline = 10 * time.Millisecond
	started := time.Now()
	result := boundedTerminalResult(deadline, func(ctx context.Context) runtimestartupownership.TerminalResult {
		<-ctx.Done()
		return runtimestartupownership.TerminalResult{
			Cause: runtimestartupownership.TerminalOwnershipSuperseded, SuccessorAuthorityID: uuid.NewString(),
		}
	})
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("terminal authority readback took %s, want bounded completion", elapsed)
	}
	if result.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("terminal result = %#v, want ownership_unprovable", result)
	}
}

func TestSQLiteSessionMonitorCancellationPreservesPossessionUntilDurableRelease(t *testing.T) {
	for _, phase := range []string{"before proof", "os proof", "sql proof"} {
		t.Run(phase, func(t *testing.T) {
			proveSQLiteSessionCancellationPreservesPossessionUntilDurableRelease(t, phase, true)
		})
	}
}

func TestSQLiteSessionOrdinaryCancellationPreservesPossessionUntilDurableRelease(t *testing.T) {
	for _, phase := range []string{"before proof", "os proof", "sql proof"} {
		t.Run(phase, func(t *testing.T) {
			proveSQLiteSessionCancellationPreservesPossessionUntilDurableRelease(t, phase, false)
		})
	}
}

func proveSQLiteSessionCancellationPreservesPossessionUntilDurableRelease(t *testing.T, phase string, monitor bool) {
	t.Helper()
	session, db := newSQLiteProofSession(t, 4)
	path := session.owner.path
	blocking := &cancellingSQLitePossession{
		delegate: session.possession, entered: make(chan struct{}), resume: make(chan struct{}), phase: phase,
	}
	session.possession = blocking
	terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 2)}
	if err := session.InstallTerminalOwner(terminal, time.Minute); err != nil {
		t.Fatalf("install terminal owner: %v", err)
	}

	monitorCtx, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()
	monitorDone := make(chan error, 1)
	if phase == "before proof" {
		cancelMonitor()
	}
	go func() {
		if monitor {
			monitorDone <- session.MonitorProveCurrent(monitorCtx, time.Minute)
			return
		}
		monitorDone <- session.ProveCurrent(monitorCtx)
	}()
	if phase != "before proof" {
		select {
		case <-blocking.entered:
		case <-time.After(time.Second):
			t.Fatalf("SQLite monitor did not enter %s", phase)
		}
		cancelMonitor()
		if phase == "sql proof" {
			close(blocking.resume)
		}
	}
	if err := <-monitorDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled monitor error=%v, want context.Canceled", err)
	}
	select {
	case result := <-terminal.results:
		t.Fatalf("cancelled monitor terminalized SQLite session: %#v", result)
	default:
	}
	if _, err := session.Authority(); err != nil {
		t.Fatalf("cancelled monitor released SQLite authority: %v", err)
	}
	if contender, err := acquireSQLiteFilePossession(path); contender != nil || !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
		t.Fatalf("possession before durable release contender=%#v err=%v, want retained lock", contender, err)
	}

	if err := session.Release(context.Background()); err != nil {
		t.Fatalf("release SQLite session: %v", err)
	}
	var durableState string
	if err := db.QueryRow(`SELECT state FROM runtime_startup_authority_facts WHERE authority_id=? ORDER BY transition_ordinal DESC LIMIT 1`, session.authority.AuthorityID).Scan(&durableState); err != nil {
		t.Fatalf("read durable release: %v", err)
	}
	if durableState != string(runtimestartupownership.StateReleased) {
		t.Fatalf("durable authority state=%q, want released", durableState)
	}
	contender, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire SQLite possession after durable release: %v", err)
	}
	if err := contender.Release(); err != nil {
		t.Fatalf("release successor SQLite possession: %v", err)
	}
}

func newSQLiteProofSession(t *testing.T, maximum int) (*sqliteSession, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(maximum)
	db.SetMaxIdleConns(4)
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve session fixture source")
	}
	source, err := yamlsource.LoadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec, err = runtimecontracts.AdmitPlatformSpecValue(source.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, plan := range plans {
		if plan.TableName != "runtime_startup_authority_facts" && plan.TableName != "author_activity_order" {
			continue
		}
		statements, err := schemastore.SQLiteStatementsForPlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range statements {
			if _, err := db.Exec(statement); err != nil {
				t.Fatalf("create canonical %s: %v", plan.TableName, err)
			}
		}
		created++
	}
	if created != 2 {
		t.Fatalf("session fixture requires authority and mutation-order tables: created=%d", created)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatalf("construct SQLite backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "sqlite-monitor-release", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "sqlite_retained_owner")
	if err != nil {
		t.Fatalf("construct authority: %v", err)
	}
	if err := backend.RunTransaction(context.Background(), "seed runtime process authority", func(ctx context.Context, tx *sql.Tx) error {
		return recordAuthorityTransitionTx(ctx, tx, nil, authority, true)
	}); err != nil {
		t.Fatalf("seed authority: %v", err)
	}

	retained, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire retained SQLite possession: %v", err)
	}
	t.Cleanup(func() { _ = retained.Release() })
	proof, err := backend.RetainOwnershipProof(context.Background())
	if err != nil {
		t.Fatalf("retain SQLite proof access: %v", err)
	}
	owner := &StartupSQLiteOwner{backend: backend, path: path, schemaGuard: func() error { return nil }}
	return &sqliteSession{owner: owner, authority: authority, possession: retained, proof: proof}, db
}
