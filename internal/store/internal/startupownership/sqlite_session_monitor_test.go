//go:build darwin || linux

package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
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
	path := filepath.Join(t.TempDir(), "runtime.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE runtime_startup_authority_facts (
			fact_id TEXT PRIMARY KEY,
			authority_id TEXT NOT NULL,
			authority_generation INTEGER NOT NULL,
			transition_ordinal INTEGER NOT NULL,
			state_version INTEGER NOT NULL,
			state TEXT NOT NULL,
			owner_id TEXT NOT NULL,
			boot_id TEXT NOT NULL,
			runtime_instance_id TEXT NOT NULL,
			backend TEXT NOT NULL,
			acquisition_id TEXT NOT NULL,
			acquisition_request_hash TEXT NOT NULL,
			acquisition_kind TEXT NOT NULL,
			predecessor_authority_id TEXT,
			successor_authority_id TEXT,
			snapshot TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("create authority table: %v", err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatalf("construct SQLite backend: %v", err)
	}
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
	blocking := &cancellingSQLitePossession{
		delegate: retained, entered: make(chan struct{}), resume: make(chan struct{}), phase: phase,
	}
	owner := &StartupSQLiteOwner{backend: backend, path: path, schemaGuard: func() error { return nil }}
	session := &sqliteSession{owner: owner, authority: authority, possession: blocking}
	terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 1)}
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
	if err := db.QueryRow(`SELECT state FROM runtime_startup_authority_facts WHERE authority_id=? ORDER BY transition_ordinal DESC LIMIT 1`, authority.AuthorityID).Scan(&durableState); err != nil {
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
