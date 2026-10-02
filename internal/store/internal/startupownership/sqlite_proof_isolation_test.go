//go:build darwin || linux

package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func TestSQLiteOwnershipProofSurvivesWorkloadPoolPressure(t *testing.T) {
	for _, maximum := range []int{1, 4} {
		t.Run(fmt.Sprintf("workload_capacity_%d", maximum), func(t *testing.T) {
			session, db := newSQLiteProofSession(t, maximum)
			terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 2)}
			if err := session.InstallTerminalOwner(terminal, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			var held []*sql.Conn
			closeHeld := func() {
				for _, conn := range held {
					_ = conn.Close()
				}
				held = nil
			}
			t.Cleanup(closeHeld)
			for i := 0; i < maximum; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := db.Conn(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, conn)
			}
			before := db.Stats()
			if before.InUse != maximum+1 || before.MaxOpenConnections != maximum+1 {
				t.Fatalf("incorrect proof reservation: %#v", before)
			}
			for i := 0; i < 3; i++ {
				if err := session.MonitorProveCurrent(context.Background(), 2*time.Second); err != nil {
					t.Fatalf("healthy real monitor under pool-only pressure: %v", err)
				}
				if err := session.ProveCurrent(context.Background()); err != nil {
					t.Fatalf("ordinary proof under pool-only pressure: %v", err)
				}
			}
			if after := db.Stats(); after.WaitCount != before.WaitCount {
				t.Fatalf("proof acquired workload capacity: before=%#v after=%#v", before, after)
			}
			select {
			case result := <-terminal.results:
				t.Fatalf("healthy ownership terminalized: %#v", result)
			default:
			}
			if contender, err := acquireSQLiteFilePossession(session.owner.path); contender != nil || !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
				t.Fatalf("healthy possession lost: contender=%#v err=%v", contender, err)
			}
			closeHeld()
			if err := session.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if stats := db.Stats(); stats.MaxOpenConnections != maximum || stats.InUse != 0 {
				t.Fatalf("release leaked proof reservation: %#v", stats)
			}
		})
	}
}

func TestSQLiteOwnershipProofFreshFailuresFenceAndClose(t *testing.T) {
	for _, fault := range []string{"missing head", "corrupt head", "changed head", "database replacement", "coordinate replacement", "backend disposal", "database locked"} {
		t.Run(fault, func(t *testing.T) {
			session, db := newSQLiteProofSession(t, 1)
			terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 2)}
			if err := session.InstallTerminalOwner(terminal, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			if err := session.MonitorProveCurrent(context.Background(), 2*time.Second); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "missing head":
				if _, err := db.Exec(`DELETE FROM runtime_startup_authority_facts`); err != nil {
					t.Fatal(err)
				}
			case "corrupt head":
				if _, err := db.Exec(`UPDATE runtime_startup_authority_facts SET snapshot = '{'`); err != nil {
					t.Fatal(err)
				}
			case "changed head":
				next, err := runtimestartupownership.ReleasedAuthority(session.authority)
				if err != nil {
					t.Fatal(err)
				}
				if err := session.owner.backend.RunTransaction(context.Background(), "change durable ownership proof", func(ctx context.Context, tx *sql.Tx) error {
					return recordAuthorityTransitionTx(ctx, tx, &session.authority, next, true)
				}); err != nil {
					t.Fatal(err)
				}
			case "database replacement", "coordinate replacement":
				path := session.owner.path
				if fault == "coordinate replacement" {
					path += ".possession"
				}
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "backend disposal":
				if err := session.owner.backend.Close(); err != nil {
					t.Fatal(err)
				}
			case "database locked":
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK"); _ = conn.Close() })
				if _, err := conn.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
					t.Fatal(err)
				}
			}
			started := time.Now()
			if err := session.MonitorProveCurrent(context.Background(), 2*time.Second); err == nil {
				t.Fatal("fresh fault hidden by retained connection or cached proof")
			}
			if elapsed := time.Since(started); elapsed > 5*time.Second {
				t.Fatalf("proof and readback exceeded existing budgets: %s", elapsed)
			}
			if result := <-terminal.results; result.Cause != runtimestartupownership.TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
				t.Fatalf("fault invented successor: %#v", result)
			}
			if _, err := session.Authority(); err == nil {
				t.Fatal("failed proof retained live authority")
			}
			if stats := db.Stats(); stats.MaxOpenConnections != 1 {
				t.Fatalf("terminalization leaked capacity: %#v", stats)
			}
		})
	}
}

func TestSQLiteOwnershipProofFailedReleaseRearmsAndSerializes(t *testing.T) {
	session, db := newSQLiteProofSession(t, 1)
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	if _, err := db.Exec(`CREATE TRIGGER fail_release BEFORE INSERT ON runtime_startup_authority_facts WHEN NEW.state = 'released' BEGIN SELECT RAISE(ABORT, 'injected release failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(context.Background()); err == nil {
		t.Fatal("failed durable release reported success")
	}
	if _, terminal := capability.TerminalResult(); terminal {
		t.Fatal("failed release retired healthy possession")
	}
	if stats := db.Stats(); stats.MaxOpenConnections != 2 || stats.InUse != 1 {
		t.Fatalf("failed release disposed its reproof resource: %#v", stats)
	}
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("failed-release reproof: %v", err)
	}
	if _, err := db.Exec("DROP TRIGGER fail_release"); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(3)
	start := make(chan struct{})
	for i := 0; i < 3; i++ {
		go func() { defer group.Done(); <-start; _ = capability.ProveCurrent(context.Background()) }()
	}
	close(start)
	if err := capability.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.Wait()
	if result, terminal := capability.TerminalResult(); !terminal || result.Cause != runtimestartupownership.TerminalReleased {
		t.Fatalf("release raced with proof: %#v terminal=%t", result, terminal)
	}
	if stats := db.Stats(); stats.MaxOpenConnections != 1 || stats.InUse != 0 {
		t.Fatalf("release did not join proof access: %#v", stats)
	}
}

func TestSQLiteOwnershipProofStartupFailureCleansUp(t *testing.T) {
	for _, phase := range []string{"reservation", "authority transaction", "pipeline assembly"} {
		t.Run(phase, func(t *testing.T) {
			session, db := newSQLiteProofSession(t, 1)
			if err := session.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			owner := session.owner
			owner.fanOutPipeline = &storepipeline.PipelineSQLiteOwner{}
			if _, err := db.Exec(`DELETE FROM runtime_startup_authority_facts`); err != nil {
				t.Fatal(err)
			}
			switch phase {
			case "reservation":
				if err := owner.backend.Close(); err != nil {
					t.Fatal(err)
				}
			case "authority transaction":
				if _, err := db.Exec(`CREATE TRIGGER fail_acquire BEFORE INSERT ON runtime_startup_authority_facts BEGIN SELECT RAISE(ABORT, 'injected acquisition failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			capability, err := owner.AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{OwnerID: "startup-cleanup", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
			if err == nil || capability != nil {
				t.Fatalf("startup did not refuse: capability=%#v err=%v", capability, err)
			}
			if stats := db.Stats(); stats.MaxOpenConnections != 1 || stats.InUse != 0 {
				t.Fatalf("startup leaked reservation: %#v", stats)
			}
			contender, err := acquireSQLiteFilePossession(owner.path)
			if err != nil {
				t.Fatalf("startup leaked file possession: %v", err)
			}
			if err := contender.Release(); err != nil {
				t.Fatal(err)
			}
			if phase != "reservation" {
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 0
				if phase == "pipeline assembly" {
					want = 1
				}
				if count != want {
					t.Fatalf("startup changed transaction semantics: facts=%d want=%d", count, want)
				}
			}
		})
	}
}

func TestSQLiteOwnershipProofStallStillUsesTwoSecondDeadline(t *testing.T) {
	session, _ := newSQLiteProofSession(t, 1)
	session.possession = &cancellingSQLitePossession{delegate: session.possession, entered: make(chan struct{}), phase: "os proof"}
	terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 2)}
	if err := session.InstallTerminalOwner(terminal, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err := session.MonitorProveCurrent(context.Background(), 2*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled real monitor = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 2*time.Second || elapsed > 3*time.Second {
		t.Fatalf("existing two-second budget changed: %s", elapsed)
	}
	if result := <-terminal.results; result.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("stalled proof not fenced: %#v", result)
	}
}

func TestSQLiteOwnershipProofSuccessorReadbackUsesReservedFreshAccess(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt_successor_%t", corrupt), func(t *testing.T) {
			session, db := newSQLiteProofSession(t, 1)
			terminal := &sqliteSessionTerminalProbe{results: make(chan runtimestartupownership.TerminalResult, 2)}
			if err := session.InstallTerminalOwner(terminal, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			if err := session.MonitorProveCurrent(context.Background(), 2*time.Second); err != nil {
				t.Fatal(err)
			}
			successor, err := runtimestartupownership.NewAuthority(runtimestartupownership.AcquireRequest{
				OwnerID: "fresh-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
			}, session.authority.Backend, 2, session.authority.AuthorityID, runtimestartupownership.AcquisitionCrashTakeover)
			if err != nil {
				t.Fatal(err)
			}
			superseded, err := runtimestartupownership.SupersededAuthority(session.authority, successor.AuthorityID)
			if err != nil {
				t.Fatal(err)
			}
			if err := session.owner.backend.RunTransaction(context.Background(), "seed exact successor proof", func(ctx context.Context, tx *sql.Tx) error {
				if err := recordAuthorityTransitionTx(ctx, tx, &session.authority, superseded, true); err != nil {
					return err
				}
				return recordAuthorityTransitionTx(ctx, tx, nil, successor, true)
			}); err != nil {
				t.Fatal(err)
			}
			held, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if corrupt {
				if _, err := held.ExecContext(context.Background(), `UPDATE runtime_startup_authority_facts SET snapshot = '{' WHERE authority_id = ?`, successor.AuthorityID); err != nil {
					t.Fatal(err)
				}
			}
			before := db.Stats()
			if err := session.ProveCurrent(context.Background()); err == nil {
				t.Fatal("retained connection hid a later authority transition")
			}
			if first := <-terminal.results; first.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
				t.Fatalf("readback preceded local fencing: %#v", first)
			}
			result := <-terminal.results
			if corrupt {
				if result.Cause != runtimestartupownership.TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
					t.Fatalf("malformed successor accepted: %#v", result)
				}
			} else if result.Cause != runtimestartupownership.TerminalOwnershipSuperseded || result.SuccessorAuthorityID != successor.AuthorityID {
				t.Fatalf("lost exact fresh successor: %#v", result)
			}
			if after := db.Stats(); after.WaitCount != before.WaitCount || after.MaxOpenConnections != 1 || after.InUse != 1 {
				t.Fatalf("terminal readback used workload pool or leaked its reservation: before=%#v after=%#v", before, after)
			}
		})
	}
}

type notifyingSQLitePossession struct {
	sqlitePossession
	observed chan struct{}
}

func (p *notifyingSQLitePossession) ProveCurrent(ctx context.Context) error {
	if err := p.sqlitePossession.ProveCurrent(ctx); err != nil {
		return err
	}
	select {
	case p.observed <- struct{}{}:
	default:
	}
	return nil
}

func TestSQLiteOwnershipProofPeriodicMonitorSurvivesPoolPressure(t *testing.T) {
	session, db := newSQLiteProofSession(t, 1)
	observed := make(chan struct{}, 1)
	session.possession = &notifyingSQLitePossession{sqlitePossession: session.possession, observed: observed}
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	held, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("real periodic monitor did not attempt proof")
	}
	// This call waits for the observed monitor's serialized proof to finish.
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("periodic monitor withdrew healthy ownership: %v", err)
	}
	if _, terminal := capability.TerminalResult(); terminal {
		t.Fatal("pool-only pressure terminalized the real capability")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
