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
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
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

func TestSQLiteProcessObservationSeparatesPossessionFromAdmittedStamp(t *testing.T) {
	session, db := newSQLiteProofSession(t, 1)
	ctx := context.Background()
	session.owner.catalogEmpty = func(context.Context) (bool, error) { return false, nil }
	before, err := generationauthority.ReadProcessGeneration(ctx, db, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, released := range []bool{false, true} {
		if released {
			if err := session.Release(ctx); err != nil {
				t.Fatal(err)
			}
		}
		observation, err := session.owner.ProbePossession(ctx)
		if err != nil || observation.Available != released || observation.Validate() != nil {
			t.Fatalf("native availability: released=%t observation=%#v, %v", released, observation, err)
		}
		inspection, err := session.owner.InspectAuthority(ctx)
		if err != nil || inspection.Authority == nil || *inspection.Authority != session.authority || inspection.Possession != nil {
			t.Fatalf("MVCC stamp reader inferred native possession: %#v, %v", inspection, err)
		}
	}
	after, err := generationauthority.ReadProcessGeneration(ctx, db, true)
	if err != nil || after != before {
		t.Fatalf("read-only native observation changed generation: %#v -> %#v, %v", before, after, err)
	}
}

func TestSQLiteOwnershipProofFreshFailuresFenceWithoutEarlyUnlock(t *testing.T) {
	for _, fault := range []string{"missing generation", "corrupt generation", "crossed stamp", "database replacement", "coordinate replacement", "backend disposal", "database locked"} {
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
			case "missing generation":
				if _, err := db.Exec(`DELETE FROM runtime_process_generation`); err != nil {
					t.Fatal(err)
				}
			case "corrupt generation":
				if _, err := db.Exec(`UPDATE runtime_process_generation SET lease_id = 'not-a-uuid'`); err != nil {
					t.Fatal(err)
				}
			case "crossed stamp":
				if _, err := db.Exec(`UPDATE runtime_process_generation SET lease_id = ?`, uuid.NewString()); err != nil {
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
			if fault != "backend disposal" && db.Stats().MaxOpenConnections != 2 {
				t.Fatalf("terminal notification disposed reserved access before joined release: %#v", db.Stats())
			}
			// A replaced coordinate is already a native ownership loss, not a safe
			// unlock. Every still-valid coordinate must exclude a successor until join.
			if fault != "coordinate replacement" {
				if contender, err := acquireSQLiteFilePossession(session.owner.path); contender != nil || !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
					t.Fatalf("terminal notification unlocked before accepted work joined: contender=%#v err=%v", contender, err)
				}
			}
			if err := session.Release(context.Background()); err != nil {
				t.Fatalf("joined release: %v", err)
			}
			if stats := db.Stats(); stats.MaxOpenConnections != 1 {
				t.Fatalf("joined release leaked reserved capacity: %#v", stats)
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
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := capability.Release(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled release error=%v, want caller cancellation", err)
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
	stamp, err := generationauthority.ReadProcessGeneration(context.Background(), db, true)
	if err != nil || stamp != session.authority.LeaseStamp() {
		t.Fatalf("release rewrote last-admitted evidence: %#v, %v", stamp, err)
	}
}

type joinedSQLiteRelease struct {
	sqlitePossession
	entered, resume chan struct{}
	once            sync.Once
	failure         error
}

func (p *joinedSQLiteRelease) Release() error {
	p.once.Do(func() { close(p.entered); <-p.resume })
	if p.failure != nil {
		return p.failure
	}
	return p.sqlitePossession.Release()
}

func TestSQLiteProcessReleaseJoinsNativeDisposal(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("release_failure_%t", fail), func(t *testing.T) {
			session, db := newSQLiteProofSession(t, 1)
			injected := errors.New("injected native possession release failure")
			release := &joinedSQLiteRelease{sqlitePossession: session.possession, entered: make(chan struct{}), resume: make(chan struct{})}
			t.Cleanup(func() {
				select {
				case <-release.resume:
				default:
					close(release.resume)
				}
			})
			if fail {
				release.failure = injected
			}
			session.possession = release
			done := make(chan error, 1)
			go func() { done <- session.Release(context.Background()) }()
			select {
			case <-release.entered:
			case <-time.After(time.Second):
				t.Fatal("native release did not start")
			}
			select {
			case err := <-done:
				t.Fatalf("release returned before joining native disposal: %v", err)
			default:
			}
			if peer, err := acquireSQLiteFilePossession(session.owner.path); peer != nil || !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
				t.Fatalf("unjoined native release admitted a successor: %#v, %v", peer, err)
			}
			close(release.resume)
			err := <-done
			if fail {
				if !errors.Is(err, injected) {
					t.Fatalf("native failure lost: %v", err)
				}
				if _, err := session.Authority(); err == nil {
					t.Fatal("failed disposal reissued a capability")
				}
				if peer, err := acquireSQLiteFilePossession(session.owner.path); peer != nil || !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
					t.Fatalf("failed disposal unlocked possession: %#v, %v", peer, err)
				}
				release.failure = nil
				if err := session.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			peer, err := acquireSQLiteFilePossession(session.owner.path)
			if err != nil {
				t.Fatalf("joined disposal did not release possession: %v", err)
			}
			if err := peer.Release(); err != nil {
				t.Fatal(err)
			}
			stamp, err := generationauthority.ReadProcessGeneration(context.Background(), db, true)
			if err != nil || stamp != session.authority.LeaseStamp() {
				t.Fatalf("disposal changed durable evidence: %#v, %v", stamp, err)
			}
		})
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
			switch phase {
			case "reservation":
				if err := owner.backend.Close(); err != nil {
					t.Fatal(err)
				}
			case "authority transaction":
				if _, err := db.Exec(`CREATE TRIGGER fail_acquire BEFORE UPDATE ON runtime_process_generation BEGIN SELECT RAISE(ABORT, 'injected acquisition failure'); END`); err != nil {
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
				var count, generation int
				if err := db.QueryRow(`SELECT COUNT(*), generation FROM runtime_process_generation`).Scan(&count, &generation); err != nil {
					t.Fatal(err)
				}
				want := 1
				if phase == "pipeline assembly" {
					want = 2
				}
				if count != 1 || generation != want {
					t.Fatalf("startup changed acknowledged transaction semantics: rows=%d generation=%d want=%d", count, generation, want)
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
			// This injects diagnostic readback only, not a second admitted holder.
			successor := session.authority
			successor.AuthorityID = uuid.NewString()
			successor.AuthorityGeneration++
			if _, err := db.Exec(`UPDATE runtime_process_generation SET lease_id = ?, generation = ?`, successor.AuthorityID, successor.AuthorityGeneration); err != nil {
				t.Fatal(err)
			}
			held, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if corrupt {
				if _, err := held.ExecContext(context.Background(), `UPDATE runtime_process_generation SET lease_id = 'not-a-uuid'`); err != nil {
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
			if after := db.Stats(); after.WaitCount != before.WaitCount || after.MaxOpenConnections != 2 || after.InUse != 2 {
				t.Fatalf("terminal readback used workload pool or released before join: before=%#v after=%#v", before, after)
			}
			if err := session.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if after := db.Stats(); after.MaxOpenConnections != 1 || after.InUse != 1 {
				t.Fatalf("joined release leaked its reservation: %#v", after)
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
