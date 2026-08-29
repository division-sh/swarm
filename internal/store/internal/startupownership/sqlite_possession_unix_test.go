//go:build darwin || linux

package startupownership

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

func TestSQLitePossessionCoordinateDoesNotBlockDatabaseWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	guard, err := AcquireSQLiteConstructionGuard(path)
	if err != nil {
		t.Fatalf("acquire construction guard: %v", err)
	}
	defer func() {
		if err := guard.Release(); err != nil {
			t.Errorf("release construction guard: %v", err)
		}
	}()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite backend: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`CREATE TABLE possession_probe (value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table while possession is held: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO possession_probe (value) VALUES ('engine-disjoint')`); err != nil {
		t.Fatalf("write while possession is held: %v", err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM possession_probe`).Scan(&value); err != nil || value != "engine-disjoint" {
		t.Fatalf("read while possession is held value=%q err=%v", value, err)
	}
}

func TestSQLiteProcessCapabilityRejectsAliasPath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	if err := os.WriteFile(path, []byte("sqlite-identity"), 0o600); err != nil {
		t.Fatal(err)
	}

	possession, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire canonical possession: %v", err)
	}
	t.Cleanup(func() { _ = possession.Release() })
	if _, err := acquireSQLiteFilePossession(path); !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
		t.Fatalf("concurrent canonical acquisition error=%v, want takeover_required", err)
	}

	symlink := filepath.Join(root, "runtime-symlink.db")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireSQLiteFilePossession(symlink); !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionPriorOwnerAmbiguous) {
		t.Fatalf("symlink acquisition error=%v, want prior_owner_ambiguous", err)
	}
}

func TestSQLiteFilePossessionRejectsHardLinkIdentity(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	alias := filepath.Join(root, "runtime-hardlink.db")
	if err := os.WriteFile(path, []byte("sqlite-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireSQLiteFilePossession(path); !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionPriorOwnerAmbiguous) {
		t.Fatalf("hard-linked canonical acquisition error=%v, want prior_owner_ambiguous", err)
	}
	if _, err := acquireSQLiteFilePossession(alias); !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionPriorOwnerAmbiguous) {
		t.Fatalf("hard-link alias acquisition error=%v, want prior_owner_ambiguous", err)
	}
}

func TestSQLiteProcessCapabilityFileIdentityReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	if err := os.WriteFile(path, []byte("sqlite-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	possession, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire possession: %v", err)
	}
	t.Cleanup(func() { _ = possession.Release() })
	if err := os.Rename(path, path+".retired"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = possession.ProveCurrent(context.Background())
	var possessionErr *runtimestartupownership.PossessionError
	if !errors.As(err, &possessionErr) || possessionErr.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("replacement proof error=%v, want ownership_unprovable", err)
	}
	contender, contenderErr := acquireSQLiteFilePossession(path)
	if contender != nil {
		_ = contender.Release()
	}
	if !isSQLitePossessionFailure(contenderErr, runtimestartupownership.AcquisitionTakeoverRequired) {
		t.Fatalf("contender after database replacement error=%v, want takeover_required", contenderErr)
	}
}

func TestSQLiteProcessCapabilityCoordinateReplacementFailsClosed(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	if err := os.WriteFile(path, []byte("sqlite-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	possession, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire possession: %v", err)
	}
	t.Cleanup(func() { _ = possession.Release() })
	coordinatePath := path + sqlitePossessionSuffix
	if err := os.Rename(coordinatePath, coordinatePath+".retired"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(coordinatePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err = possession.ProveCurrent(context.Background())
	var possessionErr *runtimestartupownership.PossessionError
	if !errors.As(err, &possessionErr) || possessionErr.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("coordinate replacement proof error=%v, want ownership_unprovable", err)
	}
}

func TestSQLiteProcessCapabilityMissingIdentityFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name       string
		removePath func(databasePath string) string
	}{
		{name: "database", removePath: func(databasePath string) string { return databasePath }},
		{name: "coordinate", removePath: func(databasePath string) string { return databasePath + sqlitePossessionSuffix }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.db")
			possession, err := acquireSQLiteConstructionPossession(path)
			if err != nil {
				t.Fatalf("acquire possession: %v", err)
			}
			defer func() { _ = possession.Release() }()

			if err := os.Remove(test.removePath(path)); err != nil {
				t.Fatalf("remove retained %s identity: %v", test.name, err)
			}
			err = possession.ProveCurrent(context.Background())
			var possessionErr *runtimestartupownership.PossessionError
			if !errors.As(err, &possessionErr) || possessionErr.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
				t.Fatalf("missing %s identity proof error=%v, want ownership_unprovable", test.name, err)
			}
		})
	}
}

func TestSQLitePossessionCoordinatePersistsAcrossRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	first, err := acquireSQLiteConstructionPossession(path)
	if err != nil {
		t.Fatalf("acquire initial possession: %v", err)
	}
	coordinatePath := path + sqlitePossessionSuffix
	before, err := os.Stat(coordinatePath)
	if err != nil {
		t.Fatalf("stat initial coordinate: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release initial possession: %v", err)
	}
	after, err := os.Stat(coordinatePath)
	if err != nil {
		t.Fatalf("coordinate was removed on release: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("coordinate identity changed on release")
	}
	second, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("reacquire persistent coordinate: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release successor possession: %v", err)
	}
}

func TestSQLitePossessionCoordinateIsCloseOnExec(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	possession, err := acquireSQLiteConstructionPossession(path)
	if err != nil {
		t.Fatalf("acquire construction possession: %v", err)
	}
	defer func() { _ = possession.Release() }()
	retained, ok := possession.(*sqliteFilePossession)
	if !ok {
		t.Fatalf("possession type=%T, want retained file possession", possession)
	}
	flags, err := unix.FcntlInt(retained.coordinate.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("read coordinate descriptor flags: %v", err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("possession coordinate descriptor is inherited across exec")
	}
}

func TestSQLiteBackendIdentityMatchingUsesDatabaseAndCoordinate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	reference, err := acquireSQLiteConstructionPossession(path)
	if err != nil {
		t.Fatalf("acquire reference possession: %v", err)
	}
	if err := reference.Release(); err != nil {
		t.Fatalf("release reference possession: %v", err)
	}
	samePair, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("reacquire unchanged pair: %v", err)
	}
	if !sameSQLitePossessionResource(reference, samePair) {
		t.Fatal("unchanged database/coordinate pair did not match")
	}
	if err := samePair.Release(); err != nil {
		t.Fatalf("release unchanged pair: %v", err)
	}

	coordinatePath := path + sqlitePossessionSuffix
	if err := os.Rename(coordinatePath, coordinatePath+".retired"); err != nil {
		t.Fatalf("replace coordinate identity: %v", err)
	}
	replacedPair, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire replaced coordinate pair: %v", err)
	}
	defer func() { _ = replacedPair.Release() }()
	if sameSQLitePossessionResource(reference, replacedPair) {
		t.Fatal("backend identity matched after possession-coordinate replacement")
	}
}

func TestSQLitePossessionRejectsUnsafeCoordinate(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, coordinatePath string)
	}{
		{
			name: "symlink",
			prepare: func(t *testing.T, coordinatePath string) {
				t.Helper()
				target := coordinatePath + ".target"
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, coordinatePath); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "hard_link",
			prepare: func(t *testing.T, coordinatePath string) {
				t.Helper()
				if err := os.WriteFile(coordinatePath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(coordinatePath, coordinatePath+".alias"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "non_owner_only_mode",
			prepare: func(t *testing.T, coordinatePath string) {
				t.Helper()
				if err := os.WriteFile(coordinatePath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(coordinatePath, 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.db")
			if err := os.WriteFile(path, []byte("sqlite-identity"), 0o600); err != nil {
				t.Fatal(err)
			}
			test.prepare(t, path+sqlitePossessionSuffix)
			possession, err := acquireSQLiteFilePossession(path)
			if possession != nil {
				_ = possession.Release()
			}
			if !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionPriorOwnerAmbiguous) {
				t.Fatalf("unsafe coordinate acquisition error=%v, want prior_owner_ambiguous", err)
			}
		})
	}
}

const sqlitePossessionChildMarker = "SWARM_TEST_SQLITE_POSSESSION_CHILD"

func TestSQLitePossessionSubprocessContentionAndSIGKILLReclaim(t *testing.T) {
	if os.Getenv(sqlitePossessionChildMarker) == "1" {
		path := os.Getenv("SWARM_TEST_SQLITE_POSSESSION_PATH")
		readyPath := os.Getenv("SWARM_TEST_SQLITE_POSSESSION_READY")
		possession, err := acquireSQLiteConstructionPossession(path)
		if err != nil {
			t.Fatalf("child acquire possession: %v", err)
		}
		defer func() { _ = possession.Release() }()
		if err := os.WriteFile(readyPath, nil, 0o600); err != nil {
			t.Fatalf("child signal ready: %v", err)
		}
		select {}
	}

	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	readyPath := filepath.Join(root, "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestSQLitePossessionSubprocessContentionAndSIGKILLReclaim$")
	cmd.Env = append(os.Environ(),
		sqlitePossessionChildMarker+"=1",
		"SWARM_TEST_SQLITE_POSSESSION_PATH="+path,
		"SWARM_TEST_SQLITE_POSSESSION_READY="+readyPath,
	)
	var childOutput bytes.Buffer
	cmd.Stdout = &childOutput
	cmd.Stderr = &childOutput
	if err := cmd.Start(); err != nil {
		t.Fatalf("start possession child: %v", err)
	}
	childExited := false
	t.Cleanup(func() {
		if !childExited && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	waitForSQLitePossessionChild(t, cmd, readyPath, &childOutput)

	coordinatePath := path + sqlitePossessionSuffix
	before, err := os.Stat(coordinatePath)
	if err != nil {
		t.Fatalf("stat child coordinate: %v", err)
	}
	contender, err := acquireSQLiteConstructionPossession(path)
	if contender != nil {
		_ = contender.Release()
	}
	if !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
		t.Fatalf("subprocess contention error=%v, want takeover_required", err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL possession child: %v", err)
	}
	waitErr := cmd.Wait()
	childExited = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("child wait error=%v, want SIGKILL exit; output:\n%s", waitErr, childOutput.String())
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child wait status=%v, want SIGKILL; output:\n%s", exitErr.Sys(), childOutput.String())
	}

	successor, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("acquire after SIGKILL: %v; child output:\n%s", err, childOutput.String())
	}
	defer func() { _ = successor.Release() }()
	after, err := os.Stat(coordinatePath)
	if err != nil {
		t.Fatalf("stat reclaimed coordinate: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("SIGKILL reclaim replaced the persistent coordinate")
	}
}

func waitForSQLitePossessionChild(t *testing.T, cmd *exec.Cmd, readyPath string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyPath); err == nil {
			return
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("possession child exited before ready: %v\n%s", cmd.ProcessState, output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("possession child did not become ready; output:\n%s", output.String())
}

func isSQLitePossessionFailure(err error, failure runtimestartupownership.AcquisitionFailure) bool {
	var acquisitionErr *runtimestartupownership.AcquisitionError
	return errors.As(err, &acquisitionErr) && acquisitionErr.Failure == failure
}
