//go:build darwin || linux

package startupownership

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestPostgresPossessionObservationUsesIndependentExactSessionAndKey(t *testing.T) {
	dsn, db, _ := testutil.StartEmptyPostgres(t)
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := postgresbackend.NewWithInspectionConfig(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	owner := &StartupPostgresOwner{backend: b}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lease, held, err := postgresbackend.AcquireAdvisoryLockLease(ctx, b, runtimeSharedStoreOwnershipLock)
	if err != nil || !held {
		t.Fatalf("boot possession: held=%t err=%v", held, err)
	}
	defer func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	for range 3 {
		result, err := owner.ProbePossession(ctx)
		if err != nil || result.Available || result.Validate() != nil {
			t.Fatalf("held exact lock looked free: %+v %v", result, err)
		}
	}
	if err := lease.ProveCurrent(ctx); err != nil {
		t.Fatalf("observation unlocked boot's session: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		result, err := owner.ProbePossession(ctx)
		if err != nil || !result.Available || result.Validate() != nil {
			t.Fatalf("released exact lock: %+v %v", result, err)
		}
	}
	// A past successful observation grants nothing to a future boot.
	competitor, held, err := postgresbackend.AcquireAdvisoryLockLease(ctx, b, runtimeSharedStoreOwnershipLock)
	if err != nil || !held {
		t.Fatalf("contender after observation: held=%t err=%v", held, err)
	}
	defer func() { _ = competitor.Release(context.Background()) }()
	if observed, err := owner.ProbePossession(ctx); err != nil || observed.Available {
		t.Fatalf("observation carried stale permission: %+v %v", observed, err)
	}
	var tables int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("possession observation created authority/schema: %d %v", tables, err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := owner.ProbePossession(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-acquisition cancellation: %v", err)
	}
}

func TestPossessionObservationExternalProcessReleaseAndCrashBothStores(t *testing.T) {
	testPossessionObservationExternalProcessReleaseAndCrash(t, []string{"sqlite", "postgres"})
}

func TestSQLitePossessionObservationExternalProcessReleaseAndCrash(t *testing.T) {
	testPossessionObservationExternalProcessReleaseAndCrash(t, []string{"sqlite"})
}

func testPossessionObservationExternalProcessReleaseAndCrash(t *testing.T, backends []string) {
	t.Helper()
	if backend := os.Getenv("SWARM_TEST_POSSESSION_OBSERVER_CHILD"); backend != "" {
		ctx := context.Background()
		var release func() error
		if backend == "sqlite" {
			possession, err := acquireSQLiteFilePossession(os.Getenv("SWARM_TEST_POSSESSION_OBSERVER_PATH"))
			if err != nil {
				t.Fatal(err)
			}
			release = possession.Release
		} else {
			db, err := sql.Open("postgres", os.Getenv("SWARM_TEST_POSSESSION_OBSERVER_DSN"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			lease, held, err := postgresbackend.AcquireAdvisoryLockLease(ctx, backend, runtimeSharedStoreOwnershipLock)
			if err != nil || !held {
				t.Fatalf("child possession: %t %v", held, err)
			}
			release = func() error { return lease.Release(ctx) }
		}
		if _, err := os.Stdout.WriteString("possession-held\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := bufio.NewReader(os.Stdin).ReadByte(); err != nil {
			t.Fatal(err)
		}
		if err := release(); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, backend := range backends {
		for _, ending := range []string{"release", "crash"} {
			t.Run(backend+"/"+ending, func(t *testing.T) {
				var probe func(context.Context) (bool, error)
				var path, dsn string
				if backend == "sqlite" {
					path = filepath.Join(t.TempDir(), "store.db")
					possession, err := acquireSQLiteConstructionPossession(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := possession.Release(); err != nil {
						t.Fatal(err)
					}
					identity, err := CaptureSQLiteInspectionIdentity(path)
					if err != nil {
						t.Fatal(err)
					}
					probe = func(ctx context.Context) (bool, error) { return probeSQLitePossession(ctx, path, identity) }
				} else {
					var db *sql.DB
					dsn, db, _ = testutil.StartEmptyPostgres(t)
					cfg, err := pq.NewConfig(dsn)
					if err != nil {
						t.Fatal(err)
					}
					b, err := postgresbackend.NewWithInspectionConfig(db, cfg)
					if err != nil {
						t.Fatal(err)
					}
					probe = func(ctx context.Context) (bool, error) {
						return b.ObserveAdvisoryLock(ctx, runtimeSharedStoreOwnershipLock)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPossessionObservationExternalProcessReleaseAndCrashBothStores$")
				cmd.Env = append(os.Environ(), "SWARM_TEST_POSSESSION_OBSERVER_CHILD="+backend, "SWARM_TEST_POSSESSION_OBSERVER_PATH="+path, "SWARM_TEST_POSSESSION_OBSERVER_DSN="+dsn)
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				stdin, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				var diagnostic bytes.Buffer
				cmd.Stderr = &diagnostic
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				joined := false
				defer func() {
					if !joined {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
					}
					_ = stdin.Close()
				}()
				if ready, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || ready != "possession-held\n" {
					t.Fatalf("child not held: %q %v %s", ready, err, &diagnostic)
				}
				if available, err := probe(ctx); available || (backend == "postgres" && err != nil) || (backend == "sqlite" && !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired)) {
					t.Fatalf("external contention: %t %v", available, err)
				}
				if ending == "crash" {
					if err := cmd.Process.Kill(); err != nil {
						t.Fatal(err)
					}
				} else if _, err := stdin.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				waitErr := cmd.Wait()
				joined = true
				if ending == "release" && waitErr != nil {
					t.Fatalf("child release: %v %s", waitErr, &diagnostic)
				}
				deadline := time.Now().Add(time.Second)
				for {
					available, err := probe(ctx)
					if available && err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("released child escaped possession: %t %v", available, err)
					}
					time.Sleep(time.Millisecond)
				}
			})
		}
	}
}

func TestSQLitePossessionObservationDoesNotCreateOrRepairCoordinates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := CaptureSQLiteInspectionIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if free, err := probeSQLitePossession(ctx, path, identity); err == nil || free {
		t.Fatalf("missing coordinate looked free: %t %v", free, err)
	}
	if _, err := os.Lstat(path + sqlitePossessionSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("probe created coordinate: %v", err)
	}
	if err := os.WriteFile(path+sqlitePossessionSuffix, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err = CaptureSQLiteInspectionIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	possession, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = possession.Release() }()
	if free, err := probeSQLitePossession(ctx, path, identity); free || err == nil {
		t.Fatalf("held coordinate looked free: %t %v", free, err)
	} else {
		var acquisition *runtimestartupownership.AcquisitionError
		if !errors.As(err, &acquisition) || acquisition.Failure != runtimestartupownership.AcquisitionTakeoverRequired {
			t.Fatalf("lost exact held classification: %v", err)
		}
	}
	if err := possession.ProveCurrent(ctx); err != nil {
		t.Fatalf("probe unlocked holder: %v", err)
	}
	if err := possession.Release(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if free, err := probeSQLitePossession(ctx, path, identity); !free || err != nil {
			t.Fatalf("released coordinate: %t %v", free, err)
		}
	}
	contender, err := acquireSQLiteFilePossession(path)
	if err != nil {
		t.Fatalf("probe escaped possession: %v", err)
	}
	if err := contender.Release(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".previous"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if free, err := probeSQLitePossession(ctx, path, identity); free || err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replacement after inspection looked free: %t %v", free, err)
	}
}

func TestSQLitePossessionObservationRefusesUnsafeAndReplacedIdentities(t *testing.T) {
	for _, target := range []string{"database", "coordinate"} {
		changes := []string{"missing", "replace", "symlink", "hardlink", "directory"}
		if target == "coordinate" {
			changes = append(changes, "permissions")
		}
		for _, change := range changes {
			t.Run(target+"/"+change, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "store.db")
				for _, file := range []string{path, path + sqlitePossessionSuffix} {
					if err := os.WriteFile(file, nil, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				identity, err := CaptureSQLiteInspectionIdentity(path)
				if err != nil {
					t.Fatal(err)
				}
				file := path
				if target == "coordinate" {
					file += sqlitePossessionSuffix
				}
				switch change {
				case "hardlink":
					err = os.Link(file, file+".alias")
				case "permissions":
					err = os.Chmod(file, 0o666)
				default:
					if err := os.Rename(file, file+".previous"); err != nil {
						t.Fatal(err)
					}
					switch change {
					case "replace":
						err = os.WriteFile(file, nil, 0o600)
					case "symlink":
						err = os.Symlink(file+".previous", file)
					case "directory":
						err = os.Mkdir(file, 0o700)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				before, beforeErr := os.Lstat(file)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if free, err := probeSQLitePossession(ctx, path, identity); free || err == nil {
					t.Fatalf("unsafe identity admitted: %t %v", free, err)
				}
				after, afterErr := os.Lstat(file)
				if (beforeErr != nil && (!errors.Is(beforeErr, os.ErrNotExist) || !errors.Is(afterErr, os.ErrNotExist))) ||
					(beforeErr == nil && (afterErr != nil || !os.SameFile(before, after) || before.Mode() != after.Mode())) {
					t.Fatalf("inspection repaired or replaced the unsafe path: before=%v after=%v", beforeErr, afterErr)
				}
			})
		}
	}
}
