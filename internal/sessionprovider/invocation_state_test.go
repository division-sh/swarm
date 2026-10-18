//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
)

func TestNativeInvocationStateLazyConcurrentAdmission(t *testing.T) {
	parent := t.TempDir()
	t.Setenv("TMPDIR", parent)
	process := worklifetime.NewProcess()
	state, err := NewInvocationState(process)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInvocationState(nil); err == nil {
		t.Fatal("missing process admitted invocation state")
	}
	if _, err := state.directory(context.Background()); err == nil {
		t.Fatal("unowned context created native state")
	}
	work, err := process.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(work.Context())
	cancel()
	if _, err := state.directory(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled construction opened state", err)
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
		t.Fatal("unadmitted construction was not lazy", entries, err)
	}
	const count = 16
	paths, failures := make([]string, count), make([]error, count)
	var workers sync.WaitGroup
	for index := range count {
		workers.Go(func() { paths[index], failures[index] = state.directory(work.Context()) })
	}
	workers.Wait()
	for index := range count {
		if failures[index] != nil || paths[index] == "" || paths[index] != paths[0] {
			t.Fatal("concurrent admitted occurrences selected different bases", paths, failures)
		}
	}
	info, err := os.Stat(paths[0])
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatal("native invocation base is not restrictive", info, err)
	}
	process.Retire()
	stopped, stop := context.WithCancel(context.Background())
	stop()
	receipt, err := process.Join(stopped)
	if !errors.Is(err, context.Canceled) || state.Retire(receipt) == nil {
		t.Fatal("an incomplete join authorized native deletion", err)
	}
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatal("unjoined admitted work lost private state", err)
	}
	work.Done()
	receipt, err = process.Join(context.Background())
	if err != nil || state.Retire(receipt) != nil {
		t.Fatal("eventual complete join failed retirement", err)
	}
	if _, err := os.Stat(paths[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retired invocation left state", err)
	}
	if _, err := state.directory(work.Context()); err == nil {
		t.Fatal("late construction reopened retired namespace")
	}
	if err := state.Retire(receipt); err != nil {
		t.Fatal("exact retirement replay failed", err)
	}
}

func TestNativeInvocationStateCleanupRefusesUnsafeEvidence(t *testing.T) {
	for _, mutation := range []string{"base_permissions", "base_replaced", "base_symlink", "parent_replaced", "file_permissions", "file_hardlink", "file_symlink", "possession"} {
		t.Run(mutation, func(t *testing.T) {
			parent := t.TempDir()
			t.Setenv("TMPDIR", parent)
			process := worklifetime.NewProcess()
			state, err := NewInvocationState(process)
			if err != nil {
				t.Fatal(err)
			}
			work, err := process.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			base, err := state.directory(work.Context())
			if err != nil {
				t.Fatal(err)
			}
			connection, err := openSessionDirectory(base, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			header := filepath.Join(connection.path, "session.json")
			if mutation != "possession" {
				if err := connection.release(); err != nil {
					t.Fatal(err)
				}
			}
			restore := mutateNativeInvocationFixture(t, mutation, parent, base, header, connection)
			work.Done()
			process.Retire()
			receipt, err := process.Join(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := state.Retire(receipt); err == nil || state.parent == nil || state.base == nil {
				t.Fatal("unsafe cleanup succeeded or lost its exact ownership", err)
			}
			restore()
			if err := state.Retire(receipt); err != nil {
				t.Fatal("repair of cleanup obstruction could not finish retained responsibility", err)
			}
			if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("successful cleanup retry left private state", err)
			}
		})
	}
}

func mutateNativeInvocationFixture(t *testing.T, mutation, parent, base, header string, connection *sessionDirectory) func() {
	t.Helper()
	require := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	switch mutation {
	case "base_permissions":
		require(os.Chmod(base, 0o755))
		return func() { require(os.Chmod(base, 0o700)) }
	case "base_replaced", "base_symlink":
		require(os.Rename(base, base+"-original"))
		if mutation == "base_symlink" {
			require(os.Symlink(base+"-original", base))
		} else {
			require(os.Mkdir(base, 0o700))
		}
		return func() { require(os.Remove(base)); require(os.Rename(base+"-original", base)) }
	case "parent_replaced":
		require(os.Rename(parent, parent+"-original"))
		require(os.Mkdir(parent, 0o700))
		return func() { require(os.Remove(parent)); require(os.Rename(parent+"-original", parent)) }
	case "file_permissions":
		require(os.Chmod(header, 0o644))
		return func() { require(os.Chmod(header, 0o600)) }
	case "file_hardlink":
		link := filepath.Join(t.TempDir(), "foreign-header")
		require(os.Link(header, link))
		return func() { require(os.Remove(link)) }
	case "file_symlink":
		require(os.Rename(header, header+"-original"))
		require(os.Symlink(header+"-original", header))
		return func() { require(os.Remove(header)); require(os.Rename(header+"-original", header)) }
	case "possession":
		return func() { require(connection.release()) }
	default:
		t.Fatal("unknown fixture mutation", mutation)
		return nil
	}
}

func TestNativeInvocationStatePartialStartupAndForeignReceipt(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	process := worklifetime.NewProcess()
	state, err := NewInvocationState(process)
	if err != nil {
		t.Fatal(err)
	}
	work, err := process.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	base, err := state.directory(work.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(base, uuid.NewString()), 0o700); err != nil {
		t.Fatal(err)
	}
	foreign := worklifetime.NewProcess()
	foreign.Retire()
	wrong, err := foreign.Join(context.Background())
	if err != nil || state.Retire(wrong) == nil {
		t.Fatal("another process join deleted partially constructed native state", err)
	}
	work.Done()
	process.Retire()
	receipt, err := process.Join(context.Background())
	if err != nil || state.Retire(receipt) != nil {
		t.Fatal("fully joined partial startup could not clean its exact namespace", err)
	}
	if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial startup leaked invocation state", err)
	}
}
