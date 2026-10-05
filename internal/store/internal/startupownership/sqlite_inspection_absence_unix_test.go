//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestIssue2567SQLiteAbsenceRejectsAliasesAndNonDirectories(t *testing.T) {
	for _, shape := range []string{"missing_parent", "missing_leaf", "existing", "dangling_parent", "linked_parent", "dangling_leaf", "file_parent"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "state", "runtime.db")
			wantAbsent, wantError := false, false
			switch shape {
			case "missing_parent":
				wantAbsent = true
			case "missing_leaf":
				wantAbsent = true
				if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
			case "existing":
				path = filepath.Join(root, "runtime.db")
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "dangling_parent":
				wantError = true
				if err := os.Symlink(filepath.Join(root, "missing"), filepath.Dir(path)); err != nil {
					t.Fatal(err)
				}
			case "linked_parent":
				wantError = true
				target := t.TempDir()
				if err := os.Symlink(target, filepath.Dir(path)); err != nil {
					t.Fatal(err)
				}
			case "dangling_leaf":
				wantError = true
				path = filepath.Join(root, "runtime.db")
				if err := os.Symlink(filepath.Join(root, "missing.db"), path); err != nil {
					t.Fatal(err)
				}
			case "file_parent":
				wantError = true
				if err := os.WriteFile(filepath.Dir(path), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			absent, err := ObserveSQLiteInspectionAbsence(context.Background(), path)
			if absent != wantAbsent || (err != nil) != wantError {
				t.Fatalf("absence=%t error=%v, want %t/%t", absent, err, wantAbsent, wantError)
			}
			if shape == "missing_parent" {
				if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created parent: %v", err)
				}
			}
			if shape == "missing_leaf" {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created store: %v", err)
				}
			}
		})
	}
}

func TestIssue2567SQLiteAbsencePreservesSystemCanonicalAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "runtime.db")
	if runtime.GOOS == "darwin" {
		root, err := os.MkdirTemp("/var/tmp", "swarm-absence-alias-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(root); err != nil {
				t.Error(err)
			}
		})
		path = filepath.Join(root, "missing", "runtime.db")
		if !strings.HasPrefix(path, "/var/") {
			t.Fatalf("temp directory does not exercise the system alias: %s", path)
		}
	}
	if absent, err := ObserveSQLiteInspectionAbsence(context.Background(), path); err != nil || !absent {
		t.Fatalf("system coordinate was rejected: %t/%v", absent, err)
	}
}

func TestIssue2567SQLiteObservedExistingDisappearanceIsNotAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if absent, err := ObserveSQLiteInspectionAbsence(context.Background(), path); err != nil || absent {
		t.Fatalf("existing observation=%t/%v", absent, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureSQLiteInspectionIdentity(path); err == nil {
		t.Fatal("disappeared existing identity was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if absent, err := ObserveSQLiteInspectionAbsence(ctx, path); absent || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation became absence: %t/%v", absent, err)
	}
}
