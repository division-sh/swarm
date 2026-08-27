package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestDevScratchEpochRejectsBorrowedProject(t *testing.T) {
	root := t.TempDir()
	state := LocalRuntimeStateResolution{
		Project: localRuntimeStateProject{CanonicalProjectRoot: root, ProjectLocal: false},
		StoreSelection: storebackend.Selection{
			Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "borrowed.db"), SQLitePathSource: storebackend.SourceSwarmDirDefault,
		},
	}
	if _, err := ResolveDevScratch(state); err == nil || !strings.Contains(err.Error(), "project-local contracts") {
		t.Fatalf("ResolveDevScratch error = %v, want borrowed-project teaching refusal", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
		t.Fatalf("borrowed project state stat error = %v, want no mutation", err)
	}
}

func TestDevScratchEpochRejectsConfiguredSQLitePath(t *testing.T) {
	root := t.TempDir()
	custom := filepath.Join(t.TempDir(), "custom.db")
	state := LocalRuntimeStateResolution{
		Project: localRuntimeStateProject{CanonicalProjectRoot: root, ProjectLocal: true},
		StoreSelection: storebackend.Selection{
			Backend: storebackend.BackendSQLite, SQLitePath: custom, SQLitePathSource: storebackend.SourceRuntimeConfig,
		},
	}
	if _, err := ResolveDevScratch(state); err == nil || !strings.Contains(err.Error(), "cannot use authored or shared SQLite path") {
		t.Fatalf("ResolveDevScratch error = %v, want custom-path teaching refusal", err)
	}
	if _, err := os.Stat(custom); !os.IsNotExist(err) {
		t.Fatalf("custom SQLite path stat error = %v, want no mutation", err)
	}
}

func TestDevScratchEpochRejectsPostgres(t *testing.T) {
	root := t.TempDir()
	state := LocalRuntimeStateResolution{
		Project:        localRuntimeStateProject{CanonicalProjectRoot: root, ProjectLocal: true},
		StoreSelection: storebackend.Selection{Backend: storebackend.BackendPostgres, BackendSource: storebackend.SourceFlag},
	}
	if _, err := ResolveDevScratch(state); err == nil || !strings.Contains(err.Error(), "project-local SQLite scratch store") {
		t.Fatalf("ResolveDevScratch error = %v, want PostgreSQL teaching refusal", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
		t.Fatalf("PostgreSQL refusal state stat error = %v, want no mutation", err)
	}
}

func TestResolveDevScratchAcceptsExplicitSQLiteWithPlatformCoordinate(t *testing.T) {
	root := t.TempDir()
	state := LocalRuntimeStateResolution{
		Project: localRuntimeStateProject{CanonicalProjectRoot: root, ProjectLocal: true},
		StoreSelection: storebackend.Selection{
			Backend: storebackend.BackendSQLite, BackendSource: storebackend.SourceFlag,
			SQLitePath: filepath.Join(root, ".swarm", "stores", "dev.db"), SQLitePathSource: storebackend.SourceProjectDefault,
		},
	}
	resolved, err := ResolveDevScratch(state)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, ".swarm", "stores", "dev-scratch.db")
	if resolved.Selection.SQLitePath != want || resolved.Selection.SQLitePathSource != storebackend.SourceProjectScratch {
		t.Fatalf("selection = %#v, want project scratch %q", resolved.Selection, want)
	}
}
