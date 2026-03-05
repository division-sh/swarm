package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverManagedMigrationSpecs(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"010_feature.sql",
		"001_initial.sql",
		"README.md",
		"bad_name.sql",
	}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("-- test\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	specs, err := discoverManagedMigrationSpecs(filepath.Join(dir, "001_initial.sql"))
	if err != nil {
		t.Fatalf("discoverManagedMigrationSpecs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 migration specs, got %d", len(specs))
	}
	if specs[0].Version != 1 || specs[0].Name != "001_initial" {
		t.Fatalf("unexpected first migration: %+v", specs[0])
	}
	if specs[1].Version != 10 || specs[1].Name != "010_feature" {
		t.Fatalf("unexpected second migration: %+v", specs[1])
	}
}

func TestDiscoverManagedMigrationSpecs_NoMatches(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	_, err := discoverManagedMigrationSpecs(filepath.Join(dir, "001_initial.sql"))
	if err == nil {
		t.Fatal("expected error when no migration files match pattern")
	}
}

func TestDiscoverManagedMigrationSpecs_SingleFileMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ddl-canonical.sql")
	if err := os.WriteFile(path, []byte("-- canonical\n"), 0o644); err != nil {
		t.Fatalf("write canonical migration: %v", err)
	}
	specs, err := discoverManagedMigrationSpecs(path)
	if err != nil {
		t.Fatalf("discoverManagedMigrationSpecs(single): %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 migration spec, got %d", len(specs))
	}
	if specs[0].Version != 1 || specs[0].Name != "ddl-canonical" {
		t.Fatalf("unexpected single migration spec: %+v", specs[0])
	}
	if specs[0].Path != path {
		t.Fatalf("unexpected path: %s", specs[0].Path)
	}
}
