package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHostPrerequisiteObservationPreservesMissingRoot(t *testing.T) {
	ancestor := t.TempDir()
	cfg := DefaultHostConfig()
	cfg.WorkspaceRoot = filepath.Join(ancestor, "missing", "workspace")
	inspection, err := InspectHostPrerequisites(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	wantAncestor, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ExistingAncestor != wantAncestor || !inspection.RequiresCreation || inspection.RootPath != filepath.Join(wantAncestor, "missing", "workspace") {
		t.Fatalf("incorrect prerequisite evidence: %+v", inspection)
	}
	if _, err := os.Stat(filepath.Join(ancestor, "missing")); !os.IsNotExist(err) {
		t.Fatalf("observation created a workspace: %v", err)
	}
}

func TestHostPrerequisiteObservationRejectsFileAndCanceledContext(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{file, filepath.Join(file, "child")} {
		cfg := DefaultHostConfig()
		cfg.WorkspaceRoot = root
		if _, err := InspectHostPrerequisites(context.Background(), cfg); err == nil {
			t.Fatalf("non-directory ancestor admitted: %s", root)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectHostPrerequisites(ctx, DefaultHostConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestHostPrerequisiteObservationRejectsUnavailableAccess(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("effective root identity is permitted to write/search this mode; no guessed mode rejection")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(root, 0700); err != nil {
			t.Error(err)
		}
	})
	cfg := DefaultHostConfig()
	cfg.WorkspaceRoot = root
	if _, err := InspectHostPrerequisites(context.Background(), cfg); err == nil {
		t.Fatal("non-writable workspace prerequisite admitted")
	}
}
