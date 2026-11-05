package testpostgres

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePrivateStatePathIgnoresOnlyVanishedChildAuthority(t *testing.T) {
	root := t.TempDir()
	missingChild := filepath.Join(root, "services-v1.json.tmp-retired")
	if err := validatePrivateStatePath(root, missingChild, os.ErrNotExist); err != nil {
		t.Fatalf("walk error for vanished child = %v", err)
	}
	if err := validatePrivateStatePath(root, missingChild, nil); err != nil {
		t.Fatalf("lstat error for vanished child = %v", err)
	}

	missingRoot := filepath.Join(t.TempDir(), "missing")
	if err := validatePrivateStatePath(missingRoot, missingRoot, os.ErrNotExist); !os.IsNotExist(err) {
		t.Fatalf("missing root walk error = %v, want not-exist", err)
	}
	if err := validatePrivateStatePath(missingRoot, missingRoot, nil); !os.IsNotExist(err) {
		t.Fatalf("missing root lstat error = %v, want not-exist", err)
	}
}
