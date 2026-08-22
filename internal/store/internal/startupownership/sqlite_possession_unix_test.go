//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

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
}

func TestSQLiteProcessCapabilityFormerSidecarReplacementCannotSplitPossession(t *testing.T) {
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
	lockPath := path + ".swarm-owner.lock"
	if err := os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, []byte("non-authoritative"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := possession.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("former sidecar replacement changed canonical possession: %v", err)
	}
	contender, err := acquireSQLiteFilePossession(path)
	if contender != nil {
		_ = contender.Release()
	}
	if !isSQLitePossessionFailure(err, runtimestartupownership.AcquisitionTakeoverRequired) {
		t.Fatalf("contender after former sidecar replacement error=%v, want takeover_required", err)
	}
}

func isSQLitePossessionFailure(err error, failure runtimestartupownership.AcquisitionFailure) bool {
	var acquisitionErr *runtimestartupownership.AcquisitionError
	return errors.As(err, &acquisitionErr) && acquisitionErr.Failure == failure
}
