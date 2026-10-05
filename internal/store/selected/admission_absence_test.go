package selected

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestIssue2567SelectedStoreAbsenceIsTypedAndNonCreating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "store.db")
	inspection, err := OpenAdmissionInspection(context.Background(), AuthorityRequest{Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: path}})
	var absent *AbsentSQLiteStore
	if inspection != nil || !errors.As(err, &absent) || absent.Path != path {
		t.Fatalf("inspection=%v cause=%v", inspection, err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created parent: %v", err)
	}
	root := t.TempDir()
	link := filepath.Join(root, "alias")
	if err := os.Symlink(filepath.Join(root, "missing"), link); err != nil {
		t.Fatal(err)
	}
	_, err = OpenAdmissionInspection(context.Background(), AuthorityRequest{Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(link, "store.db")}})
	if err == nil || errors.As(err, &absent) {
		t.Fatalf("dangling parent minted absence: %v", err)
	}
}
