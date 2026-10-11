//go:build darwin || linux

package startupownership

import (
	"context"
	"runtime"
	"strings"
	"testing"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

func TestSQLiteUnsupportedFilesystemInspectionOnly(t *testing.T) {
	session, db := newSQLiteProofSession(t, 1)
	owner := session.owner
	owner.path = unsupportedSQLiteFilesystemPath()
	owner.catalogEmpty = func(context.Context) (bool, error) { return false, nil }
	inspection, err := owner.InspectAuthority(context.Background())
	if err != nil {
		t.Fatalf("InspectAuthority: %v", err)
	}
	if inspection.Status != runtimestartupownership.AuthorityInspectionValid || inspection.Authority == nil || *inspection.Authority != session.authority {
		t.Fatalf("inspection must read immutable generation without claiming possession: %#v", inspection)
	}
	_, err = owner.ProbePossession(context.Background())
	if err == nil || !strings.Contains(err.Error(), "filesystem cannot prove process ownership") {
		t.Fatalf("ProbePossession error = %v, want unsupported-filesystem refusal", err)
	}
	stamp, err := generationauthority.ReadProcessGeneration(context.Background(), db, true)
	if err != nil || stamp != session.authority.LeaseStamp() {
		t.Fatalf("unsupported native observation changed the admitted generation: %#v, %v", stamp, err)
	}
}

func unsupportedSQLiteFilesystemPath() string {
	if runtime.GOOS == "darwin" {
		return "/dev/null"
	}
	return "/proc/version"
}
