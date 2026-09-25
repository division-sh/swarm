package testplanning

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testchanged"
)

func TestPRChangeOptionsConservativelyRoutesParityAndSoak(t *testing.T) {
	const root = "/repo"
	base := RootInventory{ImpactPackages: []testchanged.Package{
		{ImportPath: "module/disjoint", RelDir: "internal/disjoint"},
		{ImportPath: "module/shared", RelDir: "internal/shared"},
		{ImportPath: SoakPackage, RelDir: "internal/runtime/conformance", Imports: []string{"module/shared"}},
	}}
	head := base
	for _, tc := range []struct {
		name         string
		change       ChangedPath
		parity, soak bool
	}{
		{"unknown docs path", ChangedPath{Status: "M", Path: "docs/guide.md"}, true, true},
		{"readme consumed by structural tests", ChangedPath{Status: "M", Path: "README.md"}, false, false},
		{"known disjoint Go package", ChangedPath{Status: "M", Path: "internal/disjoint/file.go"}, true, false},
		{"shared dependency", ChangedPath{Status: "M", Path: "internal/shared/file.go"}, true, true},
		{"fanout package", ChangedPath{Status: "M", Path: "internal/runtime/conformance/soak_test.go"}, true, true},
		{"shared harness", ChangedPath{Status: "M", Path: "internal/testplanning/plan.go"}, true, true},
		{"fixture", ChangedPath{Status: "M", Path: "internal/runtime/conformance/testdata/input.yaml"}, true, true},
		{"specification", ChangedPath{Status: "M", Path: "platform-spec.yaml"}, true, true},
		{"workflow", ChangedPath{Status: "M", Path: ".github/workflows/ci.yml"}, true, true},
		{"unknown path", ChangedPath{Status: "M", Path: "other/file.go"}, true, true},
		{"new file", ChangedPath{Status: "A", Path: "internal/disjoint/new.go"}, true, true},
		{"deleted prose", ChangedPath{Status: "D", Path: "README.md"}, true, true},
		{"renamed prose", ChangedPath{Status: "R100", OldPath: "docs/old.md", Path: "docs/new.md"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PRChangeOptions(root, root, base, head, []ChangedPath{tc.change})
			if err != nil {
				t.Fatal(err)
			}
			if got.IncludeParityFull != tc.parity || got.IncludeSoak != tc.soak {
				t.Fatalf("options = %+v, want parity=%t soak=%t", got, tc.parity, tc.soak)
			}
		})
	}
	baseOnly := base
	headOnly := base
	headOnly.ImpactPackages = append([]testchanged.Package(nil), base.ImpactPackages...)
	headOnly.ImpactPackages[2].Imports = nil
	got, err := PRChangeOptions(root, root, baseOnly, headOnly, []ChangedPath{{Status: "M", Path: "internal/shared/file.go"}})
	if err != nil || !got.IncludeSoak {
		t.Fatalf("base-only reverse dependency = %+v, %v", got, err)
	}
}

func TestParseNameStatusPreservesRenameAndRejectsMissingDelta(t *testing.T) {
	changes, err := ParseNameStatusZ([]byte("M\x00a.go\x00R100\x00old.go\x00new.go\x00D\x00gone.go\x00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 || changes[1].OldPath != "old.go" || changes[1].Path != "new.go" || changes[2].Status != "D" {
		t.Fatalf("changed-path ledger = %+v", changes)
	}
	for _, raw := range [][]byte{nil, []byte("M\x00a.go"), []byte("R100\x00old.go\x00"), []byte("M\x00../escape\x00")} {
		if _, err := ParseNameStatusZ(raw); err == nil {
			t.Fatalf("invalid status accepted: %q", raw)
		}
	}
}

func TestParseNameStatusReadsRealGitRenameRecord(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) []byte {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return out
	}
	runGit("init", "-q")
	old := filepath.Join(repo, "old.go")
	if err := os.WriteFile(old, []byte("package fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "old.go")
	runGit("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture")
	if err := os.Rename(old, filepath.Join(repo, "new.go")); err != nil {
		t.Fatal(err)
	}
	runGit("add", "-A")
	changes, err := ParseNameStatusZ(runGit("diff", "--cached", "--name-status", "-z", "--find-renames", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].OldPath != "old.go" || changes[0].Path != "new.go" || !strings.HasPrefix(changes[0].Status, "R") {
		t.Fatalf("actual Git rename parsed as %+v", changes)
	}
}

func TestForcedPRProfileCannotBypassRouting(t *testing.T) {
	policy := testPolicy()
	_, _, err := policy.ResolveProfile("pull_request", []string{"README.md"}, ProfileFull)
	if err == nil || !strings.Contains(err.Error(), "workflow_dispatch") {
		t.Fatalf("forced PR profile = %v", err)
	}
}
