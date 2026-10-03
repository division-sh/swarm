package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCadenceUsesObservedMasterFirstParentNotReachableSideBranch(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root, "-c", "user.name=Cadence", "-c", "user.email=cadence@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	git("init", "-q", "-b", "master")
	if err := os.WriteFile(filepath.Join(root, "root"), []byte("root"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "initial")
	base := git("rev-parse", "HEAD")
	git("checkout", "-qb", "feature")
	git("commit", "--allow-empty", "-qm", "feature")
	side := git("rev-parse", "HEAD")
	git("checkout", "master")
	git("commit", "--allow-empty", "-qm", "master")
	middle := git("rev-parse", "HEAD")
	git("merge", "--no-ff", "-qm", "merge feature", "feature")
	head := git("rev-parse", "HEAD")
	git("update-ref", "refs/remotes/origin/master", head)
	if got := cadenceMasterLineage(context.Background(), root, head); !reflect.DeepEqual(got, []string{head, middle, base}) {
		t.Fatal("wrong first-parent lag input", got)
	}
	if got := cadenceMasterLineage(context.Background(), root, side); len(got) != 0 {
		t.Fatal("reachable side branch was guessed to be detected master", got)
	}
	git("commit", "--allow-empty", "-qm", "future")
	if got := cadenceMasterLineage(context.Background(), root, git("rev-parse", "HEAD")); len(got) != 0 {
		t.Fatal("unobserved future master earned lineage", got)
	}
}
