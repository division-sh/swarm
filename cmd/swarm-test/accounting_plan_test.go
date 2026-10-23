package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"os"

	"path/filepath"

	"strings"
	"testing"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func TestAccountingFiniteWriteRefusesMalformedHistoryWithoutPartialOutput(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, formatproof.Successors)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	before := []byte(`{"version":1,"records":[]}`)
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeFiniteHistory(root, formatproof.History{Version: 2, Records: []formatproof.Record{}}); err == nil {
		t.Fatal("unvalidated output written")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refused output partially changed the accounting artifact")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("failed output left temporary state")
	}
}

func TestAccountingSourceTreeExcludesOnlyItsExactOwnedArtifact(t *testing.T) {
	repo, base := completionRepo(t)
	original, err := accountingSourceTree(context.Background(), repo, base)
	if err != nil {
		t.Fatal(err)
	}
	put := func(path string) {
		t.Helper()
		full := filepath.Join(repo, path)
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("owned output\n"), 0600); err != nil {
			t.Fatal(err)
		}
		completionGit(t, repo, "add", ".")
		completionGit(t, repo, "commit", "-qm", "source tree witness")
	}
	put(formatproof.Successors)
	exact, err := accountingSourceTree(context.Background(), repo, completionGit(t, repo, "rev-parse", "HEAD"))
	if err != nil || exact != original {
		t.Fatal("owned artifact was not independently excluded")
	}
	put("foreign\t" + formatproof.Successors)
	foreign, err := accountingSourceTree(context.Background(), repo, completionGit(t, repo, "rev-parse", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if foreign == exact {
		t.Fatal("foreign tab-delimited path borrowed the owned artifact's exclusion")
	}
}

func TestAccountingExecutorArchiveRejectsSymlinkInputs(t *testing.T) {
	repo, base := completionRepo(t)
	directory, err := archiveAccountingExecutor(context.Background(), repo, base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(repo, "foreign")); err != nil {
		t.Fatal(err)
	}
	completionGit(t, repo, "add", "foreign")
	completionGit(t, repo, "commit", "-qm", "hostile archive symlink")
	if directory, err := archiveAccountingExecutor(context.Background(), repo, completionGit(t, repo, "rev-parse", "HEAD")); err == nil || directory != "" {
		t.Fatal("nonregular executor input accepted")
	}
}

func TestAccountingApprovedSourceCannotBeReboundByLocalGitReplacement(t *testing.T) {
	repo, base := completionRepo(t)
	before, err := accountingGit(context.Background(), repo, "ls-tree", "-r", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "foreign.go"), []byte("package proof\n"), 0600); err != nil {
		t.Fatal(err)
	}
	completionGit(t, repo, "add", "foreign.go")
	completionGit(t, repo, "commit", "-qm", "foreign executor input")
	foreign := completionGit(t, repo, "rev-parse", "HEAD")
	completionGit(t, repo, "replace", base, foreign)
	after, err := accountingGit(context.Background(), repo, "ls-tree", "-r", base)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("local replacement rebound approved source identity")
	}
	directory, err := archiveAccountingExecutor(context.Background(), repo, base)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	if _, err := os.Stat(filepath.Join(directory, "foreign.go")); !os.IsNotExist(err) {
		t.Fatal("replacement contents entered the trusted executor archive")
	}
}

func TestAccountingSavedPolicyCannotInventItsApprovalOrForceLatestRebase(t *testing.T) {
	repo, unapproved := completionRepo(t)
	policy := approvedAccountingPolicy(t)
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repo, accountingPolicyPath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "-qm", "reviewed policy fixture")
	old := accountingPolicyObservation{Commit: completionGit(t, repo, "rev-parse", "HEAD"), Policy: policy, Digest: formatproof.Digest(raw), Blob: fmt.Sprintf("%x", sha1.Sum(append([]byte(fmt.Sprintf("blob %d\x00", len(raw))), raw...)))}
	completionGit(t, repo, "commit", "--allow-empty", "-qm", "unrelated master advance")
	current := old
	current.Commit = completionGit(t, repo, "rev-parse", "HEAD")
	if err := verifyAccountingPolicyHistory(context.Background(), repo, old, current); err != nil {
		t.Fatalf("unchanged historical approval required a latest-master rebase: %v", err)
	}
	for _, field := range []string{"commit", "blob", "digest", "executor"} {
		t.Run(field, func(t *testing.T) {
			forged := old
			switch field {
			case "commit":
				forged.Commit = unapproved
			case "blob":
				forged.Blob = strings.Repeat("0", 40)
			case "digest":
				forged.Digest = strings.Repeat("0", 64)
			case "executor":
				forged.Policy.ExecutorSource = strings.Repeat("c", 40)
			}
			if verifyAccountingPolicyHistory(context.Background(), repo, forged, current) == nil {
				t.Fatal("saved plan invented its historical authority")
			}
		})
	}
}
