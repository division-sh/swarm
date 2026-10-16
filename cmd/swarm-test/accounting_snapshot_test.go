package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

var accountingRuntimeFixturePaths = []string{
	"tests/tier3-list-processing/test-fan-out-basic/schema.yaml",
	"examples/routing/notify-all-children/account/schema.yaml",
	"internal/store/testdata/persistence_authority_debt_baseline.tsv",
	"internal/store/testdata/persistence_authority_debt_baseline.collector.json",
	"tools/fixture-codemod/pipeline-observations/recipes.json",
	"internal/runtime/scanned_as_data.go",
}

func accountingSnapshotFixture(t *testing.T) (string, string, string, accountingPolicyObservation) {
	t.Helper()
	repo := t.TempDir()
	completionGit(t, repo, "init", "-q")
	for _, name := range append(append([]string{}, accountingRuntimeFixturePaths...), "go.mod", ".gitignore", ".gitattributes") {
		body := "original proof input\n"
		switch name {
		case "go.mod":
			body = "module fixture\n\ngo 1.26.8\n"
		case ".gitignore":
			body = "test-results/\nignored.yaml\n"
		case ".gitattributes":
			body = "*.yaml export-subst\ninternal/store/testdata/ export-ignore\n"
		}
		path := filepath.Join(repo, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "-qm", "immutable proof inputs")
	base := completionGit(t, repo, "rev-parse", "HEAD")
	if err := os.MkdirAll(filepath.Join(repo, ".github"), 0700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string][]string{"fixture": {base}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".github/test-proof-history-inputs.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "-qm", "canonical history inputs")
	head := completionGit(t, repo, "rev-parse", "HEAD")
	return repo, base, head, accountingPolicyObservation{Commit: head, Policy: accountingPolicy{ExecutorSource: head}}
}

func freezeAccountingFixture(t *testing.T, repo, head string, policy accountingPolicyObservation) accountingProofSnapshot {
	t.Helper()
	snapshot, err := freezeAccountingProof(context.Background(), repo, head, head, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	return snapshot
}

func TestAccountingProofSnapshotIncludesRuntimeDataAndHistory(t *testing.T) {
	repo, base, head, policy := accountingSnapshotFixture(t)
	snapshot := freezeAccountingFixture(t, repo, head, policy)
	for _, path := range accountingRuntimeFixturePaths {
		body, err := os.ReadFile(filepath.Join(snapshot.Root, path))
		if err != nil || string(body) != "original proof input\n" {
			t.Fatalf("runtime data omitted or exported differently: %s: %q %v", path, body, err)
		}
	}
	if _, err := accountingGit(context.Background(), snapshot.Root, "merge-base", "--is-ancestor", base, head); err != nil {
		t.Fatalf("exact objects without required ancestry: %v", err)
	}
	body, err := accountingGit(context.Background(), snapshot.Root, "show", base+":"+accountingRuntimeFixturePaths[0])
	if err != nil || string(body) != "original proof input\n" {
		t.Fatalf("historical read changed: %q %v", body, err)
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root, ".git/objects/info/alternates")); !os.IsNotExist(err) {
		t.Fatal("proof object database must be independent")
	}
	manifest, err := accountingTreeInputs(context.Background(), repo, head, nil)
	if err != nil || !reflect.DeepEqual(manifest, snapshot.Manifest) {
		t.Fatalf("manifest does not reproduce from exact objects: %v", err)
	}
}

func TestAccountingProofSnapshotChangeAndRestoreCannotReachGuardReads(t *testing.T) {
	repo, _, head, policy := accountingSnapshotFixture(t)
	snapshot := freezeAccountingFixture(t, repo, head, policy)
	for _, name := range accountingRuntimeFixturePaths {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(repo, name)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			completionGit(t, repo, "update-index", "--assume-unchanged", name)
			if err := os.WriteFile(path, []byte("untrusted intermediate bytes\n"), 0600); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(path, before, 0600); err != nil {
					t.Error(err)
				}
			}()
			if _, err := completionSource(repo, head, true); err != nil {
				t.Fatalf("hostile data must be hidden from clean Git status: %v", err)
			}
			read := func(root string) string {
				var out bytes.Buffer
				_, err := runPreflightProcessDirectory(context.Background(), root, []string{"sh", "-c", "cat -- \"$1\"", "guard", name}, &out, io.Discard, accountingBuildEnv(runtime.GOOS, runtime.GOARCH))
				if err != nil {
					t.Fatal(err)
				}
				return out.String()
			}
			// The old caller-cwd path deterministically reads the hostile interval.
			if got := read(repo); got != "untrusted intermediate bytes\n" {
				t.Fatalf("RED control did not expose mutable runtime data: %q", got)
			}
			if got := read(snapshot.Root); got != string(before) {
				t.Fatalf("caller mutation reached owned guard: %q", got)
			}
			if err := os.WriteFile(path, before, 0600); err != nil {
				t.Fatal(err)
			}
			if err := snapshot.Validate(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := os.WriteFile(filepath.Join(repo, "ignored.yaml"), []byte("untrusted ignored input"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root, "ignored.yaml")); !os.IsNotExist(err) {
		t.Fatal("ignored caller entry entered proof tree")
	}
}

func TestAccountingProofManifestIndirectionIsFrozen(t *testing.T) {
	repo, _, _, _ := accountingSnapshotFixture(t)
	if err := os.WriteFile(filepath.Join(repo, "inputs.list"), []byte(accountingRuntimeFixturePaths[0]+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	completionGit(t, repo, "add", "inputs.list")
	completionGit(t, repo, "commit", "-qm", "runtime manifest indirection")
	head := completionGit(t, repo, "rev-parse", "HEAD")
	policy := accountingPolicyObservation{Commit: head, Policy: accountingPolicy{ExecutorSource: head}}
	snapshot := freezeAccountingFixture(t, repo, head, policy)
	completionGit(t, repo, "update-index", "--assume-unchanged", "inputs.list")
	if err := os.WriteFile(filepath.Join(repo, "ignored.yaml"), []byte("untrusted replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "inputs.list"), []byte("ignored.yaml\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := accountingSource(context.Background(), repo, head, true); err != nil {
		t.Fatalf("fixture must hide both indirection and target: %v", err)
	}
	var out bytes.Buffer
	args := []string{"sh", "-c", "read -r input < inputs.list; cat -- \"$input\""}
	if _, err := runPreflightProcessDirectory(context.Background(), snapshot.Root, args, &out, io.Discard, accountingBuildEnv(runtime.GOOS, runtime.GOARCH)); err != nil {
		t.Fatal(err)
	}
	if out.String() != "original proof input\n" {
		t.Fatalf("mutable manifest redirected the frozen reader: %q", out.String())
	}
}

func TestAccountingProofRealSourceManifestCoversRuntimeReaders(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	head, err := accountingSource(context.Background(), root, "", false)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := accountingTreeInputs(context.Background(), root, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, input := range manifest.Inputs {
		if found[input.Path] || !hexIdentity(input.Object, 40) || !hexIdentity(input.SHA256, 64) {
			t.Fatalf("invalid full proof input: %#v", input)
		}
		found[input.Path] = true
	}
	for _, path := range append(append([]string{}, accountingRuntimeFixturePaths[:5]...), "scripts/rewrite-stages-2566/entries.json", "scripts/rewrite-stages-2566/intent.json", "scripts/rewrite-stages-2566/finite-fixtures.json", ".github/test-proof-history-inputs.json", ".github/workflows/ci.yml", "platform-spec.yaml") {
		if !found[path] {
			t.Fatalf("runtime reader input omitted from actual source manifest: %s", path)
		}
	}
	t.Logf("real-source proof manifest: head=%s tree=%s inputs=%d digest=%s", head, manifest.Tree, len(manifest.Inputs), accountingManifestDigest(manifest))
}

func TestAccountingProofRefusesWrongOrMissingHistoryAndControls(t *testing.T) {
	repo, _, head, policy := accountingSnapshotFixture(t)
	for _, name := range []string{"objects/info/alternates", "info/attributes", "info/grafts", "info/exclude", "shallow", "refs/replace", "config.worktree", "config", "origin"} {
		t.Run(name, func(t *testing.T) {
			snapshot := freezeAccountingFixture(t, repo, head, policy)
			path := filepath.Join(snapshot.Root, ".git", name)
			if name == "origin" {
				if _, err := accountingGit(context.Background(), snapshot.Root, "update-ref", "refs/remotes/origin/master", head+"^"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if name == "config" {
					if err := os.Chmod(path, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(path, []byte("unapproved Git input"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := snapshot.Validate(context.Background()); err == nil {
				t.Fatal("modified Git proof input accepted")
			}
		})
	}
	policy.Policy.ExecutorSource = strings.Repeat("f", 40)
	if snapshot, err := freezeAccountingProof(context.Background(), repo, head, head, policy); err == nil {
		_ = snapshot.Close()
		t.Fatal("missing exact historical object accepted")
	}
}

func TestAccountingExecutorArchiveCannotHideGitEntriesWithAttributes(t *testing.T) {
	repo, _, head, _ := accountingSnapshotFixture(t)
	if directory, err := archiveAccountingExecutor(context.Background(), repo, head); err == nil {
		_ = os.RemoveAll(directory)
		t.Fatal("export-ignore hid a real reviewed Git input")
	}
}

func TestAccountingProofInputPathsKeepTheirCallerMeaning(t *testing.T) {
	root := t.TempDir()
	for _, flag := range []string{"--plan", "-plan", "--receipt", "-receipt"} {
		for _, inline := range []bool{false, true} {
			args := []string{"certify", flag, "relative.json"}
			want := []string{"certify", flag, filepath.Join(root, "relative.json")}
			if inline {
				args = []string{"certify", flag + "=relative.json"}
				want = []string{"certify", flag + "=" + filepath.Join(root, "relative.json")}
			}
			actual, err := absoluteAccountingInputs(root, args)
			if err != nil || !reflect.DeepEqual(actual, want) {
				t.Fatalf("input path rebound to proof cwd: %v %v", actual, err)
			}
		}
	}
	for _, args := range [][]string{{"certify", "--plan"}, {"certify", "--receipt="}, {"certify", "-plan", ""}} {
		if _, err := absoluteAccountingInputs(root, args); err == nil {
			t.Fatal("missing input path accepted")
		}
	}
}

func TestAccountingProofSnapshotRejectsWritesMissingAndReplacedInputs(t *testing.T) {
	repo, _, head, policy := accountingSnapshotFixture(t)
	for _, kind := range []string{"write", "missing", "symlink", "bytes", "extra", "manifest"} {
		t.Run(kind, func(t *testing.T) {
			snapshot := freezeAccountingFixture(t, repo, head, policy)
			path := filepath.Join(snapshot.Root, accountingRuntimeFixturePaths[0])
			if kind == "write" {
				if err := os.WriteFile(path, []byte("corrupt"), 0600); !os.IsPermission(err) {
					t.Fatalf("proof inputs must reject ordinary writes: %v", err)
				}
				if err := snapshot.Validate(context.Background()); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing", "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(filepath.Join(repo, accountingRuntimeFixturePaths[0]), path); err != nil {
						t.Fatal(err)
					}
				}
			case "bytes":
				if err := os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), "extra.yaml"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "manifest":
				snapshot.Manifest.Inputs[0].SHA256 = strings.Repeat("0", 64)
			}
			if err := os.Chmod(filepath.Dir(path), 0555); err != nil {
				t.Fatal(err)
			}
			if err := snapshot.Validate(context.Background()); err == nil {
				t.Fatal("modified proof input accepted")
			}
		})
	}
}

func TestAccountingProofGitIgnoresCallerConfigurationInjection(t *testing.T) {
	repo, _, head, policy := accountingSnapshotFixture(t)
	for name, value := range map[string]string{
		"GIT_DIR": "/missing/caller/git", "GIT_WORK_TREE": "/missing/caller/tree", "GIT_INDEX_FILE": "/missing/caller/index",
		"GIT_OBJECT_DIRECTORY": "/missing/caller/objects", "GIT_ALTERNATE_OBJECT_DIRECTORIES": "/missing/caller/alternates",
		"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.bare", "GIT_CONFIG_VALUE_0": "true",
		"GIT_CONFIG_GLOBAL": "/missing/caller/config", "GIT_REPLACE_REF_BASE": "refs/hostile/", "GIT_CONFIG_PARAMETERS": "invalid injected config",
	} {
		t.Setenv(name, value)
	}
	snapshot := freezeAccountingFixture(t, repo, head, policy)
	if err := snapshot.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
}
