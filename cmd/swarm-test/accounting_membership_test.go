package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func accountingClosureRepo(t *testing.T) (string, string, accountingPolicy, []byte) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	put := func(path string, body []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, path), body, 0644); err != nil {
			t.Fatal(err)
		}
	}
	// Admission only lists inputs; it does not execute this fixture. Keep its
	// graph small. The separate real-source closure test covers the complete graph.
	for path, name := range map[string]string{
		"cmd/swarm-test": "main", "cmd/swarm-openrpc-gen": "main", "cmd/swarm-complexity": "main",
		"scripts/rewrite-stages-2566": "main", "internal/testplanning": "testplanning",
		"internal/testtiming": "testtiming", "internal/store": "store",
	} {
		put(path+"/main.go", []byte("package "+name+"\n"))
	}
	put("cmd/swarm-test/main.go", []byte("package main\nimport _ \"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting\"\n"))
	accountingGuardFixtureFiles(t, root, put)
	put("go.mod", []byte("module github.com/division-sh/swarm\n\ngo 1.26.8\n"))
	put("go.sum", nil)
	for _, path := range []string{
		".github/test-proof-plan.yaml", ".github/test-proof-history-inputs.json", ".github/scripts/prepare-proof-history.sh",
		testplanning.GeneratedWeightModelPath, ".github/flake-ledger.json", "internal/apiv1/testdata/public_surface_backend_matrix.yaml",
		formatproof.Ledger, "scripts/rewrite-stages-2566/intent.json", "scripts/rewrite-stages-2566/entries.json",
		"scripts/rewrite-stages-2566/formatproof/testdata/original.go.txt",
	} {
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		put(path, body)
	}
	original, err := os.ReadFile(filepath.Join(root, formatproof.Source))
	if err != nil {
		t.Fatal(err)
	}
	if formatproof.Digest(original) != formatproof.Origin {
		raw, err := os.ReadFile(filepath.Join(root, formatproof.Successors))
		if err != nil {
			t.Fatal(err)
		}
		history, err := formatproof.Read(raw)
		if err != nil || len(history.Records) == 0 {
			t.Fatalf("immutable source unavailable: %v", err)
		}
		original = []byte(history.Records[0].Before)
		if formatproof.Digest(original) != formatproof.Origin {
			t.Fatal("immutable source changed")
		}
	}
	put(formatproof.Source, original)
	put(formatproof.Successors, []byte(`{"version":1,"records":[]}`))
	inputs, err := accountingClosure(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(inputs, func(input accountingInput) bool { return input.Path == formatproof.Source }) {
		t.Fatal("fixture must include the exact F1 source in its executable closure")
	}
	completionGit(t, repo, "init", "-q")
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "-qm", "approved executor closure fixture")
	base := completionGit(t, repo, "rev-parse", "HEAD")
	policy := approvedAccountingPolicy(t)
	policy.ExecutorSource, policy.Inputs = base, inputs
	raw, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	policy.DependencyDigest = formatproof.Digest(raw)
	return repo, base, policy, original
}

func accountingGuardFixtureFiles(t *testing.T, root string, put func(string, []byte)) {
	t.Helper()
	for path, name := range map[string]string{
		"internal/runtime": "runtime", "internal/serveapp": "serveapp", "internal/releasee2e": "releasee2e",
		"internal/store/internal/runtimepersistence": "runtimepersistence",
	} {
		files, err := os.ReadDir(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		for index, file := range files {
			filename := file.Name()
			if file.IsDir() || !strings.HasSuffix(filename, ".go") || (!strings.Contains(filename, "guard") && !strings.HasSuffix(filename, "_static_test.go") && filename != "boundary_test.go") {
				continue
			}
			put(path+"/"+filename, []byte(fmt.Sprintf("package %s\nimport \"testing\"\nfunc TestFixtureOwner%d(t *testing.T) {}\n", name, index)))
		}
	}
}

func TestAccountingCandidateClosureRejectsAddedExecutableMembers(t *testing.T) {
	repo, approved, policy, original := accountingClosureRepo(t)
	ctx := context.Background()
	for _, test := range []struct {
		name, path, body string
		allowed          bool
	}{
		{"ordinary_runner", "cmd/swarm-test/new_init.go", "package main\nfunc init() { panic(\"unapproved candidate code executed\") }\n", false},
		{"test_runner", "cmd/swarm-test/new_member_test.go", "package main\nimport \"testing\"\nfunc TestAddedMember(t *testing.T) {}\n", false},
		{"conditional_runner", "cmd/swarm-test/new_conditional.go", "//go:build issue2413\n\npackage main\n", false},
		{"darwin_runner", "cmd/swarm-test/new_darwin.go", "//go:build darwin\n\npackage main\n", false},
		{"guard_test", "internal/store/new_guard_test.go", "package store_test\n", false},
		{"dependency", "internal/testplanning/new_owner.go", "package testplanning\n", false},
		{"unrelated_document", "accounting-unrelated.md", "unrelated protected-master progress\n", true},
		{"unexecuted_package", "accounting-unrelated/member.go", "package unrelated\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			completionGit(t, repo, "switch", "--detach", approved)
			path := filepath.Join(repo, test.path)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			completionGit(t, repo, "add", ".")
			completionGit(t, repo, "commit", "-qm", "protected-master advance without policy change")
			base := completionGit(t, repo, "rev-parse", "HEAD")
			current := bytes.Replace(original, []byte("OmitOutputPin               bool"), []byte("OmitOutputPin                bool"), 1)
			if bytes.Equal(current, original) {
				t.Fatal("missing reviewed whitespace witness")
			}
			if err := os.WriteFile(filepath.Join(repo, formatproof.Source), current, 0644); err != nil {
				t.Fatal(err)
			}
			completionGit(t, repo, "add", ".")
			completionGit(t, repo, "commit", "-qm", "candidate changes only the approved F1 source")
			head := completionGit(t, repo, "rev-parse", "HEAD")
			observation := accountingPolicyObservation{Commit: base, Policy: policy}
			if err := checkAccountingBase(ctx, repo, base, observation); err != nil {
				t.Fatalf("legitimate protected-base advance refused: %v", err)
			}
			plan, err := deriveFiniteAccountingPlan(ctx, repo, base, head, observation)
			if test.allowed {
				if err != nil || plan.Rows != 1 {
					t.Fatalf("unrelated master advance refused: rows=%d err=%v", plan.Rows, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "candidate executable closure") {
				t.Fatalf("candidate's additional executable closure member was not rejected: %v", err)
			}
		})
	}
	t.Run("ignored_runner", func(t *testing.T) {
		if err := os.WriteFile(filepath.Join(repo, ".git/info/exclude"), []byte("cmd/swarm-test/ignored_init.go\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, "cmd/swarm-test/ignored_init.go"), []byte("package main\nfunc init() { panic(\"ignored candidate code executed\") }\n"), 0600); err != nil {
			t.Fatal(err)
		}
		head := completionGit(t, repo, "rev-parse", "HEAD")
		if _, err := completionSource(repo, head, true); err != nil {
			t.Fatalf("fixture must demonstrate code outside clean Git status: %v", err)
		}
		base := completionGit(t, repo, "rev-parse", "HEAD^")
		_, err := deriveFiniteAccountingPlan(ctx, repo, base, head, accountingPolicyObservation{Commit: base, Policy: policy})
		if err == nil || !strings.Contains(err.Error(), "candidate executable closure gained unapproved member cmd/swarm-test/ignored_init.go") {
			t.Fatalf("ignored executable member escaped the actual build-input check: %v", err)
		}
	})
	t.Run("frozen_member_cannot_be_hidden_by_working_bytes", func(t *testing.T) {
		if err := os.Remove(filepath.Join(repo, "cmd/swarm-test/ignored_init.go")); err != nil {
			t.Fatal(err)
		}
		completionGit(t, repo, "switch", "--detach", approved)
		path := filepath.Join(repo, "cmd/swarm-test/main.go")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(append([]byte{}, body...), '\n'), 0644); err != nil {
			t.Fatal(err)
		}
		completionGit(t, repo, "add", ".")
		completionGit(t, repo, "commit", "-qm", "protected-base changed a pinned owner")
		base := completionGit(t, repo, "rev-parse", "HEAD")
		current := bytes.Replace(original, []byte("OmitOutputPin               bool"), []byte("OmitOutputPin                bool"), 1)
		if err := os.WriteFile(filepath.Join(repo, formatproof.Source), current, 0644); err != nil {
			t.Fatal(err)
		}
		completionGit(t, repo, "add", ".")
		completionGit(t, repo, "commit", "-qm", "F1 candidate over changed pinned owner")
		head := completionGit(t, repo, "rev-parse", "HEAD")
		completionGit(t, repo, "update-index", "--assume-unchanged", "cmd/swarm-test/main.go")
		if err := os.WriteFile(path, body, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := completionSource(repo, head, true); err != nil {
			t.Fatalf("fixture must conceal working bytes from clean Git status: %v", err)
		}
		_, err = deriveFiniteAccountingPlan(ctx, repo, base, head, accountingPolicyObservation{Commit: base, Policy: policy})
		if err == nil || !strings.Contains(err.Error(), "candidate changed approved owner/guard dependency cmd/swarm-test/main.go") {
			t.Fatalf("working bytes masked a changed pinned Git object: %v", err)
		}
	})
}
