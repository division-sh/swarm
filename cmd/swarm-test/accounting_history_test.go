package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type accountingReplayFixture struct {
	root, base, source, final, predecessor, origin string
	previous, current                              accountingPolicyObservation
	plan                                           accountingPlan
}

func accountingReplayCutsFixture(t *testing.T) accountingReplayFixture {
	t.Helper()
	root, _, executor, _ := accountingSnapshotFixture(t)
	put := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(message string) string {
		t.Helper()
		completionGit(t, root, "add", ".")
		completionGit(t, root, "commit", "-qm", message)
		return completionGit(t, root, "rev-parse", "HEAD")
	}
	put(accountingRuntimeFixturePaths[0], "comparison cut\n")
	base := commit("independent comparison cut")
	policy := approvedAccountingPolicy(t)
	policy.ExecutorSource = executor
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	put(accountingPolicyPath, string(raw))
	master := commit("controlled protected policy")
	remote := accountingPolicyRemote(t, &policy, &master)
	previous, err := observeAccountingPolicy(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	put(accountingRuntimeFixturePaths[0], "later protected source\n")
	predecessor := commit("protected tip advances with unchanged policy")
	put("later.txt", "unrelated protected progress\n")
	master = commit("another unchanged-policy protected tip")
	current, err := observeAccountingPolicy(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	completionGit(t, root, "switch", "--detach", base)
	put(accountingRuntimeFixturePaths[0], "planned source cut\n")
	source := commit("source before accounting output")
	put("output.txt", "committed accounting output\n")
	final := commit("output commit after planning")
	completionGit(t, root, "update-ref", "refs/remotes/origin/master", master)
	plan := accountingPlan{Version: 1, Base: base, Head: source, Policy: previous}
	return accountingReplayFixture{root, base, source, final, predecessor, master, previous, current, plan}
}

func writeAccountingReplayInput(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "input.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAccountingProofSnapshotRetainsAuthenticatedReplayCuts(t *testing.T) {
	f := accountingReplayCutsFixture(t)
	ctx := context.Background()
	// Reproduce the omitted-tree mechanism independently of F1 eligibility:
	// retaining all ancestor commit objects alone does not retain their trees.
	old, err := freezeAccountingProof(ctx, f.root, f.final, f.origin, f.current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gitAccountingBlob(ctx, old.Root, f.source, accountingRuntimeFixturePaths[0]); err == nil {
		t.Fatal("counterexample must require the intermediate source tree")
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	planFile := writeAccountingReplayInput(t, f.plan)
	cuts, err := accountingProofReadCuts(ctx, f.root, f.final, f.origin, f.current, []string{"certify", "--plan", planFile})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := freezeAccountingProof(ctx, f.root, f.final, f.origin, f.current, cuts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	for _, test := range []struct{ cut, path, want string }{
		{f.source, accountingRuntimeFixturePaths[0], "planned source cut\n"},
		{f.base, accountingRuntimeFixturePaths[0], "comparison cut\n"},
		{f.previous.Commit, accountingPolicyPath, "\"status\":\"active\""},
	} {
		body, err := gitAccountingBlob(ctx, snapshot.Root, test.cut, test.path)
		if err != nil || !strings.Contains(string(body), test.want) {
			t.Fatalf("replay cut %s/%s: %q %v", test.cut, test.path, body, err)
		}
	}
	if err = verifyAccountingPolicyHistory(ctx, snapshot.Root, f.previous, f.current); err != nil {
		t.Fatalf("later protected policy tip lost the old authenticated policy blob: %v", err)
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(snapshot.Root); !os.IsNotExist(err) {
		t.Fatal("old proof snapshot was not disposed")
	}
	receipt := accountingCertificate{Version: 1, Head: f.final, Plan: f.plan, Policy: f.previous}
	receiptFile := writeAccountingReplayInput(t, receipt)
	cuts, err = accountingProofReadCuts(ctx, f.root, f.final, f.origin, f.current, []string{"certify", "--receipt", receiptFile})
	if err != nil {
		t.Fatal(err)
	}
	next, err := freezeAccountingProof(ctx, f.root, f.final, f.origin, f.current, cuts...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = next.Close() }()
	if _, err = gitAccountingBlob(ctx, next.Root, f.source, accountingRuntimeFixturePaths[0]); err != nil {
		t.Fatalf("subsequent receipt use depended on the disposed snapshot: %v", err)
	}
	// Copying these inputs does not admit a partial synthetic certificate.
	if err = validateAccountingReceipt(ctx, next.Root, f.final, receiptFile, nil, f.current); err == nil {
		t.Fatal("retained read cuts granted receipt authority")
	}
}

func TestAccountingProofReadCutsRetainCanonicalPreflightPredecessor(t *testing.T) {
	f := accountingReplayCutsFixture(t)
	ctx := context.Background()
	cuts, err := accountingProofReadCuts(ctx, f.root, f.origin, f.origin, f.current, []string{"plan"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := freezeAccountingProof(ctx, f.root, f.origin, f.origin, f.current, cuts...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Close() }()
	if _, err = gitAccountingBlob(ctx, snapshot.Root, f.predecessor, accountingRuntimeFixturePaths[0]); err != nil {
		t.Fatalf("canonical predecessor comparison tree missing: %v", err)
	}
}

func TestAccountingProofReadCutsRefuseMissingForeignAndUnboundRequests(t *testing.T) {
	f := accountingReplayCutsFixture(t)
	for name, mutate := range map[string]func(*accountingPlan){
		"missing_source":  func(p *accountingPlan) { p.Head = strings.Repeat("1", 40) },
		"foreign_source":  func(p *accountingPlan) { p.Head = f.origin },
		"foreign_base":    func(p *accountingPlan) { p.Base = f.origin },
		"missing_base":    func(p *accountingPlan) { p.Base = strings.Repeat("1", 40) },
		"foreign_policy":  func(p *accountingPlan) { p.Policy.Commit = f.source },
		"unbound_policy":  func(p *accountingPlan) { p.Policy.Digest = strings.Repeat("1", 64) },
		"unbound_version": func(p *accountingPlan) { p.Version = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			plan := f.plan
			mutate(&plan)
			path := writeAccountingReplayInput(t, plan)
			if _, err := accountingProofReadCuts(context.Background(), f.root, f.final, f.origin, f.current, []string{"certify", "--plan", path}); err == nil {
				t.Fatal("untrusted read-cut request admitted")
			}
		})
	}
}
