package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func TestAccountingFinitePlanBindsEveryChangedRowToTrustedSource(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", repo}, args...)...)
		command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=fixture", "GIT_AUTHOR_EMAIL=fixture@example.test", "GIT_COMMITTER_NAME=fixture", "GIT_COMMITTER_EMAIL=fixture@example.test")
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %s: %v", args, raw, err)
		}
		return strings.TrimSpace(string(raw))
	}
	put := func(path string, raw []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	commit := func() string {
		t.Helper()
		git("add", ".")
		git("-c", "commit.gpgsign=false", "commit", "-qm", "fixture")
		return git("rev-parse", "HEAD")
	}
	git("init", "-q")
	original, err := os.ReadFile(filepath.Join("..", "..", formatproof.Source))
	if err != nil {
		t.Fatal(err)
	}
	if formatproof.Digest(original) != formatproof.Origin {
		raw, err := os.ReadFile(filepath.Join("..", "..", formatproof.Successors))
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
	ledger, err := os.ReadFile(filepath.Join("..", "..", formatproof.Ledger))
	if err != nil {
		t.Fatal(err)
	}
	put(formatproof.Source, original)
	put(formatproof.Ledger, ledger)
	put(formatproof.Successors, []byte(`{"version":1,"records":[]}`))
	put("go.mod", []byte("module fixture\n"))
	base := commit()
	policy := approvedAccountingPolicy(t)
	policy.Inputs = []accountingInput{{Path: "go.mod", SHA256: formatproof.Digest([]byte("module fixture\n"))}}
	raw, err := json.Marshal(policy.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	policy.DependencyDigest = formatproof.Digest(raw)
	observation := accountingPolicyObservation{Commit: base, Policy: policy}
	first := bytes.Replace(original, []byte("OmitOutputPin               bool"), []byte("OmitOutputPin                bool"), 1)
	put(formatproof.Source, first)
	head := commit()
	plan, err := deriveFiniteAccountingPlan(ctx, repo, base, head, observation)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Rows != 1 || len(plan.Output.Records) != 1 || len(plan.Previous.Records) != 0 || plan.SourceTree == "" {
		t.Fatalf("unbound output: %#v", plan)
	}
	if err := checkAccountingBase(ctx, repo, base, observation); err != nil {
		t.Fatal(err)
	}
	if err := checkAccountingBase(ctx, repo, head, observation); err == nil {
		t.Fatal("candidate commit impersonated protected-master base")
	}
	if err := writeFiniteHistory(repo, plan.Output); err != nil {
		t.Fatal(err)
	}
	final := commit()
	actual, err := accountingHistoryAt(ctx, repo, final)
	if err != nil || !reflect.DeepEqual(actual, plan.Output) {
		t.Fatalf("append did not preserve all executable provenance: %v", err)
	}
	tree, err := accountingSourceTree(ctx, repo, final)
	if err != nil || tree != plan.SourceTree {
		t.Fatal("accounting append modified frozen application source")
	}
	if _, err := deriveFiniteAccountingPlan(ctx, repo, base, final, observation); err == nil {
		t.Fatal("prewritten successor accepted as author-approved evidence")
	}
	put("go.mod", []byte("module other\n"))
	foreign := commit()
	if _, err := deriveFiniteAccountingPlan(ctx, repo, final, foreign, observation); err == nil {
		t.Fatal("changed guard dependency accepted")
	}
	put("go.mod", []byte("module fixture\n"))
	commit()
	put(formatproof.Source, bytes.Replace(first, []byte("bool"), []byte("int "), 1))
	semantic := commit()
	if _, err := deriveFiniteAccountingPlan(ctx, repo, final, semantic, observation); err == nil {
		t.Fatal("semantic source change accepted as hash refresh")
	}
	if _, err := formatproof.Read([]byte(`{"version":1,"records":[],"new_exception":true}`)); err == nil {
		t.Fatal("new exception accepted")
	}
}

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
