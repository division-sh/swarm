package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func accountingExecutionFixture(t *testing.T) (string, accountingProofSnapshot) {
	t.Helper()
	repo, _, _, _ := accountingSnapshotFixture(t)
	files := map[string]string{
		"cmd/swarm-test/main.go": `package main
import ("fmt"; "os"; "os/exec")
func main() {
 if len(os.Args) > 1 && os.Args[1] == "preflight" {
  body, err := os.ReadFile("tests/tier3-list-processing/test-fan-out-basic/schema.yaml")
  if err != nil { panic(err) }
  cwd, err := os.Getwd(); if err != nil { panic(err) }
  fmt.Printf("owned preflight cwd=%s input=%s", cwd, body)
  os.Exit(7)
 }
 if len(os.Args) < 3 || os.Args[1] != "--" { panic("unexpected fixture command") }
 cmd := exec.Command("go", append([]string{"test"}, os.Args[2:]...)...)
 cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
 if err := cmd.Run(); err != nil { os.Exit(1) }
}
`,
		"guard/guard_test.go": `package guard
import ("os"; "testing")
func TestFrozenData(t *testing.T) {
 body, err := os.ReadFile("../tests/tier3-list-processing/test-fan-out-basic/schema.yaml")
 if err != nil || string(body) != "original proof input\n" { t.Fatalf("mutable runtime input: %q %v", body, err) }
}
`,
	}
	for name, body := range files {
		path := filepath.Join(repo, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "-qm", "controlled guard process fixture")
	head := completionGit(t, repo, "rev-parse", "HEAD")
	snapshot := freezeAccountingFixture(t, repo, head, accountingPolicyObservation{Commit: head, Policy: accountingPolicy{ExecutorSource: head}})
	oldRoot, oldDigest, oldOrigin, oldConfig := accountingProofRoot, accountingProofDigest, accountingProofOrigin, accountingProofConfig
	accountingProofRoot, accountingProofDigest = snapshot.Root, accountingManifestDigest(snapshot.Manifest)
	accountingProofOrigin, accountingProofConfig = snapshot.Origin, snapshot.ConfigDigest
	t.Cleanup(func() {
		accountingProofRoot, accountingProofDigest, accountingProofOrigin, accountingProofConfig = oldRoot, oldDigest, oldOrigin, oldConfig
	})
	return repo, snapshot
}

func TestAccountingGuardChildrenUseFrozenSourceAndRetainTheirActualLogs(t *testing.T) {
	repo, snapshot := accountingExecutionFixture(t)
	if err := os.WriteFile(filepath.Join(repo, accountingRuntimeFixturePaths[0]), []byte("caller-only hostile data"), 0600); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	guard, err := runAccountingGuards(context.Background(), snapshot.Root, directory, "fixture/guard", "^TestFrozenData$", []string{"TestFrozenData"})
	if err != nil {
		t.Fatalf("actual guard child read the caller rather than its proof source: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	// This verifies retained execution integrity, not permission to publish a
	// certificate: the actual receipt consumer must re-execute mandatory proofs.
	required := []accountingGuardReceipt{{Package: "fixture/guard", Run: "^TestFrozenData$", Roots: []string{"TestFrozenData"}}}
	if err := validateAccountingGuards(directory, required, []accountingGuardReceipt{guard}); err != nil {
		t.Fatalf("proof disposal invalidated actual guard evidence: %v", err)
	}
}

func TestAccountingPreflightChildUsesFrozenSourceAndCannotFabricateCompletion(t *testing.T) {
	repo, snapshot := accountingExecutionFixture(t)
	if err := os.WriteFile(filepath.Join(repo, accountingRuntimeFixturePaths[0]), []byte("caller-only hostile data"), 0600); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path, digest, err := runAccountingPreflight(context.Background(), snapshot.Root, directory, snapshot.Manifest.Head, snapshot.Origin)
	if err == nil || path != "" || digest != "" {
		t.Fatalf("failed preflight fabricated retained completion: %q %q %v", path, digest, err)
	}
	log, readErr := os.ReadFile(filepath.Join(directory, "preflight-command.log"))
	if readErr != nil || !strings.Contains(string(log), "owned preflight cwd="+snapshot.Root+" input=original proof input") {
		t.Fatalf("preflight child was not rooted in its exact snapshot: %s %v", log, readErr)
	}
}
