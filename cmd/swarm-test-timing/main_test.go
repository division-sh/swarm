package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testtiming"
)

func TestGenerateShardSnapshotUsesSourceLabel(t *testing.T) {
	dir := t.TempDir()
	packagesPath := filepath.Join(dir, "packages.txt")
	weightsPath := filepath.Join(dir, "weights.json")
	snapshotPath := filepath.Join(dir, "snapshot.json")

	if err := os.WriteFile(packagesPath, []byte("github.com/division-sh/swarm/a\ngithub.com/division-sh/swarm/b\n"), 0o600); err != nil {
		t.Fatalf("write packages: %v", err)
	}
	weights := []byte(`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":3}` + "\n" +
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Elapsed":1}` + "\n")
	if err := os.WriteFile(weightsPath, weights, 0o600); err != nil {
		t.Fatalf("write weights: %v", err)
	}

	if err := run(runConfig{
		packagesPath:   packagesPath,
		weightsPath:    weightsPath,
		snapshotPath:   snapshotPath,
		sourceLabel:    "github-actions-run-123",
		shardCount:     2,
		maxImbalance:   0.25,
		generateShards: true,
	}); err != nil {
		t.Fatalf("run generate shards: %v", err)
	}

	snapshot, err := readShardSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if snapshot.Source != "github-actions-run-123" {
		t.Fatalf("source = %q, want source label", snapshot.Source)
	}
	if snapshot.ShardCount != 2 || len(snapshot.Shards) != 2 {
		t.Fatalf("snapshot shards = %+v, want 2 shards", snapshot.Shards)
	}
}

func TestPrintShardPackagesUsesCanonicalLineFormat(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	if err := writeJSONFile(snapshotPath, testtiming.ShardSnapshot{
		Version:    testtiming.ShardSnapshotVersion,
		ShardCount: 1,
		Shards: []testtiming.PackageShard{{
			ID:       1,
			Packages: []string{"github.com/division-sh/swarm/a", "github.com/division-sh/swarm/b"},
		}},
	}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	snapshot, err := readShardSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var output bytes.Buffer
	if err := writeShardPackages(&output, snapshot, 1); err != nil {
		t.Fatalf("writeShardPackages: %v", err)
	}
	parsed, err := testtiming.ReadPackageList(strings.NewReader(output.String()))
	if err != nil {
		t.Fatalf("ReadPackageList: %v", err)
	}
	want := []string{"github.com/division-sh/swarm/a", "github.com/division-sh/swarm/b"}
	if strings.Join(parsed, " ") != strings.Join(want, " ") {
		t.Fatalf("parsed packages = %v, want %v", parsed, want)
	}
}

func TestRecordCommandEvidenceWritesCompleteTypedReport(t *testing.T) {
	dir := t.TempDir()
	packagesPath := filepath.Join(dir, "packages.txt")
	inputPath := filepath.Join(dir, "go-test.json")
	evidencePath := filepath.Join(dir, "shard-1-primary-evidence.json")
	writeTestFile(t, packagesPath, "github.com/division-sh/swarm/a\n")
	writeTestFile(t, inputPath, `{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":12.5}`+"\n")

	err := run(runConfig{
		inputPath:      inputPath,
		packagesPath:   packagesPath,
		evidencePath:   evidencePath,
		surface:        testtiming.ShardSurface(1),
		attempt:        testtiming.AttemptPrimary,
		elapsedSeconds: 15,
		exitCode:       0,
		environmentID:  "ci-postgres-v1",
		countMode:      testtiming.CountModeCacheDefault,
		recordEvidence: true,
	})
	if err != nil {
		t.Fatalf("run record evidence: %v", err)
	}
	evidence, err := readCommandEvidence(evidencePath)
	if err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	if problems := testtiming.ValidateCommandEvidence(evidence); len(problems) > 0 {
		t.Fatalf("evidence problems = %v", problems)
	}
	if len(evidence.Report.Packages) != 1 || evidence.Report.Packages[0].Elapsed != 12.5 {
		t.Fatalf("complete report = %+v", evidence.Report)
	}
}

func TestEvaluateTimingBudgetWritesSharedPassResult(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.yaml")
	snapshotPath := filepath.Join(dir, "snapshot.json")
	fullPackagesPath := filepath.Join(dir, "full-packages.txt")
	evidenceRoot := filepath.Join(dir, "evidence")
	resultPath := filepath.Join(dir, "result.json")
	markdownPath := filepath.Join(dir, "result.md")
	if err := os.MkdirAll(evidenceRoot, 0o755); err != nil {
		t.Fatalf("mkdir evidence: %v", err)
	}
	writeTestFile(t, policyPath, testPolicyYAML())
	writeTestFile(t, fullPackagesPath, "catalog\n")
	if err := writeJSONFile(snapshotPath, testtiming.ShardSnapshot{
		Version:    testtiming.ShardSnapshotVersion,
		ShardCount: 1,
		Shards:     []testtiming.PackageShard{{ID: 1, Packages: []string{"a"}}},
	}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	evidence := commandEvidenceFixture(testtiming.ShardSurface(1), 10)
	if err := writeJSONFile(filepath.Join(evidenceRoot, "shard-1-primary-evidence.json"), evidence); err != nil {
		t.Fatalf("write evidence: %v", err)
	}

	err := run(runConfig{
		policyPath:       policyPath,
		snapshotPath:     snapshotPath,
		fullPackagesPath: fullPackagesPath,
		evidenceRoot:     evidenceRoot,
		resultJSONPath:   resultPath,
		markdownPath:     markdownPath,
		evaluateBudget:   true,
	})
	if err != nil {
		t.Fatalf("run evaluate budget: %v", err)
	}
	var result testtiming.BudgetResult
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	markdown, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	if result.Status != testtiming.BudgetPass || !strings.Contains(string(markdown), "**Status: PASS**") {
		t.Fatalf("result=%+v markdown=%s", result, markdown)
	}
}

func TestEvaluateTimingBudgetWritesIncompleteResultWhenArtifactMissing(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.yaml")
	snapshotPath := filepath.Join(dir, "snapshot.json")
	fullPackagesPath := filepath.Join(dir, "full-packages.txt")
	resultPath := filepath.Join(dir, "result.json")
	markdownPath := filepath.Join(dir, "result.md")
	writeTestFile(t, policyPath, testPolicyYAML())
	writeTestFile(t, fullPackagesPath, "catalog\n")
	if err := writeJSONFile(snapshotPath, testtiming.ShardSnapshot{
		Version:    testtiming.ShardSnapshotVersion,
		ShardCount: 1,
		Shards:     []testtiming.PackageShard{{ID: 1, Packages: []string{"a"}}},
	}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	err := run(runConfig{
		policyPath:       policyPath,
		snapshotPath:     snapshotPath,
		fullPackagesPath: fullPackagesPath,
		evidenceRoot:     filepath.Join(dir, "missing"),
		resultJSONPath:   resultPath,
		markdownPath:     markdownPath,
		evaluateBudget:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "INCOMPLETE") {
		t.Fatalf("run error = %v, want INCOMPLETE", err)
	}
	data, readErr := os.ReadFile(resultPath)
	if readErr != nil {
		t.Fatalf("read result: %v", readErr)
	}
	if !strings.Contains(string(data), `"status": "INCOMPLETE"`) {
		t.Fatalf("result = %s", data)
	}
}

func commandEvidenceFixture(surface string, elapsed float64) testtiming.CommandEvidence {
	return testtiming.CommandEvidence{
		Version:        testtiming.CommandEvidenceVersion,
		Surface:        surface,
		Attempt:        testtiming.AttemptPrimary,
		ElapsedSeconds: elapsed,
		Packages:       []string{"a"},
		EnvironmentID:  "ci-postgres-v1",
		CountMode:      testtiming.CountModeCacheDefault,
		Report: testtiming.Report{
			Summary:  testtiming.Summary{Events: 1, Packages: 1, PackageElapsedSec: 1},
			Packages: []testtiming.PackageTiming{{Package: "a", Result: "pass", Elapsed: 1}},
		},
	}
}

func testPolicyYAML() string {
	return `version: 1
hard:
  max_shard_command_seconds:
    limit_seconds: 270
    justification: Test broad ceiling.
  full_conformance_command_seconds:
    limit_seconds: 330
    justification: Test full ceiling.
package_reference_seconds:
  a: 1
  catalog: 1
`
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
