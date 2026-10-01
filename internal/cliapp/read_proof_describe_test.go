package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadProofFactoringCompiledDescribe(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "swarm")
	repo := RepoRoot()
	build := exec.Command("go", "build", "-o", binary, "./cmd/swarm")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public describe binary: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(repo, "internal/releasee2e/testdata/read_proof_describe_baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	var baseline struct {
		Baseline string `json:"baseline"`
		Results  []struct {
			Fixture      string `json:"fixture"`
			Surface      string `json:"surface"`
			StdoutSHA256 string `json:"stdout_sha256"`
		} `json:"results"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil || len(baseline.Results) != 45 {
		t.Fatalf("pre-extraction baseline: %v, rows=%d", err, len(baseline.Results))
	}
	surfaces := map[string][]string{
		"describe-text": {"describe"}, "describe-json": {"describe", "--json"},
		"describe-quiet": {"describe", "--quiet"}, "describe-no-color": {"describe", "--no-color"},
		"describe-graph-text": {"describe", "--graph"}, "describe-graph-json": {"describe", "--graph", "--json"},
		"routes-text": {"describe", "routes"}, "routes-json": {"describe", "routes", "--json"},
		"routes-quiet": {"describe", "routes", "--quiet"},
	}
	env := readProofCompiledScopeEnv(filepath.Join(root, "home"))
	for _, row := range baseline.Results {
		t.Run(row.Fixture+"/"+row.Surface, func(t *testing.T) {
			args, ok := surfaces[row.Surface]
			if !ok {
				t.Fatal(row.Surface)
			}
			for repetition := 0; repetition < 2; repetition++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				cmd := exec.CommandContext(ctx, binary, append(append([]string{}, args...), filepath.Join(repo, row.Fixture))...)
				cmd.Dir, cmd.Env = repo, env
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				err := cmd.Run()
				cancel()
				if err != nil || stderr.Len() != 0 {
					t.Fatalf("compiled %v: err=%v stdout=%s stderr=%s", args, err, &stdout, &stderr)
				}
				out := stdout.String()
				got := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.ReplaceAll(out, repo, "<repo>"))))
				if got != row.StdoutSHA256 {
					t.Fatalf("%s pre-extraction %s output changed: sha=%s want=%s\n%s", baseline.Baseline, row.Surface, got, row.StdoutSHA256, out)
				}
			}
		})
	}
}
