package cliapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReadProofFactoringCompiledScenario(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	build := exec.Command("go", "build", "-o", binary, "./cmd/swarm")
	build.Dir = RepoRoot()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public scenario binary: %v\n%s", err, output)
	}
	for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
		t.Run(anchor, func(t *testing.T) {
			root := canonicalrouting.CopyMailboxCompletionMatrix(t)
			scenario, err := os.ReadFile(filepath.Join(RepoRoot(), "internal/releasee2e/testdata/read_proof_scenarios", anchor+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tests", anchor+".yaml"), string(scenario))
			env := readProofCompiledScopeEnv(t.TempDir())
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "test", root, "tests/"+anchor+".yaml", "--timeout", "20s", "--poll-interval", "25ms")
			cmd.Dir, cmd.Env = RepoRoot(), env
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil || stderr.Len() != 0 || !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
				t.Fatalf("compiled public MockOnly %s: err=%v stdout=%s stderr=%s", anchor, err, &stdout, &stderr)
			}
			// Public authored event/entity expectations settle each root. This is
			// fresh command-owned T proof, not retained serve lifecycle evidence.
			t.Logf("proof_surface=T anchor=%s; exact authored public-read expectations passed\n%s", anchor, &stdout)
		})
	}
	for _, flag := range []string{"--json", "--quiet"} {
		t.Run("unsupported-"+flag[2:], func(t *testing.T) {
			root := t.TempDir()
			env := readProofCompiledScopeEnv(root)
			before := readProofFileCensus(t, root)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "test", filepath.Join(root, "source-does-not-exist"), flag, "--config", filepath.Join(root, "config-does-not-exist"), "--api-server", server.URL)
			cmd.Dir, cmd.Env = root, env
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			var exit *exec.ExitError
			if err := cmd.Run(); !errors.As(err, &exit) || exit.ExitCode() != 2 {
				t.Fatalf("unsupported %s: err=%v stdout=%s stderr=%s", flag, err, &stdout, &stderr)
			}
			if stdout.Len() != 0 || strings.TrimSpace(stderr.String()) != "unknown flag: "+flag || calls.Load() != 0 || !reflect.DeepEqual(before, readProofFileCensus(t, root)) {
				t.Fatalf("refusal must precede source/session/RPC: stdout=%q stderr=%q calls=%d", stdout.String(), stderr.String(), calls.Load())
			}
		})
	}
}

func readProofFileCensus(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, fmt.Sprintf("%s:%s", rel, entry.Type()))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return entries
}
