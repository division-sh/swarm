package releasee2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
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

func TestReadProofFactoringCompiledDescribe(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	repo := releaseE2ERepoRoot(t)
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
	for _, row := range baseline.Results {
		t.Run(row.Fixture+"/"+row.Surface, func(t *testing.T) {
			args, ok := surfaces[row.Surface]
			if !ok {
				t.Fatal(row.Surface)
			}
			for repetition := 0; repetition < 2; repetition++ {
				out := readProofCompiledCommand(t, binary, repo, goldenProcessEnv(t, root, "", 0), append(append([]string{}, args...), filepath.Join(repo, row.Fixture))...)
				got := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.ReplaceAll(out, repo, "<repo>"))))
				if got != row.StdoutSHA256 {
					t.Fatalf("%s pre-extraction %s output changed: sha=%s want=%s\n%s", baseline.Baseline, row.Surface, got, row.StdoutSHA256, out)
				}
			}
		})
	}
}

func TestReadProofFactoringCompiledSurfaces(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	lifecycle := buildOwnedMockLifecycleBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			cwd := t.TempDir()
			store := goldenSQLiteStore(cwd)
			if backend == "postgres" {
				store = goldenPostgresStore(t, os.Getenv(goldenPostgresEnv))
			}
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/golden_agent_workload"), filepath.Join(cwd, "contracts"))
			writeReleaseFile(t, filepath.Join(cwd, "swarm.yaml"), goldenRuntimeConfig(store))
			writeReleaseFile(t, filepath.Join(cwd, "api-token"), goldenAPIToken+"\n")
			env := goldenProcessEnv(t, cwd, store.passwordEnv, 0)
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, InternalMockLifecycleBinary: lifecycle, WorkingDir: cwd, ConfigPath: "swarm.yaml", Source: "contracts", Store: backend, APIPort: 0, TokenFile: "api-token", Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatal(err)
			}
			runID := goldenPublishIngress(t, process.rpc, goldenServedBundleHash(t, process.rpc, "mock_only"), goldenSmokeCandidateIDs)
			waitForGoldenTerminalRun(t, process, store, runID, goldenRunDeadline)
			conversations := listGoldenConversations(t, ctx, process.rpc, runID)
			if len(conversations) == 0 {
				t.Fatal("no real conversation")
			}
			// Materialized candidate agents are retired after run completion. The
			// declared scout remains readable by the operational agent owner.
			var conversation goldenConversation
			for _, item := range conversations {
				if item.AgentID == "scout-worker" {
					conversation = item
					break
				}
			}
			if conversation.SessionID == "" {
				t.Fatalf("scout conversation absent: %#v", conversations)
			}
			turns := listGoldenTurns(t, ctx, process.rpc, conversation.SessionID)
			if len(turns) == 0 {
				t.Fatal("no real turn")
			}
			for _, method := range []string{"agent.list", "agent.get", "agent.diagnose", "agent.delivery_diagnostics", "agent.delivery_lifecycle", "agent.usage", "conversation.list", "conversation.list_turns", "conversation.get_turn"} {
				params := map[string]any{"run_id": runID, "agent_id": conversation.AgentID, "session_id": conversation.SessionID, "turn_id": turns[0].TurnID}
				// Public schema admission requires only the parameters owned by each method.
				switch method {
				case "agent.list":
					params = map[string]any{}
				case "conversation.list":
					params = map[string]any{"run_id": runID}
				case "conversation.list_turns":
					params = map[string]any{"session_id": conversation.SessionID}
				case "conversation.get_turn":
					params = map[string]any{"session_id": conversation.SessionID, "turn_id": turns[0].TurnID}
				default:
					params = map[string]any{"run_id": runID, "agent_id": conversation.AgentID}
				}
				var result json.RawMessage
				if err := process.rpc.call(ctx, method, params, &result); err != nil {
					t.Fatalf("real %s: %v", method, err)
				}
				if len(result) == 0 {
					t.Fatal(method + " empty response")
				}
			}
			commands := [][]string{
				{"agent", "list"}, {"agent", "view", conversation.AgentID}, {"agent", "diagnose", conversation.AgentID},
				{"agent", "deliveries", conversation.AgentID},
				{"agent", "deliveries", conversation.AgentID, "--run-id", runID},
				{"conversation", "list", "--agent-id", conversation.AgentID},
				{"conversation", "list", "--run-id", runID}, {"conversation", "view", conversation.SessionID}, {"conversation", "turn", conversation.SessionID, turns[0].TurnID},
			}
			for _, command := range commands {
				modes := []string{""}
				if command[0] != "agent" || command[1] != "list" {
					modes = append(modes, "--json", "--quiet")
				}
				for _, mode := range modes {
					args := append(append([]string{}, command...), "--config", "swarm.yaml", "--api-server", process.apiBase, "--api-token-file", "api-token")
					if mode != "" {
						args = append(args, mode)
					}
					t.Run(strings.Join(command[:2], "-")+fmt.Sprintf("-%d", len(command))+mode, func(t *testing.T) {
						out := readProofCompiledCommand(t, binary, cwd, env, args...)
						if strings.TrimSpace(out) == "" {
							t.Fatalf("empty public read %v", args)
						}
					})
				}
			}
			t.Log("proof_surface=H for retained state; public RPC and compiled read commands; no live-provider or C11 credit")
		})
	}
}

func readProofCompiledCommand(t *testing.T, binary, cwd string, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = cwd, env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil || stderr.Len() != 0 {
		t.Fatalf("compiled %v: err=%v stdout=%s stderr=%s", args, err, &stdout, &stderr)
	}
	return stdout.String()
}

func TestReadProofFactoringCompiledScenario(t *testing.T) {
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
		t.Run(anchor, func(t *testing.T) {
			root := canonicalrouting.CopyMailboxCompletionMatrix(t)
			scenario, err := os.ReadFile(filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/read_proof_scenarios", anchor+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			writeReleaseFile(t, filepath.Join(root, "tests", anchor+".yaml"), string(scenario))
			env := goldenProcessEnv(t, filepath.Join(releaseRoot, anchor), "", 0)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "test", root, "tests/"+anchor+".yaml", "--timeout", "20s", "--poll-interval", "25ms")
			cmd.Dir, cmd.Env = releaseE2ERepoRoot(t), env
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil || stderr.Len() != 0 || !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
				t.Fatalf("compiled public MockOnly %s: err=%v stdout=%s stderr=%s", anchor, err, &stdout, &stderr)
			}
			// The authored expectations are executed through public event/entity reads.
			// Each scenario decides its anchor and settles the root work. Defer moves
			// a card out of the pending selector and is proven separately.
			t.Logf("proof_surface=T anchor=%s; fresh public command, exact authored public-read expectations passed\n%s", anchor, &stdout)
		})
	}
	for _, flag := range []string{"--json", "--quiet"} {
		t.Run("unsupported-"+flag[2:], func(t *testing.T) {
			root := t.TempDir()
			env := goldenProcessEnv(t, root, "", 0)
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
