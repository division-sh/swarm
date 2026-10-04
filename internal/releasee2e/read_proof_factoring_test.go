package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadProofFactoringCompiledSurfaces(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	lifecycle := buildOwnedMockLifecycleBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			cwd := goldenReleaseRoot(t)
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
				{"agent", "list"}, {"agent", "view", conversation.AgentID, "--run-id", runID}, {"agent", "diagnose", conversation.AgentID, "--run-id", runID},
				{"agent", "view", "scout-w", "--run-id", runID},
				{"agent", "diagnose", "scout-w", "--run-id", runID},
				{"agent", "deliveries", "scout-w", "--run-id", runID},
				{"agent", "deliveries", conversation.AgentID, "--run-id", runID},
				{"conversation", "list", "--agent-id", conversation.AgentID, "--run-id", runID},
				{"conversation", "list"},
				{"conversation", "list", "--run-id", runID}, {"conversation", "view", conversation.SessionID}, {"conversation", "turn", conversation.SessionID, turns[0].TurnID},
			}
			for i, command := range commands {
				modes := []string{""}
				if command[0] != "agent" || command[1] != "list" {
					modes = append(modes, "--json", "--quiet")
				}
				for _, mode := range modes {
					args := append(append([]string{}, command...), "--config", "swarm.yaml", "--api-server", process.apiBase, "--api-token-file", "api-token")
					if mode != "" {
						args = append(args, mode)
					}
					t.Run(strings.Join(command[:2], "-")+fmt.Sprintf("-%d", i)+mode, func(t *testing.T) {
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
