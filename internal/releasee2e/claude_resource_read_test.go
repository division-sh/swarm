package releasee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const releaseResourceReadEnv = "RELEASE_E2E_RESOURCE_READ"

const releaseResourceReadUsage = "Read only data admitted for this actor and run. Use static_file with a delivered static_id, resource_row with one structured declaration and its key or position, or resource_rows with one structured declaration and a bounded page. Resource reads use the run-pinned version; continue only with the returned cursor. Do not provide filenames or host paths, select latest versions, or use this tool for mutable artifacts."

func TestClaudeResourceReadSupportedServeRestart(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for dual-store public MCP proof", goldenPostgresEnv)
	}
	base := t.TempDir()
	binary := buildReleaseBinary(t, base)
	unlock := acquireReleaseMCPPortLock(t)
	defer unlock()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			contracts := filepath.Join(root, "contracts")
			copyReleaseTree(t, canonicalrouting.CopyClaudeResourceRead(t), contracts)
			rows := "{\"id\":\"a\",\"reference_text\":\"alpha\"}\n{\"id\":\"b\",\"reference_text\":\"beta\"}\n"
			writeReleaseFile(t, filepath.Join(root, "rows.jsonl"), rows)
			writeReleaseFile(t, filepath.Join(root, "payload.json"), "{\"request\":[\"read pinned references\"]}\n")
			writeReleaseFile(t, filepath.Join(root, "go.mod"), "module resource-read-public\n\ngo 1.23.0\n")
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			configPath := filepath.Join(root, ".swarm/swarm.yaml")
			writeReleaseFile(t, configPath, "runtime:\n  recovery_on_startup: true\nworkspace:\n  backend: docker\n"+store.configYAML)
			writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
			home, fakeRoot, fakeBin := filepath.Join(root, "home"), filepath.Join(root, "docker-state"), filepath.Join(root, "bin")
			for _, dir := range []string{fakeRoot, fakeBin} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			testBinary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			writeExecutable(t, filepath.Join(fakeBin, "docker"), fmt.Sprintf("#!/bin/sh\n%s=1 exec %s -- \"$@\"\n", fakeDockerHelperEnv, shellQuote(testBinary)))
			env := append(releaseProcessEnv(fakeBin, fakeRoot, home), releaseResourceReadEnv+"=1")
			if store.passwordEnv != "" {
				env = append(env, goldenPostgresPass+"="+store.passwordEnv)
			}
			writeReleaseFile(t, filepath.Join(home, ".config/swarm/swarm.yaml"), "connection:\n  api_token_file: "+filepath.Join(root, "api-token")+"\n")
			for _, args := range [][]string{{"verify", contracts, "--config", configPath}, {"secrets", "set", "CLAUDE_CODE_OAUTH_TOKEN", "--stdin", "--config", configPath}} {
				result := runReleaseCommand(t, goldenStartupTimeout, root, env, releaseE2EOAuthToken+"\n", binary, args...)
				if result.err != nil {
					t.Fatalf("public prerequisite %v: %v\n%s", args, result.err, result.output)
				}
			}
			start := func(hash string) *releaseServeProcess {
				requireDefaultMCPPortAvailable(t)
				options := releaseProcessSpec{BinaryPath: binary, WorkingDir: root, ConfigPath: configPath, Store: backend, WorkspaceBackend: "docker", APIPort: freeReleaseTCPPort(t), MCPListenPort: 8082, TokenFile: filepath.Join(root, "api-token"), Token: goldenAPIToken, Env: env, ShutdownGrace: goldenShutdownGrace}
				if hash == "" {
					options.Source = contracts
				} else {
					options.BundleHash = hash
				}
				process := startReleaseServe(t, options)
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := process.waitReady(ctx); err != nil {
					t.Fatalf("public serve: %v\n%s\n%s", err, process.output.String(), fakeDockerLogText(t, fakeRoot))
				}
				return process
			}
			process := start("")
			hash := goldenServedBundleHash(t, process.rpc, "live")
			runID := uuid.NewString()
			var firstEvidence fullLifecycleEvidenceSnapshot
			for turn := 1; turn <= 2; turn++ {
				args := []string{"run", "start", "--connect", process.apiBase, "--bundle-hash", hash, "--run-id", runID, "--idempotency-key", runID, "--event", "task.assigned", "--payload", "payload.json", "--data", "worker/records.loaded=rows.jsonl", "--no-follow"}
				if turn == 2 {
					args = []string{"agent", "directive", "release-worker", "read the same pinned references again", "--api-server", process.apiBase, "--run-id", runID, "--flow-instance", "worker", "--idempotency-key", uuid.NewString()}
				}
				result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
				if result.err != nil {
					events, readErr := listGoldenEvents(context.Background(), process.rpc, runID)
					encoded, _ := json.Marshal(events)
					t.Fatalf("public turn: %v\n%s\nserve=%s\ndocker=%s\nevents=%s read=%v", result.err, result.output, process.output.String(), fakeDockerLogText(t, fakeRoot), encoded, readErr)
				}
				waitResourceReadDeliveries(t, process.rpc, runID, turn)
				evidence := captureFullLifecycleEvidence(t, process.rpc, runID)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				events, err := listGoldenEvents(ctx, process.rpc, runID)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				completed := 0
				for _, event := range events {
					if event.EventName == "worker/agent.completed" {
						completed++
					}
					for _, delivery := range event.Deliveries {
						if !delivery.Terminal || delivery.Status != "delivered" || len(event.DeadLetters) != 0 {
							t.Fatalf("unsettled public delivery %+v", delivery)
						}
					}
				}
				if completed != turn {
					t.Fatalf("emitted event cardinality %d, want %d", completed, turn)
				}
				if turn == 2 {
					// The identical public keyed directive must return its receipt,
					// not invoke a third provider or create new event/delivery facts.
					replayed := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
					if replayed.err != nil {
						t.Fatalf("directive replay: %v\n%s", replayed.err, replayed.output)
					}
					afterReplay := captureFullLifecycleEvidence(t, process.rpc, runID)
					if !reflect.DeepEqual(evidence.EventFacts, afterReplay.EventFacts) {
						t.Fatal("keyed directive replay changed event/delivery facts")
					}
				}
				if turn == 1 {
					firstEvidence = evidence
					if err := process.stopAndWait(goldenShutdownGrace); err != nil {
						t.Fatal(err)
					}
					process = start(hash)
					restored := captureFullLifecycleEvidence(t, process.rpc, runID)
					for eventID, want := range firstEvidence.EventFacts {
						if got := restored.EventFacts[eventID]; got != want {
							t.Fatalf("restart changed old event/delivery %s: before=%s after=%s", eventID, want, got)
						}
					}
				}
			}
			if err := process.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatal(err)
			}
			assertReleaseExternalProcessesExited(t, fakeRoot)
			var receipts []struct {
				Definition      string
				Session, Resume string
				Row, Page       map[string]any
			}
			raw, err := os.ReadFile(filepath.Join(fakeRoot, "resource-receipts.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				var receipt struct {
					Definition      string
					Session, Resume string
					Row, Page       map[string]any
				}
				if err := json.Unmarshal([]byte(line), &receipt); err != nil {
					t.Fatal(err)
				}
				receipts = append(receipts, receipt)
			}
			if len(receipts) != 2 || receipts[0].Resume != "" || receipts[1].Resume != receipts[0].Session || receipts[1].Session == receipts[0].Session || receipts[0].Definition != receipts[1].Definition || !reflect.DeepEqual(receipts[0].Row, receipts[1].Row) || !reflect.DeepEqual(receipts[0].Page, receipts[1].Page) {
				t.Fatalf("public MCP identities/pinned rows across restart %+v", receipts)
			}
			t.Log("proof_surface=public compiled serve/hash restart/run start + turn-authorized HTTP MCP; deterministic provider, not genuine Claude/Telegram")
		})
	}
}

func runResourceReadClaudeTurn(root string, invocation fakeClaudeInvocation) int {
	if resumed := dockerOptionValue(invocation.commandArgs, "--resume"); resumed != "" {
		if err := fakeResourceProviderHead(root, invocation.container, resumed, false); err != nil {
			return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
		}
	}
	client := &http.Client{Timeout: 15 * time.Second}
	listed, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/list", map[string]any{}, "resource-list", true)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	var definition map[string]any
	for _, raw := range listed["tools"].([]any) {
		tool := raw.(map[string]any)
		if tool["name"] == "read_flow_data" {
			definition = tool
		}
	}
	if definition == nil || !strings.Contains(definition["description"].(string), "\n\nUsage:\n"+releaseResourceReadUsage) {
		return fakeDockerUnexpected(root, invocation.commandArgs, "wrong delivered resource description")
	}
	call := func(kind string, fields map[string]any) (map[string]any, error) {
		fields["kind"], fields["declaration"] = kind, map[string]any{"flow_path": "worker", "event": "worker/records.loaded"}
		result, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/call", map[string]any{"name": "read_flow_data", "arguments": fields, "_meta": map[string]any{"claudecode/toolUseId": "toolu-resource-" + kind}}, kind, true)
		if err != nil {
			return nil, err
		}
		return resourceReadMCPResult(result)
	}
	row, err := call("resource_row", map[string]any{"key": "a"})
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	page, err := call("resource_rows", map[string]any{"page": map[string]any{"limit": 2}})
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	if row["row"].(map[string]any)["value"].(map[string]any)["reference_text"] != "alpha" || page["rows"].(map[string]any)["item_count"] != float64(2) || row["version_id"] != page["version_id"] {
		return fakeDockerUnexpected(root, invocation.commandArgs, "wrong pinned resource result")
	}
	file, err := os.OpenFile(filepath.Join(root, "resource-receipts.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	definitionBytes, err := json.Marshal(definition)
	if err != nil {
		_ = file.Close()
		return fakeDockerUnexpected(root, invocation.commandArgs, "resource definition encoding failed")
	}
	err = json.NewEncoder(file).Encode(struct {
		Definition      string
		Session, Resume string
		Row, Page       map[string]any
	}{string(definitionBytes), invocation.sessionID, dockerOptionValue(invocation.commandArgs, "--resume"), row, page})
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, "resource receipt write failed")
	}
	writeClaudeInit(invocation.sessionID, splitAllowedTools(dockerOptionValue(invocation.commandArgs, "--allowedTools")))
	result, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/call", map[string]any{"name": "emit_agent_completed", "arguments": map[string]any{"flow_result": "resource read complete"}, "_meta": map[string]any{"claudecode/toolUseId": "toolu-resource-emit"}}, "resource-emit", true)
	if err != nil || result["isError"] == true {
		return fakeDockerUnexpected(root, invocation.commandArgs, fmt.Sprintf("resource emit %v %+v", err, result))
	}
	if err := fakeResourceProviderHead(root, invocation.container, invocation.sessionID, true); err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"type": "result", "subtype": "success", "session_id": invocation.sessionID, "result": "resource read complete"})
	return 0
}

func waitResourceReadDeliveries(t *testing.T, rpc *releaseRPCClient, runID string, turns int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	for ctx.Err() == nil {
		events, err := listGoldenEvents(ctx, rpc, runID)
		if err != nil {
			t.Fatal(err)
		}
		completed, rows, pending := 0, 0, false
		for _, event := range events {
			if len(event.DeadLetters) != 0 {
				t.Fatalf("public resource journey dead letter: %+v", event)
			}
			if event.EventName == "worker/agent.completed" {
				completed++
			}
			if event.EventName == "worker/records.loaded" {
				rows++
			}
			for _, delivery := range event.Deliveries {
				pending = pending || !delivery.Terminal
			}
		}
		if completed == turns && rows == 2 && !pending {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("resource turn/feed deliveries did not settle")
}

func resourceReadMCPResult(result map[string]any) (map[string]any, error) {
	if result["isError"] == true {
		return nil, fmt.Errorf("resource tool error %+v", result)
	}
	if structured, ok := result["structuredContent"].(map[string]any); ok {
		return structured, nil
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		return nil, fmt.Errorf("resource content %+v", result)
	}
	entry := content[0].(map[string]any)
	var payload map[string]any
	err := json.Unmarshal([]byte(entry["text"].(string)), &payload)
	return payload, err
}
