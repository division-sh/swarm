package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// This uses an actually completed source turn, native worker, real MCP HTTP,
// and the public fork/chat APIs. The existing API-provider fork test remains a
// separate provider control; it cannot earn mock transport credit.
func TestMockForkChatPublicMCPTransportBothStores(t *testing.T) {
	proveMockForkChatPublicMCPTransportBothStores(t, false, false)
}

func TestMockForkChatRealDockerPublicMCPTransportBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Docker gateway journey; a skip earns no Docker proof")
	}
	proveMockForkChatPublicMCPTransportBothStores(t, true, false)
}

func TestMockForkChatOversizedPublicMCPTransportBothStores(t *testing.T) {
	proveMockForkChatPublicMCPTransportBothStores(t, false, true)
}

func TestMockForkChatOversizedRealDockerPublicMCPTransportBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Docker oversized gateway journey; a skip earns no Docker proof")
	}
	proveMockForkChatPublicMCPTransportBothStores(t, true, true)
}

func proveMockForkChatPublicMCPTransportBothStores(t *testing.T, docker, oversized bool) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyRootIngressServedConversationFork(t)
			if oversized {
				installOversizedForkChatSnapshotFixture(t, root)
			}
			path := filepath.Join(root, "fork-source", "agents.yaml")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(data, []byte("  mock: {kind: python, module: mocks/fork-source.py}\n")...), 0o600); err != nil {
				t.Fatal(err)
			}
			if oversized {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				sibling := "fork-sibling-agent:\n  role: researcher\n  intent: prompts/fork-sibling-agent.md\n  model: regular\n  memory: true\n  subscriptions: [fork.source_message]\n  mock: {kind: python, module: mocks/fork-source.py}\n"
				if err := os.WriteFile(path, append(data, []byte(sibling)...), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "fork-source", "prompts", "fork-sibling-agent.md"), []byte("Preserve the independent sibling workspace during the fork proof.\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			module := filepath.Join(root, "fork-source", "mocks", "fork-source.py")
			if err := os.MkdirAll(filepath.Dir(module), 0o700); err != nil {
				t.Fatal(err)
			}
			performance := `OVERSIZED_SNAPSHOT = False

def handle(input):
    names = [tool["name"] for tool in input["tools"]]
    if "fork_snapshot_read_entities" in names:
        if input["messages"][-1]["role"] != "tool":
            return {"calls": [{"name": "fork_snapshot_read_entities", "arguments": {}}, {"name": "emit_event", "arguments": {"event_name": "forkchat.note"}}], "usage": {"input_tokens": 4, "output_tokens": 2}}
        if OVERSIZED_SNAPSHOT:
            import json
            content = input["messages"][-1]["content"]
            reads = [entry for entry in json.loads(content) if entry["name"] == "fork_snapshot_read_entities"]
            assert len(reads) == 1 and reads[0]["ok"] is True
            assert reads[0]["result"]["truncated"] is True
            assert "follow_up" not in reads[0]["result"] and len(content) < 32768
        return {"text": "snapshot inspected; event remained sandboxed", "usage": {"input_tokens": 5, "output_tokens": 3}}
    if input["messages"][-1]["role"] != "tool" and "source receiver still executes" in str(input["messages"][-1]["content"]):
        return {"calls": [{"name": "notify_human", "arguments": {"summary": "source receiver still executes after sandbox cleanup"}}], "usage": {"input_tokens": 2, "output_tokens": 1}}
    return {"text": "source conversation preserved", "usage": {"input_tokens": 2, "output_tokens": 1}}
`
			if oversized {
				performance = strings.Replace(performance, "OVERSIZED_SNAPSHOT = False", "OVERSIZED_SNAPSHOT = True", 1)
			}
			if err := os.WriteFile(module, []byte(performance), 0o600); err != nil {
				t.Fatal(err)
			}
			var owner workspace.Resolver
			var refuseAuthority, disconnect atomic.Bool
			var disconnectedTarget, lastForkTarget atomic.Value
			factory := func(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error) {
				if docker {
					manager := workspace.NewDockerManager()
					cfg := workspace.DefaultDockerConfig()
					cfg.SourceProjection = projection
					// Explicit hardened-host topology is separate from default-Linux
					// acceptance, which runs in the hosted workspace-image journey.
					if network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK"); network != "" {
						cfg.WorkspaceNetwork = network
					}
					manager.SetConfig(cfg)
					manager.SetSemanticSource(source)
					owner = manager
					return &forkChatFaultDockerWorkspace{DockerManager: manager, refuseAuthority: &refuseAuthority, disconnect: &disconnect, disconnectedTarget: &disconnectedTarget, lastForkTarget: &lastForkTarget, network: cfg.WorkspaceNetwork}, nil
				}
				manager := workspace.NewHostManager()
				cfg := workspace.DefaultHostConfig()
				cfg.WorkspaceRoot, cfg.SourceProjection = t.TempDir(), projection
				manager.SetConfig(cfg)
				manager.SetSemanticSource(source)
				owner = manager
				return &forkChatFaultHostWorkspace{HostManager: manager, refuseAuthority: &refuseAuthority, lastForkTarget: &lastForkTarget}, nil
			}
			listener := "127.0.0.1:0"
			targetBackend := workspace.BackendHost
			if docker {
				listener = "0.0.0.0:0"
				targetBackend = workspace.BackendDocker
			}
			rt := startWorkspaceGatewayProofRuntime(t, backend, root, targetBackend, factory, listener)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "external.observed", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{}, "idempotency_key": "mock-forkchat-source",
			})
			ready := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "fork.source_message", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"note": "actual source conversation"}, "idempotency_key": "mock-forkchat-turn",
			})
			waitWorkspaceProofSourceAgentReady(t, rt, seed.RunID, ready.EventID)
			var sourceActor actors.AgentConfig
			for _, actor := range rt.Runtime.Manager.ListAgentConfigs() {
				if actor.ID == "fork-source-agent" && actor.Identity.RunID == seed.RunID {
					sourceActor = actor
				}
			}
			if sourceActor.Identity.IsZero() {
				t.Fatal("completed source turn has no live concrete receiver")
			}
			sourceTarget, err := owner.ResolveWorkspace(correlation.WithRunID(workspaceProofAuthorActivityContext(t, rt), seed.RunID), sourceActor)
			if err != nil {
				t.Fatal(err)
			}
			var verifySource func()
			if docker {
				manager := owner.(*workspace.DockerManager)
				initial, err := manager.InspectManagedContainer(context.Background(), sourceTarget.Container)
				if err != nil || !initial.Exists || !initial.Running || !initial.HasIdentity {
					t.Fatalf("source container baseline: %+v %v", initial, err)
				}
				if _, err := manager.RunDocker(context.Background(), "exec", "--user", "0", sourceTarget.Container, "sh", "-c", "printf source-workspace-survives > /workspace/forkchat-source-sentinel"); err != nil {
					t.Fatal(err)
				}
				verifySource = func() {
					t.Helper()
					current, err := manager.InspectManagedContainer(context.Background(), sourceTarget.Container)
					if err != nil || !reflect.DeepEqual(current, initial) {
						t.Fatalf("sandbox changed exact source container/labels/loop: before=%+v after=%+v err=%v", initial, current, err)
					}
					value, err := manager.RunDocker(context.Background(), "exec", sourceTarget.Container, "cat", "/workspace/forkchat-source-sentinel")
					if err != nil || value != "source-workspace-survives" {
						t.Fatalf("sandbox changed source file: %q %v", value, err)
					}
				}
			} else {
				path := filepath.Join(sourceTarget.Workdir, "forkchat-source-sentinel")
				if err := os.WriteFile(path, []byte("source-workspace-survives"), 0o600); err != nil {
					t.Fatal(err)
				}
				verifySource = func() {
					t.Helper()
					value, err := os.ReadFile(path)
					if err != nil || string(value) != "source-workspace-survives" {
						t.Fatalf("sandbox changed host source file: %q %v", value, err)
					}
				}
			}
			readLifecycle := func() manager.AgentLifecycleState {
				t.Helper()
				ctx := workspaceProofAuthorActivityContext(t, rt)
				state, found, err := rt.Lifecycle.LoadAgentLifecycleState(ctx, sourceActor.Identity)
				if err != nil || !found || state.Phase != manager.AgentLifecycleRunning {
					t.Fatalf("source lifecycle is not executable: %+v found=%v err=%v", state, found, err)
				}
				return state
			}
			initialLifecycle := readLifecycle()
			verifyWorkspace := verifySource
			verifySibling := func() {}
			if oversized {
				waitForkChatSiblingReady(t, rt, ready.EventID)
				requireWorkspaceProofMockTurn(t, rt, seed.RunID, "fork-sibling-agent", ready.EventID)
				var siblingActor actors.AgentConfig
				for _, actor := range rt.Runtime.Manager.ListAgentConfigs() {
					if actor.ID == "fork-sibling-agent" && actor.Identity.RunID == seed.RunID {
						siblingActor = actor
					}
				}
				if siblingActor.Identity.IsZero() {
					t.Fatal("oversized proof lost the actual sibling receiver")
				}
				sibling, err := owner.ResolveWorkspace(correlation.WithRunID(workspaceProofAuthorActivityContext(t, rt), seed.RunID), siblingActor)
				if err != nil || sibling.ExecutionTarget().Container == sourceTarget.Container && docker || !docker && sibling.Workdir == sourceTarget.Workdir {
					t.Fatalf("sibling did not retain a distinct workspace: %+v %v", sibling, err)
				}
				verifySibling = forkChatSiblingWorkspaceWitness(t, docker, owner, sibling)
			}
			verifySource = func() {
				t.Helper()
				verifyWorkspace()
				verifySibling()
				if current := readLifecycle(); !reflect.DeepEqual(current, initialLifecycle) {
					t.Fatalf("fork sandbox changed the exact source lifecycle generation: before=%+v after=%+v", initialLifecycle, current)
				}
			}
			turn := requireWorkspaceProofMockTurn(t, rt, seed.RunID, "fork-source-agent", ready.EventID)
			sessionID, turnID := turn.SessionID, turn.TurnID
			var created struct {
				Fork runfork.OperatorConversationForkSession `json:"fork"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork", map[string]any{
				"source_session_id": sessionID, "fork_point": map[string]any{"kind": "turn", "turn_id": turnID}, "idempotency_key": "mock-forkchat-create",
			}, &created)
			before := mockForkChatDomainCounts(t, rt, seed.RunID)
			chatRequest := func(params map[string]any, out *runfork.ConversationForkChatResult) {
				t.Helper()
				// A native two-frame WASI conversation and its HTTP tools are not
				// the API-stub response exercised by the five-second helper.
				response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "conversation.fork_chat", params, 30*time.Second)
				if response.Error != nil {
					turns, err := storetest.ReadConversationForkTurnDiagnostics(context.Background(), rt.Events, created.Fork.ForkID)
					var rows []string
					for _, turn := range turns {
						rows = append(rows, fmt.Sprintf("turn=%d state=%s failure=%s", turn.Index, turn.State, turn.Failure))
					}
					t.Fatalf("public mock fork chat: %+v\nconversation_fork_turns: %s read_error=%v", response.Error, strings.Join(rows, "; "), err)
				}
				if err := json.Unmarshal(response.Result, out); err != nil {
					t.Fatal(err)
				}
			}
			params := map[string]any{"fork_id": created.Fork.ForkID, "message": "inspect snapshot and attempt event", "idempotency_key": "mock-forkchat-chat"}
			var chat runfork.ConversationForkChatResult
			chatRequest(params, &chat)
			verifySource()
			if oversized {
				requireOversizedForkChatTargetAndProjection(t, chat, lastForkTarget.Load(), sourceTarget.ExecutionTarget(), docker)
				target := lastForkTarget.Load().(workspace.ExecutionTarget)
				if docker {
					info, err := owner.(*workspace.DockerManager).InspectManagedContainer(context.Background(), target.Container)
					if err != nil || info.Exists {
						t.Fatalf("oversized fork left its exact disposable container: %+v %v", info, err)
					}
				} else {
					path, err := target.ResolveHostPath(workspace.LogicalWorkspaceMount, workspace.PathAccessRead)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := os.Stat(path.HostPath); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("oversized fork left its exact disposable host target: %s %v", path.HostPath, err)
					}
				}
			}
			if chat.IdempotencyReplayed || chat.Snapshot.SourceTurn.TurnID != turnID || chat.Turn.TurnID == "" || !chat.Turn.ParseOK {
				t.Fatalf("exact public mock fork response: %+v", chat)
			}
			var read, stub int
			for _, call := range chat.Turn.ToolCalls {
				var result map[string]any
				if err := json.Unmarshal(call.Result, &result); err != nil {
					t.Fatal(err)
				}
				switch call.Name {
				case "fork_snapshot_read_entities":
					if result["status"] != "read_from_snapshot" || result["snapshot_owner"] != runfork.ConversationForkChatSnapshotOwner {
						t.Fatalf("HTTP tool did not consume frozen snapshot: %s", call.Result)
					}
					read++
				case "emit_event":
					if result["status"] != "stubbed" || result["live_mutation"] != false {
						t.Fatalf("HTTP tool escaped sandbox: %s", call.Result)
					}
					stub++
				default:
					t.Fatalf("unexpected tool %q", call.Name)
				}
			}
			if read != 1 || stub != 1 {
				t.Fatalf("mock sandbox tool cardinality: reads=%d stubs=%d", read, stub)
			}
			var replay runfork.ConversationForkChatResult
			chatRequest(params, &replay)
			verifySource()
			if !replay.IdempotencyReplayed || replay.Turn.TurnID != chat.Turn.TurnID {
				t.Fatalf("committed mock response replay: %+v", replay)
			}
			var next runfork.ConversationForkChatResult
			chatRequest(map[string]any{
				"fork_id": created.Fork.ForkID, "message": "continue the private conversation", "idempotency_key": "mock-forkchat-next",
			}, &next)
			verifySource()
			if next.Turn.TurnID == chat.Turn.TurnID || next.Turn.TurnIndex != chat.Turn.TurnIndex+1 || !next.Turn.ParseOK {
				t.Fatalf("mock fork continuation: %+v", next)
			}
			if after := mockForkChatDomainCounts(t, rt, seed.RunID); after != before {
				t.Fatalf("mock MCP sandbox mutated live facts: before=%+v after=%+v", before, after)
			}
			forkRows, err := storetest.ReadConversationForkStorage(context.Background(), rt.Events, created.Fork.ForkID)
			if err != nil || forkRows.Snapshots != 1 || forkRows.Turns != 2 {
				t.Fatalf("exact committed fork rows=%+v error=%v, want one snapshot and two turns", forkRows, err)
			}
			for _, fault := range []string{"stale-authority", "gateway-disconnect"} {
				// The oversized supplement qualifies result projection. The original
				// Docker root retains its exact network-loss/compensation proof.
				if fault == "gateway-disconnect" && (!docker || oversized) {
					continue
				}
				refuseAuthority.Store(fault == "stale-authority")
				disconnect.Store(fault == "gateway-disconnect")
				key := "mock-forkchat-" + fault
				params := map[string]any{"fork_id": created.Fork.ForkID, "message": "must refuse before the model", "idempotency_key": key}
				response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "conversation.fork_chat", params, 30*time.Second)
				if response.Error == nil {
					t.Fatalf("%s reached model execution", fault)
				}
				refused, err := storetest.ReadConversationForkTurnStorage(context.Background(), rt.Events, created.Fork.ForkID, key)
				if err != nil {
					t.Fatal(err)
				}
				state, failureRaw, refusedTurn := refused.State, refused.Failure, refused.TurnID
				failure, err := failures.UnmarshalEnvelope(failureRaw)
				wantDetail := "forkchat_workspace_authority_invalid"
				wantClass := failures.ClassLifecycleConflict
				if fault == "gateway-disconnect" {
					wantDetail = "workspace_gateway_unreachable"
					wantClass = failures.ClassDependencyUnavailable
				}
				if err != nil || state != "failed" || failure.Detail.Code != wantDetail || failure.Class != wantClass {
					t.Fatalf("%s is not a durable typed pre-model refusal: state=%s failure=%s err=%v", fault, state, failureRaw, err)
				}
				raw, err := json.Marshal(response.Error.Data)
				if err != nil {
					t.Fatal(err)
				}
				var wire struct {
					Details struct {
						Failure json.RawMessage `json:"failure"`
					} `json:"details"`
				}
				if err := json.Unmarshal(raw, &wire); err != nil {
					t.Fatal(err)
				}
				publicFailure, err := failures.UnmarshalEnvelope(wire.Details.Failure)
				if err != nil || publicFailure.Class != wantClass || publicFailure.Detail.Code != wantDetail {
					t.Fatalf("public RPC erased typed target refusal: %s err=%v", raw, err)
				}
				if refused.Completions != 0 {
					t.Fatalf("%s dispatched a provider: completions=%d", fault, refused.Completions)
				}
				replayed := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "conversation.fork_chat", params, 30*time.Second)
				if replayed.Error == nil {
					t.Fatalf("failed exact occurrence was relaunched: %s", fault)
				}
				refusedReplay, err := storetest.ReadConversationForkTurnStorage(context.Background(), rt.Events, created.Fork.ForkID, key)
				if err != nil || refusedReplay.KeyedOccurrences != 1 || refusedReplay.TurnID != refusedTurn {
					t.Fatalf("refusal replay created another occurrence: row=%+v err=%v", refusedReplay, err)
				}
				verifySource()
				if fault == "gateway-disconnect" {
					target := disconnectedTarget.Load().(string)
					current, err := owner.(*workspace.DockerManager).InspectManagedContainer(context.Background(), target)
					if err != nil || current.Exists {
						t.Fatalf("refused exact sandbox cleanup did not join: %+v %v", current, err)
					}
				}
				var retry runfork.ConversationForkChatResult
				chatRequest(map[string]any{"fork_id": created.Fork.ForkID, "message": "continue after the exact refusal", "idempotency_key": key + "-retry"}, &retry)
				if retry.Turn.TurnID == refusedTurn || retry.Snapshot.SourceTurn.TurnID != turnID || !retry.Turn.ParseOK || len(retry.Turn.ToolCalls) != 2 {
					t.Fatalf("retry bound a sibling target or lost its frozen snapshot: %+v", retry)
				}
				verifySource()
				if after := mockForkChatDomainCounts(t, rt, seed.RunID); after != before {
					t.Fatalf("refusal/retry mutated live facts: before=%+v after=%+v", before, after)
				}
			}
			var later servedAgentDirectiveProofResult
			response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "agent.send_directive", map[string]any{
				"run_id": seed.RunID, "agent_id": "fork-source-agent",
				"directive": "prove the source receiver still executes after sandbox cleanup", "idempotency_key": "mock-forkchat-source-survives",
			}, 30*time.Second)
			if response.Error != nil {
				rows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), rt.Events, seed.RunID, "fork-source-agent")
				if err == nil {
					for _, row := range rows {
						t.Logf("source turn request=%s result=%s", row.RequestPayload, row.ResponsePayload)
					}
				} else {
					t.Logf("source turn evidence: %v", err)
				}
				t.Fatalf("source receiver after fork sandbox cleanup: %+v", response.Error)
			}
			if err := json.Unmarshal(response.Result, &later); err != nil {
				t.Fatal(err)
			}
			if !later.OK || later.RunID != seed.RunID || later.Response != "source conversation preserved" {
				t.Fatalf("source receiver did not complete a later turn: %+v", later)
			}
			continued := workspaceProofMockTurns(t, rt, seed.RunID, "fork-source-agent", later.DirectiveEventID)
			if len(continued) == 0 {
				t.Fatal("source continuation has no completed mock turn")
			}
			for _, turn := range continued {
				if turn.SessionID != sessionID {
					t.Fatalf("source continuation lost its original session: %q, want %q", turn.SessionID, sessionID)
				}
			}
			verifySource()
		})
	}
}

// Faults enter through the existing owner and real target transport, not an
// injected model/tool result. Ordinary source resolution is never intercepted.
func installOversizedForkChatSnapshotFixture(t *testing.T, root string) {
	t.Helper()
	padding, err := json.Marshal(strings.Repeat("frozen-sandbox-only ", 4096))
	if err != nil {
		t.Fatal(err)
	}
	entities := "conversation:\n  context_blob: {type: text, initial: " + string(padding) + ", _unused_reason: oversized immutable fork snapshot proof}\n"
	if err := os.WriteFile(filepath.Join(root, "fork-source", "entities.yaml"), []byte(entities), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireOversizedForkChatTargetAndProjection(t *testing.T, chat runfork.ConversationForkChatResult, rawTarget any, source workspace.ExecutionTarget, docker bool) {
	t.Helper()
	target, ok := rawTarget.(workspace.ExecutionTarget)
	if !ok || (docker && (target.Mode != workspace.ExecutionModeDockerContainer || target.Container == "" || target.Container == source.Container)) ||
		(!docker && target.Mode != workspace.ExecutionModeHostLocal) {
		t.Fatalf("oversized fork chat reused the source execution target: target=%+v source=%+v", target, source)
	}
	if !docker {
		sandboxPath, sandboxErr := target.ResolveHostPath(workspace.LogicalWorkspaceMount, workspace.PathAccessRead)
		sourcePath, sourceErr := source.ResolveHostPath(workspace.LogicalWorkspaceMount, workspace.PathAccessRead)
		if sandboxErr != nil || sourceErr != nil || sandboxPath.HostPath == sourcePath.HostPath {
			t.Fatalf("oversized fork reused the source host backing: sandbox=%+v source=%+v errors=%v/%v", sandboxPath, sourcePath, sandboxErr, sourceErr)
		}
	}
	var reads int
	for _, call := range chat.Turn.ToolCalls {
		if call.Name != "fork_snapshot_read_entities" {
			continue
		}
		reads++
		if len(call.Result) <= 64*1024 || !strings.Contains(string(call.Result), "frozen-sandbox-only") {
			t.Fatalf("snapshot tool did not execute with genuinely oversized output: bytes=%d", len(call.Result))
		}
	}
	if reads != 1 || !chat.Turn.ParseOK {
		t.Fatalf("oversized sandbox read was repeated or failed: %+v", chat.Turn)
	}
	// The pinned Python performance asserts the actual model input before
	// returning this response; public operator request data is not model input.
	if string(chat.Turn.ResponsePayload) == "" || !strings.Contains(string(chat.Turn.ResponsePayload), "snapshot inspected; event remained sandboxed") {
		t.Fatalf("oversized sandbox continuation did not complete its pinned assertions: %s", chat.Turn.ResponsePayload)
	}
}

func waitForkChatSiblingReady(t *testing.T, rt servedWorkspaceProofRuntime, eventID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		count, err := storetest.ReadServedDeliveryStatusCount(context.Background(), rt.Events, eventID, "agent", "fork-sibling-agent", "delivered")
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		if count > 1 || time.Now().After(deadline) {
			t.Fatalf("sibling did not settle its exact setup occurrence once: count=%d", count)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func forkChatSiblingWorkspaceWitness(t *testing.T, docker bool, owner workspace.Resolver, target *workspace.Target) func() {
	t.Helper()
	if !docker {
		path := filepath.Join(target.Workdir, "forkchat-sibling-sentinel")
		if err := os.WriteFile(path, []byte("sibling-workspace-survives"), 0o600); err != nil {
			t.Fatal(err)
		}
		return func() {
			t.Helper()
			value, err := os.ReadFile(path)
			if err != nil || string(value) != "sibling-workspace-survives" {
				t.Fatalf("fork projection touched sibling files: %q %v", value, err)
			}
		}
	}
	manager := owner.(*workspace.DockerManager)
	initial, err := manager.InspectManagedContainer(context.Background(), target.Container)
	if err != nil || !initial.Exists || !initial.Running || !initial.HasIdentity {
		t.Fatalf("sibling container baseline: %+v %v", initial, err)
	}
	if _, err := manager.RunDocker(context.Background(), "exec", "--user", "0", target.Container, "sh", "-c", "printf sibling-workspace-survives > /workspace/forkchat-sibling-sentinel"); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		current, err := manager.InspectManagedContainer(context.Background(), target.Container)
		if err != nil || !reflect.DeepEqual(initial, current) {
			t.Fatalf("fork projection touched sibling container authority: before=%+v after=%+v err=%v", initial, current, err)
		}
		value, err := manager.RunDocker(context.Background(), "exec", target.Container, "cat", "/workspace/forkchat-sibling-sentinel")
		if err != nil || value != "sibling-workspace-survives" {
			t.Fatalf("fork projection touched sibling Docker files: %q %v", value, err)
		}
	}
}

type forkChatFaultHostWorkspace struct {
	*workspace.HostManager
	refuseAuthority *atomic.Bool
	lastForkTarget  *atomic.Value
}

func (w *forkChatFaultHostWorkspace) ResolveForkChatWorkspace(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	target, err := w.HostManager.ResolveForkChatWorkspace(forkChatFaultAuthority(ctx, w.refuseAuthority), actor)
	if err == nil {
		w.lastForkTarget.Store(target.ExecutionTarget())
	}
	return target, err
}

type forkChatFaultDockerWorkspace struct {
	*workspace.DockerManager
	refuseAuthority    *atomic.Bool
	disconnect         *atomic.Bool
	disconnectedTarget *atomic.Value
	lastForkTarget     *atomic.Value
	network            string
}

func (w *forkChatFaultDockerWorkspace) ResolveForkChatWorkspace(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	target, err := w.DockerManager.ResolveForkChatWorkspace(forkChatFaultAuthority(ctx, w.refuseAuthority), actor)
	if err == nil {
		w.lastForkTarget.Store(target.ExecutionTarget())
	}
	if err != nil || !w.disconnect.Swap(false) {
		return target, err
	}
	w.disconnectedTarget.Store(target.Container)
	if _, err := w.RunDocker(ctx, "network", "disconnect", w.network, target.Container); err != nil {
		return nil, errors.Join(err, target.Release(context.WithoutCancel(ctx)))
	}
	return target, nil
}

func forkChatFaultAuthority(ctx context.Context, refuse *atomic.Bool) context.Context {
	if !refuse.Swap(false) {
		return ctx
	}
	authority, _ := effects.AuthorityFromContext(ctx)
	authority.FenceGeneration++
	return effects.WithAuthority(ctx, authority)
}

func mockForkChatDomainCounts(t *testing.T, rt servedWorkspaceProofRuntime, runID string) servedConversationForkCounts {
	t.Helper()
	// Real gateway traffic produces the canonical observational runtime log.
	// Exclude only that event class, not business events or lifecycle mutations.
	counts, err := storetest.ReadConversationForkDomainStorage(context.Background(), rt.Events, runID)
	if err != nil {
		t.Fatal(err)
	}
	return servedConversationForkCounts{Runs: counts.Runs, Events: counts.Events, Mailbox: counts.Mailbox, Mutations: counts.Mutations}
}
