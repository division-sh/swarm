package serveapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// This is retained H composition with actual Docker/MCP execution, not public
// serve's live posture or public test's implicit-default network acceptance.
func TestMockNormalRealDockerEmissionBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Docker H emission proof; a skip earns no transport credit")
	}
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyFanOutGroupAgentCrash(t)
			// The carrier is a private persistence fixture. A separate finish
			// entrance keeps the source live while proving worker retirement.
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: native-docker-emission\nstages:\n  pending: {initial: true}\n  done: {terminal: true}\npins:\n  inputs: [items.ready, proof.finished]\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("items.ready:\n  items: '[text]'\nitems.child:\n  value: text\nitems.processed:\n  value: text\n  request_event_id: text\nproof.finished:\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			nodesPath := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodesPath)
			if err != nil {
				t.Fatal(err)
			}
			finish := "finish-controller:\n  execution_type: system_node\n  subscribes_to: [proof.finished]\n  event_handlers:\n    proof.finished:\n      advances_to: done\n"
			if err := os.WriteFile(nodesPath, append(nodes, []byte(finish)...), 0o600); err != nil {
				t.Fatal(err)
			}
			agentsPath := filepath.Join(root, "agents.yaml")
			agents, err := os.ReadFile(agentsPath)
			if err != nil {
				t.Fatal(err)
			}
			const prompt = "Emit the processed value and exact request event identity."
			if strings.Count(string(agents), prompt) != 1 {
				t.Fatal("native emission fixture no longer has its exact prompt")
			}
			if err := os.WriteFile(agentsPath, []byte(strings.Replace(string(agents), prompt, "Emit value and request_event_id exactly from the triggering event.", 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			var owner *workspace.DockerManager
			factory := func(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error) {
				owner = workspace.NewDockerManager()
				cfg := workspace.DefaultDockerConfig()
				cfg.SourceProjection = projection
				if network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK"); network != "" {
					cfg.WorkspaceNetwork = network
				}
				owner.SetConfig(cfg)
				owner.SetSemanticSource(source)
				return owner, nil
			}
			rt := startServedTestSetupEntitiesProofRuntimeWithWorkspaceFactory(t, backend, root, true, workspace.BackendDocker, factory, "0.0.0.0:0")
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "items.ready", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"items": []string{"container-emitted"}}, "idempotency_key": "normal-docker-emission",
			})
			deadline := time.Now().Add(servedProofPollDeadline)
			for {
				var settled int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id='item-worker' AND status='delivered'`, published.RunID).Scan(&settled); err != nil {
					t.Fatal(err)
				}
				if settled == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("normal container agent failed settlement: %s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, published.RunID))
				}
				time.Sleep(10 * time.Millisecond)
			}
			var callsRaw, emittedRaw, triggerID string
			if err := rt.DB.QueryRow(`SELECT CAST(tool_calls AS TEXT), CAST(emitted_events AS TEXT), trigger_event_id FROM agent_turns WHERE run_id=$1 AND agent_id='item-worker' AND execution_mode='mock'`, published.RunID).Scan(&callsRaw, &emittedRaw, &triggerID); err != nil {
				t.Fatal(err)
			}
			var calls []llm.ToolCall
			if err := json.Unmarshal([]byte(callsRaw), &calls); err != nil || len(calls) != 1 || calls[0].Name != "emit_items_processed" {
				t.Fatalf("normal container tool call: %s, err=%v", callsRaw, err)
			}
			var emitted []json.RawMessage
			// Provider completion commits before Conversation executes its tool
			// output. Prove that output at its event and consumed receipt, not by
			// attributing a later emit to this earlier model-completion snapshot.
			if err := json.Unmarshal([]byte(emittedRaw), &emitted); err != nil || len(emitted) != 0 {
				t.Fatalf("model completion claimed a later emission: %s, err=%v", emittedRaw, err)
			}
			var marker string
			var events, turns, failures, consumed int
			if err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='items.processed' AND source_event_id=$2`, published.RunID, triggerID).Scan(&marker); err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Value          string `json:"value"`
				RequestEventID string `json:"request_event_id"`
			}
			if err := json.Unmarshal([]byte(marker), &payload); err != nil || payload.Value != "container-emitted" || payload.RequestEventID != triggerID {
				t.Fatalf("gateway emission lost the exact native call: %s, err=%v", marker, err)
			}
			for query, destination := range map[string]*int{
				`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='items.processed'`: &events,
				`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND agent_id='item-worker'`:  &turns,
				`SELECT COUNT(*) FROM dead_letters WHERE run_id=$1`:                            &failures,
				`SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id='item-worker' AND a.state='settled' AND a.completion_projection_phase='response_consumed'`: &consumed,
			} {
				if err := rt.DB.QueryRow(query, published.RunID).Scan(destination); err != nil {
					t.Fatal(err)
				}
			}
			if events != 1 || turns != 1 || failures != 0 || consumed != 1 {
				t.Fatalf("normal Docker cardinality: events=%d turns=%d failures=%d consumed=%d", events, turns, failures, consumed)
			}
			for _, actor := range rt.Runtime.Manager.ListAgentConfigs() {
				if actor.ID != "item-worker" || actor.Identity.RunID != published.RunID {
					continue
				}
				ctx := correlation.WithRunID(servedControlProofAuthorActivityContext(t, rt), published.RunID)
				target, err := owner.ResolveWorkspace(ctx, actor)
				if err != nil || target == nil || target.ExecutionTarget().Container == "" {
					t.Fatalf("normal agent lost its exact container: %+v, %v", target, err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				top, err := owner.RunDocker(ctx, "top", target.Container, "-eo", "pid,args")
				if err != nil || strings.Contains(top, worker.Argument) {
					t.Fatalf("delivered agent retained its native worker: %q, %v", top, err)
				}
				return
			}
			t.Fatal("delivered native agent has no concrete runtime owner")
		})
	}
}
