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
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
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
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: native-docker-emission\nstages:\n  pending: {}\n  done: {final: true}\npins:\n  inputs: [items.ready, proof.finished]\n"), 0o600); err != nil {
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
			activationFault := &activationGatewayFault{}
			factory := func(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error) {
				owner = workspace.NewDockerManager()
				cfg := workspace.DefaultDockerConfig()
				cfg.SourceProjection = projection
				if network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK"); network != "" {
					cfg.WorkspaceNetwork = network
				}
				owner.SetConfig(cfg)
				owner.SetSemanticSource(source)
				return &activationGatewayDockerWorkspace{DockerManager: owner, fault: activationFault, network: cfg.WorkspaceNetwork}, nil
			}
			rt := startWorkspaceGatewayProofRuntime(t, backend, root, workspace.BackendDocker, factory, "0.0.0.0:0")
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "items.ready", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"items": []string{"container-emitted"}}, "idempotency_key": "normal-docker-emission",
			})
			deadline := time.Now().Add(servedProofPollDeadline)
			for {
				settled, err := storetest.ReadManagedDeliveryStorage(context.Background(), rt.Events, published.RunID, "item-worker")
				if err != nil {
					t.Fatal(err)
				}
				if settled.Delivered == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("normal container agent failed settlement: %s", workspaceProofDebugSummary(t, rt, published.RunID))
				}
				time.Sleep(10 * time.Millisecond)
			}
			turnRows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), rt.Events, published.RunID, "item-worker")
			if err != nil || len(turnRows) != 1 || turnRows[0].ExecutionMode != "mock" {
				t.Fatalf("exact normal model turn: count=%d err=%v", len(turnRows), err)
			}
			callsRaw, emittedRaw, triggerID := turnRows[0].ToolCalls, turnRows[0].EmittedEvents, turnRows[0].TriggerEventID
			if triggerID == "" {
				t.Fatal("normal model turn lost its triggering event")
			}
			var calls []llm.ToolCall
			if err := json.Unmarshal(callsRaw, &calls); err != nil || len(calls) != 1 || calls[0].Name != "emit_items_processed" {
				t.Fatalf("normal container tool call: %s, err=%v", callsRaw, err)
			}
			var emitted []json.RawMessage
			// Provider completion commits before Conversation executes its tool
			// output. Keep the original earlier-snapshot assertion separate.
			if err := json.Unmarshal(emittedRaw, &emitted); err != nil || len(emitted) != 0 {
				t.Fatalf("model completion claimed a later emission: %s, err=%v", emittedRaw, err)
			}
			page, err := rt.Observability.ListOperatorEvents(context.Background(), operatorread.OperatorEventListOptions{
				Filter: operatorread.OperatorEventListFilter{RunID: published.RunID, EventName: "items.processed"}, Limit: 2,
			})
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("exact gateway emission: page=%+v err=%v", page, err)
			}
			record := storetest.LoadCanonicalEventRecord(t, context.Background(), rt.Events, page.Events[0].EventID)
			if record.RunID() != published.RunID || record.ParentEventID() != triggerID {
				t.Fatalf("gateway emission lost its exact run/cause: %s %s, want %s %s", record.RunID(), record.ParentEventID(), published.RunID, triggerID)
			}
			marker := record.Payload()
			var payload struct {
				Value          string `json:"value"`
				RequestEventID string `json:"request_event_id"`
			}
			if err := json.Unmarshal(marker, &payload); err != nil || payload.Value != "container-emitted" || payload.RequestEventID != triggerID {
				t.Fatalf("gateway emission lost the exact native call: %s, err=%v", marker, err)
			}
			events, err := storetest.ReadLifecycleEventCardinality(context.Background(), rt.Events, published.RunID, "items.processed")
			if err != nil {
				t.Fatal(err)
			}
			reply, err := storetest.ReadReplyReturnStorage(context.Background(), rt.Events, published.RunID)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := storetest.ReadManagedTurnEffectStorage(context.Background(), rt.Events, published.RunID, "item-worker")
			if err != nil {
				t.Fatal(err)
			}
			if events != 1 || len(turnRows) != 1 || reply.EventLinkedDeadLetters != 0 || completion.ResponseConsumed != 1 {
				t.Fatalf("normal Docker cardinality: events=%d turns=%d failures=%d consumed=%d", events, len(turnRows), reply.EventLinkedDeadLetters, completion.ResponseConsumed)
			}
			for _, actor := range rt.Runtime.Manager.ListAgentConfigs() {
				if actor.ID != "item-worker" || actor.Identity.RunID != published.RunID {
					continue
				}
				ctx := correlation.WithRunID(workspaceProofAuthorActivityContext(t, rt), published.RunID)
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
				proveDockerActivationRefusalRetry(t, rt, owner, activationFault, actor)
				return
			}
			t.Fatal("delivered native agent has no concrete runtime owner")
		})
	}
}
