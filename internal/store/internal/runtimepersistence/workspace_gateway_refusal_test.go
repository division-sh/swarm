package runtimepersistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/testutil"
)

type gatewayRefusalHostWorkspace struct{ target *workspace.Target }

func (w gatewayRefusalHostWorkspace) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func (w gatewayRefusalHostWorkspace) ResolveWorkspaceForCapabilityAdmission(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

type gatewayRefusalTools struct{ calls *atomic.Int32 }

func (e gatewayRefusalTools) Execute(context.Context, string, any) (any, error) {
	e.calls.Add(1)
	return nil, fmt.Errorf("refused model must not call a tool")
}

func (gatewayRefusalTools) ToolCapabilitiesForActor(models.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.NewSet([]toolcapabilities.Capability{{Name: "emit_done", Visible: true, Callable: true}})
}

func TestWorkspaceGatewayRefusalBeforeProviderTurnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, origin := range []string{"delivery", "directive"} {
			t.Run(backend+"/"+origin, func(t *testing.T) {
				var selected completionSettlementTestStore
				var db *sql.DB
				if backend == "sqlite" {
					store := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected, db = store, store.backend.ConstructionHandle()
				} else {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
				}
				source := []byte("def handle(input):\n    raise Exception('MODEL LAUNCHED AFTER REFUSAL')\n")
				actor := models.AgentConfig{ExecutionMode: effects.ExecutionModeMock, LLMBackend: "mock", ResolvedLLMBackend: "mock", Mock: mockperformance.Performance{
					Kind: mockperformance.KindPython, Module: "mocks/refusal.py", Source: source, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(source)),
				}}
				fixture := newCompletionSettlementFixtureWithActor(t, selected, db, backend == "sqlite", agentmemory.Plan{Enabled: true}, actor)
				actor.ID, actor.Identity, actor.Role, actor.Type = fixture.agentID, fixture.authority.Normal.Identity, "worker", "managed"
				actor.Model, actor.Memory, actor.FlowID, actor.FlowPath = "regular", fixture.authority.Target.Memory, "global", "global"
				var requests, calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					http.Error(w, "private gateway refusal", http.StatusServiceUnavailable)
				}))
				defer server.Close()
				binding, err := toolgateway.NewRuntimeOwnedBinding(toolgateway.TransportHTTP, server.URL, server.URL, "private-refusal-boot", toolgateway.LifecycleOwnerServeBoot, toolgateway.SourceBoundMCPListener)
				if err != nil {
					t.Fatal(err)
				}
				registry := mcp.NewTurnContextRegistry(models.ActorFromContext)
				controller := liveTestCompletionController(selected, selected, selected, publicationGroupSpendProjection{})
				runtime := llm.NewMockRuntime(&config.Config{}, selected.(sessions.Registry), fixture.leaseHolder, selected.(llm.ConversationPersistence), nil, controller, llm.MockRuntimeOptions{
					Workspaces: gatewayRefusalHostWorkspace{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}},
					MCPTurns:   registry, ToolGateway: binding,
				})
				settlement := completionSettlementForTest(t, fixture.authority.Target, fixture, "mock_python", "", "")
				conversation := managedConsumerWithRuntime(t, fixture, settlement, runtime)
				conversation.SetToolExecutor(gatewayRefusalTools{calls: &calls})
				conversation.Session.Tools = []llm.ToolDefinition{{Name: "emit_done", Description: "Exact output", Schema: map[string]any{"type": "object"}}}
				ctx := deliverylifecycle.WithClaim(effects.WithLifecycleToken(fixture.context, fixture.authority.Normal), fixture.origin)
				event := managedCompletionTestEvent(fixture.authority)
				var operationID string
				if origin == "directive" {
					directive, operation, directiveEvent := admitProviderDirectiveOrigin(t, fixture, requireProviderDirectiveStore(t, fixture), "gateway-refusal")
					operationID, event = operation.OperationID, directiveEvent
					ctx = effects.WithDirectiveCompletionOrigin(deliverylifecycle.WithoutClaim(ctx), directive)
				}
				ctx = effects.WithExecutionMode(models.WithActor(ctx, actor), effects.ExecutionModeMock)
				ctx = managedExecutionStoreTestContext(t, storeTestWorkContext(t, ctx))
				ctx = agentmemory.WithExecution(ctx, actor.Memory, actor.Identity)
				ctx = correlation.WithInboundEvent(ctx, event)
				beforeDeliveries := providerDirectiveDeliveryCount(t, fixture)
				var beforeEvents int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&beforeEvents); err != nil {
					t.Fatal(err)
				}
				response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event})
				failure, typed := failures.As(err)
				if response != nil || !typed || failure.Failure.Class != failures.ClassDependencyUnavailable || failure.Failure.Detail.Code != "workspace_gateway_unreachable" || requests.Load() != 1 || calls.Load() != 0 {
					t.Fatalf("pre-model refusal lost exact classification: response=%+v err=%v requests=%d calls=%d", response, err, requests.Load(), calls.Load())
				}
				var workerFailure *workspace.WorkerExecutionError
				if !errors.As(err, &workerFailure) || !workerFailure.Observed || workerFailure.ModelStarted || workerFailure.RemoteCleanupUnproven {
					t.Fatalf("refusal lost observed pre-model/join evidence: %v", err)
				}
				for _, table := range []string{"runtime_external_effect_attempts", "agent_turns", "spend_ledger"} {
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("refusal dispatched/projected %s: count=%d err=%v", table, count, err)
					}
				}
				var turns int
				var history []byte
				if err := db.QueryRow(`SELECT turn_count,conversation FROM agent_sessions WHERE session_id=$1`, fixture.sessionID).Scan(&turns, &history); err != nil || turns != 0 || !json.Valid(history) || string(history) != "[]" {
					t.Fatalf("refusal changed selected session: turns=%d history=%s err=%v", turns, history, err)
				}
				requireProviderDirectiveDeliveryCount(t, fixture, beforeDeliveries)
				var afterEvents int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&afterEvents); err != nil || afterEvents != beforeEvents {
					t.Fatalf("refusal emitted output: before=%d after=%d err=%v", beforeEvents, afterEvents, err)
				}
				var delivered int
				if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE status='delivered'`).Scan(&delivered); err != nil || delivered != 0 {
					t.Fatalf("refusal fabricated delivered outcome: count=%d err=%v", delivered, err)
				}
				if operationID != "" {
					op, found, err := requireProviderDirectiveStore(t, fixture).LoadDirectiveOperation(testAuthorActivityContext(), operationID)
					if err != nil || !found || op.State != agentcontrol.DirectiveOperationExecuting {
						t.Fatalf("adapter fabricated a directive success: operation=%+v found=%v err=%v", op, found, err)
					}
				}
			})
		}
	}
}
