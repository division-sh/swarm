package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestConfiguredChannelRuntimeDispatchesDurablyAcrossSelectedStores(t *testing.T) {
	const bundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, selected := range []string{"postgres", "sqlite"} {
		t.Run(selected, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			runID := uuid.NewString()
			entityID := uuid.NewString()
			flowInstance := "channel-runtime-" + selected
			var (
				db            *sql.DB
				eventStore    runtimebus.EventStore
				workflowStore *runtimepipeline.WorkflowInstanceStore
			)
			if selected == "postgres" {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
				seedPostgresInboundGatewayRuntime(t, ctx, postgresDB, pg, runID, entityID, flowInstance, "channel-runtime", "telegram", "unused", "channel-runtime-observer")
				db, eventStore, workflowStore = postgresDB, pg, runtimepipeline.NewWorkflowInstanceStore(postgresDB)
			} else {
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "channel-runtime", "telegram", "unused", "channel-runtime-observer")
				db, eventStore = sqliteStore.DB, sqliteStore
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
			}
			seedConfiguredChannelBundleIdentity(t, ctx, db, selected, runID, bundleHash)

			var calls atomic.Int32
			requests := make(chan map[string]any, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				var decoded map[string]any
				if err := json.Unmarshal(body, &decoded); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				decoded["request_path"] = r.URL.Path
				requests <- decoded
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 99}})
			}))
			defer server.Close()

			binding := configuredTelegramChannelBinding(t, server.URL)
			publicTools, err := binding.RuntimeTools()
			if err != nil {
				t.Fatalf("RuntimeTools: %v", err)
			}
			base := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{Name: "channel_runtime", Version: "1.0.0"},
			})
			source, err := semanticview.WithRuntimeTools(base, publicTools)
			if err != nil {
				t.Fatalf("WithRuntimeTools: %v", err)
			}
			var coordinator *runtimepipeline.PipelineCoordinator
			bus, err := newRuntimeTestEventBusWithOptions(t, eventStore, runtimebus.EventBusOptions{
				ContractBundle: source,
				BundleSourceFact: runtimecorrelation.BundleSourceFact{
					BundleHash: bundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral,
				},
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if coordinator == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{coordinator}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			credentialStore := channelRuntimeCredentialStore(t, "provider-secret")
			privateTool, err := binding.Structural.OperationTool("deliver")
			if err != nil {
				t.Fatalf("OperationTool: %v", err)
			}
			privateToolID, planGeneration, err := binding.RuntimeActivityTarget("deliver")
			if err != nil {
				t.Fatalf("RuntimeActivityTarget: %v", err)
			}
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:            runtimeTestEventBusWorkOwner(t, bus),
				Module:               telegramConnectorSupportedSurfaceModule{source: source},
				WorkflowStore:        workflowStore,
				Credentials:          credentialStore,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{privateToolID: {Tool: privateTool, PlanGeneration: planGeneration}},
			})
			stopActivityNode := startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db, workflowStore)
			executor := configuredChannelExecutor(source, binding, credentialStore, coordinator)
			actor := models.AgentConfig{
				ExecutionMode: "live", ID: "channel-sender", Role: "worker", FlowID: "global",
				FlowPath: flowInstance, EntityID: entityID, Tools: []string{"channel.ops.deliver"},
			}
			input := map[string]any{
				"presentation": map[string]any{"text": "Approve deployment?"},
				"actions":      []any{map[string]any{"label": "Approve", "token": "approve_1"}},
			}
			if _, prepared, err := binding.PrepareOperation("deliver", input); err != nil {
				t.Fatalf("PrepareOperation: %v; public schema=%#v; prepared=%#v", err, publicTools["channel.ops.deliver"].InputSchema, prepared)
			}
			invalidCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "invalid-connector-native-input")
			if _, err := executor.Execute(invalidCtx, "channel.ops.deliver", map[string]any{
				"presentation": map[string]any{"text": "Bypass"}, "actions": []any{}, "chat_id": "99",
			}); err == nil {
				t.Fatal("connector-native destination bypass was accepted")
			}
			if calls.Load() != 0 {
				t.Fatalf("provider called before provider-neutral admission: %d", calls.Load())
			}
			callCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "call-1")
			result, err := executor.Execute(callCtx, "channel.ops.deliver", input)
			if err != nil {
				t.Fatalf("configured channel execute: %v", err)
			}
			wantResult := map[string]any{"delivery_reference": map[string]any{"id": float64(99)}}
			if !reflect.DeepEqual(result, wantResult) {
				t.Fatalf("channel result = %#v, want %#v", result, wantResult)
			}
			request := <-requests
			if request["chat_id"] != "42" || request["text"] != "Approve deployment?" || request["request_path"] != "/botprovider-secret/sendMessage" {
				t.Fatalf("bound connector request = %#v", request)
			}
			keyboard := request["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
			if len(keyboard) != 1 || len(keyboard[0].([]any)) != 1 || keyboard[0].([]any)[0].(map[string]any)["callback_data"] != "approve_1" {
				t.Fatalf("recursive action mapping = %#v", request["reply_markup"])
			}

			replayed, err := executor.Execute(callCtx, "channel.ops.deliver", input)
			if err != nil || !reflect.DeepEqual(replayed, wantResult) {
				t.Fatalf("channel replay = %#v, err=%v", replayed, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("provider calls after replay = %d, want one", calls.Load())
			}
			assertConfiguredChannelJournal(t, ctx, db, selected, runID, privateToolID, 1)

			if _, err := executor.Execute(callCtx, "channel.ops.deliver", map[string]any{
				"presentation": map[string]any{"text": "Changed under same identity"}, "actions": []any{},
			}); err == nil {
				t.Fatal("changed channel input under one logical identity was accepted")
			}
			if calls.Load() != 1 {
				t.Fatalf("conflicting duplicate resent provider: calls=%d", calls.Load())
			}

			ackExecutor := &channelAckLossExecutor{delegate: coordinator}
			ackExecutor.failNext.Store(true)
			ackPath := configuredChannelExecutor(source, binding, credentialStore, ackExecutor)
			ackCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "call-ack-loss")
			if _, err := ackPath.Execute(ackCtx, "channel.ops.deliver", input); err == nil {
				t.Fatal("simulated post-commit acknowledgment loss was not surfaced")
			}
			ackResult, err := ackPath.Execute(ackCtx, "channel.ops.deliver", input)
			if err != nil || !reflect.DeepEqual(ackResult, wantResult) {
				t.Fatalf("ack-loss reconciliation = %#v, err=%v", ackResult, err)
			}
			if calls.Load() != 2 {
				t.Fatalf("provider calls after ack-loss replay = %d, want two total distinct operations", calls.Load())
			}
			assertConfiguredChannelJournal(t, ctx, db, selected, runID, privateToolID, 2)

			if err := credentialStore.Delete(ctx, "telegram_bot_token"); err != nil {
				t.Fatalf("delete Telegram credential: %v", err)
			}
			missingCredentialCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "missing-credential")
			if _, err := executor.Execute(missingCredentialCtx, "channel.ops.deliver", input); err == nil {
				t.Fatal("configured channel executed without its declared credential")
			}
			if calls.Load() != 2 {
				t.Fatalf("missing credential reached provider: calls=%d", calls.Load())
			}

			if err := credentialStore.Set(ctx, "telegram_bot_token", "provider-secret"); err != nil {
				t.Fatalf("restore Telegram credential: %v", err)
			}
			replacementPlan := binding.Structural.Clone()
			replacementOperation := replacementPlan.Operations["deliver"]
			replacementText := replacementOperation.ToolSchema.InputSchema.Properties["text"]
			if replacementText.MaxLength == nil || *replacementText.MaxLength < 2 {
				t.Fatalf("replacement fixture text schema = %#v", replacementText)
			}
			replacementMaximum := *replacementText.MaxLength - 1
			replacementText.MaxLength = &replacementMaximum
			replacementOperation.ToolSchema.InputSchema.Properties["text"] = replacementText
			replacementPlan.Operations["deliver"] = replacementOperation
			replacementBinding, err := packs.NewOutboundBindingPlan("ops", replacementPlan, "42", nil)
			if err != nil {
				t.Fatalf("replacement binding: %v", err)
			}
			replacementTool, err := replacementBinding.Structural.OperationTool("deliver")
			if err != nil {
				t.Fatalf("replacement OperationTool: %v", err)
			}
			replacementToolID, replacementGeneration, err := replacementBinding.RuntimeActivityTarget("deliver")
			if err != nil {
				t.Fatalf("replacement RuntimeActivityTarget: %v", err)
			}
			if replacementToolID == privateToolID || replacementGeneration == planGeneration {
				t.Fatal("replacement plan reused the prior private target generation")
			}
			mismatchedCoordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        telegramConnectorSupportedSurfaceModule{source: source},
				WorkflowStore: workflowStore,
				Credentials:   credentialStore,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{
					privateToolID: {Tool: replacementTool, PlanGeneration: replacementGeneration},
				},
			})
			stopActivityNode()
			coordinator = mismatchedCoordinator
			stopActivityNode = startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db, workflowStore)
			mismatchedExecutor := configuredChannelExecutor(source, binding, credentialStore, mismatchedCoordinator)
			mismatchedCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "mismatched-plan-generation")
			if _, err := mismatchedExecutor.Execute(mismatchedCtx, "channel.ops.deliver", input); err == nil {
				t.Fatal("request executed through a private target carrying a different generation")
			}
			if calls.Load() != 2 {
				t.Fatalf("mismatched plan generation reached provider: calls=%d", calls.Load())
			}
			reloadedCoordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        telegramConnectorSupportedSurfaceModule{source: source},
				WorkflowStore: workflowStore,
				Credentials:   credentialStore,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{
					replacementToolID: {Tool: replacementTool, PlanGeneration: replacementGeneration},
				},
			})
			stopActivityNode()
			coordinator = reloadedCoordinator
			stopActivityNode = startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db, workflowStore)
			staleExecutor := configuredChannelExecutor(source, binding, credentialStore, reloadedCoordinator)
			staleCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "stale-plan-generation")
			if _, err := staleExecutor.Execute(staleCtx, "channel.ops.deliver", input); err == nil {
				t.Fatal("persisted old-generation request executed through replacement plan")
			}
			if calls.Load() != 2 {
				t.Fatalf("stale plan generation reached provider: calls=%d", calls.Load())
			}
		})
	}
}

func startConfiguredChannelActivityNode(t *testing.T, ctx context.Context, coordinator *runtimepipeline.PipelineCoordinator, bus *runtimebus.EventBus, db *sql.DB, store *runtimepipeline.WorkflowInstanceStore) func() {
	t.Helper()
	nodes := coordinator.BackgroundNodesWithReceiptStore(bus, db, store)
	if len(nodes) != 1 {
		t.Fatalf("configured channel background nodes = %d, want activity dispatcher", len(nodes))
	}
	ready := make(chan struct{}, len(nodes))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, len(nodes))
	for _, node := range nodes {
		if observable, ok := node.(runtimepipeline.SubscriptionReadyBackgroundNode); ok {
			observable.AddSubscriptionReadyHook(func() { ready <- struct{}{} })
		} else {
			t.Fatal("configured channel activity dispatcher does not expose subscription readiness")
		}
		go func(node runtimepipeline.BackgroundNode) {
			defer func() { done <- struct{}{} }()
			node.Run(runCtx)
		}(node)
	}
	for range nodes {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("configured channel activity dispatcher did not subscribe")
		}
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			for range nodes {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("configured channel activity dispatcher did not stop")
				}
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

type channelAckLossExecutor struct {
	delegate *runtimepipeline.PipelineCoordinator
	failNext atomic.Bool
}

func (e *channelAckLossExecutor) ExecuteDurableActivity(ctx context.Context, intent runtimeengine.ActivityIntent) (runtimepipeline.ActivityAttemptRecord, error) {
	record, err := e.delegate.ExecuteDurableActivity(ctx, intent)
	if err == nil && e.failNext.Swap(false) {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("simulated channel result acknowledgment loss")
	}
	return record, err
}

func configuredChannelExecutor(source semanticview.Source, binding packs.OutboundBindingPlan, credentials runtimecredentials.Store, activity runtimetools.DurableActivityExecutor) *runtimetools.Executor {
	return runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: source, ChannelBindings: []packs.OutboundBindingPlan{binding}, Credentials: credentials, ActivityExecutor: activity,
	})
}

func configuredChannelCallContext(t *testing.T, ctx context.Context, selectedStore any, actor models.AgentConfig, runID, entityID, flowInstance, operationID string) context.Context {
	t.Helper()
	inbound := eventtest.RunCreatingRootIngress(
		uuid.NewSHA1(uuid.NameSpaceURL, []byte(runID+"\x00"+operationID)).String(),
		events.EventType("channel.requested"), actor.ID, operationID, json.RawMessage(`{}`), 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), flowInstance), time.Now().UTC(),
	)
	storetest.CommitSemanticEvent(t, ctx, selectedStore, inbound)
	ctx = runtimebus.WithInboundEvent(ctx, inbound)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, operationID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, runtimecorrelation.BundleSourceFact{
		BundleHash:   "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BundleSource: storerunlifecycle.BundleSourceEphemeral,
	})
	return runtimetools.WithActor(ctx, actor)
}

func seedConfiguredChannelBundleIdentity(t *testing.T, ctx context.Context, db *sql.DB, selected, runID, bundleHash string) {
	t.Helper()
	query := `UPDATE runs SET bundle_hash = $1, bundle_source = $2 WHERE run_id = $3::uuid`
	if selected == "sqlite" {
		query = `UPDATE runs SET bundle_hash = ?, bundle_source = ? WHERE run_id = ?`
	}
	if _, err := db.ExecContext(ctx, query, bundleHash, storerunlifecycle.BundleSourceEphemeral, runID); err != nil {
		t.Fatalf("seed configured channel bundle identity: %v", err)
	}
}

func configuredTelegramChannelBinding(t *testing.T, serverURL string) packs.OutboundBindingPlan {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", ".."))
	version, err := platform.PlatformVersion()
	if err != nil {
		t.Fatalf("PlatformVersion: %v", err)
	}
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	triggers, _, err := providertriggers.NewCatalogSnapshotFromPackDirs(version, []string{filepath.Join(repo, "packs", "provider-triggers", "telegram")}, nil)
	if err != nil {
		t.Fatalf("load Telegram trigger: %v", err)
	}
	channels, err := packs.LoadChannelPackDirs(version, "platform", filepath.Join(repo, "packs", "channels", "telegram"))
	if err != nil {
		t.Fatalf("load Telegram channel: %v", err)
	}
	plans, err := packs.CompileChannelInventory(registry, channels, triggers.PackDescriptors(), providerconnectors.DefaultPackRegistry().PackDescriptors())
	if err != nil || len(plans) != 1 {
		t.Fatalf("CompileChannelInventory = %#v, %v", plans, err)
	}
	plan := plans[0].Clone()
	operation := plan.Operations["deliver"]
	httpSpec := *operation.ToolSchema.HTTP
	httpSpec.URL = strings.TrimRight(serverURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage"
	operation.ToolSchema.HTTP = &httpSpec
	plan.Operations["deliver"] = operation
	binding, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	return binding
}

func channelRuntimeCredentialStore(t *testing.T, token string) runtimecredentials.Store {
	t.Helper()
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := credentials.Set(context.Background(), "telegram_bot_token", token); err != nil {
		t.Fatalf("Set Telegram credential: %v", err)
	}
	return credentials
}

func assertConfiguredChannelJournal(t *testing.T, ctx context.Context, db *sql.DB, selected, runID, privateTool string, want int) {
	t.Helper()
	requestQuery := `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`
	attemptQuery := `SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid AND tool = $2 AND status = 'succeeded'`
	args := []any{runID}
	attemptArgs := []any{runID, privateTool}
	if selected == "sqlite" {
		requestQuery = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`
		attemptQuery = `SELECT COUNT(*) FROM activity_attempts WHERE run_id = ? AND tool = ? AND status = 'succeeded'`
	}
	var requests, attempts int
	if err := db.QueryRowContext(ctx, requestQuery, args...).Scan(&requests); err != nil {
		t.Fatalf("count channel request events: %v", err)
	}
	if err := db.QueryRowContext(ctx, attemptQuery, attemptArgs...).Scan(&attempts); err != nil {
		t.Fatalf("count channel activity attempts: %v", err)
	}
	if requests != want || attempts != want {
		t.Fatalf("durable channel journal = requests:%d attempts:%d, want %d/%d", requests, attempts, want, want)
	}
}
