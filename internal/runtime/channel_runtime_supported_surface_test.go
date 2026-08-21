package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packruntime"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestProjectImportedChannelRuntimeDispatchesDurablyAcrossSelectedStores(t *testing.T) {
	const bundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, selected := range []string{"postgres", "sqlite"} {
		t.Run(selected, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			runID := uuid.NewString()
			entityID := uuid.NewString()
			flowInstanceID := "channel-runtime-" + selected
			flowInstance := "global/" + flowInstanceID
			var (
				db                  *sql.DB
				eventStore          runtimebus.EventStore
				workflowPersistence runtimepipeline.WorkflowPersistence
				runLifecycle        runtimerunlifecycle.OperationOwner
				deliveryStore       runtimedelivery.Store
			)
			if selected == "postgres" {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
				seedPostgresInboundGatewayRuntime(t, ctx, postgresDB, pg, runID, entityID, flowInstance, "channel-runtime", "telegram", "unused", "channel-runtime-observer")
				db, eventStore, workflowPersistence, runLifecycle, deliveryStore = postgresDB, pg, runtimepipeline.NewWorkflowPersistence(pg), pg, pg
			} else {
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "channel-runtime", "telegram", "unused", "channel-runtime-observer")
				db, eventStore = storetest.Database(sqliteStore), sqliteStore
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
				runLifecycle, deliveryStore = sqliteStore, sqliteStore
			}
			seedConfiguredChannelBundleIdentity(t, ctx, db, selected, runID, bundleHash)
			obligationProvider, ok := eventStore.(interface {
				PipelineObligations() runtimepipelineobligation.Store
			})
			if !ok {
				t.Fatal("selected channel runtime store does not expose pipeline obligations")
			}
			pipelineObligations := obligationProvider.PipelineObligations()

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
			agentOwner := "test://channel-runtime/global/channel-sender"
			agentRef := runtimecontracts.ContractURIRef{Kind: "agent", FlowID: "global", LocalID: "channel-sender", Full: agentOwner}
			global := runtimecontracts.FlowContractView{
				Paths: runtimecontracts.FlowContractPaths{
					ID: "global", Flow: "global", Mode: runtimecontracts.FlowModeTemplate,
					PackageKey: "channel-runtime", AgentsFile: "/contracts/channel-runtime/flows/global/agents.yaml",
				},
				Path:   "global",
				Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"channel-sender": runtimecontracts.EffectiveAgentRegistryEntry("channel-sender", runtimecontracts.AgentRegistryEntry{ID: "channel-sender", Role: "worker"}),
				},
				AgentURIs: map[string]string{"channel-sender": agentOwner},
			}
			root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{global}}
			base := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{Name: "channel_runtime", Version: "1.0.0"},
				FlowTree: runtimecontracts.FlowTree{
					Root: &root,
					ByID: map[string]*runtimecontracts.FlowContractView{"global": &root.Children[0]},
				},
				FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"global": global.Schema},
				URIRegistry: runtimecontracts.ContractURIRegistry{
					Agents: map[string]runtimecontracts.ContractURIRef{"global/channel-sender": agentRef},
					ByURI:  map[string]runtimecontracts.ContractURIRef{agentOwner: agentRef},
				},
			})
			source, err := semanticview.WithRuntimeTools(base, publicTools)
			if err != nil {
				t.Fatalf("WithRuntimeTools: %v", err)
			}
			var coordinator *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, eventStore, runtimebus.EventBusOptions{
				ContractBundle:   source,
				BundleSourceFact: testBundleSourceFact(t, bundleHash),
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
			privateTool, err := binding.OperationTool("deliver")
			if err != nil {
				t.Fatalf("OperationTool: %v", err)
			}
			privateIdentity, err := binding.RuntimeActivityTarget("deliver")
			if err != nil {
				t.Fatalf("RuntimeActivityTarget: %v", err)
			}
			privateTarget, err := runtimepipeline.NewChannelActivityTarget(privateTool, privateIdentity.Generation())
			if err != nil {
				t.Fatalf("NewChannelActivityTarget: %v", err)
			}
			privateToolID := privateIdentity.ToolID()
			coordinator = newExternalRuntimeTestPipelineCoordinator(t, bus, db, eventStore, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:            runtimeTestEventBusWorkOwner(t, bus),
				Module:               telegramConnectorSupportedSurfaceModule{source: source},
				Persistence:          workflowPersistence,
				RunLifecycle:         runLifecycle,
				DeliveryStore:        deliveryStore,
				PipelineObligations:  pipelineObligations,
				Credentials:          credentialStore,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{privateToolID: privateTarget},
				FlowRoutes:           bus,
			})

			stopActivityNode := startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db)
			executor := configuredChannelExecutor(source, binding, credentialStore, coordinator)
			actor := models.AgentConfig{
				ExecutionMode: "live", ID: "channel-sender", Identity: agentidentitytest.Declared(t, "channel-sender", agentOwner, "global", flowInstanceID, flowInstance), Role: "worker", FlowID: "global",
				FlowPath: flowInstance, EntityID: entityID, Tools: []string{"channel.ops.deliver"},
			}
			input := map[string]any{
				"presentation": map[string]any{"text": "Approve deployment?"},
				"actions":      []any{map[string]any{"label": "Approve", "token": "approve_1"}},
			}
			if _, prepared, err := binding.PrepareOperation("deliver", input); err != nil {
				t.Fatalf("PrepareOperation: %v; public schema=%#v; prepared=%#v", err, publicTools["channel.ops.deliver"].InputSchema(), prepared)
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
			assertConfiguredChannelJournal(t, ctx, db, selected, runID, privateToolID, flowInstance, entityID, 1)

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
			assertConfiguredChannelJournal(t, ctx, db, selected, runID, privateToolID, flowInstance, entityID, 2)

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
			replacementBinding := configuredTelegramChannelBindingWithTextLimit(t, server.URL, true)
			replacementTool, err := replacementBinding.OperationTool("deliver")
			if err != nil {
				t.Fatalf("replacement OperationTool: %v", err)
			}
			replacementIdentity, err := replacementBinding.RuntimeActivityTarget("deliver")
			if err != nil {
				t.Fatalf("replacement RuntimeActivityTarget: %v", err)
			}
			replacementTarget, err := runtimepipeline.NewChannelActivityTarget(replacementTool, replacementIdentity.Generation())
			if err != nil {
				t.Fatalf("replacement NewChannelActivityTarget: %v", err)
			}
			replacementToolID := replacementIdentity.ToolID()
			if replacementToolID == privateToolID || replacementIdentity.Generation().Equal(privateIdentity.Generation()) {
				t.Fatal("replacement plan reused the prior private target generation")
			}
			mismatchedCoordinator := newExternalRuntimeTestPipelineCoordinator(t, bus, db, eventStore, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              telegramConnectorSupportedSurfaceModule{source: source},
				Persistence:         workflowPersistence,
				RunLifecycle:        runLifecycle,
				DeliveryStore:       deliveryStore,
				PipelineObligations: pipelineObligations,
				Credentials:         credentialStore,
				FlowRoutes:          bus,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{
					privateToolID: replacementTarget,
				},
			})

			stopActivityNode()
			coordinator = mismatchedCoordinator
			stopActivityNode = startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db)
			mismatchedExecutor := configuredChannelExecutor(source, binding, credentialStore, mismatchedCoordinator)
			mismatchedCtx := configuredChannelCallContext(t, ctx, eventStore, actor, runID, entityID, flowInstance, "mismatched-plan-generation")
			if _, err := mismatchedExecutor.Execute(mismatchedCtx, "channel.ops.deliver", input); err == nil {
				t.Fatal("request executed through a private target carrying a different generation")
			}
			if calls.Load() != 2 {
				t.Fatalf("mismatched plan generation reached provider: calls=%d", calls.Load())
			}
			reloadedCoordinator := newExternalRuntimeTestPipelineCoordinator(t, bus, db, eventStore, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              telegramConnectorSupportedSurfaceModule{source: source},
				Persistence:         workflowPersistence,
				RunLifecycle:        runLifecycle,
				DeliveryStore:       deliveryStore,
				PipelineObligations: pipelineObligations,
				Credentials:         credentialStore,
				FlowRoutes:          bus,
				ChannelActivityTools: map[string]runtimepipeline.ChannelActivityTarget{
					replacementToolID: replacementTarget,
				},
			})

			stopActivityNode()
			coordinator = reloadedCoordinator
			stopActivityNode = startConfiguredChannelActivityNode(t, ctx, coordinator, bus, db)
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

func startConfiguredChannelActivityNode(t *testing.T, ctx context.Context, coordinator *runtimepipeline.PipelineCoordinator, bus *runtimebus.EventBus, db *sql.DB) func() {
	t.Helper()
	if coordinator == nil || bus == nil || db == nil {
		t.Fatal("configured channel claimed-dispatch fixture requires coordinator, bus, and store")
	}
	if nodes := coordinator.BackgroundNodes(); len(nodes) != 0 {
		t.Fatalf("configured channel background nodes = %d, want claimed event dispatch only", len(nodes))
	}
	return func() {}
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
	return runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
		WorkflowSource: source, ChannelBindings: []packs.OutboundBindingPlan{binding}, Credentials: credentials, ActivityExecutor: activity,
	})
}

func configuredChannelCallContext(t *testing.T, ctx context.Context, selectedStore any, actor models.AgentConfig, runID, entityID, flowInstance, operationID string) context.Context {
	t.Helper()
	sourceFact := testBundleSourceFact(t, "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.RuntimeInstanceID == "" {
		t.Fatal("configured channel call context requires a runtime author-activity scope")
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, sourceFact.BundleHash()))
	inbound := eventtest.ExistingRunRootIngress(
		uuid.NewSHA1(uuid.NameSpaceURL, []byte(runID+"\x00"+operationID)).String(),
		events.EventType("channel.requested"), actor.ID, operationID, json.RawMessage(`{}`), 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), flowInstance), time.Now().UTC(),
	)
	storetest.CommitSemanticEvent(t, ctx, selectedStore, inbound)
	ctx = runtimebus.WithInboundEvent(ctx, inbound)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, operationID)
	return runtimetools.WithActor(ctx, actor)
}

func seedConfiguredChannelBundleIdentity(t *testing.T, ctx context.Context, db *sql.DB, selected, runID, bundleHash string) {
	t.Helper()
	source := testBundleSourceFact(t, bundleHash)
	if selected == "sqlite" {
		if err := runlifecyclefixture.ReviseSQLiteSource(ctx, db, runID, source); err != nil {
			t.Fatalf("seed configured SQLite channel bundle identity: %v", err)
		}
		return
	}
	if err := runlifecyclefixture.RevisePostgresSource(ctx, db, runID, source); err != nil {
		t.Fatalf("seed configured PostgreSQL channel bundle identity: %v", err)
	}
}

func configuredTelegramChannelBinding(t *testing.T, serverURL string) packs.OutboundBindingPlan {
	return configuredTelegramChannelBindingWithTextLimit(t, serverURL, false)
}

func configuredTelegramChannelBindingWithTextLimit(t *testing.T, serverURL string, tightenTextLimit bool) packs.OutboundBindingPlan {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", ".."))
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
	inventory := projectImportedTelegramChannelInventory(t, serverURL)
	triggers, _, err := providertriggers.NewCatalogSnapshotFromInventory(inventory, packfixture.PlatformVersion)
	if err != nil {
		t.Fatalf("load project trigger catalog: %v", err)
	}
	channels, err := packruntime.LoadChannelPacks(inventory, packfixture.PlatformVersion)
	if err != nil {
		t.Fatalf("load project channel packs: %v", err)
	}
	connectorRegistry, err := providerconnectors.NewPackRegistryFromInventory(inventory, packfixture.PlatformVersion)
	if err != nil {
		t.Fatalf("load project connector registry: %v", err)
	}
	connectors := connectorRegistry.PackDescriptors()
	var telegramConnectorIdentity packs.PackIdentity
	for descriptorIndex := range connectors {
		if connectors[descriptorIndex].Provider != "telegram" {
			continue
		}
		telegramConnectorIdentity = connectors[descriptorIndex].Identity
		tool, ok := connectors[descriptorIndex].Tools["telegram.send_interactive"]
		if !ok {
			t.Fatal("Telegram connector descriptor has no send_interactive tool")
		}
		httpSpec, ok := tool.HTTP()
		if !ok {
			t.Fatal("Telegram connector descriptor has no HTTP execution contract")
		}
		wantURL := strings.TrimRight(serverURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage"
		if httpSpec.URL != wantURL {
			t.Fatalf("project Telegram HTTP endpoint = %q, want %q", httpSpec.URL, wantURL)
		}
		if tightenTextLimit {
			input := tool.InputSchema()
			text, ok := input.Property("text")
			if !ok {
				t.Fatal("Telegram connector descriptor has no text input")
			}
			maximum, ok := text.MaxLength()
			if !ok || maximum < 2 {
				t.Fatalf("Telegram text maximum = %d, present=%v", maximum, ok)
			}
			text, err = text.WithMaxLength(maximum - 1)
			if err != nil {
				t.Fatalf("tighten Telegram text maximum: %v", err)
			}
			input, err = input.WithProperty("text", text)
			if err != nil {
				t.Fatalf("replace Telegram text schema: %v", err)
			}
			tool, err = tool.WithSchemas(input, tool.OutputSchema())
			if err != nil {
				t.Fatalf("replace Telegram input schema: %v", err)
			}
		}
		connectors[descriptorIndex].Tools["telegram.send_interactive"] = tool
	}
	plans, err := packs.CompileChannelInventory(registry, channels, triggers.PackDescriptors(), connectors)
	if err != nil || len(plans) != 1 {
		t.Fatalf("CompileChannelInventory = %#v, %v", plans, err)
	}
	triggerEntry, _ := inventory.Lookup("provider.telegram")
	connectorEntry, _ := inventory.Lookup("provider.telegram.connector")
	channelEntry, _ := inventory.Lookup("provider.telegram.hitl_channel")
	if telegramConnectorIdentity.ID() != connectorEntry.ID() || telegramConnectorIdentity.ManifestHash() != connectorEntry.ManifestHash() || telegramConnectorIdentity.Source().Provenance() != packartifact.ProvenanceProject {
		t.Fatalf("connector descriptor identity = %#v, want project entry %#v", telegramConnectorIdentity, connectorEntry)
	}
	channelIdentity := plans[0].ChannelIdentity()
	if channelIdentity.ID() != channelEntry.ID() || channelIdentity.ManifestHash() != channelEntry.ManifestHash() || channelIdentity.Source().Provenance() != packartifact.ProvenanceProject {
		t.Fatalf("channel plan identity = %#v, want project entry %#v", channelIdentity, channelEntry)
	}
	structuralSubject, err := plans[0].CapabilitySubject()
	if err != nil {
		t.Fatalf("channel plan capability subject: %v", err)
	}
	if structuralSubject.Provenance != packartifact.ProvenanceProject || len(structuralSubject.Evidence) != 1 ||
		structuralSubject.Evidence[0].Fields["channel_hash"] != channelEntry.ManifestHash() ||
		structuralSubject.Evidence[0].Fields["trigger_hash"] != triggerEntry.ManifestHash() ||
		structuralSubject.Evidence[0].Fields["connector_hash"] != connectorEntry.ManifestHash() {
		t.Fatalf("channel capability subject = %#v", structuralSubject)
	}
	binding, err := packs.NewOutboundBindingPlan("ops", plans[0], "42", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	bindingSubject, err := binding.CapabilitySubject()
	if err != nil {
		t.Fatalf("channel binding capability subject: %v", err)
	}
	if bindingSubject.Provenance != packartifact.ProvenanceProject || len(bindingSubject.Evidence) != 1 ||
		bindingSubject.Evidence[0].Fields["channel_hash"] != channelEntry.ManifestHash() ||
		bindingSubject.Evidence[0].Fields["trigger_hash"] != triggerEntry.ManifestHash() ||
		bindingSubject.Evidence[0].Fields["connector_hash"] != connectorEntry.ManifestHash() {
		t.Fatalf("channel binding capability subject = %#v", bindingSubject)
	}
	return binding
}

func projectImportedTelegramChannelInventory(t *testing.T, serverURL string) *packartifact.EffectivePackInventory {
	t.Helper()
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "package.yaml"), []byte("name: imported-channel-proof\nversion: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := packfixture.EmbeddedBase(t)
	for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
		if changed, err := packartifact.ImportEmbeddedPack(project, id, base); err != nil || !changed {
			t.Fatalf("import %s changed=%t: %v", id, changed, err)
		}
	}
	connectorPath := filepath.Join(project, "packs", "provider.telegram.connector", packartifact.ConnectorManifestFileName)
	body, err := os.ReadFile(connectorPath)
	if err != nil {
		t.Fatal(err)
	}
	wantURL := strings.TrimRight(serverURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage"
	edited := strings.ReplaceAll(string(body), "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage", wantURL)
	if edited == string(body) {
		t.Fatal("project connector endpoint edit found no canonical URL")
	}
	if err := os.WriteFile(connectorPath, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	projectPacks, err := packartifact.LoadProjectPackSet(project)
	if err != nil {
		t.Fatalf("load imported project packs: %v", err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, projectPacks.Sources)
	if err != nil {
		t.Fatalf("build imported effective inventory: %v", err)
	}
	for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
		entry, ok := inventory.Lookup(id)
		if !ok || entry.Source() != packartifact.ProvenanceProject || !entry.ShadowsBase() || !entry.Origin().Valid() {
			t.Fatalf("effective imported pack %s = %#v, present=%t", id, entry, ok)
		}
		if id == "provider.telegram.connector" && !entry.Modified() {
			t.Fatalf("edited connector %s is not marked modified", id)
		}
	}
	return inventory
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

func assertConfiguredChannelJournal(t *testing.T, ctx context.Context, db *sql.DB, selected, runID, privateTool, flowInstance, entityID string, want int) {
	t.Helper()
	requestQuery := `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`
	attemptQuery := `SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid AND tool = $2 AND status = 'succeeded'`
	sourceQuery := `SELECT source_route FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested' ORDER BY created_at, event_id LIMIT 1`
	args := []any{runID}
	attemptArgs := []any{runID, privateTool}
	if selected == "sqlite" {
		requestQuery = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`
		attemptQuery = `SELECT COUNT(*) FROM activity_attempts WHERE run_id = ? AND tool = ? AND status = 'succeeded'`
		sourceQuery = `SELECT source_route FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested' ORDER BY created_at, event_id LIMIT 1`
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
	var rawRoute []byte
	if err := db.QueryRowContext(ctx, sourceQuery, runID).Scan(&rawRoute); err != nil {
		t.Fatalf("read channel activity routing source: %v", err)
	}
	var route events.RouteIdentity
	if err := json.Unmarshal(rawRoute, &route); err != nil {
		t.Fatalf("decode channel activity routing source %q: %v", rawRoute, err)
	}
	wantRoute := events.RouteIdentity{FlowID: "global", FlowInstance: flowInstance, EntityID: entityID}
	if got := route.Normalized(); got != wantRoute {
		t.Fatalf("durable channel routing source = %#v, want %#v", got, wantRoute)
	}
}
