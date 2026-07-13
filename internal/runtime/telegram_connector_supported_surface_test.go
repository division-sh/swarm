package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

const telegramConnectorSupportedSurfaceTimeout = 15 * time.Second

// This direct-gateway fixture remains bounded connector integration proof.
// The process-served standing-ingress tests own the supported E2E claim.
func TestTelegramConnectorBoundedIntegrationRoundTripThroughInboundGateway(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "6a000000-0000-0000-0000-000000000001"
			entityID     = "6a000000-0000-0000-0000-000000000002"
			flowInstance = "telegram-connector-supported-surface-pg"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		pg := &store.PostgresStore{DB: db}
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "telegram-supported-surface-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runTelegramConnectorSupportedSurfaceRoundTrip(t, telegramConnectorSupportedSurfaceBackend{
			name:          "postgres",
			ctx:           ctx,
			db:            db,
			eventStore:    pg,
			inboundStore:  pg,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        false,
		})
	})

	t.Run("sqlite", func(t *testing.T) {
		const (
			runID        = "6b000000-0000-0000-0000-000000000001"
			entityID     = "6b000000-0000-0000-0000-000000000002"
			flowInstance = "telegram-connector-supported-surface-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "telegram-supported-surface-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runTelegramConnectorSupportedSurfaceRoundTrip(t, telegramConnectorSupportedSurfaceBackend{
			name:          "sqlite",
			ctx:           ctx,
			db:            sqliteStore.DB,
			eventStore:    sqliteStore,
			inboundStore:  sqliteStore,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        true,
		})
	})
}

type telegramConnectorSupportedSurfaceBackend struct {
	name          string
	ctx           context.Context
	db            *sql.DB
	eventStore    runtimebus.EventStore
	inboundStore  runtimepkg.InboundPersistence
	workflowStore *runtimepipeline.WorkflowInstanceStore
	runID         string
	entityID      string
	flowInstance  string
	sqlite        bool
}

type telegramConnectorSupportedSurfaceCall struct {
	path string
	body map[string]any
	raw  string
}

func runTelegramConnectorSupportedSurfaceRoundTrip(t *testing.T, backend telegramConnectorSupportedSurfaceBackend) {
	t.Helper()
	calls := make(chan telegramConnectorSupportedSurfaceCall, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		calls <- telegramConnectorSupportedSurfaceCall{
			path: r.URL.Path,
			body: body,
			raw:  string(raw),
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 99,
				"chat":       map[string]any{"id": 42},
				"text":       body["text"],
			},
		})
	}))
	defer server.Close()

	credentialStore := telegramConnectorSupportedSurfaceCredentialStore(t, "telegram_bot_token", "provider-secret")
	source := telegramConnectorSupportedSurfaceSource(t, server.URL, backend.flowInstance)
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(backend.eventStore, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("%s NewEventBusWithOptions: %v", backend.name, err)
	}
	pc = startTelegramConnectorSupportedSurfaceCoordinator(t, backend.ctx, bus, backend.db, backend.workflowStore, source, credentialStore)

	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)
	validBody := []byte(`{"update_id":123456789,"message":{"message_id":7,"chat":{"id":42},"text":"hello from telegram"}}`)
	validReq := newSignedTelegramRequest(webhookPath, "telegram-secret", validBody).WithContext(backend.ctx)
	validRec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundStore, validRec, validReq, backend.runID, backend.entityID, "telegram", "telegram-secret")
	if validRec.Code != http.StatusAccepted {
		t.Fatalf("%s gateway status = %d, want 202 body=%s", backend.name, validRec.Code, validRec.Body.String())
	}
	if strings.Contains(validRec.Body.String(), "telegram-secret") || strings.Contains(validRec.Body.String(), "provider-secret") {
		t.Fatalf("%s gateway response leaked a Telegram secret: %s", backend.name, validRec.Body.String())
	}

	call := requireTelegramConnectorSupportedSurfaceCall(t, calls, backend.name, backend)
	if call.path != "/botprovider-secret/sendMessage" {
		t.Fatalf("%s Telegram path = %q, want token path sendMessage", backend.name, call.path)
	}
	if got := telegramConnectorSupportedSurfaceString(call.body["chat_id"]); got != "42" {
		t.Fatalf("%s chat_id = %#v, want 42", backend.name, call.body["chat_id"])
	}
	if got := telegramConnectorSupportedSurfaceString(call.body["text"]); got != "hello from telegram" {
		t.Fatalf("%s text = %#v, want inbound text", backend.name, call.body["text"])
	}

	waitForInboundBusQuiescence(t, bus)

	inboundEventID := loadTelegramConnectorSupportedSurfaceInboundEventID(t, backend, "123456789")
	if got := countTelegramConnectorSupportedSurfaceNodeDeliveries(t, backend, inboundEventID, telegramConnectorSupportedSurfaceNodeID); got != 1 {
		t.Fatalf("%s node delivery rows for inbound event = %d, want 1", backend.name, got)
	}

	attempt := waitForTelegramConnectorSupportedSurfaceTerminalActivityAttempt(t, backend)
	if attempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s activity attempt status = %q, want succeeded", backend.name, attempt.Status)
	}
	if attempt.SourceEventID != inboundEventID {
		t.Fatalf("%s activity attempt source_event_id = %q, want inbound event %q", backend.name, attempt.SourceEventID, inboundEventID)
	}
	if got := telegramConnectorSupportedSurfaceString(attempt.ResultPayload["result"]); strings.Contains(got, "provider-secret") {
		t.Fatalf("%s activity result leaked provider token: %s", backend.name, got)
	}
	requireTelegramConnectorSupportedSurfaceEventEventually(t, backend, attempt.ResultEventID, attempt.ResultEventType)

	duplicateReq := newSignedTelegramRequest(webhookPath, "telegram-secret", validBody).WithContext(backend.ctx)
	duplicateRec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundStore, duplicateRec, duplicateReq, backend.runID, backend.entityID, "telegram", "telegram-secret")
	if duplicateRec.Code != http.StatusOK {
		t.Fatalf("%s duplicate gateway status = %d, want 200 body=%s", backend.name, duplicateRec.Code, duplicateRec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
	requireNoTelegramConnectorSupportedSurfaceCall(t, calls, backend.name, "duplicate webhook")
	if got := countTelegramConnectorSupportedSurfaceActivityAttempts(t, backend); got != 1 {
		t.Fatalf("%s activity attempts after duplicate webhook = %d, want 1", backend.name, got)
	}

	requestEvent := loadTelegramConnectorSupportedSurfaceActivityRequestEvent(t, backend, attempt.RequestEventID)
	if err := bus.EngineDispatcher().DispatchPostCommit(backend.ctx, []runtimeengine.EmitIntent{{Event: requestEvent}}); err != nil {
		t.Fatalf("%s duplicate activity request dispatch: %v", backend.name, err)
	}
	waitForInboundBusQuiescence(t, bus)
	requireNoTelegramConnectorSupportedSurfaceCall(t, calls, backend.name, "duplicate activity request")
	if got := countTelegramConnectorSupportedSurfaceActivityAttempts(t, backend); got != 1 {
		t.Fatalf("%s activity attempts after duplicate activity request = %d, want 1", backend.name, got)
	}

	assertTelegramConnectorSupportedSurfaceNoStoredSecret(t, backend, "provider-secret")
	assertTelegramConnectorSupportedSurfaceMissingToken(t, backend, server.URL, calls)
}

func assertTelegramConnectorSupportedSurfaceMissingToken(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, baseURL string, calls <-chan telegramConnectorSupportedSurfaceCall) {
	t.Helper()
	credentialStore := telegramConnectorSupportedSurfaceCredentialStore(t, "", "")
	source := telegramConnectorSupportedSurfaceSource(t, baseURL, backend.flowInstance)
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(backend.eventStore, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("%s missing-token NewEventBusWithOptions: %v", backend.name, err)
	}
	pc = startTelegramConnectorSupportedSurfaceCoordinator(t, backend.ctx, bus, backend.db, backend.workflowStore, source, credentialStore)

	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)
	missingTokenBody := []byte(`{"update_id":123456790,"message":{"message_id":8,"chat":{"id":42},"text":"missing token"}}`)
	req := newSignedTelegramRequest(webhookPath, "telegram-secret", missingTokenBody).WithContext(backend.ctx)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundStore, rec, req, backend.runID, backend.entityID, "telegram", "telegram-secret")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%s missing-token gateway status = %d, want 202 body=%s", backend.name, rec.Code, rec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
	requireNoTelegramConnectorSupportedSurfaceCall(t, calls, backend.name, "missing token")
	requireTelegramConnectorSupportedSurfaceFailureEventEventually(t, backend, "123456790")
	if got := countTelegramConnectorSupportedSurfaceActivityAttemptsForEvent(t, backend, "123456790"); got != 1 {
		t.Fatalf("%s missing-token activity attempts = %d, want one failed claim", backend.name, got)
	}
	if got := telegramConnectorSupportedSurfaceActivityStatusForEvent(t, backend, "123456790"); got != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s missing-token activity status = %q, want failed", backend.name, got)
	}
	assertTelegramConnectorSupportedSurfaceNoStoredSecret(t, backend, "provider-secret")
}

const telegramConnectorSupportedSurfaceNodeID = "telegram-responder"

func telegramConnectorSupportedSurfaceSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{
			ID:   "telegram_send_message",
			Tool: "telegram.send_message",
			Input: map[string]runtimecontracts.ExpressionValue{
				"chat_id": runtimecontracts.CELExpression("payload.payload.message.chat.id"),
				"text":    runtimecontracts.CELExpression("payload.payload.message.text"),
			},
		},
	}
	node := runtimecontracts.SystemNodeContract{
		ID:            telegramConnectorSupportedSurfaceNodeID,
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"inbound.telegram": handler,
		},
	}
	base := semanticview.Wrap(boundedStandingConnectorBundle(flowInstance, &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"inbound.telegram"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			telegramConnectorSupportedSurfaceNodeID: node,
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "telegram_connector_supported_surface",
			Version: "1.0.0",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				telegramConnectorSupportedSurfaceNodeID: {
					ID:                   telegramConnectorSupportedSurfaceNodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.telegram"},
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				telegramConnectorSupportedSurfaceNodeID: {
					"inbound.telegram": handler,
				},
			},
			EventOwners: map[string][]string{
				"inbound.telegram": {telegramConnectorSupportedSurfaceNodeID},
			},
		},
	}))
	importSource := telegramConnectorSupportedSurfacePackImportSource{
		Source: base,
		projectScopes: []semanticview.ProjectScope{
			{
				Key: ".",
				Manifest: runtimecontracts.ProjectPackageDocument{
					ConnectorPacks: runtimecontracts.ConnectorPackImports{
						Imports: []runtimecontracts.ConnectorPackImport{{Provider: "telegram", Tool: "telegram.send_message"}},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, telegramConnectorSupportedSurfacePackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func telegramConnectorSupportedSurfacePackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tool, ok := providerconnectors.BuiltinTool("telegram", "telegram.send_message")
	if !ok {
		t.Fatal("provider connector pack telegram.send_message not found")
	}
	if tool.HTTP == nil {
		t.Fatal("provider connector pack telegram.send_message missing http block")
	}
	httpSpec := *tool.HTTP
	tool.HTTP = &httpSpec
	tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage"
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.telegram.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "telegram",
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": tool,
			},
		},
		Source: "test:provider.telegram.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

type telegramConnectorSupportedSurfacePackImportSource struct {
	semanticview.Source
	projectScopes []semanticview.ProjectScope
}

func (s telegramConnectorSupportedSurfacePackImportSource) BaseSemanticSource() semanticview.Source {
	return s.Source
}

func (s telegramConnectorSupportedSurfacePackImportSource) ProjectScopes() []semanticview.ProjectScope {
	return append([]semanticview.ProjectScope(nil), s.projectScopes...)
}

type telegramConnectorSupportedSurfaceModule struct {
	source  semanticview.Source
	nodes   []runtimepipeline.WorkflowNode
	guards  runtimepipeline.GuardRegistry
	actions runtimepipeline.ActionRegistry
}

func (m telegramConnectorSupportedSurfaceModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m telegramConnectorSupportedSurfaceModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}

func (m telegramConnectorSupportedSurfaceModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m telegramConnectorSupportedSurfaceModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m telegramConnectorSupportedSurfaceModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

func startTelegramConnectorSupportedSurfaceCoordinator(
	t *testing.T,
	ctx context.Context,
	bus *runtimebus.EventBus,
	db *sql.DB,
	workflowStore *runtimepipeline.WorkflowInstanceStore,
	source semanticview.Source,
	credentialStore runtimecredentials.Store,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	module := telegramConnectorSupportedSurfaceModule{
		source:  source,
		nodes:   nodes,
		guards:  runtimepipeline.NewContractGuardRegistry(source),
		actions: runtimepipeline.NewContractActionRegistry(source),
	}
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        module,
		WorkflowStore: workflowStore,
		Credentials:   credentialStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	subscribed := make(chan struct{}, 1)
	pc.SetTestSubscribeHook(func() {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go pc.Run(runCtx)
	select {
	case <-subscribed:
	case <-time.After(telegramConnectorSupportedSurfaceTimeout):
		t.Fatal("pipeline coordinator did not subscribe")
	}
	return pc
}

func telegramConnectorSupportedSurfaceCredentialStore(t *testing.T, key, value string) runtimecredentials.Store {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if strings.TrimSpace(key) != "" {
		if err := store.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set credential: %v", err)
		}
	}
	return store
}

func seedTelegramConnectorSupportedSurfaceWorkflowVersion(t *testing.T, ctx context.Context, db *sql.DB, flowInstance string, sqlite bool) {
	t.Helper()
	if sqlite {
		if _, err := db.ExecContext(ctx, `
			UPDATE flow_instances
			SET config = json_set(COALESCE(config, '{}'), '$.workflow_version', '1.0.0')
			WHERE instance_id = ?
		`, flowInstance); err != nil {
			t.Fatalf("seed sqlite workflow version: %v", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE flow_instances
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{workflow_version}', '"1.0.0"'::jsonb, true)
		WHERE instance_id = $1
	`, flowInstance); err != nil {
		t.Fatalf("seed postgres workflow version: %v", err)
	}
}

func requireTelegramConnectorSupportedSurfaceCall(t *testing.T, calls <-chan telegramConnectorSupportedSurfaceCall, backendName string, backend telegramConnectorSupportedSurfaceBackend) telegramConnectorSupportedSurfaceCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(telegramConnectorSupportedSurfaceTimeout):
		t.Fatalf("%s timed out waiting for fake Telegram dispatch; diagnostics: %s", backendName, telegramConnectorSupportedSurfaceDiagnostics(t, backend))
		return telegramConnectorSupportedSurfaceCall{}
	}
}

func requireNoTelegramConnectorSupportedSurfaceCall(t *testing.T, calls <-chan telegramConnectorSupportedSurfaceCall, backend, context string) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("%s %s: unexpected fake Telegram dispatch: path=%s body=%s", backend, context, call.path, call.raw)
	default:
	}
}

func loadTelegramConnectorSupportedSurfaceInboundEventID(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, providerEventID string) string {
	t.Helper()
	return loadTelegramConnectorSupportedSurfaceInboundEventIDByRun(t, backend, backend.runID, providerEventID)
}

func loadTelegramConnectorSupportedSurfaceInboundEventIDByRun(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, runID, providerEventID string) string {
	t.Helper()
	var eventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id
			FROM events
			WHERE run_id = ?
			  AND entity_id = ?
			  AND event_name = 'inbound.telegram'
			  AND json_extract(payload, '$.provider_event_id') = ?
			ORDER BY created_at DESC
			LIMIT 1
		`, runID, backend.entityID, providerEventID).Scan(&eventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id::text
			FROM events
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND event_name = 'inbound.telegram'
			  AND payload->>'provider_event_id' = $3
			ORDER BY created_at DESC
			LIMIT 1
		`, runID, backend.entityID, providerEventID).Scan(&eventID)
	}
	if err != nil {
		t.Fatalf("%s load inbound event id for %s: %v", backend.name, providerEventID, err)
	}
	return eventID
}

func countTelegramConnectorSupportedSurfaceNodeDeliveries(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, eventID, nodeID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
		`, eventID, nodeID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
		`, eventID, nodeID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count node deliveries: %v", backend.name, err)
	}
	return count
}

func loadTelegramConnectorSupportedSurfaceActivityAttempt(t *testing.T, backend telegramConnectorSupportedSurfaceBackend) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	rec, ok, err := tryLoadTelegramConnectorSupportedSurfaceActivityAttempt(backend)
	if err != nil {
		t.Fatalf("%s load activity attempt: %v", backend.name, err)
	}
	if !ok {
		t.Fatalf("%s activity attempt not found", backend.name)
	}
	return rec
}

func waitForTelegramConnectorSupportedSurfaceTerminalActivityAttempt(t *testing.T, backend telegramConnectorSupportedSurfaceBackend) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(telegramConnectorSupportedSurfaceTimeout)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadTelegramConnectorSupportedSurfaceActivityAttempt(backend)
		if err != nil {
			t.Fatalf("%s load activity attempt while waiting: %v", backend.name, err)
		}
		if ok {
			last = rec
			saw = true
			if rec.Status != runtimepipeline.ActivityAttemptStatusStarted {
				return rec
			}
		}
		if time.Now().After(deadline) {
			if saw {
				t.Fatalf("%s activity attempt did not reach terminal status; last=%q diagnostics: %s", backend.name, last.Status, telegramConnectorSupportedSurfaceDiagnostics(t, backend))
			}
			t.Fatalf("%s activity attempt was not created; diagnostics: %s", backend.name, telegramConnectorSupportedSurfaceDiagnostics(t, backend))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryLoadTelegramConnectorSupportedSurfaceActivityAttempt(backend telegramConnectorSupportedSurfaceBackend) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'telegram.send_message'
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'telegram.send_message'
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID).Scan(&requestEventID)
	}
	if err == sql.ErrNoRows {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	rec, ok, err := backend.workflowStore.LoadActivityAttempt(backend.ctx, requestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	return rec, ok, nil
}

func countTelegramConnectorSupportedSurfaceActivityAttempts(t *testing.T, backend telegramConnectorSupportedSurfaceBackend) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'telegram.send_message'
		`, backend.runID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'telegram.send_message'
		`, backend.runID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts: %v", backend.name, err)
	}
	return count
}

func countTelegramConnectorSupportedSurfaceActivityAttemptsForEvent(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, providerEventID string) int {
	t.Helper()
	inboundEventID := loadTelegramConnectorSupportedSurfaceInboundEventIDByRun(t, backend, backend.runID, providerEventID)
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'telegram.send_message'
			  AND source_event_id = ?
		`, backend.runID, inboundEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'telegram.send_message'
			  AND source_event_id = $2::uuid
		`, backend.runID, inboundEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts for provider event %s: %v", backend.name, providerEventID, err)
	}
	return count
}

func telegramConnectorSupportedSurfaceActivityStatusForEvent(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, providerEventID string) string {
	t.Helper()
	inboundEventID := loadTelegramConnectorSupportedSurfaceInboundEventIDByRun(t, backend, backend.runID, providerEventID)
	var status string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `SELECT status FROM activity_attempts WHERE run_id = ? AND tool = 'telegram.send_message' AND source_event_id = ?`, backend.runID, inboundEventID).Scan(&status)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `SELECT status FROM activity_attempts WHERE run_id = $1::uuid AND tool = 'telegram.send_message' AND source_event_id = $2::uuid`, backend.runID, inboundEventID).Scan(&status)
	}
	if err != nil {
		t.Fatalf("%s load activity status for provider event %s: %v", backend.name, providerEventID, err)
	}
	return status
}

func countTelegramConnectorSupportedSurfaceFailureEventsForEvent(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, providerEventID string) int {
	t.Helper()
	inboundEventID := loadTelegramConnectorSupportedSurfaceInboundEventIDByRun(t, backend, backend.runID, providerEventID)
	failureEventType := boundedProviderFlowID + ".telegram_send_message.failed"
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND event_name = ?
			  AND source_event_id = ?
		`, backend.runID, failureEventType, inboundEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name = $2
			  AND source_event_id = $3::uuid
		`, backend.runID, failureEventType, inboundEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count failure events for provider event %s: %v", backend.name, providerEventID, err)
	}
	return count
}

func requireTelegramConnectorSupportedSurfaceFailureEventEventually(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, providerEventID string) {
	t.Helper()
	deadline := time.Now().Add(telegramConnectorSupportedSurfaceTimeout)
	last := 0
	for {
		if got := countTelegramConnectorSupportedSurfaceFailureEventsForEvent(t, backend, providerEventID); got == 1 {
			return
		} else {
			last = got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s missing-token generated failure events = %d, want 1; diagnostics: %s", backend.name, last, telegramConnectorSupportedSurfaceDiagnostics(t, backend))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireTelegramConnectorSupportedSurfaceEventEventually(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, eventID, eventType string) {
	t.Helper()
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(eventType) == "" {
		t.Fatalf("%s result event identity missing: id=%q type=%q", backend.name, eventID, eventType)
	}
	deadline := time.Now().Add(telegramConnectorSupportedSurfaceTimeout)
	for {
		if telegramConnectorSupportedSurfaceEventExists(t, backend, eventID, eventType) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s event rows for %s/%s = 0, want 1; diagnostics: %s", backend.name, eventID, eventType, telegramConnectorSupportedSurfaceDiagnostics(t, backend))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func telegramConnectorSupportedSurfaceEventExists(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, eventID, eventType string) bool {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE event_id = ?
			  AND event_name = ?
		`, eventID, eventType).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE event_id = $1::uuid
			  AND event_name = $2
		`, eventID, eventType).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s require event %s/%s: %v", backend.name, eventID, eventType, err)
	}
	return count == 1
}

func loadTelegramConnectorSupportedSurfaceActivityRequestEvent(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, eventID string) events.Event {
	t.Helper()
	var raw []byte
	var created time.Time
	var err error
	if backend.sqlite {
		var payload string
		var createdRaw string
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT payload, created_at
			FROM events
			WHERE event_id = ?
			  AND event_name = 'platform.activity_requested'
		`, eventID).Scan(&payload, &createdRaw)
		raw = []byte(payload)
		created, _ = time.Parse(time.RFC3339Nano, createdRaw)
		if created.IsZero() {
			created = time.Now().UTC()
		}
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT payload, created_at
			FROM events
			WHERE event_id = $1::uuid
			  AND event_name = 'platform.activity_requested'
		`, eventID).Scan(&raw, &created)
	}
	if err != nil {
		t.Fatalf("%s load activity request event %s: %v", backend.name, eventID, err)
	}
	return eventtest.PersistedProjection(
		eventID,
		events.EventType("platform.activity_requested"),
		"workflow-runtime",
		"",
		raw,
		2,
		backend.runID,
		"",
		events.EventEnvelope{EntityID: backend.entityID},
		created,
	)
}

func assertTelegramConnectorSupportedSurfaceNoStoredSecret(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, secret string) {
	t.Helper()
	var eventLeaks int
	var attemptLeaks int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND payload LIKE '%' || ? || '%'
		`, backend.runID, secret).Scan(&eventLeaks)
		if err == nil {
			err = backend.db.QueryRowContext(backend.ctx, `
				SELECT COUNT(*)
				FROM activity_attempts
				WHERE run_id = ?
				  AND (
					COALESCE(result_payload, '') LIKE '%' || ? || '%'
					OR COALESCE(failure, '') LIKE '%' || ? || '%'
				  )
			`, backend.runID, secret, secret).Scan(&attemptLeaks)
		}
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND payload::text LIKE '%' || $2 || '%'
		`, backend.runID, secret).Scan(&eventLeaks)
		if err == nil {
			err = backend.db.QueryRowContext(backend.ctx, `
				SELECT COUNT(*)
				FROM activity_attempts
				WHERE run_id = $1::uuid
				  AND (
					COALESCE(result_payload::text, '') LIKE '%' || $2 || '%'
					OR COALESCE(failure::text, '') LIKE '%' || $2 || '%'
				  )
			`, backend.runID, secret).Scan(&attemptLeaks)
		}
	}
	if err != nil {
		t.Fatalf("%s no-secret query: %v", backend.name, err)
	}
	if eventLeaks != 0 || attemptLeaks != 0 {
		t.Fatalf("%s stored secret leaks: events=%d activity_attempts=%d", backend.name, eventLeaks, attemptLeaks)
	}
}

func telegramConnectorSupportedSurfaceDiagnostics(t *testing.T, backend telegramConnectorSupportedSurfaceBackend) string {
	t.Helper()
	type row struct {
		name   string
		query  string
		args   []any
		sqlite string
	}
	rows := []row{
		{
			name:   "events",
			query:  `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`,
			sqlite: `SELECT COUNT(*) FROM events WHERE run_id = ?`,
			args:   []any{backend.runID},
		},
		{
			name:   "inbound_telegram_events",
			query:  `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'inbound.telegram'`,
			sqlite: `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'inbound.telegram'`,
			args:   []any{backend.runID},
		},
		{
			name:   "node_deliveries",
			query:  `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`,
			sqlite: `SELECT COUNT(*) FROM event_deliveries WHERE run_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`,
			args:   []any{backend.runID, telegramConnectorSupportedSurfaceNodeID},
		},
		{
			name:   "activity_requests",
			query:  `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`,
			sqlite: `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`,
			args:   []any{backend.runID},
		},
		{
			name:   "activity_attempts",
			query:  `SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid`,
			sqlite: `SELECT COUNT(*) FROM activity_attempts WHERE run_id = ?`,
			args:   []any{backend.runID},
		},
		{
			name:   "dead_letters",
			query:  `SELECT COUNT(*) FROM dead_letters WHERE entity_id = $1::uuid`,
			sqlite: `SELECT COUNT(*) FROM dead_letters WHERE entity_id = ?`,
			args:   []any{backend.entityID},
		},
	}
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		query := r.query
		if backend.sqlite {
			query = r.sqlite
		}
		var count int
		if err := backend.db.QueryRowContext(backend.ctx, query, r.args...).Scan(&count); err != nil {
			parts = append(parts, r.name+"=ERR("+err.Error()+")")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%d", r.name, count))
	}
	parts = append(parts, "delivery_status="+telegramConnectorSupportedSurfaceScalarDiagnostic(t, backend,
		`SELECT COALESCE(status, '') || ':' || COALESCE(reason_code, '') || ':' || COALESCE(failure->'detail'->>'code', '') FROM event_deliveries WHERE run_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2 ORDER BY created_at DESC LIMIT 1`,
		`SELECT COALESCE(status, '') || ':' || COALESCE(reason_code, '') || ':' || COALESCE(json_extract(failure, '$.detail.code'), '') FROM event_deliveries WHERE run_id = ? AND subscriber_type = 'node' AND subscriber_id = ? ORDER BY created_at DESC LIMIT 1`,
		backend.runID, telegramConnectorSupportedSurfaceNodeID))
	parts = append(parts, "event_names="+telegramConnectorSupportedSurfaceScalarDiagnostic(t, backend,
		`SELECT COALESCE(string_agg(event_id::text || ':' || event_name, ',' ORDER BY created_at), '') FROM events WHERE run_id = $1::uuid`,
		`SELECT COALESCE(group_concat(event_id || ':' || event_name, ','), '') FROM (SELECT event_id, event_name FROM events WHERE run_id = ? ORDER BY created_at)`,
		backend.runID))
	parts = append(parts, "activity_receipt="+telegramConnectorSupportedSurfaceScalarDiagnostic(t, backend,
		`SELECT COALESCE(r.outcome, '') || ':' || COALESCE(r.failure->>'class', '') || ':' || COALESCE(r.failure->'detail'->>'code', '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid AND e.event_name = 'platform.activity_requested' AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' ORDER BY r.processed_at DESC LIMIT 1`,
		`SELECT COALESCE(r.outcome, '') || ':' || COALESCE(json_extract(r.failure, '$.class'), '') || ':' || COALESCE(json_extract(r.failure, '$.detail.code'), '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? AND e.event_name = 'platform.activity_requested' AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' ORDER BY r.processed_at DESC LIMIT 1`,
		backend.runID))
	parts = append(parts, "dead_letter="+telegramConnectorSupportedSurfaceScalarDiagnostic(t, backend,
		`SELECT COALESCE(failure->>'class', '') || ':' || COALESCE(failure->'detail'->>'code', '') FROM dead_letters WHERE entity_id = $1::uuid ORDER BY created_at DESC LIMIT 1`,
		`SELECT COALESCE(json_extract(failure, '$.class'), '') || ':' || COALESCE(json_extract(failure, '$.detail.code'), '') FROM dead_letters WHERE entity_id = ? ORDER BY created_at DESC LIMIT 1`,
		backend.entityID))
	return strings.Join(parts, " ")
}

func telegramConnectorSupportedSurfaceScalarDiagnostic(t *testing.T, backend telegramConnectorSupportedSurfaceBackend, pgQuery, sqliteQuery string, args ...any) string {
	t.Helper()
	query := pgQuery
	if backend.sqlite {
		query = sqliteQuery
	}
	var value string
	if err := backend.db.QueryRowContext(backend.ctx, query, args...).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return "<none>"
		}
		return "ERR(" + err.Error() + ")"
	}
	return value
}

func telegramConnectorSupportedSurfaceString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprint(v)
	}
}
