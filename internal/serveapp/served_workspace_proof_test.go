package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// All read roles refer to the original serve projection. This fixture neither
// exposes a pool nor reconstructs a store around another owner's connection.
type servedWorkspaceProofRuntime struct {
	Endpoint, Backend, BundleHash string
	Runtime                       *runtimepkg.Runtime
	Events                        runtimebus.EventStore
	Lifecycle                     manager.AgentLifecycleStateReader
	Observability                 apiv1.ObservabilityReadStore
	WorkflowTargets               pipeline.WorkflowTargetPersistenceReader
	Inbound                       interface {
		LoadInboundPublicationByIdentity(context.Context, inboundpublication.Identity) (inboundpublication.Record, bool, error)
	}
	FlowRuns    runlifecycle.LandingRunReader
	ForkRuntime selectedForkRuntimeProofOptions
	Restart     func() servedWorkspaceProofRuntime
	Telegram    *channelOnboardingTelegramProvider
}

func requireServedLandingRun(t testing.TB, rt *runtimepkg.Runtime, flow string) runlifecycle.LandingRun {
	t.Helper()
	reader, ok := rt.Bus.Store().(runlifecycle.LandingRunReader)
	if !ok {
		t.Fatalf("served store %T lacks native landing readback", rt.Bus.Store())
	}
	row, found, err := reader.LoadLandingRun(t.Context(), runlifecycle.LiveRunScope{FlowPath: flow})
	if err != nil || !found || row.RunID == "" || row.Source != rt.Options.SourceArtifactFact {
		t.Fatalf("flow %s landing=%+v found=%t err=%v", flow, row, found, err)
	}
	return row
}

func readServedLandingRuns(ctx context.Context, rt *runtimepkg.Runtime) ([]runlifecycle.LandingRun, error) {
	reader, ok := rt.Bus.Store().(runlifecycle.LandingRunReader)
	if !ok {
		return nil, fmt.Errorf("served store %T lacks native landing readback", rt.Bus.Store())
	}
	scopes, err := rt.PlanLiveFlowScopes()
	if err != nil {
		return nil, err
	}
	var rows []runlifecycle.LandingRun
	for _, scope := range scopes {
		row, found, err := reader.LoadLandingRun(ctx, scope)
		if err != nil || !found {
			return nil, fmt.Errorf("flow %s landing found=%t: %w", scope.FlowPath, found, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func configureHostWorkspaceProofLifecycle(t *testing.T) {
	t.Helper()
	previous := cliapp.ConfiguredWorkspaceLifecycleForServe
	root := t.TempDir()
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		owner := workspace.NewHostManager()
		cfg := workspace.DefaultHostConfig()
		cfg.WorkspaceRoot, cfg.SourceProjection = root, projection
		owner.SetConfig(cfg)
		owner.SetSemanticSource(source)
		return owner, nil
	}
	t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = previous })
}

func startWorkspaceGatewayProofRuntime(t *testing.T, backend servedparity.Backend, sourceRoot, targetBackend string, factory func(*sourceartifact.RuntimeProjection, semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error), mcpListen string, hooks ...pipeline.WorkflowNodeHandlerStartHook) servedWorkspaceProofRuntime {
	t.Helper()
	return startWorkspaceGatewayProofRuntimeWithPosture(t, backend, sourceRoot, targetBackend, factory, mcpListen, executionposture.MockOnly, hooks...)
}

func startWorkspaceLiveGatewayProofRuntime(t *testing.T, backend servedparity.Backend, sourceRoot, targetBackend string, factory func(*sourceartifact.RuntimeProjection, semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error), mcpListen string, hooks ...pipeline.WorkflowNodeHandlerStartHook) servedWorkspaceProofRuntime {
	t.Helper()
	return startWorkspaceGatewayProofRuntimeWithPosture(t, backend, sourceRoot, targetBackend, factory, mcpListen, executionposture.Live, hooks...)
}

func startWorkspaceGatewayProofRuntimeWithPosture(t *testing.T, backend servedparity.Backend, sourceRoot, targetBackend string, factory func(*sourceartifact.RuntimeProjection, semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error), mcpListen string, posture executionposture.Posture, hooks ...pipeline.WorkflowNodeHandlerStartHook) servedWorkspaceProofRuntime {
	t.Helper()
	if len(hooks) > 1 {
		t.Fatal("at most one handler-start barrier is supported")
	}
	proof := servedWorkspaceProofRuntime{BundleHash: servedEventPublishFixtureBundleHash(t, sourceRoot)}
	var accounts []string
	if posture == executionposture.Live {
		bundle := loadWorkflowValidationBundleAt(t, sourceRoot)
		rooted, err := bundle.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, flow := range providers.FlowPaths() {
			scope, err := providers.Scope(flow)
			if err != nil {
				t.Fatal(err)
			}
			provider, placement, err := scope.ConnectionAccount(bundle)
			if err != nil {
				t.Fatal(err)
			}
			plan, held := provider.ChannelPlan()
			if !held || placement.FlowID == "" || plan.Provider() != "telegram" {
				continue
			}
			account, assigned := placement.Value(nil)
			if !assigned {
				t.Fatal("live Telegram fixture must explicitly own its receiving account")
			}
			if !seen[account] {
				accounts = append(accounts, account)
				seen[account] = true
			}
		}
		sort.Strings(accounts)
	}
	forkOptions := captureServedForkRuntimeOptions(t)
	previousProjection := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		projection := previousProjection(owner)
		proof.Events = projection.deps.EventStore
		proof.Lifecycle = projection.deps.ManagerPersistenceRoles.LifecycleState
		proof.Observability = owner.Observability()
		proof.WorkflowTargets, _ = projection.deps.EventStore.(pipeline.WorkflowTargetPersistenceReader)
		proof.Inbound = projection.deps.InboundStore
		proof.FlowRuns, _ = projection.deps.EventStore.(runlifecycle.LandingRunReader)
		return projection
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previousProjection })
	if factory == nil {
		configureHostWorkspaceProofLifecycle(t)
	} else {
		previousWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return factory(projection, source)
		}
		t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = previousWorkspace })
	}
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, WorkspaceBackend: targetBackend, WorkspaceBackendSet: targetBackend != "",
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: mcpListen, SelfCheck: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	if posture == executionposture.Live {
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/v1/messages" || request.Header.Get("x-api-key") != "workspace-live-proof-key" {
				http.Error(w, "unexpected controlled provider request", http.StatusBadRequest)
				return
			}
			var turn struct {
				Tools []struct {
					Name        string `json:"name"`
					InputSchema struct {
						Properties map[string]json.RawMessage `json:"properties"`
					} `json:"input_schema"`
				} `json:"tools"`
				Messages []struct {
					Content json.RawMessage `json:"content"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&turn); err != nil {
				t.Errorf("decode controlled provider turn: %v", err)
				http.Error(w, "invalid provider turn", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			for _, tool := range turn.Tools {
				var ids struct {
					Enum []string `json:"enum"`
				}
				if tool.Name != "read_flow_data" || json.Unmarshal(tool.InputSchema.Properties["static_id"], &ids) != nil || len(ids.Enum) != 1 {
					continue
				}
				for _, message := range turn.Messages {
					var text string
					if json.Unmarshal(message.Content, &text) == nil && strings.HasPrefix(text, "Tool result:\n") && strings.Contains(text, "selected-only-mcp-resource") {
						_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"selected-only-mcp-resource verified"}]}`))
						return
					}
					var blocks []struct {
						Type    string          `json:"type"`
						Content json.RawMessage `json:"content"`
					}
					if json.Unmarshal(message.Content, &blocks) != nil {
						continue
					}
					for _, block := range blocks {
						if block.Type == "tool_result" && strings.Contains(string(block.Content), "selected-only-mcp-resource") {
							_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"selected-only-mcp-resource verified"}]}`))
							return
						}
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"model": "claude-test", "usage": map[string]int{"input_tokens": 1, "output_tokens": 1},
					"content": []map[string]any{{"type": "tool_use", "id": "workspace-native-read", "name": tool.Name,
						"input": map[string]string{"kind": "static_file", "static_id": ids.Enum[0]}}},
				})
				return
			}
			_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"Observed delivery."}]}`))
		}))
		t.Cleanup(provider.Close)
		redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
		credentialStore, err := cliapp.CredentialFileStore()
		if err != nil {
			t.Fatal(err)
		}
		if err := credentialStore.Set(context.Background(), "ANTHROPIC_API_KEY", "workspace-live-proof-key"); err != nil {
			t.Fatal(err)
		}
	}
	if len(hooks) == 1 {
		opts.TestWorkflowNodeHandlerStartHook = hooks[0]
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		proof.Backend = "sqlite"
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), ".swarm", "dev.db"))
		if posture == executionposture.Live {
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), ".swarm", "dev.db"))
		}
	case servedparity.BackendExplicitPostgres:
		proof.Backend = "postgres"
		dsn := testutil.StartEmptyPostgresDSN(t)
		previous := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			if selection.Backend != storebackend.BackendPostgres {
				t.Fatal("native workspace fixture selected another backend")
			}
			return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
				Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg),
			})
		}
		t.Cleanup(func() { buildStoresForServe = previous })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		if posture == executionposture.Live {
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		}
		if len(accounts) > 0 {
			opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
		}
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	default:
		t.Fatalf("unknown served workspace backend %q", backend)
	}
	if len(accounts) > 0 {
		proof.Telegram = &channelOnboardingTelegramProvider{}
		provider := httptest.NewServer(proof.Telegram)
		t.Cleanup(provider.Close)
		redirectExternalHosts(t, map[string]string{"api.telegram.org": provider.URL})
		for _, account := range accounts {
			id, err := strconv.ParseInt(account, 10, 64)
			if err != nil || id <= 0 {
				t.Fatal("live Telegram fixture account must be a canonical bot ID")
			}
			token := "fixture-token-" + account
			proof.Telegram.SetResourceID(token, id)
			registerChannelFixtureAccount(t, opts.ConfigPath, token, account)
		}
		opts.PublicWebhookBaseURL = "https://hooks.channel-onboarding.test"
	}
	retainedRoot := ownedMockLifecycleRoot(t)
	var process *serveRuntimeTestProcess
	start := func() {
		if proof.Telegram != nil {
			opts.PublicWebhookListener = reserveChannelOnboardingListener(t)
			opts.PublicWebhookListen = opts.PublicWebhookListener.Addr().String()
			redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + opts.PublicWebhookListen})
		}
		if proof.Telegram != nil {
			process = startServeRuntimeTestProcess(t, opts)
		} else {
			process = startOwnedLifecycleTestProcess(t, repoRootForTest(), retainedRoot, opts, posture)
		}
		process.waitForReadyLine()
		proof.Endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
		proof.Runtime = servedTestProcessRuntime(t, process)
		if proof.Runtime.ExecutionPosture != posture {
			t.Fatalf("retained runtime posture=%s, want %s", proof.Runtime.ExecutionPosture, posture)
		}
		proof.ForkRuntime = *forkOptions
	}
	proof.Restart = func() servedWorkspaceProofRuntime {
		if code := process.stop(); code != 0 {
			t.Fatalf("retained workspace predecessor exit=%d", code)
		}
		start()
		return proof
	}
	start()
	if proof.Events == nil || proof.Lifecycle == nil || proof.Observability == nil || proof.WorkflowTargets == nil || proof.Inbound == nil || proof.FlowRuns == nil {
		t.Fatal("workspace proof requires the original selected read roles")
	}
	return proof
}

func workspaceProofAuthorActivityContext(t *testing.T, proof servedWorkspaceProofRuntime) context.Context {
	t.Helper()
	return servedRuntimeProofAuthorActivityContext(t, proof.Runtime, proof.BundleHash)
}

func readWorkspaceProofApplication(t *testing.T, owner runtimebus.EventStore) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func expireWorkspaceProofResetTransportCache(t *testing.T, owner apiv1.APIIdempotencyStore, reader runtimebus.EventStore) {
	t.Helper()
	_, _, err := owner.WithAPIIdempotency(context.Background(), apiidempotency.Request{
		Method: "test.expire-transport-cache", Actor: apiidempotency.BearerActor("expiry-proof"),
		IdempotencyKey: uuid.NewString(), RequestHash: "expiry-proof",
		Now: time.Now().UTC().Add(48 * time.Hour), TTL: time.Minute,
	}, func(context.Context) (apiidempotency.Completion, error) {
		return apiidempotency.Completion{Response: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	count, err := storetest.ReadResetTransportCacheEntryCount(context.Background(), reader)
	if err != nil || count != 0 {
		t.Fatalf("reset transport cache was not expired: count=%d err=%v", count, err)
	}
}

func requireWorkspaceProofDiagnosticEventCount(t *testing.T, rows []storetest.SelectedForkLifecycleDiagnosticLog, id, runID string, want int) {
	t.Helper()
	var count int
	for _, row := range rows {
		var event struct {
			Details map[string]any `json:"details"`
		}
		if err := json.Unmarshal(row.Payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.Details["outbox_id"] == id {
			count++
			if !row.RunPresent || row.RunID != runID || event.Details["run_id"] != runID {
				t.Fatalf("diagnostic %s changed original run identity: %s %+v", id, row.RunID, event.Details)
			}
		}
	}
	if count != want {
		t.Fatalf("diagnostic %s log count=%d want=%d", id, count, want)
	}
}

func waitWorkspaceProofPipelineHandoff(t *testing.T, proof servedWorkspaceProofRuntime, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		count, err := storetest.ReadServedIncompletePipelineHandoffCount(context.Background(), proof.Events, runID)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("source pipeline completion did not finish: %s", workspaceProofDebugSummary(t, proof, runID))
}

func workspaceProofDebugSummary(t *testing.T, proof servedWorkspaceProofRuntime, runID string) string {
	t.Helper()
	summary, err := storetest.ReadServedRunDebugSummary(context.Background(), proof.Events, runID)
	if err != nil {
		t.Fatalf("read workspace proof diagnostic: %v", err)
	}
	return summary
}

func waitWorkspaceProofSourceAgentReady(t *testing.T, proof servedWorkspaceProofRuntime, runID, eventID string) {
	t.Helper()
	waitServedSourceAgentReadyFromOwner(t, proof.Events, proof.Backend, runID, eventID)
}

func requireWorkspaceProofMockTurn(t *testing.T, proof servedWorkspaceProofRuntime, runID, agentID, eventID string) storetest.ManagedAgentTurnStorageRow {
	t.Helper()
	matches := workspaceProofMockTurns(t, proof, runID, agentID, eventID)
	if len(matches) != 1 {
		t.Fatalf("exact mock turn for run=%s agent=%s event=%s: count=%d, want one", runID, agentID, eventID, len(matches))
	}
	return matches[0]
}

func workspaceProofMockTurns(t *testing.T, proof servedWorkspaceProofRuntime, runID, agentID, eventID string) []storetest.ManagedAgentTurnStorageRow {
	t.Helper()
	event := storetest.LoadCanonicalEventRecord(t, context.Background(), proof.Events, eventID)
	mode := proof.Runtime.ExecutionPosture.RootMode()
	if event.RunID() != runID || event.ExecutionMode() != mode {
		t.Fatalf("exact turn event run=%s mode=%s, want run=%s mode=%s", event.RunID(), event.ExecutionMode(), runID, mode)
	}
	rows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), proof.Events, runID, agentID)
	if err != nil {
		t.Fatal(err)
	}
	var matches []storetest.ManagedAgentTurnStorageRow
	for _, row := range rows {
		if row.ExecutionMode == string(mode) && row.TriggerEventID == eventID {
			matches = append(matches, row)
		}
	}
	return matches
}

func requireWorkspaceProofWorkReadyEvent(t *testing.T, endpoint string, owner runtimebus.EventStore, runID string) string {
	t.Helper()
	params := map[string]any{
		"filter": map[string]any{"run_id": runID, "event_name": "producer/work.ready"},
		"limit":  1,
	}
	seenCursors := map[string]bool{}
	var found []operatorread.OperatorEventFull
	for {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", params, &page)
		for _, event := range page.Events {
			if event.RunID != runID || event.EventName != "producer/work.ready" {
				t.Fatalf("work-ready lookup escaped its exact run/name: %+v", event)
			}
			found = append(found, event)
		}
		if page.NextCursor == "" {
			break
		}
		if seenCursors[page.NextCursor] {
			t.Fatal("work-ready lookup repeated its pagination cursor")
		}
		seenCursors[page.NextCursor] = true
		params["cursor"] = page.NextCursor
	}
	if len(found) != 1 {
		t.Fatalf("exact work-ready event count=%d, want one", len(found))
	}
	stored := storetest.LoadCanonicalEventRecord(t, context.Background(), owner, found[0].EventID)
	if stored.RunID() != runID || string(stored.Type()) != "producer/work.ready" {
		t.Fatalf("work-ready lookup disagrees with the canonical stored record: %+v", stored)
	}
	return stored.ID()
}

func readWorkspaceProofReceiverRows(t *testing.T, owner runtimebus.EventStore, runID string) map[string]forkReceiverRow {
	t.Helper()
	rows, err := storetest.ReadReceiverJoinedInventoryStorage(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	out, err := workspaceProofReceiverRowsByFlow(rows)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func workspaceProofReceiverRowsByFlow(rows []storetest.ReceiverJoinedInventoryStorageRow) (map[string]forkReceiverRow, error) {
	out := map[string]forkReceiverRow{}
	for _, stored := range rows {
		row := forkReceiverRow{ID: stored.EntityID, Flow: stored.FlowInstance, Type: stored.EntityType, State: stored.CurrentState}
		if err := json.Unmarshal([]byte(stored.Fields), &row.Fields); err != nil {
			return nil, err
		}
		if _, duplicate := out[row.Flow]; duplicate {
			return nil, fmt.Errorf("fixture has duplicate owner rows for %s", row.Flow)
		}
		out[row.Flow] = row
	}
	return out, nil
}

func TestWorkspaceProofReceiverInventoryRefusesDuplicateOrMalformedRows(t *testing.T) {
	row := storetest.ReceiverJoinedInventoryStorageRow{
		EntityID: uuid.NewString(), FlowInstance: "consumer", EntityType: "receipt", CurrentState: "done",
		Fields: `{"integer":7,"decimal":7.0,"nested":[null,{"flag":true}]}`,
	}
	got, err := workspaceProofReceiverRowsByFlow([]storetest.ReceiverJoinedInventoryStorageRow{row})
	if err != nil || len(got) != 1 || got[row.FlowInstance].ID != row.EntityID || got[row.FlowInstance].State != "done" || got[row.FlowInstance].Fields["integer"] != float64(7) || got[row.FlowInstance].Fields["decimal"] != float64(7) {
		t.Fatalf("original receiver field decoding changed: %+v %v", got, err)
	}
	duplicate := row
	duplicate.EntityID = uuid.NewString()
	malformed := row
	malformed.FlowInstance, malformed.Fields = "other", "{"
	for _, rows := range [][]storetest.ReceiverJoinedInventoryStorageRow{{row, duplicate}, {row, malformed}} {
		if got, err := workspaceProofReceiverRowsByFlow(rows); err == nil || got != nil {
			t.Fatalf("duplicate or late malformed row leaked partial evidence: %+v %v", got, err)
		}
	}
}

func readWorkspaceProofClaimStorage(t *testing.T, reader runtimebus.EventStore, owner deliverylifecycle.Store, claim deliverylifecycle.Claim) (string, deliverylifecycle.Snapshot) {
	t.Helper()
	rows, err := storetest.ReadReceiverDeliveryStorage(context.Background(), reader, claim.RunID())
	if err != nil {
		t.Fatal(err)
	}
	var exact []storetest.ReceiverDeliveryStorageEvidence
	for _, row := range rows {
		if row.DeliveryID == claim.DeliveryID() {
			exact = append(exact, row)
		}
	}
	if len(exact) != 1 {
		t.Fatalf("exact claimed physical delivery count=%d, want one", len(exact))
	}
	snapshot, err := owner.Snapshot(context.Background(), claim.DeliveryID())
	if err != nil || snapshot.RunID != claim.RunID() || snapshot.EventID != exact[0].EventID || string(snapshot.Status) != exact[0].Status {
		t.Fatalf("claim owner disagrees with physical delivery: snapshot=%+v physical=%+v err=%v", snapshot, exact[0], err)
	}
	return exact[0].Target, snapshot
}

func readWorkspaceProofReceiverFields(t *testing.T, reader pipeline.WorkflowTargetPersistenceReader, runID string, receiver forkReceiverRow) (string, int64) {
	t.Helper()
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute("consumer", "", receiver.Flow))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.LoadWorkflowTargetPersistence(context.Background(), owner, identity.NormalizeEntityID(receiver.ID))
	if err != nil {
		t.Fatalf("exact receiver target owner unavailable: %v", err)
	}
	instance, err := record.DecodeComplete(owner.Route, identity.NormalizeEntityID(receiver.ID))
	if err != nil || instance.EntityID != receiver.ID || instance.StorageRef != receiver.Flow || instance.EntityType != receiver.Type {
		t.Fatalf("exact receiver header/field owner unavailable: instance=%+v err=%v", instance, err)
	}
	return string(record.State.Fields), instance.Revision
}

// These are detached physical snapshot rows, not a second SQL reader or a
// decoder for construction receipts. Field values retain their stored JSON text.
func workspaceProofPhysicalRows(table storetest.SelectedForkStorageTableSnapshot) ([]map[string]storetest.SelectedForkSnapshotCell, error) {
	return storetest.DecodeSelectedForkSnapshotRows(table)
}

func workspaceProofPhysicalText(row map[string]storetest.SelectedForkSnapshotCell, column string) (string, error) {
	cell, found := row[column]
	if !found {
		return "", fmt.Errorf("physical snapshot is missing column %s", column)
	}
	return cell.Text()
}

func workspaceProofClaimAttemptCounts(table storetest.SelectedForkStorageTableSnapshot, deliveryID string, claimVersion int64, outcome string) (int, int, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return 0, 0, err
	}
	settled, open := 0, 0
	for _, row := range rows {
		actualDeliveryID, err := workspaceProofPhysicalText(row, "delivery_id")
		if err != nil {
			return 0, 0, err
		}
		if actualDeliveryID != deliveryID {
			continue
		}
		marker, found := row["open_marker"]
		if !found {
			return 0, 0, fmt.Errorf("physical attempt is missing open_marker")
		}
		switch string(marker.Value) {
		case "true", "1":
			open++
		case "false", "0", "null":
		default:
			return 0, 0, fmt.Errorf("physical attempt has invalid open_marker: %s", marker.Value)
		}
		var version int64
		if err := json.Unmarshal(row["claim_version"].Value, &version); err != nil {
			return 0, 0, err
		}
		closure, err := workspaceProofPhysicalText(row, "closure_kind")
		if err != nil {
			return 0, 0, err
		}
		actualOutcome, err := workspaceProofPhysicalText(row, "outcome")
		if err != nil {
			return 0, 0, err
		}
		if closure == "settled" && version == claimVersion && actualOutcome == outcome {
			settled++
		}
	}
	return settled, open, nil
}

func workspaceProofPhysicalRow(t *testing.T, columns []string, values ...any) string {
	t.Helper()
	row, err := storetest.EncodeSelectedForkSnapshotValues(columns, values)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestWorkspaceProofPhysicalCountsKeepExactClaimAndChildPredicates(t *testing.T) {
	columns := []string{"delivery_id", "claim_version", "closure_kind", "outcome", "open_marker"}
	attempts := storetest.SelectedForkStorageTableSnapshot{
		Columns: columns,
		Rows: []string{
			workspaceProofPhysicalRow(t, columns, "exact", int64(1), "settled", "delivered", false),
			workspaceProofPhysicalRow(t, columns, "exact", int64(2), "settled", "delivered", int64(0)),
			workspaceProofPhysicalRow(t, columns, "exact", int64(1), "claimed", nil, true),
			workspaceProofPhysicalRow(t, columns, "other", int64(1), "settled", "delivered", int64(1)),
			workspaceProofPhysicalRow(t, columns, "exact", int64(1), "settled", "dead_letter", false),
		},
	}
	settled, open, err := workspaceProofClaimAttemptCounts(attempts, "exact", 1, "delivered")
	if err != nil || settled != 1 || open != 1 {
		t.Fatalf("exact claim/outcome or all-version open marker predicate changed: settled=%d open=%d err=%v", settled, open, err)
	}
	attempts.Rows = append(attempts.Rows, attempts.Rows[0])
	if settled, open, err := workspaceProofClaimAttemptCounts(attempts, "exact", 1, "delivered"); err != nil || settled != 2 || open != 1 {
		t.Fatalf("attempt multiplicity was lost: settled=%d open=%d err=%v", settled, open, err)
	}
	for _, bad := range []string{workspaceProofPhysicalRow(t, columns, "exact", int64(1), "claimed", nil, "invalid"), `[]`, `{`} {
		failed := attempts
		failed.Rows = append(append([]string(nil), attempts.Rows...), bad)
		if settled, open, err := workspaceProofClaimAttemptCounts(failed, "exact", 1, "delivered"); err == nil || settled != 0 || open != 0 {
			t.Fatalf("late malformed physical row leaked counts: settled=%d open=%d err=%v", settled, open, err)
		}
	}
	row := func(id, parent, name, class string) string {
		return workspaceProofPhysicalRow(t, []string{"event_id", "source_event_id", "event_name", "event_class"}, id, parent, name, class)
	}
	eventsTable := storetest.SelectedForkStorageTableSnapshot{
		Columns: []string{"event_id", "source_event_id", "event_name", "event_class"},
		Rows: []string{
			row("business", "cause", "business.done", "normal"),
			row("diagnostic", "cause", string(events.EventTypePlatformRuntimeLog), string(events.EventAdmissionDiagnosticDirect)),
			row("wrong-class", "cause", string(events.EventTypePlatformRuntimeLog), "normal"),
			row("wrong-name", "cause", "other.diagnostic", string(events.EventAdmissionDiagnosticDirect)),
			row("foreign", "other-cause", "business.done", "normal"),
		},
	}
	all, business, err := workspaceProofChildEventIDs(eventsTable, "cause")
	if err != nil || len(all) != 4 || len(business) != 3 || !all["diagnostic"] || business["diagnostic"] || !business["wrong-class"] || !business["wrong-name"] {
		t.Fatalf("exact diagnostic conjunction or parent scope changed: all=%v business=%v err=%v", all, business, err)
	}
	eventsTable.Rows = append(eventsTable.Rows, eventsTable.Rows[0])
	if all, business, err := workspaceProofChildEventIDs(eventsTable, "cause"); err == nil || all != nil || business != nil {
		t.Fatalf("duplicate event identity leaked a partial count: all=%v business=%v err=%v", all, business, err)
	}
	if rows, err := workspaceProofPhysicalRows(storetest.SelectedForkStorageTableSnapshot{Columns: []string{"same", "same"}}); err == nil || rows != nil {
		t.Fatalf("ambiguous column inventory became evidence: %+v %v", rows, err)
	}
}

func workspaceProofChildEventIDs(table storetest.SelectedForkStorageTableSnapshot, eventID string) (map[string]bool, map[string]bool, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return nil, nil, err
	}
	all, business := map[string]bool{}, map[string]bool{}
	for _, row := range rows {
		parent, err := workspaceProofPhysicalText(row, "source_event_id")
		if err != nil {
			return nil, nil, err
		}
		if parent != eventID {
			continue
		}
		name, err := workspaceProofPhysicalText(row, "event_name")
		if err != nil {
			return nil, nil, err
		}
		class, err := workspaceProofPhysicalText(row, "event_class")
		if err != nil {
			return nil, nil, err
		}
		id, err := workspaceProofPhysicalText(row, "event_id")
		if err != nil {
			return nil, nil, err
		}
		if all[id] {
			return nil, nil, fmt.Errorf("physical snapshot repeated event %s", id)
		}
		all[id] = true
		if class != string(events.EventAdmissionDiagnosticDirect) || name != string(events.EventTypePlatformRuntimeLog) {
			business[id] = true
		}
	}
	return all, business, nil
}

func requireWorkspaceProofNoBusinessEmissions(t *testing.T, reader runtimebus.EventStore, runID, eventID string) {
	t.Helper()
	snapshot := readWorkspaceProofSourceDomain(t, reader, runID)
	_, ids, err := workspaceProofChildEventIDs(snapshot["events"], eventID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("receiver emitted business work=%d err=%v", len(ids), err)
	}
}

func requireWorkspaceProofBusinessMutation(t *testing.T, endpoint string, reader runtimebus.EventStore, owner deliverylifecycle.Store, runID, eventID, entityID string) {
	t.Helper()
	rows, err := storetest.ReadReceiverDeliveryStorage(context.Background(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	var exact []deliverylifecycle.Snapshot
	for _, row := range rows {
		if row.EventID != eventID {
			continue
		}
		snapshot, err := owner.Snapshot(context.Background(), row.DeliveryID)
		if err != nil || snapshot.RunID != runID || snapshot.EventID != eventID || string(snapshot.Status) != row.Status {
			t.Fatalf("business delivery owner disagrees with physical row: %+v %+v %v", snapshot, row, err)
		}
		if node, ok := snapshot.Route.Recipient.Node(); ok && node.FlowPath() == "consumer" && node.NodeID() == "collector" {
			var target events.DeliveryTargetOwnership
			if err := json.Unmarshal([]byte(row.Target), &target); err != nil {
				t.Fatal(err)
			}
			if target.Code() != snapshot.Route.Target.Code() || target.Route() != snapshot.Route.Target.Route() {
				t.Fatalf("physical business target disagrees with its route: %s %+v", row.Target, snapshot.Route)
			}
			exact = append(exact, snapshot)
		}
	}
	if len(exact) != 1 {
		t.Fatalf("business receiver deliveries=%d", len(exact))
	}
	snapshot := exact[0]
	handler, local, ok := snapshot.Route.ConnectClaim.NodeHandlerOwner()
	wantTarget := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}
	if !ok || handler.FlowPath() != "consumer" || handler.NodeID() != "collector" || local != "work.ready" || snapshot.Route.Target.Code() != "existing_entity" || snapshot.Route.Target.Route() != wantTarget {
		t.Fatalf("business receiver authority: %+v", snapshot.Route)
	}
	outcomes, err := owner.Outcomes(context.Background(), snapshot.DeliveryID)
	if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered || snapshot.ClaimVersion != 1 || len(outcomes) != 1 || outcomes[0].Outcome != "delivered" || outcomes[0].ClaimVersion != 1 {
		t.Fatalf("business receiver settlement: snapshot=%+v outcomes=%+v err=%v", snapshot, outcomes, err)
	}
	var public operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
	if public.RunID != runID || len(public.Deliveries) != 1 || public.Deliveries[0].DeliveryID != snapshot.DeliveryID || public.Deliveries[0].Status != "delivered" || public.Deliveries[0].Target != (operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}) {
		t.Fatalf("public business receiver evidence: %+v", public)
	}
	physical := readWorkspaceProofSourceDomain(t, reader, runID)
	mutations, err := workspaceProofPhysicalRows(physical["entity_mutations"])
	if err != nil {
		t.Fatal(err)
	}
	count, exactCount := 0, 0
	for _, row := range mutations {
		text := func(column string) string {
			value, err := workspaceProofPhysicalText(row, column)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
		if text("caused_by_event") != eventID || text("domain") != "authored_field" || text("path") != "processed_token" {
			continue
		}
		count++
		if text("entity_id") != entityID {
			continue
		}
		exactCount++
		var oldValue, newValue string
		if err := json.Unmarshal([]byte(text("old_value")), &oldValue); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(text("new_value")), &newValue); err != nil {
			t.Fatal(err)
		}
		if oldValue != "seeded" || newValue != "receiver-proof" || text("writer_type") != "platform" || text("writer_id") != "workflow_engine" || text("handler_step") != "mutate" {
			t.Fatalf("receiver business mutation changed: %+v", row)
		}
	}
	if count != 1 || exactCount != 1 {
		t.Fatalf("business mutation census=%d exact=%d", count, exactCount)
	}
	children, business, err := workspaceProofChildEventIDs(physical["events"], eventID)
	if err != nil || len(business) != 0 {
		t.Fatalf("business effect emission census=%d err=%v", len(business), err)
	}
	deliveries, err := workspaceProofPhysicalRows(physical["event_deliveries"])
	if err != nil {
		t.Fatal(err)
	}
	extra := 0
	for _, row := range deliveries {
		child, err := workspaceProofPhysicalText(row, "event_id")
		if err != nil {
			t.Fatal(err)
		}
		if children[child] {
			extra++
		}
	}
	if extra != 0 {
		t.Fatalf("effect-only consumer produced executable downstream work: deliveries=%d", extra)
	}
}

func waitWorkspaceProofPipelineReceipt(t *testing.T, reader runtimebus.EventStore, runID, eventID, outcome string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, context.Background(), reader, runID, eventID)
		if evidence.PipelineReceiptCount == 1 && evidence.PipelineReceiptOutcome == outcome {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("exact pipeline receipt did not settle: count=%d outcome=%s want=%s", evidence.PipelineReceiptCount, evidence.PipelineReceiptOutcome, outcome)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
