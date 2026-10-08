package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type telegramPhraseBotLLMRuntime struct {
	onContinue func(context.Context, *runtimellm.Session, runtimellm.Message) error
}

func (telegramPhraseBotLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

func (telegramPhraseBotLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.Plan{}
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID: uuid.NewString(), AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...), Memory: memory, MemoryIdentity: execution.Identity,
	}, nil
}

func (telegramPhraseBotLLMRuntime) PrepareManagedSession(context.Context, *runtimellm.Session) error {
	return nil
}

func (r telegramPhraseBotLLMRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, managedCall runtimellm.ManagedCall) (*runtimellm.Response, error) {
	message, err := managedCall.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if r.onContinue != nil {
		if err := r.onContinue(ctx, session, message); err != nil {
			return nil, err
		}
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("phrase-bot requires managed capability surface")
	}
	observed, err := runtimellm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	if message.Role == "tool" {
		return &runtimellm.Response{
			Message:   runtimellm.Message{Role: "assistant", Content: "Telegram reply sent."},
			SessionID: session.ID, CapabilitySurface: &observed,
		}, nil
	}
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(managedCall.Frame().Turn.Event.Payload)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode phrase-bot event payload: %w", err)
	}
	chatID := strings.TrimSpace(fmt.Sprint(payload["conversation_reference"]))
	messageText := strings.TrimSpace(fmt.Sprint(payload["text"]))
	if chatID == "" || messageText == "" {
		return nil, fmt.Errorf("phrase-bot requires conversation_reference and text")
	}
	call := runtimellm.ToolCall{
		ID:   "reply-" + chatID,
		Name: "emit_telegram_reply_requested",
		Arguments: map[string]any{
			"chat_id": chatID,
			"text":    "Swarm heard: " + messageText,
		},
	}
	toolOutputAuthority := runtimellm.ToolOutputAuthority{
		ProviderOperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("standing-phrase-bot-provider-operation:"+managedCall.Frame().FrameID)).String(),
		SettledAt:           time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
	}
	return &runtimellm.Response{
		Message:             runtimellm.Message{Role: "assistant", ToolCalls: []runtimellm.ToolCall{call}},
		ToolCalls:           []runtimellm.ToolCall{call},
		SessionID:           session.ID,
		CapabilitySurface:   &observed,
		ToolOutputAuthority: &toolOutputAuthority,
	}, nil
}

func (telegramPhraseBotLLMRuntime) ContinueForkChatSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ForkChatCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func (telegramPhraseBotLLMRuntime) PersistConversationSnapshot(context.Context, *sessions.Lease, *runtimellm.Session) error {
	return nil
}
func TestServedParityHarnessStandingServiceLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioStandingServiceSuspendLifecycle),
		servedparity.MustScenario(servedparity.ScenarioStandingServiceResumeLifecycle),
		servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedStandingServiceLifecycleBackendProof)
}

func runServedStandingServiceLifecycleBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	managerProbe := &servedManagerScheduleProjectionProbe{}
	telegramGate := &servedTelegramDeliveryGate{}
	telegramCalls := make(chan struct{}, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		telegramGate.awaitRelease()
		telegramCalls <- struct{}{}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(telegram.Close)
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	configureStandingLifecycleCredentials(t)

	var db *sql.DB
	var opts cliapp.ServeOptions
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "standing-lifecycle.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = selectedStoreDatabaseForTest(t, stores)
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		opts = cliapp.ServeOptions{
			ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
			SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
			StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{onContinue: managerProbe.observe},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	case servedparity.BackendExplicitPostgres:
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		oldBuildStores := buildStoresForServe
		oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return nil, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			db = storetest.DatabaseForTest(pg)
			return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(pg), cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		opts = cliapp.ServeOptions{
			ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: sourceRoot,
			PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{onContinue: managerProbe.observe},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	default:
		t.Fatalf("unknown standing served parity backend %q", backend)
	}
	debugBackend := "postgres"
	if backend == servedparity.BackendDefaultSQLite {
		debugBackend = "sqlite"
	}
	contextsReady := make(chan *runtimepkg.RuntimeContextManager, 2)
	opts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) {
		contextsReady <- manager
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstManager := waitForServedStandingContextManager(t, contextsReady, backend)
	if db == nil {
		t.Fatal("standing served parity SQLDB is required")
	}
	firstEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
	serviceID, firstRunID, firstGeneration := loadServedStandingOwner(t, db, string(backend))
	firstScheduleResult := managerProbe.arm(t, firstManager, firstRunID)
	firstRouteRelease, firstRouteStarted := telegramGate.blockNext()
	if binding := sendStandingTelegramUpdate(t, strings.TrimSuffix(firstEndpoint, "/v1/rpc"), 9000, 42); binding.ServiceID != serviceID || binding.RunID != firstRunID || binding.Generation != firstGeneration {
		t.Fatalf("%s initial standing service returned wrong binding: %+v", backend, binding)
	}
	firstSchedules := waitForServedManagerScheduleProjection(t, firstScheduleResult, backend, "suspend", func() string {
		return first.outputString() + "\n" + servedEventPublishDebugSummary(t, db, debugBackend, firstRunID)
	})
	telegramGate.waitForStart(t, firstRouteStarted, backend, "suspend")
	suspendKey := "standing-suspend-" + string(backend)
	suspendOutcome := startServedStandingOperation(firstEndpoint, "standing.suspend", serviceID, suspendKey)
	assertServedStandingOperationWaitsForRoute(t, suspendOutcome, firstRouteRelease, backend, "suspend")
	requireStandingLifecycleTelegramCall(t, telegramCalls, backend, "suspend route release")
	suspended := waitForServedStandingOperation(t, suspendOutcome, backend, "standing.suspend")
	if suspended.EffectiveState != "suspended" || suspended.Transition != "suspended" || suspended.RunID != firstRunID {
		t.Fatalf("%s suspend result = %#v", backend, suspended)
	}
	replayedSuspend := invokeServedStandingOperation(t, firstEndpoint, "standing.suspend", serviceID, suspendKey)
	if replayedSuspend != suspended {
		t.Fatalf("%s suspend replay = %#v, want %#v", backend, replayedSuspend, suspended)
	}
	assertServedStandingSchedulesRetired(t, firstSchedules, backend, "suspend")
	requireStandingTelegramUnavailable(t, strings.TrimSuffix(firstEndpoint, "/v1/rpc"), 9001)
	assertServedStandingState(t, db, string(backend), serviceID, firstRunID, firstGeneration, "suspended", "paused")
	if code := first.stop(); code != 0 {
		t.Fatalf("%s first standing serve exit = %d", backend, code)
	}

	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondManager := waitForServedStandingContextManager(t, contextsReady, backend)
	secondOutput := second.outputString()
	if !strings.Contains(secondOutput, "suspended") || !strings.Contains(secondOutput, "swarm standing resume "+serviceID) {
		t.Fatalf("%s restart readiness omitted suspended standing story:\n%s", backend, secondOutput)
	}
	secondEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, secondOutput) + "/v1/rpc"
	resumed := invokeServedStandingOperation(t, secondEndpoint, "standing.resume", serviceID, "standing-resume-"+string(backend))
	if resumed.EffectiveState != "active" || resumed.Transition != "operator_resumed" || resumed.RunID != firstRunID {
		t.Fatalf("%s resume result = %#v", backend, resumed)
	}
	resumedScheduleResult := managerProbe.arm(t, secondManager, firstRunID)
	resetRouteRelease, resetRouteStarted := telegramGate.blockNext()
	if binding := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9002, 84); binding.ServiceID != serviceID || binding.RunID != firstRunID || binding.Generation != firstGeneration {
		t.Fatalf("%s resumed standing service returned wrong binding: %+v", backend, binding)
	}
	resumedSchedules := waitForServedManagerScheduleProjection(t, resumedScheduleResult, backend, "reset", func() string {
		return second.outputString() + "\n" + servedEventPublishDebugSummary(t, db, debugBackend, firstRunID)
	})
	telegramGate.waitForStart(t, resetRouteStarted, backend, "reset")
	assertServedStandingState(t, db, string(backend), serviceID, firstRunID, firstGeneration, "active", "running")

	resetKey := "standing-reset-" + string(backend)
	resetOutcome := startServedStandingOperation(secondEndpoint, "standing.reset", serviceID, resetKey)
	assertServedStandingOperationWaitsForRoute(t, resetOutcome, resetRouteRelease, backend, "reset")
	requireStandingLifecycleTelegramCall(t, telegramCalls, backend, "reset route release")
	reset := waitForServedStandingOperation(t, resetOutcome, backend, "standing.reset")
	if reset.EffectiveState != "active" || reset.Transition != "reset" || reset.Generation != firstGeneration+1 || reset.RunID == firstRunID {
		t.Fatalf("%s reset result = %#v", backend, reset)
	}
	if binding := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9003, 126); binding.ServiceID != serviceID || binding.RunID != reset.RunID || binding.Generation != reset.Generation {
		t.Fatalf("%s reset standing service returned wrong binding: %+v", backend, binding)
	}
	requireStandingLifecycleTelegramCall(t, telegramCalls, backend, "reset")
	replayedReset := invokeServedStandingOperation(t, secondEndpoint, "standing.reset", serviceID, resetKey)
	if replayedReset != reset {
		t.Fatalf("%s reset replay = %#v, want %#v", backend, replayedReset, reset)
	}
	assertServedStandingSchedulesRetired(t, resumedSchedules, backend, "reset")
	freshUse, freshLookup, err := secondManager.AcquireIngress(context.Background(), "chat", "telegram")
	if err != nil {
		t.Fatalf("%s acquire reset successor ingress: %v", backend, err)
	}
	if freshUse == nil || !freshLookup.Found {
		t.Fatalf("%s reset successor ingress is unavailable: %#v", backend, freshLookup)
	}
	freshOwner, ok := worklifetime.OccurrenceFromContext(freshUse.WorkContext())
	if !ok || freshOwner == nil || freshOwner == resumedSchedules.occurrence {
		_ = freshUse.Done()
		t.Fatalf("%s reset did not publish a fresh standing process occurrence", backend)
	}
	if err := freshUse.Done(); err != nil {
		t.Fatalf("%s settle reset successor ingress: %v", backend, err)
	}
	assertServedStandingState(t, db, string(backend), serviceID, reset.RunID, reset.Generation, "active", "running")
	debugRun := func(runID string) func() string {
		return func() string { return servedEventPublishDebugSummary(t, db, debugBackend, runID) }
	}
	requireServedParitySettlementPostconditionsWithDebug(t, secondEndpoint, db, string(backend), firstRunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle), debugRun(firstRunID))
	requireServedParitySettlementPostconditionsWithDebug(t, secondEndpoint, db, string(backend), reset.RunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle), debugRun(reset.RunID))
	assertServedStandingResetAndShutdownJoin(t, backend, second, secondManager, secondEndpoint, db, reset, freshOwner)
}

func assertServedStandingResetAndShutdownJoin(t *testing.T, backend servedparity.Backend, process *serveRuntimeTestProcess, manager *runtimepkg.RuntimeContextManager, endpoint string, db *sql.DB, current servedStandingOperationResult, predecessor worklifetime.Occurrence) {
	t.Helper()
	origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(current.ServiceID, current.Generation)
	if err != nil {
		t.Fatal(err)
	}
	held, err := manager.BeginStandingRunRecovery(context.Background(), current.RunID, origin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Done() })
	suspend := startServedStandingOperation(endpoint, "standing.suspend", current.ServiceID, "suspend-before-process-reset-"+string(backend))
	waitForServedStandingFence(t, predecessor)
	resetDone := make(chan servedJSONRPCEnvelope, 1)
	go func() {
		resetDone <- requestServedJSONRPCWithTimeout(t, endpoint, "runtime.nuke", map[string]any{
			"include_source_artifacts": false, "idempotency_key": "process-reset-after-standing-" + string(backend),
		}, 15*time.Second)
	}()
	select {
	case outcome := <-suspend:
		t.Fatalf("standing mutation passed held child: %+v", outcome)
	case outcome := <-resetDone:
		t.Fatalf("process reset passed unsettled standing mutation: %+v", outcome)
	default:
	}
	assertServedStandingState(t, db, string(backend), current.ServiceID, current.RunID, current.Generation, "active", "running")
	if err := held.Done(); err != nil {
		t.Fatal(err)
	}
	suspended := waitForServedStandingOperation(t, suspend, backend, "standing.suspend before process reset")
	if suspended.RunID != current.RunID || suspended.Generation != current.Generation || suspended.EffectiveState != "suspended" {
		t.Fatalf("standing mutation lost exact identity across process reset: %+v", suspended)
	}
	select {
	case outcome := <-resetDone:
		if outcome.Error != nil {
			t.Fatalf("process reset after standing join: %+v", outcome.Error)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("process reset failed to settle after exact standing child joined")
	}
	serviceID, runID, generation := loadServedStandingOwner(t, db, string(backend))
	if serviceID != current.ServiceID || generation != 1 {
		t.Fatalf("fresh epoch did not reconstruct the declared generation: %s/%s/%d", serviceID, runID, generation)
	}
	assertServedStandingState(t, db, string(backend), serviceID, runID, generation, "active", "running")
	use, lookup, err := manager.AcquireIngress(context.Background(), "chat", "telegram")
	if err != nil || use == nil || !lookup.Found {
		t.Fatalf("reconstructed standing ingress unavailable: lookup=%+v err=%v", lookup, err)
	}
	owner, found := worklifetime.OccurrenceFromContext(use.WorkContext())
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	if !found || owner == nil || owner == predecessor {
		t.Fatal("process reset reused the retired standing child")
	}
	assertServedStandingInvalidChildCommands(t, backend, manager, endpoint, db, serviceID)
	resetAgain := requestServedJSONRPCWithTimeout(t, endpoint, "runtime.nuke", map[string]any{
		"include_source_artifacts": false, "idempotency_key": "process-reset-after-invalid-compositions-" + string(backend),
	}, 15*time.Second)
	if resetAgain.Error != nil {
		t.Fatalf("fresh reset after invalid composition proof: %+v", resetAgain.Error)
	}
	serviceID, runID, generation = loadServedStandingOwner(t, db, string(backend))
	use, lookup, err = manager.AcquireIngress(context.Background(), "chat", "telegram")
	if err != nil || use == nil || !lookup.Loaded() {
		t.Fatalf("second fresh reconstruction unavailable: lookup=%+v err=%v", lookup, err)
	}
	previousOwner := owner
	owner, found = worklifetime.OccurrenceFromContext(use.WorkContext())
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	if !found || owner == nil || owner == previousOwner {
		t.Fatal("invalid-composition cleanup reused the retired child")
	}
	origin, err = runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, generation)
	if err != nil {
		t.Fatal(err)
	}
	shutdownWork, err := manager.BeginStandingRunRecovery(context.Background(), runID, origin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdownWork.Done() })
	stopped := make(chan int, 1)
	go func() { stopped <- process.stop() }()
	waitForServedStandingFence(t, owner)
	select {
	case code := <-stopped:
		t.Fatalf("shutdown returned before exact standing work joined: exit=%d", code)
	default:
	}
	if err := shutdownWork.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-stopped:
		if code != 0 {
			t.Fatalf("%s standing shutdown exit = %d", backend, code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not complete after standing work joined")
	}
}

func assertServedStandingInvalidChildCommands(t *testing.T, backend servedparity.Backend, manager *runtimepkg.RuntimeContextManager, endpoint string, db *sql.DB, serviceID string) {
	t.Helper()
	use, target, err := manager.AcquireStandingService(context.Background(), serviceID)
	if err != nil || use == nil {
		t.Fatalf("select invalid-composition proof owner: %v", err)
	}
	defer func() {
		if err := use.Done(); err != nil {
			t.Error(err)
		}
	}()
	ctx, rt := use.WorkContext(), use.Runtime()
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(rt.Options.RuntimeInstanceID))
	expected, found, err := rt.Pipeline.LoadReconciledStandingService(ctx, runtimepipeline.StandingServiceCandidate{BindingEnabled: true,
		ServiceID: serviceID, FlowPath: target.FlowPath,
		Source: use.Context.SourceArtifactFact,
	})
	if err != nil || !found {
		t.Fatalf("capture invalid-composition predecessor: found=%t err=%v", found, err)
	}
	refuse := func(product, effective, state string) {
		t.Helper()
		for _, command := range []string{"suspend", "resume", "reset"} {
			result := requestServedJSONRPC(t, endpoint, "standing."+command, map[string]any{
				"service_id": serviceID, "idempotency_key": "invalid-child-" + product + "-" + command,
			})
			if result.Error == nil {
				t.Fatalf("public %s accepted %s child composition: %+v", command, product, result.Result)
			}
			assertServedStandingState(t, db, string(backend), serviceID, expected.RunID, expected.Generation, effective, state)
		}
	}
	transition, err := manager.BeginStandingServiceOperation(ctx, expected, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	refuse("fenced", "active", "running")
	if err := transition.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rt.InboundGateway.ReopenStandingServiceAdmission(serviceID); err != nil {
		t.Fatal(err)
	}
	suspended, err := rt.Pipeline.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Expected: &expected})
	if err != nil || suspended.CommittedMutation != runtimerunlifecycle.MutationApplied {
		t.Fatalf("create explicit durable suspended/live-child counterexample: result=%+v err=%v", suspended, err)
	}
	refuse("non-executable-live", "suspended", "paused")
	transition, err = manager.BeginStandingServiceTransition(ctx, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := rt.Pipeline.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Expected: &suspended})
	if err != nil || resumed.CommittedMutation != runtimerunlifecycle.MutationApplied {
		t.Fatalf("create explicit durable active/missing-child counterexample: result=%+v err=%v", resumed, err)
	}
	refuse("active-missing", "active", "running")
}

func waitForServedStandingFence(t *testing.T, owner worklifetime.Occurrence) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		lease, err := owner.Begin(context.Background())
		if errors.Is(err, worklifetime.ErrAdmissionFenced) || errors.Is(err, worklifetime.ErrRetired) {
			return
		}
		if err != nil {
			t.Fatalf("observe exact standing fence: %v", err)
		}
		if err := lease.Done(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-deadline:
			t.Fatal("standing operation did not fence exact child")
		case <-time.After(time.Millisecond):
		}
	}
}

type servedStandingScheduleProbe struct {
	scheduler  *runtimepipeline.Scheduler
	occurrence *worklifetime.StandingOccurrence
}

type servedManagerScheduleProjectionProbe struct {
	mu      sync.Mutex
	pending *servedManagerScheduleProjectionRequest
}

type servedManagerScheduleProjectionRequest struct {
	scheduler  *runtimepipeline.Scheduler
	runID      string
	standing   *worklifetime.StandingOccurrence
	completion chan servedManagerScheduleProjectionResult
}

type servedManagerScheduleProjectionResult struct {
	probe servedStandingScheduleProbe
	err   error
}

func (p *servedManagerScheduleProjectionProbe) arm(t testing.TB, manager *runtimepkg.RuntimeContextManager, runID string) <-chan servedManagerScheduleProjectionResult {
	t.Helper()
	use, lookup, err := manager.AcquireIngress(context.Background(), "chat", "telegram")
	if err != nil {
		t.Fatalf("acquire standing ingress for Manager schedule proof: %v", err)
	}
	if use == nil || !lookup.Found || use.Runtime() == nil || use.Runtime().Scheduler == nil {
		t.Fatalf("standing ingress Manager schedule owner is unavailable: %#v", lookup)
	}
	owner, ok := worklifetime.OccurrenceFromContext(use.WorkContext())
	if !ok {
		_ = use.Done()
		t.Fatal("standing ingress Manager schedule setup has no exact occurrence")
	}
	standing, ok := worklifetime.StandingProjection(owner)
	if !ok {
		_ = use.Done()
		t.Fatalf("standing ingress Manager schedule setup owner %T has no standing projection", owner)
	}
	request := &servedManagerScheduleProjectionRequest{
		scheduler:  use.Runtime().Scheduler,
		runID:      strings.TrimSpace(runID),
		standing:   standing,
		completion: make(chan servedManagerScheduleProjectionResult, 1),
	}
	if err := use.Done(); err != nil {
		t.Fatalf("settle standing ingress Manager schedule setup: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending != nil {
		t.Fatal("Manager schedule projection proof is already armed")
	}
	p.pending = request
	return request.completion
}

func (p *servedManagerScheduleProjectionProbe) observe(ctx context.Context, _ *runtimellm.Session, message runtimellm.Message) error {
	if message.Role == "tool" {
		return nil
	}
	p.mu.Lock()
	request := p.pending
	p.pending = nil
	p.mu.Unlock()
	if request == nil {
		return nil
	}
	fail := func(err error) error {
		request.completion <- servedManagerScheduleProjectionResult{err: err}
		return err
	}
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return fail(errors.New("Manager event execution has no exact occurrence"))
	}
	if _, ok := owner.(*worklifetime.ManagerWorkOccurrence); !ok {
		return fail(fmt.Errorf("Manager event execution owner = %T, want *worklifetime.ManagerWorkOccurrence", owner))
	}
	standing, ok := worklifetime.StandingProjection(owner)
	if !ok || standing != request.standing {
		return fail(fmt.Errorf("Manager event execution standing projection = %p/%t, want %p", standing, ok, request.standing))
	}
	for _, activationID := range []string{uuid.NewString(), uuid.NewString()} {
		wakeup, err := runtimegenericschedule.NewWakeup(activationID, time.Now().Add(time.Hour))
		if err != nil {
			return fail(fmt.Errorf("construct Manager-composed served standing wakeup: %w", err))
		}
		if err := request.scheduler.RegisterGenericScheduleWakeup(ctx, wakeup); err != nil {
			return fail(fmt.Errorf("register Manager-composed served standing wakeup: %w", err))
		}
	}
	result := servedManagerScheduleProjectionResult{probe: servedStandingScheduleProbe{scheduler: request.scheduler, occurrence: standing}}
	request.completion <- result
	return nil
}

func waitForServedManagerScheduleProjection(
	t testing.TB,
	result <-chan servedManagerScheduleProjectionResult,
	backend servedparity.Backend,
	operation string,
	debug func() string,
) servedStandingScheduleProbe {
	t.Helper()
	select {
	case completed := <-result:
		if completed.err != nil {
			t.Fatalf("%s Manager-composed schedule registration before %s: %v\n%s", backend, operation, completed.err, debug())
		}
		return completed.probe
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for Manager-composed schedule registration before %s\n%s", backend, operation, debug())
		return servedStandingScheduleProbe{}
	}
}

type servedTelegramDeliveryGate struct {
	mu      sync.Mutex
	pending chan struct{}
	started chan struct{}
}

func (g *servedTelegramDeliveryGate) blockNext() (chan struct{}, <-chan struct{}) {
	release := make(chan struct{})
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending != nil {
		panic("Telegram delivery gate is already armed")
	}
	g.pending = release
	g.started = make(chan struct{})
	return release, g.started
}

func (g *servedTelegramDeliveryGate) awaitRelease() {
	g.mu.Lock()
	release := g.pending
	started := g.started
	g.pending = nil
	g.started = nil
	g.mu.Unlock()
	if release == nil {
		return
	}
	close(started)
	<-release
}

func (g *servedTelegramDeliveryGate) waitForStart(t testing.TB, started <-chan struct{}, backend servedparity.Backend, operation string) {
	t.Helper()
	if started == nil {
		t.Fatalf("%s Telegram delivery gate was not armed before %s", backend, operation)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for routed Telegram descendant before %s", backend, operation)
	}
}

func waitForServedStandingContextManager(t testing.TB, ready <-chan *runtimepkg.RuntimeContextManager, backend servedparity.Backend) *runtimepkg.RuntimeContextManager {
	t.Helper()
	select {
	case manager := <-ready:
		if manager == nil {
			t.Fatalf("%s served standing context manager is nil", backend)
		}
		return manager
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for served standing context manager", backend)
		return nil
	}
}

func assertServedStandingSchedulesRetired(t testing.TB, probe servedStandingScheduleProbe, backend servedparity.Backend, operation string) {
	t.Helper()
	remaining, err := probe.scheduler.ParkOccurrence(context.Background(), probe.occurrence)
	if err != nil {
		t.Fatalf("%s inspect %s standing schedules: %v", backend, operation, err)
	}
	if remaining.Count() != 0 {
		t.Fatalf("%s %s left predecessor schedules reachable: %#v", backend, operation, remaining)
	}
}

type servedStandingOperationResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
	Transition     string `json:"transition"`
}

func invokeServedStandingOperation(t *testing.T, endpoint, method, serviceID, idempotencyKey string) servedStandingOperationResult {
	t.Helper()
	var result servedStandingOperationResult
	response := requestServedJSONRPCWithTimeout(t, endpoint, method, map[string]any{
		"service_id": serviceID, "reason": "served parity proof", "idempotency_key": idempotencyKey,
	}, 15*time.Second)
	if response.Error != nil {
		t.Fatalf("%s error = %#v", method, response.Error)
	}
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode %s result: %v\n%s", method, err, string(response.Result))
	}
	return result
}

type servedStandingOperationOutcome struct {
	result servedStandingOperationResult
	err    error
}

func startServedStandingOperation(endpoint, method, serviceID, idempotencyKey string) <-chan servedStandingOperationOutcome {
	completed := make(chan servedStandingOperationOutcome, 1)
	go func() {
		result, err := requestServedStandingOperation(endpoint, method, serviceID, idempotencyKey)
		completed <- servedStandingOperationOutcome{result: result, err: err}
	}()
	return completed
}

func requestServedStandingOperation(endpoint, method, serviceID, idempotencyKey string) (servedStandingOperationResult, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": method + "-ownership-proof", "method": method,
		"params": map[string]any{
			"service_id": serviceID, "reason": "served parity proof", "idempotency_key": idempotencyKey,
		},
	})
	if err != nil {
		return servedStandingOperationResult{}, fmt.Errorf("marshal %s request: %w", method, err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return servedStandingOperationResult{}, fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return servedStandingOperationResult{}, fmt.Errorf("post %s request: %w", method, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return servedStandingOperationResult{}, fmt.Errorf("%s HTTP status = %d, want 200", method, response.StatusCode)
	}
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return servedStandingOperationResult{}, fmt.Errorf("decode %s envelope: %w", method, err)
	}
	if envelope.Error != nil {
		return servedStandingOperationResult{}, fmt.Errorf("%s error = %#v", method, envelope.Error)
	}
	var result servedStandingOperationResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		return servedStandingOperationResult{}, fmt.Errorf("decode %s result: %w", method, err)
	}
	return result, nil
}

func assertServedStandingOperationWaitsForRoute(t testing.TB, outcome <-chan servedStandingOperationOutcome, release chan struct{}, backend servedparity.Backend, operation string) {
	t.Helper()
	select {
	case completed := <-outcome:
		close(release)
		t.Fatalf("%s %s completed before its routed Manager descendant settled: result=%#v err=%v", backend, operation, completed.result, completed.err)
	case <-time.After(100 * time.Millisecond):
		close(release)
	}
}

func waitForServedStandingOperation(t testing.TB, outcome <-chan servedStandingOperationOutcome, backend servedparity.Backend, method string) servedStandingOperationResult {
	t.Helper()
	select {
	case completed := <-outcome:
		if completed.err != nil {
			t.Fatalf("%s %s: %v", backend, method, completed.err)
		}
		return completed.result
	case <-time.After(15 * time.Second):
		t.Fatalf("%s timed out waiting for %s after routed descendant settlement", backend, method)
		return servedStandingOperationResult{}
	}
}

func configureStandingLifecycleCredentials(t *testing.T) {
	t.Helper()
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentials, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentials.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
}

func loadServedStandingOwner(t *testing.T, db *sql.DB, backend string) (serviceID, runID string, generation int64) {
	t.Helper()
	query := `SELECT service_id, current_run_id, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	if backend == "postgres" || backend == string(servedparity.BackendExplicitPostgres) {
		query = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	}
	if err := db.QueryRowContext(context.Background(), query).Scan(&serviceID, &runID, &generation); err != nil {
		t.Fatalf("%s load standing owner: %v", backend, err)
	}
	return serviceID, runID, generation
}

func assertServedStandingState(t *testing.T, db *sql.DB, backend, serviceID, runID string, generation int64, effectiveState, runStatus string) {
	t.Helper()
	query := `
		SELECT ss.current_run_id, ss.current_generation, ss.effective_state, r.status
		FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id
		WHERE ss.service_id = ?`
	args := []any{serviceID}
	if backend == "postgres" || backend == string(servedparity.BackendExplicitPostgres) {
		query = `
			SELECT ss.current_run_id::text, ss.current_generation, ss.effective_state, r.status
			FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id
			WHERE ss.service_id = $1::uuid`
	}
	var gotRunID, gotState, gotRunStatus string
	var gotGeneration int64
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&gotRunID, &gotGeneration, &gotState, &gotRunStatus); err != nil {
		t.Fatalf("%s load standing state: %v", backend, err)
	}
	if gotRunID != runID || gotGeneration != generation || gotState != effectiveState || gotRunStatus != runStatus {
		t.Fatalf("%s standing state = run:%s generation:%d state:%s run_status:%s", backend, gotRunID, gotGeneration, gotState, gotRunStatus)
	}
}

func TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	calls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode Telegram call: %v", err)
		}
		calls <- body
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer telegram.Close()

	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	dataRoot := t.TempDir()
	sqlitePath := filepath.Join(dataRoot, "standing.sqlite")
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
	opts := cliapp.ServeOptions{
		ConfigPath: configPath, SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck:      true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstBinding := sendStandingTelegramUpdate(t, firstURL, 101, 42)
	secondBinding := sendStandingTelegramUpdate(t, firstURL, 102, 42)
	if firstBinding != secondBinding {
		t.Fatalf("delivery bindings = %+v and %+v, want one standing generation", firstBinding, secondBinding)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 42, 42)
	waitForStandingDeliveryQuiescence(t, sqlitePath)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	firstOutput := first.outputString()
	for _, want := range []string{
		"swarm serve · ",
		"store                      sqlite · " + sqlitePath,
		"workspace                  host · agent work runs on this machine",
		"listeners                  api 127.0.0.1:",
		"ready in ",
		"telegram webhook",
		"webhook_signing.telegram bound",
		"shutdown · complete",
	} {
		if !strings.Contains(firstOutput, want) {
			t.Fatalf("concise supported serve output missing %q:\n%s", want, firstOutput)
		}
	}
	if strings.Contains(firstOutput, "[1/22]") || strings.Contains(firstOutput, "telegram-secret") || strings.Contains(firstOutput, "\x1b[") {
		t.Fatalf("concise supported serve output leaked verbose, secret, or terminal decoration:\n%s", firstOutput)
	}
	if strings.Count(firstOutput, "workspace                  host · agent work runs on this machine") != 1 || strings.Count(firstOutput, "ready in ") != 1 {
		t.Fatalf("concise supported serve output retained parallel lifecycle writers:\n%s", firstOutput)
	}
	for _, forbidden := range []string{"request_authentication=", "catalog_generation=", "manifest_hash=", "policy_source=", "provenance=", "source_path=", "standing ingress admitted:"} {
		if strings.Contains(firstOutput, forbidden) {
			t.Fatalf("concise supported serve output leaked diagnostic field %q:\n%s", forbidden, firstOutput)
		}
	}

	enableServeRuntimeRecovery(t, configPath)
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	restartedBinding := sendStandingTelegramUpdate(t, secondURL, 103, 84)
	if restartedBinding != firstBinding {
		t.Fatalf("restart binding = %+v, want %+v", restartedBinding, firstBinding)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 84)
	waitForStandingDeliveryQuiescence(t, sqlitePath)
	requireStandingDeclarationPublicationReadback(t, secondURL, firstBinding, []string{"101", "102", "103"})
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}

	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite runtime store: %v", err)
	}
	defer func() {
		if sqliteStore != nil {
			_ = sqliteStore.Close()
		}
	}()
	var runs, instances, entities int
	var standingRunID string
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT current_run_id
		FROM standing_services
		WHERE flow_path = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil || standingRunID != firstBinding.RunID {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	_, receiver := requireSingleReceiverTargetState(t, requireReceiverProofStateReader(t, sqliteStore), standingRunID, "telegram-ingress")
	standingEntityID := receiver.EntityID
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_path = 'telegram-ingress'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-ingress'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`, standingEntityID).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND json_extract(anchor, '$.entity_id') = ? AND status = 'pending' AND json_extract(snapshot, '$.decision') = 'retire_service'`, standingEntityID).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close SQLite inspection store before restart matrix: %v", err)
	}
	sqliteStore = nil
	disableServeRuntimeRecovery(t, opts.ConfigPath)
	requireChangedStandingColdStartMatrix(t, opts, sourceRoot, standingRunID, nil)
}

func TestServeAuthorActivityAttachmentFailureKeepsRuntimeHealthy(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()

	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	dataRoot := t.TempDir()
	sqlitePath := filepath.Join(dataRoot, "standing.sqlite")
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Dev: true, LocalRun: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
		TestAfterAuthorActivityHead: func() error { return errors.New("author activity head unavailable") },
	})
	process.waitForReadyLine()
	output := process.outputString()
	for _, want := range []string{"author activity head unavailable", "inspect with swarm logs --follow"} {
		if !strings.Contains(output, want) {
			t.Fatalf("attachment failure output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "ready — waiting for events") {
		t.Fatalf("failed attachment claimed to be waiting for events:\n%s", output)
	}
	response, err := http.Get("http://" + serveRuntimeAPIListenerFromOutput(t, output) + "/healthz")
	if err != nil {
		t.Fatalf("healthy runtime after feed attachment failure: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status after feed attachment failure = %d", response.StatusCode)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exit after feed attachment failure = %d\n%s", code, process.outputString())
	}
}

func TestStandingIngressUnsupportedAliasFailsBeforeServeReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	sourceRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
	schemaPath := filepath.Join(sourceRoot, "telegram-ingress", "schema.yaml")
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read ingress schema: %v", err)
	}
	writeStandingCandidateFile(t, schemaPath, strings.Replace(string(raw), "alias: chat", "alias: chat/support", 1))
	sqlitePath := filepath.Join(t.TempDir(), "invalid-alias.sqlite")
	process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Dev: true, LocalRun: true, Verbose: true,
	})
	code, exited := process.waitForExit(15 * time.Second)
	if !exited {
		process.cleanup()
		t.Fatal("serve reached a live process with an unreachable standing ingress alias")
	}
	process.recordStopped(code)
	output := process.outputString()
	if code == 0 || !strings.Contains(output, "one URL-safe path segment") || strings.Contains(output, "swarm runtime ready") {
		t.Fatalf("invalid alias exit/output = %d\n%s", code, output)
	}
}

func TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	oldBuildStores := buildStoresForServe
	oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		storetest.BootstrapPostgresRuntimeStore(t, runtimePG)
		return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(runtimePG), cfg), nil
	}
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
	})

	calls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		calls <- body
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
	opts := cliapp.ServeOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "postgres", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Verbose: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	binding := sendStandingTelegramUpdate(t, baseURL, 201, 42)
	if got := sendStandingTelegramUpdate(t, baseURL, 202, 42); got != binding {
		t.Fatalf("second binding = %+v, want %+v", got, binding)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 42, 42)
	waitForStandingDeliveryQuiescence(t, "postgres:"+dsn)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore: %v", err)
	}
	enableServeRuntimeRecovery(t, opts.ConfigPath)
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	if got := sendStandingTelegramUpdate(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), 203, 84); got != binding {
		t.Fatalf("restart binding = %+v, want %+v", got, binding)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 84)
	waitForStandingDeliveryQuiescence(t, "postgres:"+dsn)
	requireStandingDeclarationPublicationReadback(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), binding, []string{"201", "202", "203"})
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open Postgres: %v", err)
	}
	defer db.Close()
	var runs, instances, entities int
	var standingRunID string
	if err := db.QueryRow(`
		SELECT current_run_id::text
		FROM standing_services
		WHERE flow_path = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil || standingRunID != binding.RunID {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	observer, err := storetest.OpenReleaseProcessReadOnlyInspection("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, receiver := requireSingleReceiverTargetState(t, requireReceiverProofStateReader(t, observer), standingRunID, "telegram-ingress")
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	entity := receiver.EntityID
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_path = 'telegram-ingress'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-ingress'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = $1::uuid`, entity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = $1::uuid THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND anchor->>'entity_id' = $1 AND status = 'pending' AND snapshot->>'decision' = 'retire_service'`, entity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	disableServeRuntimeRecovery(t, opts.ConfigPath)
	requireChangedStandingColdStartMatrix(t, opts, sourceRoot, standingRunID, func(t *testing.T) {
		var reopenErr error
		runtimePG, reopenErr = store.NewPostgresStore(dsn)
		if reopenErr != nil {
			t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", reopenErr)
		}
	})
}

func TestStandingRestartMixedHealthyAndTerminalProcessParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			calls := make(chan map[string]any, 4)
			telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode Telegram call: %v", err)
				}
				calls <- body
				w.Header().Set("content-type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
			}))
			t.Cleanup(telegram.Close)
			sourceRoot := writeMixedStandingTelegramServeFixture(t, telegram.URL)
			configureStandingLifecycleCredentials(t)

			var (
				db            *sql.DB
				postgresStore *store.PostgresStore
				sqliteStore   *store.SQLiteRuntimeStore
			)
			captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
				db, postgresStore, sqliteStore = selectedRuntimeStoreForTest(t, persistence)
			})
			runtimes := make(chan *runtimepkg.Runtime, 2)
			opts := cliapp.ServeOptions{
				SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SelfCheck: true, Verbose: true,
				TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
				TestRuntimeReadyHook: func(rt *runtimepkg.Runtime) { runtimes <- rt },
			}
			selectedStore := ""
			if backend == "sqlite" {
				sqlitePath := filepath.Join(t.TempDir(), "mixed-standing.sqlite")
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
				opts.StoreMode = "sqlite"
				selectedStore = sqlitePath
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				var opened *store.PostgresStore
				oldBuildStores := buildStoresForServe
				oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
				buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
					var err error
					opened, err = store.NewPostgresStore(dsn)
					if err != nil {
						return nil, err
					}
					storetest.BootstrapPostgresRuntimeStore(t, opened)
					return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(opened), cfg), nil
				}
				cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
					return serveRuntimeWorkspaceStub{}, nil
				}
				t.Cleanup(func() {
					buildStoresForServe = oldBuildStores
					cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
				})
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode = "postgres"
				opts.StoreModeSet = true
				selectedStore = "postgres:" + dsn
			}

			first := startServeRuntimeTestProcess(t, opts)
			first.waitForReadyLine()
			firstRuntime := waitForStandingRuntime(t, runtimes, backend, "first boot")
			if db == nil {
				t.Fatalf("%s mixed standing database was not captured", backend)
			}
			healthyService, healthyRun, healthyGeneration := loadServedStandingOwnerByFlow(t, db, backend, "telegram-ingress")
			terminalService, terminalRun, terminalGeneration := loadServedStandingOwnerByFlow(t, db, backend, "telegram-stopped")
			firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
			if binding := sendStandingTelegramUpdate(t, firstURL, 301, 42); binding.ServiceID != healthyService || binding.RunID != healthyRun || binding.Generation != healthyGeneration {
				t.Fatalf("%s healthy standing service returned wrong binding before restart: %+v", backend, binding)
			}
			requireStandingTelegramCalls(t, calls, selectedStore, 42)
			waitForStandingDeliveryQuiescence(t, selectedStore)
			terminalizeStandingRunFromRuntime(t, firstRuntime, postgresStore, sqliteStore, terminalRun)
			assertServedStandingState(t, db, backend, terminalService, terminalRun, terminalGeneration, "active", "cancelled")
			if code := first.stop(); code != 0 {
				t.Fatalf("%s first mixed standing serve exit = %d", backend, code)
			}

			enableServeRuntimeRecovery(t, opts.ConfigPath)
			second := startServeRuntimeTestProcess(t, opts)
			second.waitForReadyLine()
			_ = waitForStandingRuntime(t, runtimes, backend, "restart")
			secondOutput := second.outputString()
			for _, want := range []string{terminalService, "terminal_declared", "swarm standing reset " + terminalService} {
				if !strings.Contains(secondOutput, want) {
					t.Fatalf("%s mixed standing restart output missing %q:\n%s", backend, want, secondOutput)
				}
			}
			secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, secondOutput)
			if binding := sendStandingTelegramUpdate(t, secondURL, 302, 84); binding.ServiceID != healthyService || binding.RunID != healthyRun || binding.Generation != healthyGeneration {
				t.Fatalf("%s healthy standing service returned wrong binding after restart: %+v", backend, binding)
			}
			requireStandingTelegramCalls(t, calls, selectedStore, 84)
			waitForStandingDeliveryQuiescence(t, selectedStore)
			assertNoStandingTelegramCall(t, calls, backend)
			assertServedStandingState(t, db, backend, healthyService, healthyRun, healthyGeneration, "active", "running")
			assertServedStandingState(t, db, backend, terminalService, terminalRun, terminalGeneration, "active", "cancelled")
			if code := second.stop(); code != 0 {
				t.Fatalf("%s second mixed standing serve exit = %d", backend, code)
			}
		})
	}
}

func waitForStandingRuntime(t testing.TB, runtimes <-chan *runtimepkg.Runtime, backend, phase string) *runtimepkg.Runtime {
	t.Helper()
	select {
	case rt := <-runtimes:
		return rt
	case <-time.After(15 * time.Second):
		t.Fatalf("%s timed out waiting for runtime during %s", backend, phase)
		return nil
	}
}

func terminalizeStandingRunFromRuntime(t testing.TB, rt *runtimepkg.Runtime, postgresStore *store.PostgresStore, sqliteStore *store.SQLiteRuntimeStore, runID string) {
	t.Helper()
	if rt == nil || rt.WorkOccurrence() == nil {
		t.Fatal("standing terminalization requires the live runtime occurrence")
	}
	fact := rt.Options.SourceArtifactFact
	runtimeInstanceID := strings.TrimSpace(rt.Options.RuntimeInstanceID)
	if runtimeInstanceID == "" || fact.BundleHash() == "" {
		t.Fatalf("standing terminalization runtime identity = %q/%q", runtimeInstanceID, fact.BundleHash())
	}
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeInstanceID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash()))
	ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
	request := runtimerunlifecycle.TerminalRequest{
		RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
	}
	var err error
	if postgresStore != nil {
		_, _, err = postgresStore.MarkTerminalRun(ctx, request)
	} else if sqliteStore != nil {
		_, _, err = sqliteStore.MarkTerminalRun(ctx, request)
	} else {
		t.Fatal("standing terminalization requires a selected-store lifecycle owner")
	}
	if err != nil {
		t.Fatalf("terminalize standing run %s: %v", runID, err)
	}
}

func loadServedStandingOwnerByFlow(t testing.TB, db *sql.DB, backend, flowID string) (serviceID, runID string, generation int64) {
	t.Helper()
	query := `SELECT service_id, current_run_id, current_generation FROM standing_services WHERE flow_path = ?`
	args := []any{flowID}
	if backend == "postgres" {
		query = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services WHERE flow_path = $1`
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&serviceID, &runID, &generation); err != nil {
		t.Fatalf("%s load standing owner for flow %s: %v", backend, flowID, err)
	}
	return serviceID, runID, generation
}

func assertNoStandingTelegramCall(t testing.TB, calls <-chan map[string]any, backend string) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("%s emitted an unexpected duplicate Telegram call: %#v", backend, call)
	case <-time.After(250 * time.Millisecond):
	}
}

func enableServeRuntimeRecovery(t *testing.T, configPath string) {
	t.Helper()
	setServeRuntimeRecovery(t, configPath, false, true)
}

func disableServeRuntimeRecovery(t *testing.T, configPath string) {
	t.Helper()
	setServeRuntimeRecovery(t, configPath, true, false)
}

func setServeRuntimeRecovery(t *testing.T, configPath string, from, to bool) {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read serve runtime config: %v", err)
	}
	fromLine := fmt.Sprintf("recovery_on_startup: %t", from)
	toLine := fmt.Sprintf("recovery_on_startup: %t", to)
	if count := strings.Count(string(raw), fromLine); count != 1 {
		t.Fatalf("%s count = %d, want 1", fromLine, count)
	}
	updated := strings.Replace(string(raw), fromLine, toLine, 1)
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("set serve runtime recovery to %t: %v", to, err)
	}
}

func requireChangedStandingColdStartMatrix(t *testing.T, opts cliapp.ServeOptions, sourceRoot, originalRunID string, prepare func(*testing.T)) {
	t.Helper()
	manifestPath := filepath.Join(sourceRoot, "manifest.yaml")
	baseManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read standing manifest: %v", err)
	}
	rootSchemaPath := filepath.Join(sourceRoot, "schema.yaml")
	baseRootSchema, err := os.ReadFile(rootSchemaPath)
	if err != nil {
		t.Fatalf("read root connection schema: %v", err)
	}
	removeConnectedReceiver := func(t *testing.T) {
		t.Helper()
		// The removed chat flow owns the tool used by the root smoke scenario.
		// Retire its executable test declaration too, rather than leave a dangling double.
		for _, name := range []string{"telegram-chat", "tests"} {
			original := filepath.Join(sourceRoot, name)
			removed := filepath.Join(t.TempDir(), name)
			if err := os.Rename(original, removed); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Rename(removed, original); err != nil {
					t.Error(err)
				}
			})
		}
		var schema map[string]any
		if err := yaml.Unmarshal(baseRootSchema, &schema); err != nil {
			t.Fatal(err)
		}
		delete(schema, "connect")
		raw, err := yaml.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, rootSchemaPath, string(raw))
	}
	flowDir := filepath.Join(sourceRoot, "telegram-ingress")
	flowSchemaPath := filepath.Join(flowDir, "schema.yaml")
	baseFlowSchema, err := os.ReadFile(flowSchemaPath)
	if err != nil {
		t.Fatalf("read standing flow schema: %v", err)
	}
	type candidateMutation struct {
		name       string
		apply      func(*testing.T)
		wantOutput []string
	}
	mutations := []candidateMutation{
		{name: "source revision one", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.1"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision two", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.2"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision three", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.3"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "manifest name revised", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), "name: telegram-agent", "name: renamed-telegram-agent", 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "standing declaration removed", apply: func(t *testing.T) {
			removeConnectedReceiver(t)
			removedDir := filepath.Join(t.TempDir(), filepath.Base(flowDir))
			if err := os.Rename(flowDir, removedDir); err != nil {
				t.Fatalf("remove standing flow directory from the admitted tree: %v", err)
			}
			t.Cleanup(func() {
				_ = os.Rename(removedDir, flowDir)
			})
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "standing changed to non-standing", apply: func(t *testing.T) {
			removeConnectedReceiver(t)
			var schema map[string]any
			if err := yaml.Unmarshal(baseFlowSchema, &schema); err != nil {
				t.Fatal(err)
			}
			delete(schema, "pins")
			delete(schema, "activation")
			delete(schema, "ingress")
			raw, err := yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, flowSchemaPath, string(raw))
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "flow identity renamed", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, rootSchemaPath, strings.Replace(string(baseRootSchema), "from: telegram-ingress", "from: telegram-ingress-v2", 1))
			renamedDir := filepath.Join(sourceRoot, "telegram-ingress-v2")
			if err := os.Rename(flowDir, renamedDir); err != nil {
				t.Fatalf("rename flow directory: %v", err)
			}
			t.Cleanup(func() {
				_ = os.Rename(renamedDir, flowDir)
				_ = os.WriteFile(flowSchemaPath, baseFlowSchema, 0o600)
			})
			writeStandingCandidateFile(t, filepath.Join(renamedDir, "schema.yaml"), strings.Replace(string(baseFlowSchema), "name: telegram-ingress", "name: telegram-ingress-v2", 1))
		}, wantOutput: []string{" created run=", " orphaned declaration_removed=true"}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			writeStandingCandidateFile(t, rootSchemaPath, string(baseRootSchema))
			writeStandingCandidateFile(t, manifestPath, string(baseManifest))
			writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
			if _, err := os.Stat(flowDir); err != nil {
				t.Fatalf("standing flow directory unavailable before mutation: %v", err)
			}
			mutation.apply(t)
			if prepare != nil {
				prepare(t)
			}
			requireChangedStandingColdStartReconciled(t, opts, mutation.wantOutput...)
		})
	}
	writeStandingCandidateFile(t, rootSchemaPath, string(baseRootSchema))
	writeStandingCandidateFile(t, manifestPath, string(baseManifest))
	writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
}

func writeStandingCandidateFile(t testing.TB, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write standing candidate %s: %v", path, err)
	}
}

func requireChangedStandingColdStartReconciled(t *testing.T, opts cliapp.ServeOptions, wantOutput ...string) {
	t.Helper()
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	if code := process.stop(); code != 0 {
		t.Fatalf("changed standing bundle exit = %d\n%s", code, process.outputString())
	}
	output := process.outputString()
	if !strings.Contains(output, "shutdown · complete") {
		t.Fatalf("changed standing bundle did not release its selected-store authority:\n%s", output)
	}
	for _, want := range wantOutput {
		if !strings.Contains(output, want) {
			t.Fatalf("changed standing bundle output omitted %q:\n%s", want, output)
		}
	}
}

func requireStandingTelegramUnavailable(t testing.TB, baseURL string, updateID int) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello %d"}}`, updateID, updateID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new suspended webhook request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send suspended webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		var payload any
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		t.Fatalf("suspended webhook status=%d payload=%v, want %d", resp.StatusCode, payload, http.StatusServiceUnavailable)
	}
}

func requireStandingLifecycleTelegramCall(t testing.TB, calls <-chan struct{}, backend servedparity.Backend, phase string) {
	t.Helper()
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for standing Telegram side effect after %s", backend, phase)
	}
}

func sendStandingTelegramUpdate(t testing.TB, baseURL string, updateID, chatID int, diagnostics ...func() string) standingTelegramBinding {
	t.Helper()
	return sendStandingTelegramUpdatePublication(t, baseURL, updateID, chatID, diagnostics...).standingTelegramBinding
}

func standingWebhookDiagnostics(diagnostics []func() string) string {
	if len(diagnostics) == 0 || diagnostics[0] == nil {
		return ""
	}
	return "\nserve output:\n" + diagnostics[0]()
}

func requireStandingTelegramCalls(t testing.TB, calls <-chan map[string]any, sqlitePath string, chatIDs ...int) {
	t.Helper()
	for i, chatID := range chatIDs {
		select {
		case call := <-calls:
			if got := strings.TrimSpace(fmt.Sprint(call["chat_id"])); got != fmt.Sprint(chatID) {
				t.Fatalf("Telegram chat_id = %v, want %d", call["chat_id"], chatID)
			}
		case <-time.After(30 * time.Second):
			diagnostics := "unavailable"
			if strings.HasPrefix(sqlitePath, "postgres:") {
				diagnostics = standingPostgresDiagnostics(strings.TrimPrefix(sqlitePath, "postgres:"))
			} else if strings.TrimSpace(sqlitePath) != "" {
				diagnostics = standingSQLiteDiagnostics(sqlitePath)
			}
			t.Fatalf("timed out waiting for Telegram reply %d/%d; diagnostics: %s", i+1, len(chatIDs), diagnostics)
		}
	}
}

func waitForStandingDeliveryQuiescence(t testing.TB, selectedStore string) {
	t.Helper()
	driver, dsn := "sqlite", selectedStore
	if strings.HasPrefix(selectedStore, "postgres:") {
		driver, dsn = "postgres", strings.TrimPrefix(selectedStore, "postgres:")
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s delivery observation: %v", driver, err)
	}
	defer db.Close()

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var unfinished int
		err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE status IN ('pending', 'failed', 'in_progress')`).Scan(&unfinished)
		if err == nil && unfinished == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("wait for %s delivery quiescence: unfinished=%d err=%v", driver, unfinished, err)
		case <-ticker.C:
		}
	}
}

func standingPostgresDiagnostics(dsn string) string {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err.Error()
	}
	defer db.Close()
	rows, err := db.Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	parts := []string{}
	for rows.Next() {
		var name string
		var count int
		if rows.Scan(&name, &count) == nil {
			parts = append(parts, fmt.Sprintf("%s=%d", name, count))
		}
	}
	logRows, err := db.Query(`SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := db.Query(`SELECT event_id::text, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := db.Query(`SELECT event_id::text, status, COALESCE(failure::text, '') FROM pipeline_receipts ORDER BY updated_at`)
	if err == nil {
		defer receiptRows.Close()
		for receiptRows.Next() {
			var eventID, status, failure string
			if receiptRows.Scan(&eventID, &status, &failure) == nil {
				parts = append(parts, fmt.Sprintf("receipt:%s:%s:%s", eventID, status, failure))
			}
		}
	}
	return strings.Join(parts, ",")
}

func standingSQLiteDiagnostics(path string) string {
	selected, err := store.NewSQLiteRuntimeStore(path)
	if err != nil {
		return err.Error()
	}
	defer selected.Close()
	rows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	parts := []string{}
	for rows.Next() {
		var name string
		var count int
		if err := rows.Scan(&name, &count); err != nil {
			return err.Error()
		}
		parts = append(parts, fmt.Sprintf("%s=%d", name, count))
	}
	logRows, err := storetest.DatabaseForTest(selected).Query(`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_id, status, COALESCE(failure, '') FROM pipeline_receipts ORDER BY updated_at`)
	if err == nil {
		defer receiptRows.Close()
		for receiptRows.Next() {
			var eventID, status, failure string
			if receiptRows.Scan(&eventID, &status, &failure) == nil {
				parts = append(parts, fmt.Sprintf("receipt:%s:%s:%s", eventID, status, failure))
			}
		}
	}
	return strings.Join(parts, ",")
}

func writeStandingTelegramServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	redirectExternalHosts(t, map[string]string{"api.telegram.org": telegramBaseURL})
	return root
}

func writeMixedStandingTelegramServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	root := writeStandingTelegramServeFixture(t, telegramBaseURL)
	flowDir := filepath.Join(root, "telegram-stopped")
	if err := os.MkdirAll(flowDir, 0o755); err != nil {
		t.Fatalf("create terminal standing flow directory: %v", err)
	}
	baseSchema, err := os.ReadFile(filepath.Join(root, "telegram-ingress", "schema.yaml"))
	if err != nil {
		t.Fatalf("read healthy standing flow schema: %v", err)
	}
	stoppedSchema := strings.Replace(string(baseSchema), "name: telegram-ingress", "name: telegram-stopped", 1)
	stoppedSchema = canonicalrouting.WithoutStandingIngressPins(t, stoppedSchema)
	if ingress := strings.Index(stoppedSchema, "\ningress:\n"); ingress >= 0 {
		stoppedSchema = stoppedSchema[:ingress+1]
	}
	stoppedSchema += "schedules:\n  retained: {every: 24h, emit: service.tick}\n"
	writeStandingCandidateFile(t, filepath.Join(flowDir, "schema.yaml"), stoppedSchema)
	writeStandingCandidateFile(t, filepath.Join(flowDir, "events.yaml"), "service.tick:\n")
	writeStandingCandidateFile(t, filepath.Join(flowDir, "nodes.yaml"), "hold:\n  execution_type: system_node\n  subscribes_to: [service.tick]\n  event_handlers:\n    service.tick:\n      guard: {id: hold, check: true}\n")
	baseEntities, err := os.ReadFile(filepath.Join(root, "telegram-ingress", "entities.yaml"))
	if err != nil {
		t.Fatalf("read healthy standing flow entities: %v", err)
	}
	writeStandingCandidateFile(t, filepath.Join(flowDir, "entities.yaml"), string(baseEntities))
	return root
}
