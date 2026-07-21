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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type telegramPhraseBotLLMRuntime struct {
	onContinue func(context.Context, *runtimellm.Session, runtimellm.Message) error
}

func (telegramPhraseBotLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

func (telegramPhraseBotLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.PlatformDefault()
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID: uuid.NewString(), AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...), Memory: memory, MemoryIdentity: execution.Identity,
	}, nil
}

type authorActivityHeadFailureEventStore struct {
	runtimebus.EventStore
}

func (authorActivityHeadFailureEventStore) HeadAuthorActivity(context.Context) (int64, error) {
	return 0, errors.New("author activity head unavailable")
}

func (authorActivityHeadFailureEventStore) ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	return runtimeauthoractivity.ListResult{}, errors.New("author activity list must not run after head failure")
}

func (r telegramPhraseBotLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
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
	const payloadPrefix = "- payload: "
	start := strings.Index(message.Content, payloadPrefix)
	if start < 0 {
		return nil, fmt.Errorf("phrase-bot input has no event payload")
	}
	raw := message.Content[start+len(payloadPrefix):]
	if end := strings.IndexByte(raw, '\n'); end >= 0 {
		raw = raw[:end]
	}
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
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
	return &runtimellm.Response{
		Message:   runtimellm.Message{Role: "assistant", ToolCalls: []runtimellm.ToolCall{call}},
		ToolCalls: []runtimellm.ToolCall{call}, SessionID: session.ID, CapabilitySurface: &observed,
	}, nil
}

func (telegramPhraseBotLLMRuntime) PersistConversationSnapshot(context.Context, *runtimellm.Session) error {
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
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	configureStandingLifecycleCredentials(t)

	var db *sql.DB
	var opts cliapp.ServeOptions
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "standing-lifecycle.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		opts = cliapp.ServeOptions{
			ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
			ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
			StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{onContinue: managerProbe.observe},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	case servedparity.BackendExplicitPostgres:
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		oldBuildStores := buildStoresForServe
		oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return storeBundle{}, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			db = pg.DB
			return selectedPostgresStoreBundle(pg, cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		opts = cliapp.ServeOptions{
			ConfigPath: writeServeRuntimeTestConfig(t), ContractsPath: contractsRoot,
			PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{onContinue: managerProbe.observe},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	default:
		t.Fatalf("unknown standing served parity backend %q", backend)
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
	if entity := sendStandingTelegramUpdate(t, strings.TrimSuffix(firstEndpoint, "/v1/rpc"), 9000, 42); entity == "" {
		t.Fatalf("%s initial standing service returned empty entity", backend)
	}
	firstSchedules := waitForServedManagerScheduleProjection(t, firstScheduleResult, backend, "suspend")
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
	if entity := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9002, 84); entity == "" {
		t.Fatalf("%s resumed standing service returned empty entity", backend)
	}
	resumedSchedules := waitForServedManagerScheduleProjection(t, resumedScheduleResult, backend, "reset")
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
	if entity := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9003, 126); entity == "" {
		t.Fatalf("%s reset standing service returned empty entity", backend)
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
	requireServedParitySettlementPostconditions(t, secondEndpoint, db, string(backend), firstRunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle))
	requireServedParitySettlementPostconditions(t, secondEndpoint, db, string(backend), reset.RunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle))
	if code := second.stop(); code != 0 {
		t.Fatalf("%s second standing serve exit = %d", backend, code)
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
	for _, schedule := range []runtimepipeline.Schedule{
		{RunID: request.runID, AgentID: "standing-manager-proof", EventType: "standing.manager.proof.once", Mode: "once", At: time.Now().Add(time.Hour), TaskID: uuid.NewString()},
		{RunID: request.runID, AgentID: "standing-manager-proof", EventType: "standing.manager.proof.cron", Mode: "cron", Cron: "@every 1h", TaskID: uuid.NewString()},
	} {
		if err := request.scheduler.Register(ctx, schedule); err != nil {
			return fail(fmt.Errorf("register Manager-composed served standing %s schedule: %w", schedule.Mode, err))
		}
	}
	result := servedManagerScheduleProjectionResult{probe: servedStandingScheduleProbe{scheduler: request.scheduler, occurrence: standing}}
	request.completion <- result
	return nil
}

func waitForServedManagerScheduleProjection(t testing.TB, result <-chan servedManagerScheduleProjectionResult, backend servedparity.Backend, operation string) servedStandingScheduleProbe {
	t.Helper()
	select {
	case completed := <-result:
		if completed.err != nil {
			t.Fatalf("%s Manager-composed schedule registration before %s: %v", backend, operation, completed.err)
		}
		return completed.probe
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for Manager-composed schedule registration before %s", backend, operation)
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
	if len(remaining) != 0 {
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
	if backend == string(servedparity.BackendExplicitPostgres) {
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
	if backend == string(servedparity.BackendExplicitPostgres) {
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

	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
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
		ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	var readyRuntime *runtimepkg.Runtime
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { readyRuntime = rt }
	opts.TestAfterAuthorActivityHead = func() error {
		if readyRuntime == nil {
			return fmt.Errorf("runtime ready hook did not run before author activity head")
		}
		return commitReadinessHandoffAuthorActivity(sqlitePath, readyRuntime)
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	waitForStandingStoryOutput(t, first, "ready — waiting for events", "handoff → message received (chat readiness) \"across head\"")
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstEntity := sendStandingTelegramUpdate(t, firstURL, 101, 42)
	secondEntity := sendStandingTelegramUpdate(t, firstURL, 102, 42)
	if firstEntity == "" || firstEntity != secondEntity {
		t.Fatalf("delivery entities = %q and %q, want one standing entity", firstEntity, secondEntity)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 42, 42)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	firstOutput := first.outputString()
	if strings.Count(firstOutput, "handoff → message received (chat readiness) \"across head\"") != 1 {
		t.Fatalf("readiness handoff occurrence was not rendered exactly once:\n%s", firstOutput)
	}
	for _, want := range []string{
		"swarm serve --dev · ",
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

	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	restartedEntity := sendStandingTelegramUpdate(t, secondURL, 103, 84)
	if restartedEntity != firstEntity {
		t.Fatalf("restart entity = %q, want %q", restartedEntity, firstEntity)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 84)
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
	if err := sqliteStore.DB.QueryRow(`
		SELECT current_run_id
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-ingress'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`, firstEntity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND json_extract(anchor, '$.entity_id') = ? AND status = 'pending' AND json_extract(snapshot, '$.decision') = 'standing_review'`, firstEntity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	var entityEvents, wrongRunEvents int
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events
		WHERE entity_id = ?
	`, standingRunID, firstEntity).Scan(&entityEvents, &wrongRunEvents); err != nil {
		t.Fatalf("inspect standing event lineage: %v", err)
	}
	if entityEvents == 0 || wrongRunEvents != 0 {
		t.Fatalf("standing entity event lineage = events:%d wrong_run:%d, want events>0/wrong_run:0", entityEvents, wrongRunEvents)
	}
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close SQLite inspection store before restart matrix: %v", err)
	}
	sqliteStore = nil
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, standingRunID, nil)
}

func TestServeAuthorActivityAttachmentFailureKeepsRuntimeHealthy(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()

	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
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
		ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
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
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
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
	packagePath := filepath.Join(contractsRoot, "package.yaml")
	raw, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read package: %v", err)
	}
	writeStandingCandidateFile(t, packagePath, strings.Replace(string(raw), "alias: chat", "alias: chat/support", 1))
	sqlitePath := filepath.Join(t.TempDir(), "invalid-alias.sqlite")
	process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Dev: true, Verbose: true,
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
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		storetest.BootstrapPostgresRuntimeStore(t, runtimePG)
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
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
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
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
		ConfigPath: writeServeRuntimeTestConfig(t), ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "postgres", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	entity := sendStandingTelegramUpdate(t, baseURL, 201, 42)
	if got := sendStandingTelegramUpdate(t, baseURL, 202, 42); got != entity {
		t.Fatalf("second entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 42, 42)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore: %v", err)
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	if got := sendStandingTelegramUpdate(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), 203, 84); got != entity {
		t.Fatalf("restart entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 84)
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
		WHERE flow_id = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
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
	if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND anchor->>'entity_id' = $1 AND status = 'pending' AND snapshot->>'decision' = 'standing_review'`, entity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	var entityEvents, wrongRunEvents int
	if err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = $1::uuid THEN 0 ELSE 1 END), 0)
		FROM events
		WHERE entity_id = $2::uuid
	`, standingRunID, entity).Scan(&entityEvents, &wrongRunEvents); err != nil {
		t.Fatalf("inspect standing event lineage: %v", err)
	}
	if entityEvents == 0 || wrongRunEvents != 0 {
		t.Fatalf("standing entity event lineage = events:%d wrong_run:%d, want events>0/wrong_run:0", entityEvents, wrongRunEvents)
	}
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, standingRunID, func(t *testing.T) {
		var reopenErr error
		runtimePG, reopenErr = store.NewPostgresStore(dsn)
		if reopenErr != nil {
			t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", reopenErr)
		}
	})
}

func requireChangedStandingColdStartMatrix(t *testing.T, opts cliapp.ServeOptions, contractsRoot, originalRunID string, prepare func(*testing.T)) {
	t.Helper()
	packagePath := filepath.Join(contractsRoot, "package.yaml")
	basePackage, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read standing package: %v", err)
	}
	flowDir := filepath.Join(contractsRoot, "flows", "telegram-ingress")
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
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.1"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision two", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.2"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision three", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.3"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "package manifest name revised", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), "name: standing-telegram-proof", "name: renamed-standing-proof", 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "standing declaration removed", apply: func(t *testing.T) {
			before := string(basePackage)
			start := strings.Index(before, "flows:\n")
			if start < 0 {
				t.Fatal("flows declaration not found")
			}
			writeStandingCandidateFile(t, packagePath, before[:start]+"flows: []\n")
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "standing changed to non-standing", apply: func(t *testing.T) {
			before := string(basePackage)
			standingBlock := `    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`
			changed := strings.Replace(before, standingBlock, "", 1)
			if changed == before {
				t.Fatal("standing activation block not found")
			}
			chatDeclaration := "  - {id: telegram-chat, flow: telegram-chat, mode: template}\n"
			withoutChat := strings.Replace(changed, chatDeclaration, "", 1)
			if withoutChat == changed {
				t.Fatal("telegram-chat declaration not found")
			}
			changed = withoutChat
			writeStandingCandidateFile(t, packagePath, changed)
			nonStandingSchema := canonicalrouting.WithoutStandingIngressPins(t, string(baseFlowSchema))
			writeStandingCandidateFile(t, flowSchemaPath, nonStandingSchema)
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "flow identity renamed", apply: func(t *testing.T) {
			renamedDir := filepath.Join(contractsRoot, "flows", "telegram-ingress-v2")
			if err := os.Rename(flowDir, renamedDir); err != nil {
				t.Fatalf("rename flow directory: %v", err)
			}
			t.Cleanup(func() {
				_ = os.Rename(renamedDir, flowDir)
				_ = os.WriteFile(flowSchemaPath, baseFlowSchema, 0o600)
			})
			writeStandingCandidateFile(t, filepath.Join(renamedDir, "schema.yaml"), strings.Replace(string(baseFlowSchema), "name: telegram-ingress", "name: telegram-ingress-v2", 1))
			writeStandingCandidateFile(t, packagePath, strings.ReplaceAll(string(basePackage), "telegram-ingress", "telegram-ingress-v2"))
		}, wantOutput: []string{" created run=", " orphaned declaration_removed=true"}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, string(basePackage))
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
	writeStandingCandidateFile(t, packagePath, string(basePackage))
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

func sendStandingTelegramUpdate(t testing.TB, baseURL string, updateID, chatID int) string {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":%d},"chat":{"id":%d,"type":"private"},"text":"hello %d"}}`, updateID, updateID, chatID, chatID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new webhook request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send webhook: %v", err)
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode webhook response status=%d: %v", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status=%d payload=%v", resp.StatusCode, payload)
	}
	return strings.TrimSpace(fmt.Sprint(payload["entity_id"]))
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
	store, err := store.NewSQLiteRuntimeStore(path)
	if err != nil {
		return err.Error()
	}
	defer store.Close()
	rows, err := store.DB.Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
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
	logRows, err := store.DB.Query(`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := store.DB.Query(`SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := store.DB.Query(`SELECT event_id, status, COALESCE(failure, '') FROM pipeline_receipts ORDER BY updated_at`)
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
	return canonicalrouting.CopyStandingTelegramServe(t, telegramBaseURL)
}

func commitReadinessHandoffAuthorActivity(sqlitePath string, rt *runtimepkg.Runtime) error {
	selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		return err
	}
	defer selected.Close()
	scope := runtimeauthoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.BundleSourceFact.BundleHash)
	ctx := runtimeauthoractivity.WithScope(context.Background(), scope)
	tx, err := selected.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	story, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	identity := uuid.NewString()
	if err := runtimeauthoractivity.Record(story, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindInboundReceived, Version: runtimeauthoractivity.Version, Transition: "received",
		SourceOwner: "events", SourceIdentity: identity, DedupKey: "readiness-handoff:" + identity,
		OccurredAt: time.Now().UTC(), Scope: scope, AuthorSafeSummary: "across head",
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "entity", SubjectID: uuid.NewString(), Provider: "handoff",
			AuthorSubjectType: "chat", AuthorSubjectID: "readiness",
		},
	}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := runtimeauthoractivity.Finalize(story); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func waitForStandingStoryOutput(t testing.TB, process *serveRuntimeTestProcess, fragments ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		output := process.outputString()
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(output, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for story output %q:\n%s", fragments, process.outputString())
}
