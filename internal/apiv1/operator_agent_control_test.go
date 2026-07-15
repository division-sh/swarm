package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	db := sqliteStore.DB
	controller := &fakeAgentControlController{
		directiveResponse: "accepted",
		replayedCount:     7,
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:          func() time.Time { return time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC) },
			Idempotency:  sqliteStore,
			AgentControl: controller,
		}),
	})

	directiveRunID := "00000000-0000-0000-0000-000000000901"
	directiveBody := agentDirectiveBodyWithRun("agent-1", directiveRunID, "run corpus", "idem-directive")
	directive := rpcCall(t, handler, directiveBody)
	if directive.Error != nil {
		t.Fatalf("agent.send_directive error = %#v", directive.Error)
	}
	if result := asMap(t, directive.Result); result["ok"] != true || result["operation_id"] != "00000000-0000-0000-0000-000000000904" || result["response"] != "accepted" || result["run_id"] != directiveRunID || result["run_id_resolution"] != runtimeagentcontrol.RunResolutionSpecified {
		t.Fatalf("agent.send_directive result = %#v", result)
	}
	if controller.directiveCalls != 1 || controller.lastDirective.Directive != "run corpus" || controller.lastDirective.RunID != directiveRunID || controller.lastDirective.Source != runtimeagentcontrol.DirectiveSourceV1RPC {
		t.Fatalf("directive call count/request = %d/%#v, want owner request", controller.directiveCalls, controller.lastDirective)
	}
	directiveReplay := rpcCall(t, handler, directiveBody)
	if directiveReplay.Error != nil {
		t.Fatalf("agent.send_directive replay error = %#v", directiveReplay.Error)
	}
	if controller.directiveCalls != 1 {
		t.Fatalf("directive calls after replay = %d, want 1", controller.directiveCalls)
	}
	staleKillPrevious := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-directive-stale","method":"agent.send_directive","params":{"agent_id":"agent-1","run_id":"00000000-0000-0000-0000-000000000901","directive":"run corpus","kill_previous":true,"idempotency_key":"idem-stale"}}`)
	if staleKillPrevious.Error == nil || staleKillPrevious.Error.Code != codeInvalidParams {
		t.Fatalf("stale kill_previous error = %#v, want invalid params", staleKillPrevious.Error)
	}
	if details := asMap(t, asMap(t, staleKillPrevious.Error.Data)["details"]); details["field"] != "kill_previous" {
		t.Fatalf("stale kill_previous details = %#v", details)
	}
	if controller.directiveCalls != 1 {
		t.Fatalf("directive calls after stale kill_previous = %d, want 1", controller.directiveCalls)
	}
	directiveConflict := rpcCall(t, handler, agentDirectiveBodyWithRun("agent-1", directiveRunID, "different", "idem-directive"))
	if directiveConflict.Error == nil {
		t.Fatal("agent.send_directive idempotency conflict error = nil")
	}
	if data := asMap(t, directiveConflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("directive conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}

	restartBody := agentControlBody("agent.restart", "agent-1", "idem-restart")
	restarted := rpcCall(t, handler, restartBody)
	if restarted.Error != nil {
		t.Fatalf("agent.restart error = %#v", restarted.Error)
	}
	if result := asMap(t, restarted.Result); result["ok"] != true {
		t.Fatalf("agent.restart result = %#v", result)
	}
	restartReplay := rpcCall(t, handler, restartBody)
	if restartReplay.Error != nil {
		t.Fatalf("agent.restart replay error = %#v", restartReplay.Error)
	}
	if controller.restartCalls != 1 {
		t.Fatalf("restart calls after replay = %d, want 1", controller.restartCalls)
	}

	replayBody := agentControlBody("agent.replay_backlog", "agent-1", "idem-replay")
	replayed := rpcCall(t, handler, replayBody)
	if replayed.Error != nil {
		t.Fatalf("agent.replay_backlog error = %#v", replayed.Error)
	}
	if result := asMap(t, replayed.Result); result["ok"] != true || result["replayed_count"] != float64(7) {
		t.Fatalf("agent.replay_backlog result = %#v", result)
	}
	replayAgain := rpcCall(t, handler, replayBody)
	if replayAgain.Error != nil {
		t.Fatalf("agent.replay_backlog idempotent replay error = %#v", replayAgain.Error)
	}
	if controller.replayCalls != 1 {
		t.Fatalf("replay calls after idempotent replay = %d, want 1", controller.replayCalls)
	}
	if count := countAPIIdempotencyRows(t, db); count != 2 {
		t.Fatalf("api_idempotency rows = %d, want 2 (directive projection belongs to its operation owner)", count)
	}
}

func TestOperatorAgentControlHandlersTypedResourceErrors(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: sqliteStore,
			AgentControl: &fakeAgentControlController{
				errs: map[string]error{
					"agent.send_directive": &runtimeagentcontrol.StateError{
						Err:           runtimeagentcontrol.ErrAgentNotRunning,
						AgentID:       "agent-1",
						CurrentStatus: runtimeagentcontrol.StatusTerminated,
					},
					"agent.restart": &runtimeagentcontrol.StateError{
						Err:     runtimeagentcontrol.ErrAgentNotFound,
						AgentID: "missing-agent",
					},
					"agent.replay_backlog": &runtimeagentcontrol.StateError{
						Err:     runtimeagentcontrol.ErrAgentNotFound,
						AgentID: "missing-agent",
					},
				},
			},
		}),
	})

	notRunning := rpcCall(t, handler, agentDirectiveBody("agent-1", "run corpus", ""))
	if notRunning.Error == nil {
		t.Fatal("agent.send_directive not-running error = nil")
	}
	if data := asMap(t, notRunning.Error.Data); data["code"] != AgentNotRunningCode {
		t.Fatalf("not-running data = %#v, want %s", data, AgentNotRunningCode)
	} else if details := asMap(t, data["details"]); details["current_status"] != runtimeagentcontrol.StatusTerminated {
		t.Fatalf("not-running details = %#v, want terminated", details)
	}

	for _, method := range []string{"agent.restart", "agent.replay_backlog"} {
		resp := rpcCall(t, handler, agentControlBody(method, "missing-agent", ""))
		if resp.Error == nil {
			t.Fatalf("%s missing-agent error = nil", method)
		}
		if data := asMap(t, resp.Error.Data); data["code"] != AgentNotFoundCode {
			t.Fatalf("%s data = %#v, want %s", method, data, AgentNotFoundCode)
		}
	}
}

func TestOperatorAgentDirectiveFailureUsesCanonicalNestedEnvelope(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	failure := runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure()
	operation := runtimeagentcontrol.DirectiveOperation{
		OperationID:      "00000000-0000-0000-0000-000000000801",
		DirectiveEventID: "00000000-0000-0000-0000-000000000802",
		ResolvedRunID:    "00000000-0000-0000-0000-000000000803",
		State:            runtimeagentcontrol.DirectiveOperationIndeterminate,
		Failure:          &failure,
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: sqliteStore,
			AgentControl: &fakeAgentControlController{errs: map[string]error{
				"agent.send_directive": &runtimeagentcontrol.DirectiveOperationError{
					Err:       runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate,
					Operation: operation,
				},
			}},
		}),
	})

	resp := rpcCall(t, handler, agentDirectiveBody("agent-1", "run corpus", ""))
	if resp.Error == nil {
		t.Fatal("agent.send_directive error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != AgentDirectiveOutcomeIndeterminateCode {
		t.Fatalf("directive code = %#v", data["code"])
	}
	details := asMap(t, data["details"])
	if _, ok := details["failure_code"]; ok {
		t.Fatalf("retired failure_code survived: %#v", details)
	}
	if _, ok := details["failure_message"]; ok {
		t.Fatalf("retired failure_message survived: %#v", details)
	}
	encoded, err := json.Marshal(details["failure"])
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtimefailures.UnmarshalEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode nested failure: %v", err)
	}
	want, _ := runtimefailures.MarshalEnvelope(failure)
	gotRaw, _ := runtimefailures.MarshalEnvelope(got)
	if string(gotRaw) != string(want) {
		t.Fatalf("nested failure = %s, want %s", gotRaw, want)
	}
}

func TestOperatorAgentSendDirectiveRunTargetErrors(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	db := sqliteStore.DB
	missingRunID := "00000000-0000-0000-0000-000000000404"
	terminalRunID := "00000000-0000-0000-0000-000000000405"
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: sqliteStore,
			AgentControl: &fakeAgentControlController{
				errs: map[string]error{
					"missing": &runtimeagentcontrol.StateError{
						Err:     runtimeagentcontrol.ErrRunNotFound,
						AgentID: "agent-1",
						RunID:   missingRunID,
					},
					"terminal": &runtimeagentcontrol.StateError{
						Err:           runtimeagentcontrol.ErrRunAlreadyTerminal,
						AgentID:       "agent-1",
						RunID:         terminalRunID,
						CurrentStatus: "completed",
					},
					"ambiguous": &runtimeagentcontrol.StateError{
						Err:     runtimeagentcontrol.ErrAmbiguousRunTarget,
						AgentID: "agent-1",
						ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{
							SessionID: "00000000-0000-0000-0000-000000000501",
							RunID:     "00000000-0000-0000-0000-000000000601",
						}},
					},
				},
			},
		}),
	})

	cases := []struct {
		name string
		body string
		code string
	}{
		{"missing", agentDirectiveBodyWithRun("agent-1", missingRunID, "missing", ""), RunNotFoundCode},
		{"terminal", agentDirectiveBodyWithRun("agent-1", terminalRunID, "terminal", ""), RunAlreadyTerminalCode},
		{"ambiguous", agentDirectiveBody("agent-1", "ambiguous", ""), AmbiguousRunTargetCode},
	}
	for _, tc := range cases {
		resp := rpcCall(t, handler, tc.body)
		if resp.Error == nil {
			t.Fatalf("%s error = nil", tc.name)
		}
		if data := asMap(t, resp.Error.Data); data["code"] != tc.code {
			t.Fatalf("%s data = %#v, want %s", tc.name, data, tc.code)
		}
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("idempotency rows after run target errors = %d, want 0", count)
	}
}

func TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	agent := &directiveIntegrationAgent{id: "agent-1"}
	manager := runtimemanager.NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, pg)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id, Model: "regular"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency:  pg,
			AgentControl: manager,
		}),
	})

	body := agentDirectiveBody("agent-1", "run corpus", "idem-directive-integration")
	first := rpcCall(t, handler, body)
	if first.Error != nil {
		t.Fatalf("first directive error = %#v", first.Error)
	}
	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("replay directive error = %#v", replay.Error)
	}
	if agent.calls != 1 {
		t.Fatalf("BoardStep calls = %d, want 1", agent.calls)
	}
	if count := countDirectiveEvents(t, db); count != 1 {
		t.Fatalf("platform.agent_directive rows = %d, want 1", count)
	}
	conflict := rpcCall(t, handler, agentDirectiveBody("agent-1", "different", "idem-directive-integration"))
	if conflict.Error == nil {
		t.Fatal("conflict error = nil")
	}
	if agent.calls != 1 {
		t.Fatalf("BoardStep calls after conflict = %d, want 1", agent.calls)
	}
	if count := countDirectiveEvents(t, db); count != 1 {
		t.Fatalf("platform.agent_directive rows after conflict = %d, want 1", count)
	}
}

func TestOperatorAgentSendDirectiveUsesLegacyRunBundleSourceUntilSourceStampingOwnerLands(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bootFingerprint := "sha256:4444444444444444444444444444444444444444444444444444444444444444"
	existingFingerprint := "sha256:5555555555555555555555555555555555555555555555555555555555555555"
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		BundleFingerprint: bootFingerprint,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	agent := &directiveIntegrationAgent{id: "agent-1"}
	manager := runtimemanager.NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, pg)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id, Model: "regular"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency:  pg,
			AgentControl: manager,
		}),
	})

	first := rpcCall(t, handler, agentDirectiveBody("agent-1", "new run", "idem-directive-bundle-new"))
	if first.Error != nil {
		t.Fatalf("new-run directive error = %#v", first.Error)
	}
	newRunID, _ := asMap(t, first.Result)["run_id"].(string)
	if newRunID == "" {
		t.Fatalf("new-run directive result = %#v", first.Result)
	}
	assertRunBundleIdentity(t, db, newRunID, "", "legacy", bootFingerprint)

	existingRunID := "00000000-0000-0000-0000-000000000755"
	if _, err := db.Exec(`
		INSERT INTO runs (run_id, status, bundle_fingerprint)
		VALUES ($1::uuid, 'running', $2)
	`, existingRunID, existingFingerprint); err != nil {
		t.Fatalf("seed existing run: %v", err)
	}
	explicit := rpcCall(t, handler, agentDirectiveBodyWithRun("agent-1", existingRunID, "existing run", "idem-directive-bundle-existing"))
	if explicit.Error != nil {
		t.Fatalf("existing-run directive error = %#v", explicit.Error)
	}
	assertRunBundleIdentity(t, db, existingRunID, "", "legacy", existingFingerprint)
}

func TestOperatorAgentControlHandlersRestrictAgentNotRunningToSendDirective(t *testing.T) {
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: sqliteStore,
			AgentControl: &fakeAgentControlController{
				errs: map[string]error{
					"agent.restart": &runtimeagentcontrol.StateError{
						Err:           runtimeagentcontrol.ErrAgentNotRunning,
						AgentID:       "agent-1",
						CurrentStatus: runtimeagentcontrol.StatusTerminated,
					},
					"agent.replay_backlog": &runtimeagentcontrol.StateError{
						Err:           runtimeagentcontrol.ErrAgentNotRunning,
						AgentID:       "agent-1",
						CurrentStatus: runtimeagentcontrol.StatusTerminated,
					},
				},
			},
		}),
	})

	for _, method := range []string{"agent.restart", "agent.replay_backlog"} {
		resp := rpcCall(t, handler, agentControlBody(method, "agent-1", ""))
		if resp.Error == nil {
			t.Fatalf("%s not-running error = nil", method)
		}
		if resp.Error.Code != codeInternalError {
			t.Fatalf("%s error code = %d, want %d", method, resp.Error.Code, codeInternalError)
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] == AgentNotRunningCode {
			t.Fatalf("%s returned undocumented application code %s", method, AgentNotRunningCode)
		}
	}
}

func TestOperatorAgentControlHandlersRequireOwner(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers:   OperatorReadHandlers(OperatorReadOptions{}),
	})
	resp := rpcCall(t, handler, agentControlBody("agent.restart", "agent-1", ""))
	if resp.Error == nil {
		t.Fatal("agent.restart without owner error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != MethodUnavailableCode {
		t.Fatalf("agent.restart without owner data = %#v, want %s", data, MethodUnavailableCode)
	}
}

type fakeAgentControlController struct {
	directiveResponse string
	replayedCount     int
	errs              map[string]error
	directiveCalls    int
	restartCalls      int
	replayCalls       int
	lastDirective     runtimeagentcontrol.SendDirectiveRequest
	directiveResults  map[string]fakeDirectiveResult
}

type fakeDirectiveResult struct {
	requestHash string
	result      runtimeagentcontrol.SendDirectiveResult
}

type directiveIntegrationAgent struct {
	id    string
	calls int
}

func (a *directiveIntegrationAgent) ID() string                      { return a.id }
func (*directiveIntegrationAgent) Type() string                      { return "stub" }
func (*directiveIntegrationAgent) Subscriptions() []events.EventType { return nil }
func (*directiveIntegrationAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *directiveIntegrationAgent) BoardStep(_ context.Context, directive runtimeagentcontrol.BoardDirective) (string, error) {
	a.calls++
	if err := runtimeagentcontrol.ValidateBoardDirective(directive); err != nil {
		return "", err
	}
	return "accepted", nil
}

func (c *fakeAgentControlController) SendDirective(_ context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	c.lastDirective = req
	if err := c.errs[req.Directive]; err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	if err := c.errs["agent.send_directive"]; err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	if req.IdempotencyKey != "" {
		if existing, ok := c.directiveResults[req.IdempotencyKey]; ok {
			if existing.requestHash != req.RequestHash {
				return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveIdempotencyConflictError{
					OriginalRequestHash:    existing.requestHash,
					ConflictingRequestHash: req.RequestHash,
					OperationID:            existing.result.OperationID,
				}
			}
			return existing.result, nil
		}
	}
	c.directiveCalls++
	runID := req.RunID
	if runID == "" {
		runID = "00000000-0000-0000-0000-000000000902"
	}
	mode := runtimeagentcontrol.RunResolutionSpecified
	if req.RunID == "" {
		mode = runtimeagentcontrol.RunResolutionNewRunAllocated
	}
	result := runtimeagentcontrol.SendDirectiveResult{
		OK:                 true,
		AgentID:            req.AgentID,
		OperationID:        "00000000-0000-0000-0000-000000000904",
		Response:           c.directiveResponse,
		RunID:              runID,
		RunIDResolution:    mode,
		DirectiveEventID:   "00000000-0000-0000-0000-000000000903",
		DirectiveEventType: runtimeagentcontrol.DirectiveEventType,
	}
	if req.IdempotencyKey != "" {
		if c.directiveResults == nil {
			c.directiveResults = map[string]fakeDirectiveResult{}
		}
		c.directiveResults[req.IdempotencyKey] = fakeDirectiveResult{requestHash: req.RequestHash, result: result}
	}
	return result, nil
}

func (c *fakeAgentControlController) Restart(_ context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	c.restartCalls++
	if err := c.errs["agent.restart"]; err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	return runtimeagentcontrol.RestartResult{AgentID: req.AgentID}, nil
}

func (c *fakeAgentControlController) ReplayBacklog(_ context.Context, req runtimeagentcontrol.ReplayBacklogRequest) (runtimeagentcontrol.ReplayBacklogResult, error) {
	c.replayCalls++
	if err := c.errs["agent.replay_backlog"]; err != nil {
		return runtimeagentcontrol.ReplayBacklogResult{}, err
	}
	return runtimeagentcontrol.ReplayBacklogResult{AgentID: req.AgentID, ReplayedCount: c.replayedCount}, nil
}

func agentControlBody(method, agentID, idempotencyKey string) string {
	if idempotencyKey == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-control","method":%q,"params":{"agent_id":%q}}`, method, agentID)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-control","method":%q,"params":{"agent_id":%q,"idempotency_key":%q}}`, method, agentID, idempotencyKey)
}

func agentDirectiveBody(agentID, directive, idempotencyKey string) string {
	if idempotencyKey == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"directive":%q}}`, agentID, directive)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"directive":%q,"idempotency_key":%q}}`, agentID, directive, idempotencyKey)
}

func agentDirectiveBodyWithRun(agentID, runID, directive, idempotencyKey string) string {
	if idempotencyKey == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"run_id":%q,"directive":%q}}`, agentID, runID, directive)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"run_id":%q,"directive":%q,"idempotency_key":%q}}`, agentID, runID, directive, idempotencyKey)
}

func countDirectiveEvents(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`).Scan(&count); err != nil {
		t.Fatalf("count directive events: %v", err)
	}
	return count
}

func assertRunBundleIdentity(t *testing.T, db *sql.DB, runID, wantHash, wantSource, wantLegacyFingerprint string) {
	t.Helper()
	var gotHash, gotSource, gotLegacyFingerprint string
	if err := db.QueryRow(`
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotLegacyFingerprint); err != nil {
		t.Fatalf("load run bundle identity: %v", err)
	}
	if gotHash != wantHash || gotSource != wantSource || gotLegacyFingerprint != wantLegacyFingerprint {
		t.Fatalf("run %s bundle identity = hash:%q source:%q fingerprint:%q, want hash:%q source:%q fingerprint:%q",
			runID, gotHash, gotSource, gotLegacyFingerprint, wantHash, wantSource, wantLegacyFingerprint)
	}
}
