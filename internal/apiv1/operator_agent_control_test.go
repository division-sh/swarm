package apiv1

import (
	"context"
	"fmt"
	"testing"
	"time"

	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	controller := &fakeAgentControlController{
		directiveResponse: "accepted",
		replayedCount:     7,
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:          func() time.Time { return time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC) },
			Idempotency:  pg,
			AgentControl: controller,
		}),
	})

	directiveBody := agentDirectiveBody("agent-1", "run corpus", true, "idem-directive")
	directive := rpcCall(t, handler, directiveBody)
	if directive.Error != nil {
		t.Fatalf("agent.send_directive error = %#v", directive.Error)
	}
	if result := asMap(t, directive.Result); result["ok"] != true || result["response"] != "accepted" {
		t.Fatalf("agent.send_directive result = %#v", result)
	}
	if controller.directiveCalls != 1 || !controller.lastDirective.KillPrevious || controller.lastDirective.Directive != "run corpus" {
		t.Fatalf("directive call count/request = %d/%#v, want owner request", controller.directiveCalls, controller.lastDirective)
	}
	directiveReplay := rpcCall(t, handler, directiveBody)
	if directiveReplay.Error != nil {
		t.Fatalf("agent.send_directive replay error = %#v", directiveReplay.Error)
	}
	if controller.directiveCalls != 1 {
		t.Fatalf("directive calls after replay = %d, want 1", controller.directiveCalls)
	}
	directiveConflict := rpcCall(t, handler, agentDirectiveBody("agent-1", "different", true, "idem-directive"))
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
	if count := countAPIIdempotencyRows(t, db); count != 3 {
		t.Fatalf("api_idempotency rows = %d, want 3", count)
	}
}

func TestOperatorAgentControlHandlersTypedResourceErrors(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: pg,
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

	notRunning := rpcCall(t, handler, agentDirectiveBody("agent-1", "run corpus", false, ""))
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

func TestOperatorAgentControlHandlersRestrictAgentNotRunningToSendDirective(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: pg,
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
}

func (c *fakeAgentControlController) SendDirective(_ context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	c.directiveCalls++
	c.lastDirective = req
	if err := c.errs["agent.send_directive"]; err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	return runtimeagentcontrol.SendDirectiveResult{AgentID: req.AgentID, Response: c.directiveResponse}, nil
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

func agentDirectiveBody(agentID, directive string, killPrevious bool, idempotencyKey string) string {
	if idempotencyKey == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"directive":%q,"kill_previous":%t}}`, agentID, directive, killPrevious)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-directive","method":"agent.send_directive","params":{"agent_id":%q,"directive":%q,"kill_previous":%t,"idempotency_key":%q}}`, agentID, directive, killPrevious, idempotencyKey)
}
