package manager_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type restartAfterNativeCommitLoss struct {
	agentcontrol.Controller
	lose  atomic.Bool
	calls atomic.Int64
}

func (owner *restartAfterNativeCommitLoss) Restart(ctx context.Context, request agentcontrol.RestartRequest) (agentcontrol.RestartResult, error) {
	result, err := owner.Controller.Restart(ctx, request)
	if err != nil {
		return result, err
	}
	owner.calls.Add(1)
	if owner.lose.Swap(false) {
		return agentcontrol.RestartResult{}, errors.New("injected native restart result loss before API completion")
	}
	return result, nil
}

func proveNativeAgentRestartAPICompletionLoss(t *testing.T, selected storetest.AgentFixtureStore, actor actors.AgentConfig, controller agentcontrol.Controller) {
	t.Helper()
	idempotency, ok := selected.(apiv1.APIIdempotencyStore)
	if !ok {
		t.Fatal("native selected owner lacks API completion authority")
	}
	owner := &restartAfterNativeCommitLoss{Controller: controller}
	owner.lose.Store(true)
	process := worklifetime.NewProcess()
	defer func() {
		process.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join public restart ingress: %v", err)
		}
	}()
	handler, err := apiv1.NewHandler(apiv1.Options{
		ProcessWorkOwner: process, AuthTokens: []string{apiv1.DefaultLoopbackAPIToken},
		Handlers: apiv1.OperatorAgentControlHandlers(apiv1.AgentControlHandlerOptions{
			Controller: owner, Idempotency: idempotency,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	params := map[string]any{"run_id": actor.Identity.RunID, "agent_id": actor.ID, "flow_instance": actor.CanonicalFlowPath(), "idempotency_key": uuid.NewString()}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "native-restart", "method": "agent.restart", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	call := func() (json.RawMessage, json.RawMessage) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(request))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("native restart HTTP status=%d decode=%v", response.StatusCode, err)
		}
		return envelope.Result, envelope.Error
	}
	ctx := context.Background()
	before, found, err := selected.LoadAgentLifecycleState(ctx, actor.Identity)
	if err != nil || !found {
		t.Fatalf("native restart predecessor found=%v err=%v", found, err)
	}
	_, failure := call()
	after, found, err := selected.LoadAgentLifecycleState(ctx, actor.Identity)
	if len(failure) == 0 || string(failure) == "null" || owner.calls.Load() != 1 || err != nil || !found || after.Generation != before.Generation+1 {
		t.Fatalf("API completion loss did not follow one native commit: failure=%s calls=%d before=%+v after=%+v err=%v", failure, owner.calls.Load(), before, after, err)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	completionCount := len(snapshot["api_idempotency"].Rows)
	result, failure := call()
	current, found, err := selected.LoadAgentLifecycleState(ctx, actor.Identity)
	if (len(failure) != 0 && string(failure) != "null") || string(result) != `{"ok":true}` || owner.calls.Load() != 2 || err != nil || !found || !reflect.DeepEqual(current, after) {
		t.Fatalf("exact native-outcome retry failed: result=%s failure=%s calls=%d current=%+v err=%v", result, failure, owner.calls.Load(), current, err)
	}
	completed, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || len(completed["api_idempotency"].Rows) != completionCount+1 {
		t.Fatalf("exact retry did not create exactly one API completion: err=%v", err)
	}
	delete(snapshot, "api_idempotency")
	withoutCompletion := make(map[string]storetest.SelectedForkStorageTableSnapshot, len(completed))
	for name, table := range completed {
		if name != "api_idempotency" {
			withoutCompletion[name] = table
		}
	}
	if !reflect.DeepEqual(snapshot, withoutCompletion) {
		t.Fatal("API completion repair repeated lifecycle/session/fact/diagnostic writes")
	}
	replayed, failure := call()
	conserved, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if (len(failure) != 0 && string(failure) != "null") || !bytes.Equal(result, replayed) || owner.calls.Load() != 2 || err != nil || !reflect.DeepEqual(completed, conserved) {
		t.Fatalf("completed API replay changed bytes or re-entered Manager: failure=%s err=%v", failure, err)
	}
}
