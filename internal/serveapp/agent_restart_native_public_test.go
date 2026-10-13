package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type publicAgentRowReader interface {
	manager.AgentLifecycleStateReader
	LoadAgents(context.Context) ([]manager.PersistedAgent, error)
}

type discardedAgentRestartReply struct {
	body []byte
}

func (lost *discardedAgentRestartReply) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	lost.body, err = io.ReadAll(response.Body)
	return nil, errors.Join(io.ErrUnexpectedEOF, err, response.Body.Close())
}

func proveStandingAgentRestartLostReply(t *testing.T, baseURL, backend, location string, sessions map[string]standingMemorySession) {
	t.Helper()
	ctx := context.Background()
	var selected publicAgentRowReader
	if backend == "sqlite" {
		owner, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location)
		selected = owner
	} else {
		owner, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, location)
		selected = owner
	}
	keys := slices.Sorted(maps.Keys(sessions))
	if len(keys) != 2 {
		t.Fatalf("restart proof requires the two actually constructed sessions: %+v", sessions)
	}
	session := sessions[keys[0]]
	records, err := selected.LoadAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var targets []manager.PersistedAgent
	for _, record := range records {
		if record.Config.ID == session.AgentID && record.Config.Identity.RunID == session.RunID && record.Config.CanonicalFlowPath() == session.FlowInstance {
			targets = append(targets, record)
		}
	}
	if len(targets) != 1 {
		t.Fatalf("public restart does not name exactly one native constructed actor: %+v", targets)
	}
	identity := targets[0].Config.Identity
	before, found, err := selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found || before.Phase != manager.AgentLifecycleRunning {
		t.Fatalf("native running predecessor: %+v found=%v err=%v", before, found, err)
	}
	params := map[string]any{"run_id": session.RunID, "agent_id": session.AgentID, "flow_instance": session.FlowInstance, "idempotency_key": uuid.NewString()}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "agent.restart-proof", "method": "agent.restart", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/rpc"
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	lost := &discardedAgentRestartReply{}
	if _, err := (&http.Client{Transport: lost, Timeout: 5 * time.Second}).Do(request); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("public restart reply was not lost after actual HTTP completion: %v", err)
	}
	var committed servedJSONRPCEnvelope
	if err := json.Unmarshal(lost.body, &committed); err != nil || committed.Error != nil || string(committed.Result) != `{"ok":true}` {
		t.Fatalf("native restart did not complete before response loss: %s err=%v", lost.body, err)
	}
	after, found, err := selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found || after.Generation != before.Generation+1 || after.Phase != manager.AgentLifecycleRunning || after.LastTransitionID == before.LastTransitionID {
		t.Fatalf("public restart did not commit one native successor: before=%+v after=%+v err=%v", before, after, err)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	replay := requestServedJSONRPC(t, endpoint, "agent.restart", params)
	if replay.Error != nil || !bytes.Equal(committed.Result, replay.Result) {
		t.Fatalf("exact public restart retry changed its committed result: original=%s replay=%+v", committed.Result, replay)
	}
	conflicting := maps.Clone(params)
	conflicting["agent_id"] = "foreign-agent"
	refusal := requireServedJSONRPCError(t, endpoint, "agent.restart", conflicting)
	if refusal.Data["code"] != apiv1.IdempotencyConflictCode {
		t.Fatalf("changed restart request did not fail at its exact API owner: %+v", refusal)
	}
	conserved, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(snapshot, conserved) {
		t.Fatalf("restart replay/refusal changed application rows: err=%v", err)
	}
}
