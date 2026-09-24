package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedReceiverAgentBusinessNamespaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Log("real runFrom serve owner, live managed agent and Anthropic adapter; local HTTP provider endpoint, not release binary/external provider")
			opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyReceiverAgentCollision(t))
			provider := &receiverCollisionProvider{t: t}
			server := httptest.NewServer(provider)
			defer server.Close()
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": server.URL})
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			t.Setenv("ANTHROPIC_API_KEY", "")
			credentials, err := runtimecredentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "ANTHROPIC_API_KEY", "collision-proof-key"); err != nil {
				t.Fatal(err)
			}
			first, rt := start()
			first.mu.Lock()
			rt.Runtime = first.runtime
			first.mu.Unlock()
			values := canonicalrouting.ReceiverAgentCollisionValues()
			params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "idempotency_key": "collision-create", "payload": map[string]any{"account_id": "business-account", "values": values}}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			want := canonicalrouting.ReceiverAgentCollisionValues()
			want["account_id"] = "business-account"
			before := requireServedReceiverAgentCollision(t, rt, seed.RunID, want, 1)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if duplicate.RunID != seed.RunID || duplicate.EventID != seed.EventID {
				t.Fatal("duplicate reminted creation")
			}
			if got := requireServedReceiverAgentCollision(t, rt, seed.RunID, want, 1); !reflect.DeepEqual(got, before) {
				t.Fatal("duplicate changed authority")
			}
			for i, incoming := range []map[string]any{{}, {"model": "replacement", "nested": map[string]any{"system_prompt": "REPLACEMENT_PROMPT"}}} {
				p := map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "idempotency_key": fmt.Sprintf("collision-reuse-%d", i), "payload": map[string]any{"account_id": "business-account", "values": incoming}}
				requireServedEventPublishRPCResult(t, rt.Endpoint, p)
				if got := requireServedReceiverAgentCollision(t, rt, seed.RunID, want, i+2); !reflect.DeepEqual(got, before) {
					t.Fatal("reuse changed authority")
				}
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("stop=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, restarted := start()
			rt = restarted
			second.mu.Lock()
			rt.Runtime = second.runtime
			second.mu.Unlock()
			if got := requireServedReceiverAgentCollision(t, rt, seed.RunID, want, 3); !reflect.DeepEqual(got, before) {
				t.Fatal("restart changed authority")
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "idempotency_key": "collision-after-restart", "payload": map[string]any{"account_id": "business-account", "values": map[string]any{}}})
			if got := requireServedReceiverAgentCollision(t, rt, seed.RunID, want, 4); !reflect.DeepEqual(got, before) {
				t.Fatal("first post-restart agent changed authority")
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			if len(provider.requests) != 4 {
				t.Fatalf("actual provider calls=%d want=4", len(provider.requests))
			}
			for _, request := range provider.requests {
				if request.Model != before["descriptor"].(map[string]any)["resolved_model"] {
					t.Fatalf("provider model=%s differs from authored descriptor", request.Model)
				}
				if !reflect.DeepEqual(request.Tools, provider.requests[0].Tools) {
					t.Fatal("reuse/restart changed actual provider tool authority")
				}
			}
		})
	}
}

func requireServedReceiverAgentCollision(t *testing.T, rt servedControlProofRuntime, runID string, want map[string]any, turns int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var completed, failed int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`, runID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID).Scan(&failed); err != nil {
			t.Fatal(err)
		}
		if failed != 0 {
			t.Fatalf("configured agent journey dead-lettered\n%s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
		}
		if completed >= turns {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent turns=%d want=%d\n%s", completed, turns, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
	var path, entityID, flowRaw, agentID, raw, descriptor, tools, permissions, memorySource, model string
	var memory bool
	if err := rt.DB.QueryRow(`SELECT f.instance_path,e.entity_id,CAST(f.config AS TEXT) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path WHERE f.run_id=$1 AND f.flow_template='account'`, runID).Scan(&path, &entityID, &flowRaw); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT agent_id,CAST(config AS TEXT),CAST(runtime_descriptor AS TEXT),CAST(tools AS TEXT),CAST(permissions AS TEXT),memory_enabled,memory_source,model FROM agents WHERE run_id=$1 AND flow_instance=$2`, runID, path).Scan(&agentID, &raw, &descriptor, &tools, &permissions, &memory, &memorySource, &model); err != nil {
		t.Fatal(err)
	}
	var envelope, flow map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &envelope); err != nil {
		t.Fatal(err)
	}
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(flowRaw), &flow); err != nil {
		t.Fatal(err)
	}
	check := func(got any) {
		a, err := canonicaljson.MarshalPreservingNumberKinds(got)
		if err != nil {
			t.Fatal(err)
		}
		b, err := canonicaljson.MarshalPreservingNumberKinds(want)
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Fatalf("business config=%s want=%s", a, b)
		}
	}
	check(flow["config"])
	check(envelope["receiver_config"])
	if len(envelope) != 2 || !reflect.DeepEqual(envelope["config"], map[string]any{}) {
		t.Fatalf("not the closed agent config envelope: %s", raw)
	}
	matched := 0
	for _, cfg := range rt.Runtime.Manager.ListAgentConfigs() {
		if cfg.Identity.RunID != runID || cfg.ID != agentID || cfg.FlowPath != path {
			continue
		}
		matched++
		var values any
		if err := canonicaljson.DecodePreservingNumberLexemes(cfg.ReceiverConfig, &values); err != nil {
			t.Fatal(err)
		}
		check(values)
		if string(cfg.Config) != "{}" || cfg.Model != "regular" || cfg.Memory.Enabled || len(cfg.Tools) != 0 || len(cfg.Permissions) != 0 || cfg.NativeTools.Any() {
			t.Fatalf("live actor authority changed: %+v", cfg)
		}
		prompt, err := cfg.DerivedSystemPrompt()
		if err != nil || !strings.Contains(prompt, "AUTHORED_COLLISION_OBSERVER") || strings.Contains(prompt, "INERT_") {
			t.Fatalf("live actor prompt changed: %q: %v", prompt, err)
		}
	}
	if matched != 1 {
		t.Fatalf("exact configured live actor count=%d want=1", matched)
	}
	var authority map[string]any
	if err := json.Unmarshal([]byte(descriptor), &authority); err != nil {
		t.Fatal(err)
	}
	if model != "regular" || memory || authority["execution_mode"] != "live" || !strings.Contains(descriptor, "AUTHORED_COLLISION_OBSERVER") {
		t.Fatalf("business values replaced authority: model=%s memory=%v descriptor=%s", model, memory, descriptor)
	}
	for _, rawList := range []string{tools, permissions} {
		var names []string
		if err := json.Unmarshal([]byte(rawList), &names); err != nil {
			t.Fatal(err)
		}
		if len(names) != 0 {
			t.Fatalf("business data granted actor tools/permissions: %s", rawList)
		}
	}
	for _, key := range []string{"INERT_ARCHIVED_PROMPT", "INERT_LIST_PROMPT", "business-model", "business-memory"} {
		if strings.Contains(descriptor, key) {
			t.Fatalf("business data in descriptor: %s", descriptor)
		}
	}
	var detail operatorread.OperatorAgentDetail
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.get", map[string]any{"run_id": runID, "agent_id": agentID, "flow_instance": path}, &detail)
	if detail.Agent.AgentID != agentID || detail.Agent.FlowInstance != path || detail.Agent.ExecutionMode != "live" {
		t.Fatalf("public agent identity: %+v", detail)
	}
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
	if entity.Entity.EntityID != entityID || entity.Entity.FlowInstance != path || entity.Fields["processed_count"] != float64(turns) {
		t.Fatalf("public receiver state: %+v", entity)
	}
	var count, deliveries int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o ON o.delivery_id=d.delivery_id WHERE d.run_id=$1 AND d.subscriber_type='agent' AND d.status='delivered' AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, runID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if count != turns || deliveries != turns {
		t.Fatalf("turns/delivered first claims=%d/%d want=%d", count, deliveries, turns)
	}
	return map[string]any{"agent": agentID, "path": path, "entity": entityID, "descriptor": authority, "tools": tools, "permissions": permissions, "memory_source": memorySource, "model": model, "config": envelope}
}

type receiverCollisionProvider struct {
	t        testing.TB
	mu       sync.Mutex
	requests []receiverCollisionRequest
}

type receiverCollisionRequest struct {
	Model  string           `json:"model"`
	System json.RawMessage  `json:"system"`
	Tools  []map[string]any `json:"tools"`
}

func (p *receiverCollisionProvider) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var request receiverCollisionRequest
	if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
		p.t.Error(err)
		http.Error(w, "bad request", 400)
		return
	}
	if req.URL.Path != "/v1/messages" || req.Header.Get("x-api-key") != "collision-proof-key" {
		p.t.Errorf("wrong provider route/credential: %s", req.URL.Path)
	}
	prompt := string(request.System)
	if !strings.Contains(prompt, "AUTHORED_COLLISION_OBSERVER") || strings.Contains(prompt, "INERT_ARCHIVED_PROMPT") || strings.Contains(prompt, "INERT_LIST_PROMPT") {
		p.t.Errorf("business JSON replaced authored provider prompt: %s", prompt)
	}
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":8,"output_tokens":2},"content":[{"type":"text","text":"Account work observed."}]}`))
}
