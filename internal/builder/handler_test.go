package builder

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

const testBuilderAuthToken = "builder-test-token"

type pagedEntityReader struct {
	pages        []store.OperatorEntityListResult
	calls        int
	getResult    store.OperatorEntityFull
	getErr       error
	lastEntityID string
	lastRunID    string
}

func (r *pagedEntityReader) ListOperatorEntities(_ context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error) {
	if r.calls >= len(r.pages) {
		return store.OperatorEntityListResult{Entities: []store.OperatorEntitySummary{}}, nil
	}
	page := r.pages[r.calls]
	r.calls++
	return page, nil
}

func (r *pagedEntityReader) LoadOperatorEntity(_ context.Context, entityID, runID string) (store.OperatorEntityFull, error) {
	r.lastEntityID = entityID
	r.lastRunID = runID
	if r.getErr != nil {
		return store.OperatorEntityFull{}, r.getErr
	}
	if r.getResult.Entity.EntityID == "" {
		return store.OperatorEntityFull{}, store.ErrEntityNotFound
	}
	return r.getResult, nil
}

func TestHandler_CredentialsListSetDelete(t *testing.T) {
	ctx := context.Background()
	fileStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	store := runtimecredentials.NewOverlayStore(runtimecredentials.NewEnvStore(), fileStore)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"email_api": {Credentials: []string{"sendgrid_api_key"}},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"prefix":          "infra",
						"credentials_key": "infra_mcp_token",
					},
				},
			},
		}},
	})
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Credentials:    store,
		AuthToken:      testBuilderAuthToken,
		SemanticSource: source,
		Version:        "test",
	})

	listResp := callBuilderRPC(t, handler, Request{JSONRPC: "2.0", ID: "1", Method: "credentials.list"})
	listResult, _ := listResp.Result.(map[string]any)
	records := extractCredentialRecords(t, listResult["credentials"])
	if len(records) != 2 {
		t.Fatalf("expected 2 credentials, got %d", len(records))
	}
	if records[0].Present || records[1].Present {
		t.Fatalf("expected missing credentials before set, got %+v", records)
	}

	setResp := callBuilderRPC(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "2",
		Method:  "credentials.set",
		Params: map[string]any{
			"key":   "sendgrid_api_key",
			"value": "secret-1",
		},
	})
	setResult, _ := setResp.Result.(map[string]any)
	credential := extractSingleCredentialRecord(t, setResult["credential"])
	if !credential.Present || credential.Source != runtimecredentials.SourceFile || !credential.Writable {
		t.Fatalf("unexpected set result %+v", credential)
	}
	listResp = callBuilderRPC(t, handler, Request{JSONRPC: "2.0", ID: "3", Method: "credentials.list"})
	listResult, _ = listResp.Result.(map[string]any)
	records = extractCredentialRecords(t, listResult["credentials"])
	if len(records) != 2 {
		t.Fatalf("expected 2 credentials after set, got %d", len(records))
	}
	missing, err := runtimecredentials.MissingRequired(ctx, store, source)
	if err != nil {
		t.Fatalf("MissingRequired: %v", err)
	}
	if len(missing) != 1 || missing[0].Key != "infra_mcp_token" {
		t.Fatalf("unexpected missing credentials %+v", missing)
	}

	deleteResp := callBuilderRPC(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "4",
		Method:  "credentials.delete",
		Params: map[string]any{
			"key": "sendgrid_api_key",
		},
	})
	deleteResult, _ := deleteResp.Result.(map[string]any)
	credential = extractSingleCredentialRecord(t, deleteResult["credential"])
	if credential.Present {
		t.Fatalf("expected credential to be deleted, got %+v", credential)
	}
}

func TestHandler_StateListInstancesDrainsCanonicalEntityPages(t *testing.T) {
	page1 := make([]store.OperatorEntitySummary, 50)
	for i := range page1 {
		page1[i] = store.OperatorEntitySummary{EntityID: "entity-page-1"}
	}
	reader := &pagedEntityReader{
		pages: []store.OperatorEntityListResult{
			{Entities: page1, NextCursor: "next"},
			{Entities: []store.OperatorEntitySummary{{EntityID: "entity-page-2"}}},
		},
	}
	handler := NewHandler(Options{
		AuthToken: testBuilderAuthToken,
		Entities:  reader,
	})

	resp := callBuilderRPC(t, handler, Request{JSONRPC: "2.0", ID: "1", Method: "state.list_instances"})
	result, _ := resp.Result.(map[string]any)
	rows, ok := result["instances"].([]store.OperatorEntitySummary)
	if !ok || len(rows) != 51 {
		t.Fatalf("instances = %#v, want 51 rows", result["instances"])
	}
	if reader.calls != 2 {
		t.Fatalf("ListOperatorEntities calls = %d, want 2", reader.calls)
	}
}

func TestHandler_StateGetEntityReturnsCanonicalEntityFull(t *testing.T) {
	reader := &pagedEntityReader{
		getResult: store.OperatorEntityFull{
			Entity: store.OperatorEntitySummary{
				EntityID:     runtimeflowidentity.EntityID("wf-1"),
				RunID:        "run-1",
				FlowInstance: "order/wf-1",
				EntityType:   "order",
				CurrentState: "reviewing",
				Slug:         "order-1",
				Name:         "Order 1",
			},
			Fields: map[string]any{"priority": "high"},
			Gates:  map[string]bool{"review_gate": true},
			Accumulated: map[string]any{
				"score":       float64(3),
				"accumulator": map[string]any{"count": float64(2)},
				"notes":       []any{"a", map[string]any{"text": "probe"}},
			},
		},
	}
	handler := NewHandler(Options{
		AuthToken: testBuilderAuthToken,
		Entities:  reader,
	})

	resp := callBuilderRPC(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "1",
		Method:  "state.get_entity",
		Params: map[string]any{
			"instance_id": "wf-1",
			"run_id":      "run-1",
		},
	})
	full, ok := resp.Result.(store.OperatorEntityFull)
	if !ok {
		t.Fatalf("state.get_entity result type = %T, want OperatorEntityFull", resp.Result)
	}
	if reader.lastEntityID != runtimeflowidentity.EntityID("wf-1") || reader.lastRunID != "run-1" {
		t.Fatalf("LoadOperatorEntity args = entity_id=%q run_id=%q", reader.lastEntityID, reader.lastRunID)
	}
	if full.Entity.CurrentState != "reviewing" || full.Fields["priority"] != "high" || !full.Gates["review_gate"] || full.Accumulated["score"] != float64(3) {
		t.Fatalf("state.get_entity canonical result = %#v", full)
	}
	if bucket, ok := full.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != float64(2) {
		t.Fatalf("state.get_entity accumulated bucket = %#v, want count", full.Accumulated["accumulator"])
	}

	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("marshal state.get_entity result: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal state.get_entity result: %v", err)
	}
	if _, ok := payload["state"]; ok {
		t.Fatalf("state.get_entity leaked legacy top-level state payload: %#v", payload)
	}
	entity := payload["entity"].(map[string]any)
	if entity["current_state"] != "reviewing" {
		t.Fatalf("entity.current_state = %#v, want reviewing", entity["current_state"])
	}
	if _, ok := entity["state"]; ok {
		t.Fatalf("state.get_entity leaked legacy entity.state payload: %#v", entity)
	}
	fields := payload["fields"].(map[string]any)
	if fields["priority"] != "high" {
		t.Fatalf("fields = %#v, want priority", fields)
	}
}

func callBuilderRPC(t *testing.T, httpHandler http.Handler, req Request) RPCResponse {
	t.Helper()
	h, ok := httpHandler.(*handler)
	if !ok {
		t.Fatalf("handler type = %T, want *handler", httpHandler)
	}
	result, rpcErr := h.dispatchRPC(context.Background(), req.Method, req.Params)
	resp := RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: result, Error: rpcErr}
	if rpcErr != nil {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
	return resp
}

func extractCredentialRecords(t *testing.T, raw any) []CredentialRecord {
	t.Helper()
	blob, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal credential records: %v", err)
	}
	var out []CredentialRecord
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("decode credential records: %v", err)
	}
	return out
}

func extractSingleCredentialRecord(t *testing.T, raw any) CredentialRecord {
	t.Helper()
	blob, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal credential record: %v", err)
	}
	var out CredentialRecord
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("decode credential record: %v", err)
	}
	return out
}
