package builder

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecredentials "swarm/internal/runtime/credentials"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

const testBuilderAuthToken = "builder-test-token"

type pagedEntityReader struct {
	pages []store.OperatorEntityListResult
	calls int
}

func (r *pagedEntityReader) ListOperatorEntities(_ context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error) {
	if r.calls >= len(r.pages) {
		return store.OperatorEntityListResult{Entities: []store.OperatorEntitySummary{}}, nil
	}
	page := r.pages[r.calls]
	r.calls++
	return page, nil
}

func (r *pagedEntityReader) LoadOperatorEntity(context.Context, string, string) (store.OperatorEntityFull, error) {
	return store.OperatorEntityFull{}, store.ErrEntityNotFound
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
