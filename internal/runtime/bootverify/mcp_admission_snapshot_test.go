package bootverify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestMCPAdmissionSnapshotRequiresCompletedObservation(t *testing.T) {
	for _, status := range []AdmissionObservationStatus{AdmissionNotRun, AdmissionUnavailable, AdmissionFailed, "unknown"} {
		report := Report{mcpObservation: &AdmissionObservation{Status: status}}
		if _, err := report.DiscoveredToolAdmission(); err == nil {
			t.Fatalf("unobserved status %s became empty catalog", status)
		}
	}
	if _, err := (Report{}).DiscoveredToolAdmission(); err == nil {
		t.Fatal("missing observation became empty catalog")
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if tools, err := Run(context.Background(), source, Options{}).DiscoveredToolAdmission(); err != nil || len(tools) != 0 {
		t.Fatalf("known absent MCP declaration not admitted: %v %v", tools, err)
	}
	if _, err := Run(context.Background(), source, Options{Purpose: StructuralValidation}).DiscoveredToolAdmission(); err == nil {
		t.Fatal("portable validation manufactured discovery")
	}
}

func TestMCPAdmissionSnapshotReusesExactDiscoveryWithoutExecution(t *testing.T) {
	var listed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var result any
		switch request["method"] {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "infra", "version": "1"}}
		case "notifications/initialized":
			result = map[string]any{}
		case "tools/list":
			listed.Add(1)
			result = map[string]any{"tools": []any{map[string]any{"name": "ping", "description": "exact observed description", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}}}}
		default:
			t.Errorf("non-observation MCP request: %v", request["method"])
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": result})
	}))
	defer server.Close()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{"infra": map[string]any{"transport": "http", "url": server.URL, "prefix": "infra"}}},
		}},
	})
	report := Run(context.Background(), source, Options{CheckMCPReachable: true})
	for i := 0; i < 3; i++ {
		tools, err := report.DiscoveredToolAdmission()
		if err != nil || len(tools) != 1 || tools["infra.ping"].Contract.Description() != "exact observed description" {
			t.Fatalf("completed exact catalog lost: %v %v", tools, err)
		}
		delete(tools, "infra.ping")
	}
	if listed.Load() != 1 {
		t.Fatalf("catalog consumption rediscovered MCP: %d lists", listed.Load())
	}
}
