package pythonmodule

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

func TestQ01ColdInterpreterPhases(t *testing.T) {
	started := time.Now()
	root, err := materializedArtifactDir()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cold_artifact=%s", time.Since(started))
	for _, phase := range []string{"cold_compile", "warm_deserialize"} {
		started := time.Now()
		engine, module, err := newInterpreterModule(root)
		if err != nil {
			t.Fatal(err)
		}
		module.Close()
		engine.Close()
		t.Logf("%s=%s", phase, time.Since(started))
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "releasee2e", "testdata", "workspace_mcp", "mocks", "worker.py"))
	if err != nil {
		t.Fatal(err)
	}
	inputs := []string{
		`{"tool_results":null,"tools":[{"name":"read_flow_data","schema":{"properties":{"static_id":{"enum":["fixture-static-id"]}}}}]}`,
		`{"tool_results":[{"content":"{\"tool_result\":[{\"ok\":true,\"result\":{\"content\":\"real-http-mcp\\n\"}}]}"}]}`,
	}
	for i, input := range inputs {
		started := time.Now()
		result, err := Execute(context.Background(), Request{
			ModuleID: "Q01", RowID: "diagnostic", Source: source, Digest: digestSource(source),
			Entry: mockperformance.EntryHandle, Input: []byte(input), Fuel: mockperformance.ExecutionFuel,
			MemoryPages: mockperformance.ExecutionMemoryPages, OutputBytes: mockperformance.ExecutionOutputBytes,
		})
		if err != nil {
			t.Fatal(err)
		}
		var value struct {
			Calls []struct{ Name string }
		}
		if err := json.Unmarshal(result.Output, &value); err != nil || len(value.Calls) != 1 {
			t.Fatalf("diagnostic fixture result: %v", err)
		}
		t.Logf("warm_model_round=%d elapsed=%s tool=%s fuel=%d", i+1, time.Since(started), value.Calls[0].Name, result.FuelConsumed)
	}
}
