package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/tests/support/testutil/mcpapi"
)

func TestRuntimeShutdownClosesExternalMCPClient(t *testing.T) {
	remote := mcpapi.New(t, "")
	source := semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{Policy: contracts.PolicyDocument{Values: map[string]contracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"configured": map[string]any{"transport": "http", "url": remote.URL, "prefix": "configured"}}},
	}}})
	client := mcp.NewClient(nil)
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	executor := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{WorkflowSource: source, MCPClient: client})
	if len(client.DiscoveredTools()) != 1 {
		t.Fatal("MCP discovery did not establish the runtime-owned catalog")
	}
	rt := &Runtime{ToolExecutor: executor}
	if err := rt.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatal(err)
	}
	if len(client.DiscoveredTools()) != 0 {
		t.Fatal("runtime shutdown did not close its external MCP client")
	}
	before := len(remote.Calls())
	if errs := client.Refresh(context.Background(), source, mcp.DiscoveryOptions{}); len(errs) == 0 || len(remote.Calls()) != before {
		t.Fatal("shutdown client reacquired external MCP resources")
	}
	if err := rt.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatal(err)
	}
}
