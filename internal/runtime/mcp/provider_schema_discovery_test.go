package mcp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/llm"
)

func TestProviderSchemaDiscoveryAdmission(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, invalid := range []bool{false, true} {
			name := transport + "/valid"
			catalog := `{"tools":[{"name":"inspect","inputSchema":{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}}]}`
			if invalid {
				name = transport + "/invalid-atomic"
				catalog = `{"tools":[{"name":"inspect","inputSchema":{"type":"object"}},{"name":"bad","inputSchema":{"type":"unsupported-provider-type"}}]}`
			}
			t.Run(name, func(t *testing.T) {
				client, server := remoteSchemaClient(t, transport, catalog)
				tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
				if invalid {
					if err == nil || len(tools) != 0 {
						t.Fatalf("invalid remote catalog published partially: %v err=%v", tools, err)
					}
					return
				}
				if err != nil || len(tools) != 1 {
					t.Fatalf("valid discovery: %v err=%v", tools, err)
				}
				for _, tool := range tools {
					if err := llm.ValidateProviderToolSchema(tool.Name, tool.Contract.InputSchema().Projection()); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
