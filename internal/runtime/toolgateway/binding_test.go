package toolgateway

import "testing"

func TestBindingNormalizesHostAndWorkspaceMCPURLs(t *testing.T) {
	binding := Binding{
		Transport:         TransportHTTP,
		HostEndpoint:      "http://127.0.0.1:18082",
		WorkspaceEndpoint: "http://host.docker.internal:18082",
		Token:             "gateway-token",
		LifecycleOwner:    LifecycleOwnerServeBoot,
		Source:            SourceBoundMCPListener,
	}

	if got := binding.HostMCPURL(); got != "http://127.0.0.1:18082/mcp" {
		t.Fatalf("host MCP URL = %q", got)
	}
	if got := binding.WorkspaceMCPURL(); got != "http://host.docker.internal:18082/mcp" {
		t.Fatalf("workspace MCP URL = %q", got)
	}
}

func TestBindingValidateRequiresWorkspaceEndpoint(t *testing.T) {
	binding := Binding{
		Transport:    TransportHTTP,
		HostEndpoint: "http://127.0.0.1:18082",
		Token:        "gateway-token",
	}

	if err := binding.Validate(); err == nil {
		t.Fatal("expected missing workspace endpoint to fail validation")
	}
}
