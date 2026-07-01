package llm

import "github.com/division-sh/swarm/internal/runtime/toolgateway"

func testToolGatewayBinding(hostURL, workspaceURL, token string) toolgateway.Binding {
	return toolgateway.Binding{
		Transport:         toolgateway.TransportHTTP,
		HostEndpoint:      hostURL,
		WorkspaceEndpoint: workspaceURL,
		Token:             token,
		LifecycleOwner:    toolgateway.LifecycleOwnerServeBoot,
		Source:            toolgateway.SourceBoundMCPListener,
	}
}
