package llm

import "github.com/division-sh/swarm/internal/runtime/toolgateway"

func testToolGatewayBinding(hostURL, workspaceURL, token string) toolgateway.Binding {
	binding, err := toolgateway.NewRuntimeOwnedBinding(
		toolgateway.TransportHTTP,
		hostURL,
		workspaceURL,
		token,
		toolgateway.LifecycleOwnerServeBoot,
		toolgateway.SourceBoundMCPListener,
	)
	if err == nil {
		return binding
	}
	return toolgateway.Binding{
		Transport:         toolgateway.TransportHTTP,
		HostEndpoint:      hostURL,
		WorkspaceEndpoint: workspaceURL,
		Token:             token,
		LifecycleOwner:    toolgateway.LifecycleOwnerServeBoot,
		Source:            toolgateway.SourceBoundMCPListener,
	}
}
