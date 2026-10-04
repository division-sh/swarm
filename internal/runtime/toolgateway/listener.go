package toolgateway

import (
	"fmt"
	"net"
	"strings"
)

// ListenerEndpoints projects the bound socket, not an unbound configuration
// address. Explicit non-local addresses stay exact for both execution targets.
func ListenerEndpoints(addr net.Addr) (hostURL, workspaceURL string, err error) {
	if addr == nil {
		return "", "", fmt.Errorf("mcp listener address is unavailable")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil || port == "" {
		return "", "", fmt.Errorf("mcp listener must have an exact host and port")
	}
	host = strings.Trim(host, "[]")
	workspaceHost := host
	switch host {
	case "", "0.0.0.0", "127.0.0.1", "localhost":
		host, workspaceHost = "127.0.0.1", "host.docker.internal"
	case "::", "::1":
		host, workspaceHost = "::1", "host.docker.internal"
	}
	return "http://" + net.JoinHostPort(host, port), "http://" + net.JoinHostPort(workspaceHost, port), nil
}
