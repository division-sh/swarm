package toolgateway

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

type Transport string

const (
	TransportHTTP Transport = "http"

	AuthTokenBytes = 32

	LifecycleOwnerServeBoot            = "serve_boot"
	LifecycleOwnerSelectedForkRuntime  = "selected_fork_agent_runtime"
	SourceBoundMCPListener             = "bound_mcp_listener"
	SourceSelectedForkEphemeralGateway = "selected_fork_ephemeral_gateway"
)

type Binding struct {
	Transport         Transport
	HostEndpoint      string
	WorkspaceEndpoint string
	Token             string
	LifecycleOwner    string
	Source            string
}

func (b Binding) Empty() bool {
	return strings.TrimSpace(string(b.Transport)) == "" &&
		strings.TrimSpace(b.HostEndpoint) == "" &&
		strings.TrimSpace(b.WorkspaceEndpoint) == "" &&
		strings.TrimSpace(b.Token) == ""
}

func (b Binding) Validate() error {
	if b.Empty() {
		return fmt.Errorf("tool gateway binding is required")
	}
	if b.Transport != TransportHTTP {
		return fmt.Errorf("tool gateway binding transport must be %q", TransportHTTP)
	}
	if NormalizeMCPServerURL(b.HostEndpoint) == "" {
		return fmt.Errorf("tool gateway binding host endpoint must be a valid http(s) MCP URL")
	}
	if NormalizeMCPServerURL(b.WorkspaceEndpoint) == "" {
		return fmt.Errorf("tool gateway binding workspace endpoint must be a valid http(s) MCP URL")
	}
	if strings.TrimSpace(b.Token) == "" {
		return fmt.Errorf("tool gateway binding token is required")
	}
	return nil
}

func (b Binding) HostMCPURL() string {
	return NormalizeMCPServerURL(b.HostEndpoint)
}

func (b Binding) WorkspaceMCPURL() string {
	return NormalizeMCPServerURL(b.WorkspaceEndpoint)
}

func (b Binding) AuthToken() string {
	return strings.TrimSpace(b.Token)
}

func GenerateAuthToken() (string, error) {
	raw := make([]byte, AuthTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func NormalizeMCPServerURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return ""
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	path := strings.TrimSpace(u.Path)
	switch path {
	case "", "/":
		u.Path = "/mcp"
	case "/mcp":
	default:
		// Preserve explicit MCP-compatible paths for non-default gateway projections.
	}
	return strings.TrimSpace(u.String())
}
