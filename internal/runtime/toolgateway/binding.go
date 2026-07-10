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

	AuthTokenBytes          = 32
	RetiredAuthTokenEnvName = "SWARM_TOOL_GATEWAY_TOKEN"

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
	runtimeOwned      *runtimeOwnedBinding
}

// runtimeOwnedBinding is intentionally unexported and never serialized. A
// Binding assembled from configuration or copied URL/token values therefore
// cannot acquire local runtime authority.
type runtimeOwnedBinding struct {
	transport         Transport
	hostEndpoint      string
	workspaceEndpoint string
	token             string
	lifecycleOwner    string
	source            string
}

func NewRuntimeOwnedBinding(transport Transport, hostEndpoint, workspaceEndpoint, token, lifecycleOwner, source string) (Binding, error) {
	binding := Binding{
		Transport:         transport,
		HostEndpoint:      hostEndpoint,
		WorkspaceEndpoint: workspaceEndpoint,
		Token:             token,
		LifecycleOwner:    strings.TrimSpace(lifecycleOwner),
		Source:            strings.TrimSpace(source),
	}
	if err := binding.Validate(); err != nil {
		return Binding{}, err
	}
	switch {
	case binding.LifecycleOwner == LifecycleOwnerServeBoot && binding.Source == SourceBoundMCPListener:
	case binding.LifecycleOwner == LifecycleOwnerSelectedForkRuntime && binding.Source == SourceSelectedForkEphemeralGateway:
	default:
		return Binding{}, fmt.Errorf("tool gateway runtime ownership pair %q/%q is not supported", binding.LifecycleOwner, binding.Source)
	}
	binding.runtimeOwned = &runtimeOwnedBinding{
		transport:         binding.Transport,
		hostEndpoint:      binding.HostEndpoint,
		workspaceEndpoint: binding.WorkspaceEndpoint,
		token:             binding.Token,
		lifecycleOwner:    binding.LifecycleOwner,
		source:            binding.Source,
	}
	return binding, nil
}

func (b Binding) IsRuntimeOwned() bool {
	return b.runtimeOwned != nil &&
		b.Transport == b.runtimeOwned.transport &&
		b.HostEndpoint == b.runtimeOwned.hostEndpoint &&
		b.WorkspaceEndpoint == b.runtimeOwned.workspaceEndpoint &&
		b.Token == b.runtimeOwned.token &&
		b.LifecycleOwner == b.runtimeOwned.lifecycleOwner &&
		b.Source == b.runtimeOwned.source
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

func RetiredAuthTokenEnvError() error {
	return fmt.Errorf("%s is retired and not accepted as gateway token configuration; unset %s because Swarm generates tool gateway auth tokens from ToolGatewayBinding", RetiredAuthTokenEnvName, RetiredAuthTokenEnvName)
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
