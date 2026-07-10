package toolgateway

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

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

func TestRuntimeOwnedBindingProvenanceIsInMemoryAndMutationSensitive(t *testing.T) {
	binding, err := NewRuntimeOwnedBinding(
		TransportHTTP,
		"http://127.0.0.1:18082",
		"http://host.docker.internal:18082",
		"gateway-token",
		LifecycleOwnerServeBoot,
		SourceBoundMCPListener,
	)
	if err != nil {
		t.Fatalf("NewRuntimeOwnedBinding: %v", err)
	}
	if !binding.IsRuntimeOwned() {
		t.Fatal("constructor did not establish runtime ownership")
	}

	mutated := binding
	mutated.Token = "copied-token"
	if mutated.IsRuntimeOwned() {
		t.Fatal("mutated binding retained runtime ownership")
	}

	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded Binding
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.IsRuntimeOwned() {
		t.Fatal("serialized binding acquired runtime ownership")
	}
}

func TestRuntimeOwnedBindingRejectsUnknownOwnershipPair(t *testing.T) {
	_, err := NewRuntimeOwnedBinding(
		TransportHTTP,
		"http://127.0.0.1:18082",
		"http://host.docker.internal:18082",
		"gateway-token",
		"configured",
		"mcp_servers",
	)
	if err == nil {
		t.Fatal("generic configuration ownership pair was accepted")
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

func TestGenerateAuthTokenReturnsURLSafeToken(t *testing.T) {
	token, err := GenerateAuthToken()
	if err != nil {
		t.Fatalf("GenerateAuthToken: %v", err)
	}
	if got, want := len(token), base64.RawURLEncoding.EncodedLen(AuthTokenBytes); got != want {
		t.Fatalf("token length = %d, want %d", got, want)
	}
	if _, err := base64.RawURLEncoding.DecodeString(token); err != nil {
		t.Fatalf("token is not raw URL-safe base64: %v", err)
	}
}
