package mcp

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

type mcpEffectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
}

func (r mcpEffectRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Helper()
	if err := r.harness.RequireState("mcp_tools_call_http", runtimeeffects.StateLaunched); err != nil {
		r.t.Fatal(err)
	}
	return nil, errors.New("injected MCP HTTP transport failure")
}

type mcpEffectWriteCloser struct {
	t       *testing.T
	harness *effecttest.Harness
}

func (w mcpEffectWriteCloser) Write([]byte) (int, error) {
	w.t.Helper()
	if err := w.harness.RequireState("mcp_tools_call_stdio", runtimeeffects.StateLaunched); err != nil {
		w.t.Fatal(err)
	}
	return 0, errors.New("injected MCP stdio write failure")
}

func (mcpEffectWriteCloser) Close() error { return nil }

func TestManagedMCPEffectOutcomes(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		harness := effecttest.New()
		client := &Client{httpClient: &http.Client{Transport: mcpEffectRoundTripper{t: t, harness: harness}}}
		_, err := client.callHTTPServer(harness.Context("mcp-http"), ServerConfig{Name: "effect-server", URL: "http://effect.test"}, RPCRequest{Method: "tools/call"})
		if err == nil {
			t.Fatal("MCP HTTP transport failure returned nil")
		}
		if err := harness.RequireState("mcp_tools_call_http", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		staleClient := &Client{httpClient: &http.Client{Transport: mcpEffectRoundTripper{t: t, harness: stale}}}
		if _, err := staleClient.callHTTPServer(stale.Context("mcp-http-stale"), ServerConfig{Name: "effect-server", URL: "http://effect.test"}, RPCRequest{Method: "tools/call"}); err == nil {
			t.Fatal("stale MCP HTTP effect was admitted")
		}
		if _, launched := stale.StateForAdapter("mcp_tools_call_http"); launched {
			t.Fatal("stale MCP HTTP effect reached dispatch")
		}
	})

	t.Run("stdio", func(t *testing.T) {
		harness := effecttest.New()
		client := &stdioRPCClient{
			stdin:  mcpEffectWriteCloser{t: t, harness: harness},
			stdout: bufio.NewReader(strings.NewReader("")),
		}
		_, err := client.Call(harness.Context("mcp-stdio"), ServerConfig{Name: "effect-server"}, RPCRequest{Method: "tools/call"})
		if err == nil {
			t.Fatal("MCP stdio write failure returned nil")
		}
		if err := harness.RequireState("mcp_tools_call_stdio", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		staleClient := &stdioRPCClient{stdin: mcpEffectWriteCloser{t: t, harness: stale}, stdout: bufio.NewReader(strings.NewReader(""))}
		if _, err := staleClient.Call(stale.Context("mcp-stdio-stale"), ServerConfig{Name: "effect-server"}, RPCRequest{Method: "tools/call"}); err == nil {
			t.Fatal("stale MCP stdio effect was admitted")
		}
		if _, launched := stale.StateForAdapter("mcp_tools_call_stdio"); launched {
			t.Fatal("stale MCP stdio effect reached write")
		}
	})
}

var _ io.WriteCloser = mcpEffectWriteCloser{}
