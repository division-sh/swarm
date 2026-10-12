package cliapp

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
)

func TestLocalPreflightUsesRetainedAPIListenerAndDetectsForeignHolders(t *testing.T) {
	for _, mode := range []string{"retained", "foreign_api", "foreign_mcp"} {
		t.Run(mode, func(t *testing.T) {
			api, err := ListenServeHTTPListener("api", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = api.Close() })
			addr := api.Addr().String()
			req := localPreflightRequest{Mode: "serve", SourceFree: true,
				Config:         &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}},
				CheckListeners: true, APIListener: api, APIListenAddr: addr, MCPListenAddr: "127.0.0.1:0"}
			if mode == "foreign_api" {
				req.APIListener = nil
			}
			if mode == "foreign_mcp" {
				mcp, err := ListenServeHTTPListener("mcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = mcp.Close() })
				req.MCPListenAddr = mcp.Addr().String()
			}
			report := runLocalClaudeCLIPreflight(context.Background(), req)
			if report.HasBlockers() != (mode != "retained") {
				t.Fatalf("%s preflight: %+v", mode, report)
			}
			want := "api_listener"
			if mode == "foreign_mcp" {
				want = "mcp_listener"
			}
			matched := false
			for _, finding := range report.Findings {
				if finding.Code == want {
					matched = true
					if mode == "retained" {
						if finding.Status != LocalPreflightStatusOK || !strings.Contains(finding.Message, addr) {
							t.Fatalf("missing retained binding evidence: %+v", finding)
						}
					} else if finding.Status != LocalPreflightStatusFailed || !strings.Contains(finding.Message, "bind") {
						t.Fatalf("missing exact foreign bind refusal: %+v", finding)
					}
				}
			}
			if !matched {
				t.Fatalf("missing %s finding: %+v", want, report)
			}
			if other, err := net.Listen("tcp", addr); err == nil {
				_ = other.Close()
				t.Fatal("preflight closed the retained or foreign API listener")
			}
			if err := api.Close(); err != nil {
				t.Fatal(err)
			}
			reclaimed, err := ListenServeHTTPListener("api", addr)
			if err != nil {
				t.Fatalf("listener not reusable after actual closure: %v", err)
			}
			if err := reclaimed.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
