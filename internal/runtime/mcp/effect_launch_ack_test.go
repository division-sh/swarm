package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

type mcpLaunchCommitProbe struct {
	*effecttest.Harness
	fault  error
	cancel context.CancelFunc
	joined bool
}

func (p *mcpLaunchCommitProbe) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) error {
	if err := p.Harness.MarkExternalAttemptLaunched(ctx, attempt, at); err != nil {
		return err
	}
	if p.cancel != nil {
		p.cancel()
	}
	err := runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationLaunch, attempt, p.fault)
	if p.joined {
		return errors.Join(err, context.Canceled)
	}
	return err
}

func mcpLaunchCommitContext(harness *effecttest.Harness, identity string, cancelAfterCommit bool, joined ...bool) context.Context {
	ctx := harness.CompletionContext(identity)
	probe := &mcpLaunchCommitProbe{Harness: harness, fault: errors.New("injected MCP launch cleanup failure")}
	if len(joined) != 0 {
		probe.joined = joined[0]
	}
	if cancelAfterCommit {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		probe.cancel = cancel
	}
	return runtimeeffects.WithController(ctx, liveTestEffectController(probe))
}

type mcpLaunchTransport struct {
	calls *int
}

func (tr mcpLaunchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	*tr.calls++
	var rpc RPCRequest
	if err := json.NewDecoder(req.Body).Decode(&rpc); err != nil {
		return nil, err
	}
	response, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": rpc.ID,
		"result": map[string]any{"content": []any{}, "structuredContent": map[string]any{"ok": true}},
	})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(response)))}, nil
}

type mcpLaunchWriter struct {
	calls *int
}

func (w mcpLaunchWriter) Write(raw []byte) (int, error) {
	*w.calls++
	return len(raw), nil
}

func (mcpLaunchWriter) Close() error { return nil }

func TestMCPLaunchPostCommitCleanupExecutesOnePrimitive(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		harness := effecttest.New()
		ctx := mcpLaunchCommitContext(harness, "mcp-launch-http", false)
		calls := 0
		client := NewClient(nil)
		client.httpClient = &http.Client{Transport: mcpLaunchTransport{calls: &calls}}
		client.servers["effect"] = &registeredServer{cfg: ServerConfig{Name: "effect", Transport: "http", URL: "http://effect.test"}}
		client.tools["effect.ping"] = DiscoveredTool{Name: "effect.ping", RemoteName: "ping", ServerName: "effect"}
		result, err := client.Call(ctx, "effect.ping", map[string]any{})
		if err != nil || calls != 1 || result == nil {
			t.Fatalf("MCP HTTP result=%+v calls=%d err=%v", result, calls, err)
		}
		if err := harness.RequireState("mcp_tools_call_http", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
		if len(harness.Settlements) != 1 {
			t.Fatalf("MCP HTTP settlements=%d, want one", len(harness.Settlements))
		}
	})
	t.Run("stdio", func(t *testing.T) {
		harness := effecttest.New()
		ctx := mcpLaunchCommitContext(harness, "mcp-launch-stdio", false)
		calls := 0
		client := &stdioRPCClient{stdin: mcpLaunchWriter{calls: &calls}, stdout: bufio.NewReader(strings.NewReader(""))}
		_, err := client.Call(ctx, ServerConfig{Name: "effect"}, RPCRequest{Method: "notifications/test"})
		if err != nil || calls != 1 {
			t.Fatalf("MCP stdio calls=%d err=%v", calls, err)
		}
		if err := harness.RequireState("mcp_tools_call_stdio", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
		if len(harness.Settlements) != 1 {
			t.Fatalf("MCP stdio settlements=%d, want one", len(harness.Settlements))
		}
	})
}

func TestMCPLaunchPostCommitCancellationRecordsNoDispatch(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		harness := effecttest.New()
		ctx := mcpLaunchCommitContext(harness, "mcp-canceled-http", true)
		calls := 0
		client := &Client{httpClient: &http.Client{Transport: mcpLaunchTransport{calls: &calls}}}
		_, err := client.callHTTPServer(ctx, ServerConfig{Name: "effect", URL: "http://effect.test"}, RPCRequest{Method: "tools/call", ID: "call-1"})
		if !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("canceled MCP HTTP calls=%d err=%v", calls, err)
		}
		if err := harness.RequireState("mcp_tools_call_http", runtimeeffects.StateTerminalFailure); err != nil {
			t.Fatal(err)
		}
		requireMCPNoDispatchSettlement(t, harness, err)
	})
	t.Run("stdio", func(t *testing.T) {
		harness := effecttest.New()
		ctx := mcpLaunchCommitContext(harness, "mcp-canceled-stdio", true)
		calls := 0
		client := &stdioRPCClient{stdin: mcpLaunchWriter{calls: &calls}, stdout: bufio.NewReader(strings.NewReader(""))}
		_, err := client.Call(ctx, ServerConfig{Name: "effect"}, RPCRequest{Method: "notifications/test"})
		if !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("canceled MCP stdio calls=%d err=%v", calls, err)
		}
		if err := harness.RequireState("mcp_tools_call_stdio", runtimeeffects.StateTerminalFailure); err != nil {
			t.Fatal(err)
		}
		requireMCPNoDispatchSettlement(t, harness, err)
	})
}

func requireMCPNoDispatchSettlement(t *testing.T, harness *effecttest.Harness, err error) {
	t.Helper()
	if len(harness.Settlements) != 1 || len(harness.Attempts) != 1 {
		t.Fatalf("attempts=%d settlements=%d, want one each", len(harness.Attempts), len(harness.Settlements))
	}
	for _, attempt := range harness.Attempts {
		var committed *runtimeeffects.PostCommitMutationError
		if !errors.As(err, &committed) || committed.Phase != runtimeeffects.MutationLaunch ||
			committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID ||
			runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, attempt) {
			t.Fatalf("blocked launch must retain typed diagnostic without granting continuation: %v", err)
		}
	}
	for _, settlement := range harness.Settlements {
		if settlement.Failure == nil || settlement.Failure.Detail.Code != "effect_launch_dispatch_not_attempted" ||
			settlement.Evidence["dispatch_attempted"] != false || settlement.Evidence["launch_rejected"] != true {
			t.Fatalf("blocked launch settlement=%+v, want known no-dispatch", settlement)
		}
	}
}

func TestMCPJoinedLaunchErrorDoesNotDispatchOrSettle(t *testing.T) {
	harness := effecttest.New()
	ctx := mcpLaunchCommitContext(harness, "mcp-joined-launch", false, true)
	calls := 0
	client := &stdioRPCClient{stdin: mcpLaunchWriter{calls: &calls}, stdout: bufio.NewReader(strings.NewReader(""))}
	_, err := client.Call(ctx, ServerConfig{Name: "effect"}, RPCRequest{Method: "notifications/test"})
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("joined launch calls=%d err=%v, want no dispatch", calls, err)
	}
	if err := harness.RequireState("mcp_tools_call_stdio", runtimeeffects.StateLaunched); err != nil {
		t.Fatal(err)
	}
	if len(harness.Settlements) != 0 {
		t.Fatalf("joined launch settlements=%d, want none", len(harness.Settlements))
	}
	for _, attempt := range harness.Attempts {
		if runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, attempt) {
			t.Fatalf("joined launch granted continuation: %v", err)
		}
	}
}

func TestMCPUnacknowledgedLaunchDoesNotDispatchOrSettle(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		t.Run(transport, func(t *testing.T) {
			harness := effecttest.New()
			fenced := errors.New("launch was not acknowledged")
			harness.MarkErr = fenced
			ctx := mcpLaunchCommitContext(harness, "mcp-unacknowledged-"+transport, false)
			calls := 0
			var adapter string
			var err error
			if transport == "http" {
				adapter = "mcp_tools_call_http"
				client := &Client{httpClient: &http.Client{Transport: mcpLaunchTransport{calls: &calls}}}
				_, err = client.callHTTPServer(ctx, ServerConfig{Name: "effect", URL: "http://effect.test"}, RPCRequest{Method: "tools/call", ID: "call-1"})
			} else {
				adapter = "mcp_tools_call_stdio"
				client := &stdioRPCClient{stdin: mcpLaunchWriter{calls: &calls}, stdout: bufio.NewReader(strings.NewReader(""))}
				_, err = client.Call(ctx, ServerConfig{Name: "effect"}, RPCRequest{Method: "notifications/test"})
			}
			if !errors.Is(err, fenced) || calls != 0 {
				t.Fatalf("unacknowledged MCP %s calls=%d err=%v", transport, calls, err)
			}
			if err := harness.RequireState(adapter, runtimeeffects.StateAuthorized); err != nil {
				t.Fatal(err)
			}
			if len(harness.Settlements) != 0 {
				t.Fatalf("unacknowledged MCP %s settlements=%d", transport, len(harness.Settlements))
			}
		})
	}
}
