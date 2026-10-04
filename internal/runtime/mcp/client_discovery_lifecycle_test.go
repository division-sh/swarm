package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestMCPAdmissionHelperProcess(t *testing.T) {
	mode := ""
	for i, arg := range os.Args {
		if arg == "mcp-admission-helper" && i+1 < len(os.Args) {
			mode = os.Args[i+1]
		}
	}
	if mode == "" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request RPCRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			os.Exit(91)
		}
		var result any
		switch request.Method {
		case "initialize":
			if mode == "hang" {
				time.Sleep(time.Hour)
			}
			if mode == "exit" {
				os.Exit(9)
			}
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}}
		case "notifications/initialized":
			continue
		case "tools/list":
			if mode == "malformed" {
				result = map[string]any{"tools": []any{7}}
			} else {
				result = map[string]any{"tools": []any{map[string]any{
					"name": "ping", "description": "discovery only",
					"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
				}}}
			}
		default:
			// In particular, discovery must never invoke tools/call.
			os.Exit(97)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}); err != nil {
			os.Exit(92)
		}
	}
	os.Exit(0)
}

func mcpAdmissionStdioSource(mode string) semanticview.Source {
	return semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"configured": map[string]any{
			"transport": "stdio", "command": os.Args[0], "args": []string{"-test.run=^TestMCPAdmissionHelperProcess$", "--", "mcp-admission-helper", mode}, "prefix": "configured",
		}}},
	}}})
}

func TestMCPDiscoveryClosesAndJoinsStdioProcess(t *testing.T) {
	client := NewClient(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if errs := client.Refresh(ctx, mcpAdmissionStdioSource("valid"), DiscoveryOptions{}); len(errs) != 0 {
		t.Fatalf("discovery: %v", errs)
	}
	stdio := client.servers["configured"].stdio
	if len(client.DiscoveredTools()) != 1 {
		t.Fatal("valid catalog was lost")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if stdio.cmd.ProcessState == nil {
		t.Fatal("owned subprocess was not joined")
	}
	select {
	case <-stdio.stderrDone:
	default:
		t.Fatal("stderr reader survived cleanup")
	}
	if len(client.DiscoveredTools()) != 0 {
		t.Fatal("closed client still exposes an executable catalog")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("repeat cleanup: %v", err)
	}
	if errs := client.Refresh(ctx, mcpAdmissionStdioSource("valid"), DiscoveryOptions{}); len(errs) == 0 {
		t.Fatal("closed client reacquired a subprocess")
	}
}

func TestMCPDiscoveryRefreshJoinsSameNameReplacement(t *testing.T) {
	client := NewClient(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	source := mcpAdmissionStdioSource("valid")
	if errs := client.Refresh(ctx, source, DiscoveryOptions{}); len(errs) != 0 {
		t.Fatalf("first discovery: %v", errs)
	}
	previous := client.servers["configured"].stdio
	if errs := client.Refresh(ctx, source, DiscoveryOptions{}); len(errs) != 0 {
		t.Fatalf("replacement discovery: %v", errs)
	}
	if client.servers["configured"].stdio == previous || previous.cmd.ProcessState == nil {
		t.Fatal("same-name refresh leaked the replaced subprocess")
	}
}

func TestMCPDiscoveryStdioFailureHasNoSurvivingCatalog(t *testing.T) {
	for _, mode := range []string{"hang", "malformed", "exit"} {
		t.Run(mode, func(t *testing.T) {
			client := NewClient(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			started := time.Now()
			errs := client.Refresh(ctx, mcpAdmissionStdioSource(mode), DiscoveryOptions{})
			if len(errs) == 0 || len(client.DiscoveredTools()) != 0 || len(client.servers) != 0 {
				t.Fatalf("failed discovery passed or retained resources: errors=%v", errs)
			}
			if elapsed := time.Since(started); elapsed > 5*time.Second {
				t.Fatalf("discovery did not respect cancellation and cleanup bounds: %s", elapsed)
			}
			if err := client.Close(context.Background()); (err != nil) != (mode == "exit") {
				t.Fatalf("cleanup outcome for %s: %v", mode, err)
			}
		})
	}
}

func TestMCPStdioCancellationJoinsOwnedReaders(t *testing.T) {
	stdio, err := newStdioRPCClient(os.Args[0], []string{"-test.run=^TestMCPAdmissionHelperProcess$", "--", "mcp-admission-helper", "hang"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(unmanagedMCPTestContext(), 100*time.Millisecond)
	defer cancel()
	_, err = stdio.Call(ctx, ServerConfig{Name: "configured", Transport: "stdio"}, RPCRequest{JSONRPC: "2.0", Method: "initialize", ID: 1})
	if err == nil {
		t.Fatal("hanging process passed initialization")
	}
	if stdio.cmd.ProcessState == nil {
		t.Fatal("cancellation returned without joining the process")
	}
	select {
	case <-stdio.stderrDone:
	default:
		t.Fatal("cancellation returned without joining stderr")
	}
	if err := stdio.Close(context.Background()); err != nil {
		t.Fatalf("repeat cleanup: %v", err)
	}
}

func TestMCPDiscoveryHTTPUsesOnlyDiscoveryMethods(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"capabilities": map[string]any{}}
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
			return
		case "tools/list":
			result = map[string]any{"tools": []any{}}
		default:
			t.Errorf("forbidden observation method %s", request.Method)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}))
	defer server.Close()
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"configured": map[string]any{"url": server.URL, "prefix": "configured"}}},
	}}})
	client := NewClient(nil)
	if errs := client.Refresh(context.Background(), source, DiscoveryOptions{}); len(errs) != 0 {
		t.Fatalf("discovery: %v", errs)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(methods, ",") != "initialize,notifications/initialized,tools/list" {
		t.Fatalf("methods=%v", methods)
	}
}

type mcpCleanupFailureBody struct{ io.Reader }

func (mcpCleanupFailureBody) Close() error { return errors.New("owned response close failed") }

type mcpCleanupFailureTransport struct{}

func (mcpCleanupFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var rpc RPCRequest
	if err := json.NewDecoder(request.Body).Decode(&rpc); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": map[string]any{"capabilities": map[string]any{}}})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: mcpCleanupFailureBody{Reader: bytes.NewReader(raw)}}, nil
}

func TestMCPDiscoveryReportsHTTPResponseCleanupFailure(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"configured": map[string]any{"url": "http://configured.invalid", "prefix": "configured"}}},
	}}})
	client := NewClient(nil)
	client.httpClient = &http.Client{Transport: mcpCleanupFailureTransport{}}
	errs := client.Refresh(context.Background(), source, DiscoveryOptions{})
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "response cleanup failed") || len(client.DiscoveredTools()) != 0 {
		t.Fatalf("cleanup failure was lost: %v", errs)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMCPDiscoveryCloseCancelsAndJoinsInflightHTTP(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Method != "initialize" {
			t.Errorf("unexpected method: %s", request.Method)
			return
		}
		close(started)
		<-r.Context().Done()
		close(finished)
	}))
	defer server.Close()
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"configured": map[string]any{"url": server.URL, "prefix": "configured"}}},
	}}})
	client := NewClient(nil)
	done := make(chan []error, 1)
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("deferred cleanup: %v", err)
		}
	}()
	go func() { done <- client.Refresh(context.Background(), source, DiscoveryOptions{}) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("discovery never reached the configured endpoint")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case errs := <-done:
		if len(errs) == 0 {
			t.Fatal("cancelled discovery passed")
		}
	case <-ctx.Done():
		t.Fatal("close did not join discovery")
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("HTTP observation survived cleanup")
	}
	if len(client.DiscoveredTools()) != 0 {
		t.Fatal("closed discovery retained an executable catalog")
	}
}

func TestMCPDiscoveryUsesSeparatePerServerBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path == "/hang" {
			<-r.Context().Done()
			return
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"capabilities": map[string]any{}}
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
			return
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{"name": "ping", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}}}}
		default:
			t.Errorf("observation invoked %s", request.Method)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}))
	defer server.Close()
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{
			"a_hang":  map[string]any{"url": server.URL + "/hang", "prefix": "hang"},
			"b_valid": map[string]any{"url": server.URL + "/valid", "prefix": "valid"},
		}},
	}}})
	client := NewClient(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := client.Refresh(ctx, source, DiscoveryOptions{ServerTimeout: 250 * time.Millisecond})
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "a_hang") {
		t.Fatalf("per-server timeout evidence: %v", errs)
	}
	if ctx.Err() != nil || len(client.DiscoveredTools()) != 1 || client.DiscoveredTools()["valid.ping"].ServerName != "b_valid" {
		t.Fatal("one server's budget cancelled the next observation or the caller")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}
