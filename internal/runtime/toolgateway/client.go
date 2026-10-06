package toolgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

const ObservationBytes = 2 << 20

// Owned gateway results carry the projected JSON value separately from MCP
// display text, whose contents cannot establish a value's type.
const ProjectedResultMetaKey = "swarm/result"

// HTTPObservation is performed by the workspace child, not by a host-side
// readiness reader. Tokens travel on the private child input, never argv.
type HTTPObservation struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type ListedDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type observationRPCError struct {
	Code int `json:"code"`
	Data struct {
		RuntimeError struct {
			Protocol *struct {
				Code string `json:"code"`
			} `json:"protocol_error"`
		} `json:"runtimeError"`
	} `json:"data"`
}

type observationRPCResponse struct {
	JSONRPC string               `json:"jsonrpc"`
	ID      int                  `json:"id"`
	Result  json.RawMessage      `json:"result"`
	Error   *observationRPCError `json:"error"`
}

func (o HTTPObservation) Probe(ctx context.Context) ([]ListedDefinition, error) {
	if err := o.Initialize(ctx); err != nil {
		return nil, err
	}
	var list struct {
		Tools      *[]ListedDefinition `json:"tools"`
		NextCursor string              `json:"nextCursor"`
	}
	if err := o.rpc(ctx, "tools/list", map[string]any{}, &list); err != nil {
		return nil, err
	}
	if list.Tools == nil || list.NextCursor != "" {
		return nil, o.mismatch("gateway_inventory_invalid")
	}
	return *list.Tools, nil
}

// Initialize earns network/authentication evidence only. It cannot list or
// execute tools and does not require a fabricated turn context for diagnostics.
func (o HTTPObservation) Initialize(ctx context.Context) error {
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := o.rpc(ctx, "initialize", map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "swarm-workspace", "version": "1"}}, &init); err != nil {
		return err
	}
	if init.ProtocolVersion != "2025-03-26" {
		return o.mismatch("gateway_protocol_mismatch")
	}
	return nil
}

func (o HTTPObservation) Call(ctx context.Context, name string, arguments any, occurrence string) (json.RawMessage, error) {
	if name == "" || occurrence == "" {
		return nil, o.mismatch("gateway_call_identity_missing")
	}
	var result json.RawMessage
	err := o.rpc(ctx, "tools/call", map[string]any{
		"name": name, "arguments": arguments,
		"_meta": map[string]any{"claudecode/toolUseId": occurrence},
	}, &result)
	return result, err
}

func (o HTTPObservation) rpc(ctx context.Context, method string, params any, result any) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return o.unavailable("deadline_before_dispatch", 0)
		}
		return err
	}
	endpoint := NormalizeMCPServerURL(o.URL)
	if endpoint == "" || o.Headers["Authorization"] == "" || (method != "initialize" && o.Headers["X-SWARM-Context-Token"] == "") {
		return o.unavailable("binding_invalid", 0)
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return o.mismatch("gateway_request_invalid")
	}
	if len(body) > ObservationBytes {
		return o.mismatch("gateway_request_over_budget")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return o.unavailable("binding_invalid", 0)
	}
	for name, value := range o.Headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("MCP-Protocol-Version", "2025-03-26")
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	// Discovery is bounded independently; business calls retain the worker's
	// caller-owned lifetime instead of inheriting the observation deadline.
	if method != "tools/call" {
		client.Timeout = 5 * time.Second
	}
	resp, err := client.Do(req)
	if err != nil {
		if method == "tools/call" {
			return o.uncertain("response_not_observed", err)
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		return o.unavailable("connect_or_timeout", 0)
	}
	defer resp.Body.Close()
	return o.consumeRPCResponse(method, resp, result)
}

func (o HTTPObservation) consumeRPCResponse(method string, resp *http.Response, result any) error {
	if resp.StatusCode != http.StatusOK {
		if method == "tools/call" {
			return o.uncertain("http_refusal_after_dispatch", nil)
		}
		return o.unavailable("http_refusal", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, ObservationBytes+1))
	if err != nil {
		if method == "tools/call" {
			return o.uncertain("response_interrupted", err)
		}
		return o.unavailable("response_interrupted", resp.StatusCode)
	}
	if len(raw) > ObservationBytes {
		if method == "tools/call" {
			return o.uncertain("response_over_budget", nil)
		}
		return o.mismatch("gateway_response_over_budget")
	}
	envelope, err := o.decodeRPCResponse(method, raw)
	if err != nil {
		return err
	}
	if envelope.Error != nil {
		return o.rpcResponseFailure(method, resp.StatusCode, envelope.Error)
	}
	if len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) || json.Unmarshal(envelope.Result, result) != nil {
		if method == "tools/call" {
			return o.uncertain("result_invalid", nil)
		}
		return o.mismatch("gateway_result_invalid")
	}
	return nil
}

func (o HTTPObservation) decodeRPCResponse(method string, raw []byte) (observationRPCResponse, error) {
	var envelope observationRPCResponse
	if _, err := canonicaljson.Decode(raw); err != nil {
		if method == "tools/call" {
			return envelope, o.uncertain("response_invalid", nil)
		}
		return envelope, o.mismatch("gateway_response_invalid")
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.JSONRPC != "2.0" {
		if method == "tools/call" {
			return envelope, o.uncertain("response_invalid", nil)
		}
		return envelope, o.mismatch("gateway_response_invalid")
	}
	if envelope.ID != 1 {
		if method == "tools/call" {
			return envelope, o.uncertain("response_identity_foreign", nil)
		}
		// Discovery cannot execute an effect; the gateway may reject its boot
		// authentication before parsing the request and therefore use a null ID.
		if envelope.Error == nil {
			return envelope, o.mismatch("gateway_response_invalid")
		}
	}
	if envelope.Error != nil && len(envelope.Result) != 0 {
		if method == "tools/call" {
			return envelope, o.uncertain("response_result_and_error", nil)
		}
		return envelope, o.mismatch("gateway_response_invalid")
	}
	return envelope, nil
}

func (o HTTPObservation) rpcResponseFailure(method string, httpStatus int, failure *observationRPCError) error {
	if failure.Code == -32001 {
		return o.unavailable("authentication_refused", httpStatus)
	}
	if failure.Code == -32003 && failure.Data.RuntimeError.Protocol != nil {
		switch failure.Data.RuntimeError.Protocol.Code {
		case "mcp_context_token_missing", "mcp_context_token_not_found", "mcp_context_token_stale_epoch":
			return o.unavailable("context_authentication_refused", httpStatus)
		}
	}
	if method == "tools/call" && failure.Code != -32600 && failure.Code != -32601 && failure.Code != -32602 {
		return o.uncertain("unclassified_rpc_failure", nil)
	}
	return o.mismatch("gateway_context_or_definition_refused")
}

func (o HTTPObservation) uncertain(status string, cause error) error {
	// Network/decoder errors can contain tokens or private request bytes. Only
	// preserve cancellation causality, not their diagnostic text.
	switch {
	case errors.Is(cause, context.Canceled):
		cause = context.Canceled
	case errors.Is(cause, context.DeadlineExceeded):
		cause = context.DeadlineExceeded
	default:
		cause = nil
	}
	return failures.Wrap(failures.ClassOutcomeUncertain, "workspace_tool_outcome_uncertain", "workspace-mcp", "execute", map[string]any{"endpoint": RedactedEndpoint(o.URL), "status": status}, cause)
}

func (o HTTPObservation) unavailable(status string, httpStatus int) error {
	return failures.New(failures.ClassDependencyUnavailable, "workspace_gateway_unreachable", "workspace-mcp", "observe", map[string]any{"endpoint": RedactedEndpoint(o.URL), "status": status, "http_status": httpStatus})
}

func (o HTTPObservation) mismatch(code string) error {
	return failures.New(failures.ClassSchemaInvalid, "managed_capability_mcp_definition_mismatch", "workspace-mcp", "observe", map[string]any{"endpoint": RedactedEndpoint(o.URL), "reason": code})
}

func RedactedEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return "<invalid>"
	}
	return fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.EscapedPath())
}
