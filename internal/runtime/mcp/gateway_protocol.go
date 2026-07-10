package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
)

const (
	contextTokenHeader = "X-SWARM-Context-Token"

	maxToolResultBytes        = 16 * 1024
	maxReadFileResultBytes    = 256 * 1024
	maxToolResultPreviewRunes = 1200
	MaxWireResponseBytes      = maxReadFileResultBytes
)

type ToolGatewayRequest struct {
	Input any `json:"input"`
}

type ToolGatewayResponse struct {
	OK           bool                 `json:"ok"`
	Result       any                  `json:"result,omitempty"`
	Error        any                  `json:"error,omitempty"`
	RuntimeError *RuntimeErrorPayload `json:"runtimeError,omitempty"`
}

type RPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
	ID      any            `json:"id,omitempty"`
}

type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func DecodeRPCResponse(raw []byte, expectedID any) (RPCResponse, error) {
	var wire struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return RPCResponse{}, fmt.Errorf("decode JSON-RPC response: %w", err)
	}
	if wire.JSONRPC != "2.0" {
		return RPCResponse{}, fmt.Errorf("JSON-RPC response version must be 2.0")
	}
	if len(wire.ID) == 0 || !rpcResponseIDEqual(wire.ID, expectedID) {
		return RPCResponse{}, fmt.Errorf("JSON-RPC response id does not match request")
	}
	if (len(wire.Result) == 0) == (len(wire.Error) == 0) {
		return RPCResponse{}, fmt.Errorf("JSON-RPC response must contain exactly one of result or error")
	}
	response := RPCResponse{JSONRPC: wire.JSONRPC, ID: expectedID}
	if len(wire.Result) != 0 {
		if err := json.Unmarshal(wire.Result, &response.Result); err != nil {
			return RPCResponse{}, fmt.Errorf("decode JSON-RPC result: %w", err)
		}
		return response, nil
	}
	var errorWire struct {
		Code    *int    `json:"code"`
		Message *string `json:"message"`
		Data    any     `json:"data,omitempty"`
	}
	if err := decodeStrictJSON(wire.Error, &errorWire); err != nil {
		return RPCResponse{}, fmt.Errorf("decode JSON-RPC error: %w", err)
	}
	if errorWire.Code == nil || errorWire.Message == nil {
		return RPCResponse{}, fmt.Errorf("JSON-RPC error requires numeric code and string message")
	}
	rpcErr := RPCError{Code: *errorWire.Code, Message: *errorWire.Message, Data: errorWire.Data}
	response.Error = &rpcErr
	return response, nil
}

func rpcResponseIDEqual(raw json.RawMessage, expected any) bool {
	expectedRaw, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	var actualCompact bytes.Buffer
	if err := json.Compact(&actualCompact, raw); err != nil {
		return false
	}
	var expectedCompact bytes.Buffer
	if err := json.Compact(&expectedCompact, expectedRaw); err != nil {
		return false
	}
	return bytes.Equal(actualCompact.Bytes(), expectedCompact.Bytes())
}

type ToolDef struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputSchema any    `json:"inputSchema"`
}

type ToolResultRelayRef struct {
	Path       string   `json:"path"`
	Chunks     []string `json:"chunks,omitempty"`
	ReadTool   string   `json:"read_tool"`
	Format     string   `json:"format"`
	Visibility string   `json:"visibility"`
}

type OversizedToolResultRelayWriter interface {
	PersistOversizedToolResultRelay(ctx context.Context, toolName string, rawJSON []byte) (ToolResultRelayRef, error)
}

func ParseToolListHeader(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, p := range strings.Split(raw, ",") {
		name := toolidentity.CanonicalName(p)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func ContextTokenFromRequest(r *http.Request) string {
	return headerValue(r, contextTokenHeader)
}

func ToolResultText(v any) string {
	switch t := v.(type) {
	case nil:
		return "ok"
	case string:
		if strings.TrimSpace(t) == "" {
			return "ok"
		}
		return t
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(raw)
	}
}

func headerValue(r *http.Request, key string) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.Header.Get(key))
}

func WriteJSON(w http.ResponseWriter, status int, payload ToolGatewayResponse) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func WriteRPCResult(w http.ResponseWriter, id any, result any) {
	resp := RPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	raw, _ := json.Marshal(resp)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("mcp-protocol-version", "2025-03-26")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func WriteRPCError(w http.ResponseWriter, id any, code int, message string) {
	writeRPCError(w, id, code, message, nil)
}

func WriteRPCErrorForRuntimeError(w http.ResponseWriter, id any, code int, err error) {
	message := "mcp error"
	var data any
	if runtimeErr := RuntimeErrorPayloadFromError(err); runtimeErr != nil {
		data = map[string]any{"runtimeError": runtimeErr}
		if runtimeErr.Failure != nil {
			message = runtimeErr.Failure.Message
		} else if runtimeErr.Protocol != nil {
			message = runtimeErr.Protocol.Message
		}
	}
	writeRPCError(w, id, code, message, data)
}

func writeRPCError(w http.ResponseWriter, id any, code int, message string, data any) {
	resp := RPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
			Code:    code,
			Message: strings.TrimSpace(message),
			Data:    data,
		},
	}
	if strings.TrimSpace(resp.Error.Message) == "" {
		resp.Error.Message = "mcp error"
	}
	raw, _ := json.Marshal(resp)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("mcp-protocol-version", "2025-03-26")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
