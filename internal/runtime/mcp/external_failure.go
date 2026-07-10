package mcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

var remoteEvidenceCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func externalMCPAttributes(cfg ServerConfig, req RPCRequest) map[string]any {
	attributes := map[string]any{}
	if server := strings.TrimSpace(cfg.Name); server != "" {
		attributes["server"] = server
	}
	if req.Method == "tools/call" {
		if tool := strings.TrimSpace(asString(req.Params["name"])); tool != "" {
			attributes["tool"] = tool
		}
	}
	return attributes
}

func externalMCPFailure(class runtimefailures.Class, detail string, cfg ServerConfig, req RPCRequest, additional map[string]any, cause error) error {
	attributes := externalMCPAttributes(cfg, req)
	for key, value := range additional {
		attributes[key] = value
	}
	return runtimefailures.Wrap(class, detail, "mcp-client", strings.TrimSpace(req.Method), attributes, cause)
}

func externalMCPTransportFailure(err error, cfg ServerConfig, req RPCRequest) error {
	if err == nil {
		return nil
	}
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return externalMCPFailure(runtimefailures.ClassTimeout, "mcp_transport_timeout", cfg, req, nil, err)
	}
	return externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_transport_failed", cfg, req, nil, err)
}

func externalMCPHTTPStatusFailure(status int, cfg ServerConfig, req RPCRequest) error {
	attributes := map[string]any{"status": status}
	switch status {
	case http.StatusUnauthorized:
		attributes["auth_kind"] = "mcp_server"
		return externalMCPFailure(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", cfg, req, attributes, nil)
	case http.StatusForbidden:
		attributes["action"] = "mcp_tool_execute"
		return externalMCPFailure(runtimefailures.ClassAuthorizationDenied, "provider_forbidden", cfg, req, attributes, nil)
	case http.StatusPaymentRequired:
		return externalMCPFailure(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", cfg, req, attributes, nil)
	case http.StatusRequestTimeout:
		return externalMCPFailure(runtimefailures.ClassTimeout, "provider_request_timeout", cfg, req, attributes, nil)
	case http.StatusGatewayTimeout:
		return externalMCPFailure(runtimefailures.ClassTimeout, "provider_gateway_timeout", cfg, req, attributes, nil)
	case http.StatusTooManyRequests:
		return externalMCPFailure(runtimefailures.ClassConnectorFailure, "provider_rate_limited", cfg, req, attributes, nil)
	default:
		return externalMCPFailure(runtimefailures.ClassConnectorFailure, "provider_http_status", cfg, req, attributes, nil)
	}
}

func externalMCPWireLimitFailure(actual int, cfg ServerConfig, req RPCRequest) error {
	return externalMCPFailure(runtimefailures.ClassDataLimitExceeded, "mcp_wire_response_limit_exceeded", cfg, req, map[string]any{
		"limit_kind": "mcp_wire_response_bytes",
		"limit":      MaxWireResponseBytes,
		"actual":     actual,
	}, nil)
}

func externalMCPRPCInvalidFailure(err error, cfg ServerConfig, req RPCRequest) error {
	return externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_rpc_response_invalid", cfg, req, nil, err)
}

func externalMCPRPCExecutionFailure(rpcErr *RPCError, cfg ServerConfig, req RPCRequest) error {
	attributes := map[string]any{}
	if rpcErr != nil {
		attributes["rpc_code"] = rpcErr.Code
	}
	if raw, ok := runtimeErrorFromRPCData(rpcErr); ok {
		evidence, err := externalRuntimeEvidence(raw)
		if err != nil {
			return externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_remote_failure_payload_invalid", cfg, req, attributes, err)
		}
		for key, value := range evidence {
			attributes[key] = value
		}
	}
	return externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_remote_rpc_execution_failed", cfg, req, attributes, nil)
}

func runtimeErrorFromRPCData(rpcErr *RPCError) (any, bool) {
	if rpcErr == nil || rpcErr.Data == nil {
		return nil, false
	}
	data, ok := rpcErr.Data.(map[string]any)
	if !ok {
		return nil, false
	}
	raw, ok := data["runtimeError"]
	return raw, ok
}

func externalRuntimeEvidence(raw any) (map[string]any, error) {
	payload, err := DecodeRuntimeErrorPayload(raw)
	if err != nil {
		return nil, err
	}
	evidence := map[string]any{"remote_typed_failure_present": true}
	switch {
	case payload.Failure != nil:
		if code := strings.TrimSpace(payload.Failure.Detail.Code); remoteEvidenceCodePattern.MatchString(code) {
			evidence["remote_detail_code"] = code
		}
	case payload.Protocol != nil:
		if code := strings.TrimSpace(payload.Protocol.Code); remoteEvidenceCodePattern.MatchString(code) {
			evidence["remote_protocol_code"] = code
		}
	default:
		return nil, fmt.Errorf("remote runtime error has no typed evidence")
	}
	return evidence, nil
}

func projectExternalToolCallResult(resp RPCResponse, cfg ServerConfig, req RPCRequest) (any, error) {
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_result_invalid", cfg, req, nil, nil)
	}
	allowed := map[string]struct{}{
		"content": {}, "isError": {}, "structuredContent": {}, "runtimeError": {}, "swarmStartupProbe": {}, "_meta": {},
	}
	for key := range result {
		if _, ok := allowed[key]; !ok {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_result_invalid", cfg, req, nil, nil)
		}
	}
	content, ok := result["content"].([]any)
	if !ok || !validMCPContent(content) {
		return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_result_invalid", cfg, req, nil, nil)
	}
	isError := false
	if raw, present := result["isError"]; present {
		var boolean bool
		boolean, ok = raw.(bool)
		if !ok {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_tool_result_invalid", cfg, req, nil, nil)
		}
		isError = boolean
	}
	runtimeEvidence, hasRuntimeEvidence := result["runtimeError"]
	if !isError {
		if hasRuntimeEvidence {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_remote_failure_payload_invalid", cfg, req, nil, nil)
		}
		if structured, present := result["structuredContent"]; present {
			return structured, nil
		}
		if len(content) == 1 {
			if item, ok := content[0].(map[string]any); ok && strings.TrimSpace(asString(item["type"])) == "text" {
				return asString(item["text"]), nil
			}
		}
		return result, nil
	}
	attributes := map[string]any{}
	if hasRuntimeEvidence {
		evidence, err := externalRuntimeEvidence(runtimeEvidence)
		if err != nil {
			return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_remote_failure_payload_invalid", cfg, req, nil, err)
		}
		for key, value := range evidence {
			attributes[key] = value
		}
	}
	return nil, externalMCPFailure(runtimefailures.ClassConnectorFailure, "mcp_remote_tool_execution_failed", cfg, req, attributes, nil)
}

func validMCPContent(content []any) bool {
	for _, raw := range content {
		item, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		kind := strings.TrimSpace(asString(item["type"]))
		if kind == "" {
			return false
		}
		if kind == "text" {
			if _, ok := item["text"].(string); !ok {
				return false
			}
		}
	}
	return true
}
