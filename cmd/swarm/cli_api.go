package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultCLIAPIEndpoint = "http://127.0.0.1:8081/v1/rpc"

type rootCommandOptions struct {
	apiEndpoint string
	httpClient  *http.Client
}

func defaultRootCommandOptions() rootCommandOptions {
	return rootCommandOptions{
		apiEndpoint: defaultCLIAPIEndpoint,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
	}
}

type cliAPIClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

func newCLIAPIClient(opts rootCommandOptions) (*cliAPIClient, error) {
	endpoint := strings.TrimSpace(opts.apiEndpoint)
	if endpoint == "" {
		endpoint = defaultCLIAPIEndpoint
	}
	token := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN"))
	if token == "" {
		return nil, fmt.Errorf("SWARM_API_TOKEN is required for runtime-state CLI commands")
	}
	client := opts.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	return &cliAPIClient{endpoint: endpoint, token: token, httpClient: client}, nil
}

type jsonRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *jsonRPCError) Error() string {
	if e == nil {
		return ""
	}
	if code := applicationErrorCode(e.Data); code != "" {
		return fmt.Sprintf("%s: %s", code, e.Message)
	}
	return e.Message
}

func applicationErrorCode(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var data struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return ""
	}
	return strings.TrimSpace(data.Code)
}

func (c *cliAPIClient) call(ctx context.Context, method string, params map[string]any, result any) error {
	body, err := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      "swarm-cli:" + method,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("encode JSON-RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build v1 RPC request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("v1 RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read v1 RPC response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		message := strings.TrimSpace(string(raw))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return fmt.Errorf("v1 RPC HTTP %d: %s", resp.StatusCode, message)
	}

	var envelope jsonRPCResponse
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode JSON-RPC response: %w", err)
	}
	if envelope.JSONRPC != "2.0" {
		return fmt.Errorf("malformed JSON-RPC response: jsonrpc=%q", envelope.JSONRPC)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("JSON-RPC response missing result")
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode JSON-RPC result: %w", err)
	}
	return nil
}
