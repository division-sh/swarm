package mcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestW5MCPRemoteSchemaIntegerTokenBounds(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, keyword := range []string{"minLength", "maxLength", "minItems", "maxItems"} {
			for _, location := range []string{"properties", "items", "additionalProperties"} {
				for _, token := range []string{"0", "1", "9007199254740991"} {
					t.Run(strings.Join([]string{transport, keyword, location, token}, "/"), func(t *testing.T) {
						schema := remoteBoundSchema(keyword, location, token)
						client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"bounded","description":"bounded tool","inputSchema":`+schema+`}]}`)
						tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
						if err != nil || len(tools) != 1 {
							t.Fatalf("integer schema discovery: tools=%d err=%v", len(tools), err)
						}
						admitted := tools[0].Contract.InputSchema()
						leaf := remoteBoundLeaf(t, admitted, location)
						var got int
						var present bool
						switch keyword {
						case "minLength":
							got, present = leaf.MinLength()
						case "maxLength":
							got, present = leaf.MaxLength()
						case "minItems":
							got, present = leaf.MinItems()
						case "maxItems":
							got, present = leaf.MaxItems()
						}
						if !present || fmt.Sprint(got) != token {
							t.Fatalf("%s=%d present=%v, want exact token %s", keyword, got, present, token)
						}
						// Actual discovery must reach the same immutable identity as a
						// programmatic declaration, including canonical schema reload.
						reloaded, err := runtimecontracts.AdmitToolInputSchemaMap(admitted.Projection())
						if err != nil || !admitted.Equal(reloaded) {
							t.Fatalf("schema projection/reload drift: %v", err)
						}
						if _, err := tools[0].Contract.CanonicalHash(); err != nil {
							t.Fatalf("discovered definition identity: %v", err)
						}
					})
				}
			}
		}
	}
}

func TestW5MCPRemoteSchemaRejectsNonIntegerBounds(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, keyword := range []string{"minLength", "maxLength", "minItems", "maxItems"} {
			for _, location := range []string{"properties", "items", "additionalProperties"} {
				for _, token := range []string{"1.5", "1.0", "1e0", `"1"`, "null", "-1", "9223372036854775808"} {
					t.Run(strings.Join([]string{transport, keyword, location, token}, "/"), func(t *testing.T) {
						schema := remoteBoundSchema(keyword, location, token)
						client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"bounded","inputSchema":`+schema+`}]}`)
						tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
						if len(tools) != 0 {
							t.Fatalf("invalid schema published %d tools", len(tools))
						}
						assertMCPFailure(t, err, runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid")
					})
				}
			}
		}
	}
}

func TestW5MCPRemoteSchemaPreservesUnconstrainedRangesAndEnums(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, schema := range []string{
			`{}`,
			`{"type":"object"}`,
			`{"type":"object","properties":{"value":{"type":"number","minimum":-0.5,"maximum":1.5,"enum":[0,1.0,1.5]}}}`,
			`{"type":"object","properties":{"value":{"type":"number","enum":[1e0,1.5]}}}`,
		} {
			t.Run(transport+"/"+schema, func(t *testing.T) {
				client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"bounded","inputSchema":`+schema+`}]}`)
				tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
				if err != nil || len(tools) != 1 {
					t.Fatalf("catalog control: tools=%d err=%v", len(tools), err)
				}
				var expected map[string]any
				if err := json.Unmarshal([]byte(schema), &expected); err != nil {
					t.Fatal(err)
				}
				admitted, err := runtimecontracts.AdmitToolInputSchemaMap(expected)
				if err != nil || !admitted.Equal(tools[0].Contract.InputSchema()) {
					t.Fatalf("range/enum/unconstrained meaning changed: %v", err)
				}
			})
		}
		for _, token := range []string{"-1e999", "null", `"1"`} {
			t.Run(transport+"/invalid-range/"+token, func(t *testing.T) {
				client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"bounded","inputSchema":{"type":"number","minimum":`+token+`}}]}`)
				if tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server); err == nil || len(tools) != 0 {
					t.Fatalf("invalid range admitted: tools=%d err=%v", len(tools), err)
				}
			})
		}
	}
}

func TestW5MCPRemoteSchemaKeepsExactFieldNamesAndCatalogAtomicity(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, invalid := range []string{
			`{"name":"bounded","InputSchema":{"type":"object"}}`,
			`{"name":"bounded","inputSchema":null}`,
			`{"name":"bounded","inputSchema":{"type":"string","minLength":1.0}}`,
			`{"name":"bounded","inputSchema":{"type":"object","unknown":1}}`,
		} {
			t.Run(transport+"/"+invalid, func(t *testing.T) {
				client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"valid","inputSchema":{}},`+invalid+`]}`)
				tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
				if len(tools) != 0 {
					t.Fatalf("invalid catalog partially published: %v", tools)
				}
				assertMCPFailure(t, err, runtimefailures.ClassConnectorFailure, "mcp_tool_catalog_invalid")
			})
		}
	}
}

func TestW5MCPSchemaIngressDoesNotChangeOrdinaryNumbers(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		t.Run(transport, func(t *testing.T) {
			catalog := `{"count":1,"TOOLS":[{},{}],"tools":[{"name":"bounded","inputSchema":{"type":"object","properties":{"value":{"type":"string","minLength":1}}},"_meta":{"revision":1}}]}`
			client, server := remoteSchemaClient(t, transport, catalog)
			init, err := client.callServer(unmanagedMCPTestContext(), server, RPCRequest{JSONRPC: "2.0", Method: "initialize", ID: "test-initialize"})
			if err != nil || init.Result.(map[string]any)["revision"] != float64(1) {
				t.Fatalf("ordinary initialize changed: %#v err=%v", init.Result, err)
			}
			list, err := client.callServer(unmanagedMCPTestContext(), server, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "test-tools-list"})
			if err != nil {
				t.Fatal(err)
			}
			result := list.Result.(map[string]any)
			tool := result["tools"].([]any)[0].(map[string]any)
			if result["count"] != float64(1) || tool["_meta"].(map[string]any)["revision"] != float64(1) {
				t.Fatalf("catalog metadata changed: %#v", result)
			}
			schema := tool["inputSchema"].(map[string]any)
			bound := schema["properties"].(map[string]any)["value"].(map[string]any)["minLength"]
			if bound != json.Number("1") {
				t.Fatalf("schema lost token evidence: %T %v", bound, bound)
			}
			client.servers["test"] = server
			client.tools["test.bounded"] = DiscoveredTool{Name: "test.bounded", RemoteName: "bounded", ServerName: "test"}
			out, err := client.Call(unmanagedMCPTestContext(), "test.bounded", map[string]any{})
			want := map[string]any{"integer": float64(1), "decimal": float64(1.5), "inputSchema": map[string]any{"minLength": float64(1)}}
			if err != nil || !reflect.DeepEqual(out, want) {
				t.Fatalf("ordinary tool result changed: %#v err=%v", out, err)
			}
		})
	}
	for _, method := range []string{"initialize", "tools/list", "tools/call"} {
		response, err := DecodeRPCResponse([]byte(`{"jsonrpc":"2.0","id":"error","error":{"code":-32602,"message":"bad input","data":{"integer":1}}}`), RPCRequest{Method: method, ID: "error"})
		if err != nil || response.Error.Data.(map[string]any)["integer"] != json.Number("1") {
			t.Fatalf("%s protocol error changed: %#v err=%v", method, response.Error, err)
		}
	}
}

func remoteBoundSchema(keyword, location, token string) string {
	kind := `"type":"string"`
	if keyword == "minItems" || keyword == "maxItems" {
		kind = `"type":"array","items":{"type":"string"}`
	}
	leaf := fmt.Sprintf(`{%s,%q:%s}`, kind, keyword, token)
	switch location {
	case "items":
		return `{"type":"object","properties":{"value":{"type":"array","items":` + leaf + `}}}`
	case "additionalProperties":
		return `{"type":"object","additionalProperties":` + leaf + `}`
	default:
		return `{"type":"object","properties":{"value":` + leaf + `}}`
	}
}

func remoteBoundLeaf(t *testing.T, schema runtimecontracts.ToolInputSchema, location string) runtimecontracts.ToolInputSchema {
	t.Helper()
	var leaf runtimecontracts.ToolInputSchema
	var ok bool
	if location == "additionalProperties" {
		leaf, ok = schema.AdditionalPropertiesSchema()
	} else {
		leaf, ok = schema.Property("value")
		if location == "items" && ok {
			leaf, ok = leaf.ItemsSchema()
		}
	}
	if !ok {
		t.Fatalf("missing %s schema", location)
	}
	return leaf
}

// Both transports exercise their real framing/response decoder and discovery
// handoff. Stdio supplies deterministic frames, not pre-decoded tool maps.
func remoteSchemaClient(t *testing.T, transport, catalog string) (*Client, *registeredServer) {
	t.Helper()
	client := NewClient(nil)
	server := &registeredServer{cfg: ServerConfig{Name: "test", Prefix: "test", Transport: transport}}
	results := map[string]string{
		"initialize":                `{"revision":1}`,
		"notifications/initialized": `{}`,
		"tools/list":                catalog,
		"tools/call":                `{"content":[],"structuredContent":{"integer":1,"decimal":1.5,"inputSchema":{"minLength":1}}}`,
	}
	if transport == "stdio" {
		frames := fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"id\":\"test-initialize\",\"result\":%s}\n{\"jsonrpc\":\"2.0\",\"id\":\"test-tools-list\",\"result\":%s}\n{\"jsonrpc\":\"2.0\",\"id\":\"test.bounded-call\",\"result\":%s}\n", results["initialize"], catalog, results["tools/call"])
		server.stdio = &stdioRPCClient{stdin: mcpTestWriteCloser{Writer: io.Discard}, stdout: bufio.NewReader(strings.NewReader(frames))}
		return client, server
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		result, ok := results[request.Method]
		if !ok {
			t.Errorf("unexpected method %q", request.Method)
			return
		}
		id, err := json.Marshal(request.ID)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, id, result)
	}))
	t.Cleanup(httpServer.Close)
	client.httpClient = httpServer.Client()
	server.cfg.URL = httpServer.URL
	return client, server
}
