package runtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"empireai/internal/commgraph"
)

func TestToolGatewayMCPEmitSchemasMatchRegistryForAllProducerRoles(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")

	roles := append([]string{}, commgraph.ProducerRoles()...)
	sort.Strings(roles)
	for _, role := range roles {
		role := strings.TrimSpace(role)
		if role == "" {
			continue
		}
		defs := GenerateEmitTools(role)
		if len(defs) == 0 {
			continue
		}
		allowed := make([]string, 0, len(defs))
		expected := make(map[string]any, len(defs))
		for _, def := range defs {
			name := strings.TrimSpace(def.Name)
			if name == "" {
				continue
			}
			allowed = append(allowed, name)
			expected[name] = normalizeJSONValue(t, def.Schema)
		}
		if len(allowed) == 0 {
			continue
		}
		sort.Strings(allowed)

		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)))
		req.Header.Set("X-Empire-Agent-Role", role)
		req.Header.Set("X-Empire-Allowed-Tools", strings.Join(allowed, ","))
		rr := httptest.NewRecorder()
		gw.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("role=%s tools/list expected 200 got %d body=%s", role, rr.Code, rr.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("role=%s decode mcp response: %v", role, err)
		}
		result, _ := resp["result"].(map[string]any)
		tools, _ := result["tools"].([]any)
		if len(tools) != len(allowed) {
			t.Fatalf("role=%s expected %d tools, got %d", role, len(allowed), len(tools))
		}
		seen := map[string]any{}
		for _, raw := range tools {
			tool, _ := raw.(map[string]any)
			name := strings.TrimSpace(asString(tool["name"]))
			if name == "" {
				t.Fatalf("role=%s mcp returned unnamed tool: %#v", role, tool)
			}
			seen[name] = normalizeJSONValue(t, tool["inputSchema"])
		}

		for _, name := range allowed {
			got, ok := seen[name]
			if !ok {
				t.Fatalf("role=%s missing tool %s in mcp tools/list", role, name)
			}
			want := expected[name]
			if !reflect.DeepEqual(got, want) {
				gotRaw, _ := json.Marshal(got)
				wantRaw, _ := json.Marshal(want)
				t.Fatalf("role=%s tool=%s schema mismatch\nwant=%s\ngot=%s", role, name, string(wantRaw), string(gotRaw))
			}
		}
	}
}

func normalizeJSONValue(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal normalize: %v", err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal normalize: %v", err)
	}
	return out
}
