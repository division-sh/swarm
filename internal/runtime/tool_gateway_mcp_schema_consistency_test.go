package runtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
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
			normalized := normalizeJSONValue(t, def.Schema)
			assertDraft202012SchemaTypes(t, "role="+role+" tool="+name, "", normalized)
			expected[name] = normalized
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
			normalized := normalizeJSONValue(t, tool["inputSchema"])
			assertDraft202012SchemaTypes(t, "role="+role+" tool="+name, "", normalized)
			seen[name] = normalized
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

var validJSONSchemaDraft202012Types = map[string]struct{}{
	"string":  {},
	"number":  {},
	"integer": {},
	"boolean": {},
	"array":   {},
	"object":  {},
	"null":    {},
}

func assertDraft202012SchemaTypes(t *testing.T, path string, parentKey string, v any) {
	t.Helper()
	switch node := v.(type) {
	case map[string]any:
		interpretsTypeKeyword := parentKey != "properties" &&
			parentKey != "patternProperties" &&
			parentKey != "$defs" &&
			parentKey != "definitions"
		if interpretsTypeKeyword {
			if rawType, ok := node["type"]; ok {
				assertAllowedSchemaTypeValue(t, path+".type", rawType)
			}
		}
		for key, child := range node {
			if key == "type" && interpretsTypeKeyword {
				continue
			}
			assertDraft202012SchemaTypes(t, path+"."+key, key, child)
		}
	case []any:
		for i, child := range node {
			assertDraft202012SchemaTypes(t, path+"["+strconv.Itoa(i)+"]", parentKey, child)
		}
	}
}

func assertAllowedSchemaTypeValue(t *testing.T, path string, raw any) {
	t.Helper()
	validate := func(typeValue string) {
		typeValue = strings.TrimSpace(typeValue)
		if _, ok := validJSONSchemaDraft202012Types[typeValue]; !ok {
			t.Fatalf("invalid JSON Schema Draft 2020-12 type at %s: %q", path, typeValue)
		}
	}

	switch tv := raw.(type) {
	case string:
		validate(tv)
	case []any:
		if len(tv) == 0 {
			t.Fatalf("invalid JSON Schema type array at %s: empty", path)
		}
		for i, entry := range tv {
			typeStr, ok := entry.(string)
			if !ok {
				t.Fatalf("invalid JSON Schema type array entry at %s[%d]: %#v", path, i, entry)
			}
			validate(typeStr)
		}
	default:
		t.Fatalf("invalid JSON Schema type encoding at %s: %#v", path, raw)
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
