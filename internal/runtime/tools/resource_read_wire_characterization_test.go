package tools

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
)

func TestResourceReadCursorWireRefusal(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "cursor-wire")
	ref := flowdata.AllowedResourceData(source, actor)[0]
	item := flowDataResourceAccessItem(t, ref, []string{"first", "second"})
	executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: &resourceReadRecordingStore{items: []durabledata.ResourceAccessItem{item}}})
	first, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"limit": 1}})
	if err != nil {
		t.Fatal(err)
	}
	encoded := first.(map[string]any)["rows"].(durabledata.PageResult[map[string]any]).Continuation.Cursor
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["checksum"] = "foreign"
	wrongChecksum, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	static, err := encodeFlowDataCursor(flowDataCursor{Version: "swarm.flow-data.cursor.v1", Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	for name, cursor := range map[string]string{
		"bad_base64":       "!",
		"oversized":        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", durabledata.MaxBusinessKeyBytes+1))),
		"unknown_field":    base64.RawURLEncoding.EncodeToString(append([]byte(`{"unknown":1,`), raw[1:]...)),
		"trailing_object":  base64.RawURLEncoding.EncodeToString(append(append([]byte(nil), raw...), []byte(` {}`)...)),
		"trailing_garbage": base64.RawURLEncoding.EncodeToString(append(append([]byte(nil), raw...), 'x')),
		"checksum":         base64.RawURLEncoding.EncodeToString(wrongChecksum),
		"static_v1":        static,
	} {
		t.Run(name, func(t *testing.T) {
			result, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"cursor": cursor}})
			if result != nil || err == nil || (!strings.Contains(err.Error(), "cursor") && !strings.Contains(err.Error(), "invalid_tool_input")) {
				t.Fatalf("invalid cursor returned result=%v err=%v", result, err)
			}
		})
	}
}

func TestResourceReadScalarKeysAndKeylessMultiplicity(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "scalar-keys")
	ref := flowdata.AllowedResourceData(source, actor)[0]
	for _, scenario := range []struct {
		name, fieldType, businessKey, input string
		key                                 any
	}{
		{"text", "string", "id", `{"id":"one"}`, "one"},
		{"number", "number", "id", `{"id":1.0}`, json.Number("1e0")},
		{"boolean", "boolean", "id", `{"id":true}`, true},
		{"keyless_duplicates", "string", "", "{\"id\":\"one\"}\n{\"id\":\"one\"}", nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			schema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": scenario.fieldType}}, "required": []any{"id"}, "additionalProperties": false}
			compiled, defects := durabledata.CompileJSONL(ref, schema, scenario.businessKey, []byte(scenario.input+"\n"))
			if len(defects) != 0 {
				t.Fatalf("compile scalar fixture: %+v", defects)
			}
			item := durabledata.ResourceAccessItem{Kind: "resource", Declaration: ref, VersionID: compiled.VersionID, SchemaDigest: compiled.Manifest.SchemaDigest, RowCount: compiled.Manifest.RowCount, BusinessKey: scenario.businessKey, Schema: compiled.CanonicalSchema, Content: compiled.CanonicalJSONL}
			executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: &resourceReadRecordingStore{items: []durabledata.ResourceAccessItem{item}}})
			input := map[string]any{"kind": "resource_row", "declaration": ref}
			if scenario.businessKey == "" {
				input["position"] = 2
			} else {
				input["key"] = scenario.key
			}
			result, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", input)
			if err != nil {
				t.Fatal(err)
			}
			row := result.(map[string]any)["row"].(map[string]any)
			if scenario.businessKey == "" {
				if row["ordinal"] != uint64(2) || row["value"].(map[string]any)["id"] != "one" || item.RowCount != 2 {
					t.Fatalf("keyless multiplicity lost: %+v", result)
				}
				for _, position := range []int{0, -1, 3} {
					input["position"] = position
					if got, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", input); got != nil || err == nil {
						t.Fatalf("invalid position %d returned %v/%v", position, got, err)
					}
				}
			} else {
				key, err := durabledata.BusinessKeyFromValue(scenario.key)
				returned, returnedErr := durabledata.BusinessKeyFromValue(row["key"])
				if err != nil || returnedErr != nil || returned != key || row["ordinal"] != uint64(1) {
					t.Fatalf("canonical key mismatch: row=%+v key=%s err=%v", row, key, err)
				}
				input["key"] = map[string]any{"id": scenario.key}
				if got, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", input); got != nil || err == nil {
					t.Fatalf("non-scalar key returned %v/%v", got, err)
				}
			}
		})
	}
}
