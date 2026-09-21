package contracts

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestReceiverConfigurationValidateCommittedNeverDefaults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		variables map[string]FlowVariable
		config    map[string]any
		wantErr   bool
	}{
		{"nil evidence", nil, nil, true},
		{"empty evidence", nil, map[string]any{}, false},
		{"optional default absent", map[string]FlowVariable{"count": {Type: "integer", IsOptional: true, HasDefault: true, Default: 99}}, map[string]any{}, false},
		{"required absent", map[string]FlowVariable{"count": {Type: "integer"}}, map[string]any{}, true},
		{"required default absent", map[string]FlowVariable{"count": {Type: "integer", HasDefault: true, Default: 99}}, map[string]any{}, true},
		{"recorded zero", map[string]FlowVariable{"count": {Type: "integer", HasDefault: true, Default: 99}}, map[string]any{"count": int64(0)}, false},
		{"recorded false", map[string]FlowVariable{"flag": {Type: "boolean", HasDefault: true, Default: true}}, map[string]any{"flag": false}, false},
		{"wrong type", map[string]FlowVariable{"count": {Type: "integer"}}, map[string]any{"count": "7"}, true},
		{"null integer", map[string]FlowVariable{"count": {Type: "integer"}}, map[string]any{"count": nil}, true},
		{"null json", map[string]FlowVariable{"value": {Type: "json"}}, map[string]any{"value": nil}, false},
		{"selected type changed", map[string]FlowVariable{"count": {Type: "boolean"}}, map[string]any{"count": int64(7)}, true},
		{"undeclared", nil, map[string]any{"other": true}, true},
		{"typed map value", map[string]FlowVariable{"values": {Type: "map[text]integer"}}, map[string]any{"values": map[string]any{"key": "bad"}}, true},
		{"typed map key", map[string]FlowVariable{"values": {Type: "map[uuid]integer"}}, map[string]any{"values": map[string]any{"bad": 7}}, true},
		{"sibling mismatch", map[string]FlowVariable{"left": {Type: "integer"}, "right": {Type: "integer", Refinements: SchemaRefinements{EqualTo: "left"}}}, map[string]any{"left": 7, "right": 8}, true},
		{"sibling match", map[string]FlowVariable{"left": {Type: "integer"}, "right": {Type: "integer", Refinements: SchemaRefinements{EqualTo: "left"}}}, map[string]any{"left": 7, "right": 7}, false},
		{"malformed runtime value", map[string]FlowVariable{"value": {Type: "json"}}, map[string]any{"value": math.Inf(1)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configuration, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: tc.variables}, TypeCatalogDocument{})
			if err != nil {
				t.Fatal(err)
			}
			if err := configuration.ValidateCommitted(tc.config); (err != nil) != tc.wantErr {
				t.Fatalf("ValidateCommitted(%#v) = %v, wantErr=%v", tc.config, err, tc.wantErr)
			}
			if tc.name == "optional default absent" && len(tc.config) != 0 {
				t.Fatal("validation applied a default")
			}
		})
	}
}

func TestReceiverConfigurationValidateCommittedPreservesRecordedRepresentations(t *testing.T) {
	configuration, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{
		"value": {Type: "json"}, "new_default": {Type: "integer", IsOptional: true, HasDefault: true, Default: 99},
	}}, TypeCatalogDocument{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"value": []any{int64(7), float64(7), json.Number("7.0"), nil}}
	config := map[string]any{"value": []any{int64(7), float64(7), json.Number("7.0"), nil}}
	for i := 0; i < 2; i++ {
		if err := configuration.ValidateCommitted(config); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(config, want) {
			t.Fatalf("validation changed recorded evidence: %#v", config)
		}
	}
}
