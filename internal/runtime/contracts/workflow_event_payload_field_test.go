package contracts

import (
	"strings"
	"testing"
)

func TestEventCatalogPayloadFieldSupportsEveryWave1Shape(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		wantType string
	}{
		{name: "scalar", yaml: "payload: json\n", wantType: "json"},
		{name: "singleton list", yaml: "payload: [json]\n", wantType: "[json]"},
		{name: "mapping", yaml: "payload: {type: json}\n", wantType: "json"},
		{name: "mapping list", yaml: "payload: {type: '[json]'}\n", wantType: "[json]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := admitEventCatalogEntryForTest(t, tc.yaml)
			if err != nil {
				t.Fatalf("admit event catalog entry: %v", err)
			}
			field, ok := entry.Payload.Properties["payload"]
			if !ok {
				t.Fatal("canonical payload property was discarded")
			}
			if field.Type != tc.wantType {
				t.Fatalf("payload field = %#v", field)
			}
		})
	}
}

func TestEventCatalogPayloadFieldDistinguishesRetiredBlocksFromMalformedFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{name: "properties block", yaml: "payload:\n  properties:\n    message: json\n", wantErr: "not a nested payload block"},
		{name: "child field block", yaml: "payload:\n  message: json\n", wantErr: "not a nested payload block"},
		{name: "missing type", yaml: "payload: {}\n", wantErr: "event payload field type is required"},
		{name: "unknown metadata with type", yaml: "payload:\n  type: json\n  mystery: true\n", wantErr: "event payload field field \"mystery\" is not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
