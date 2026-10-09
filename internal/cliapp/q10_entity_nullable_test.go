package cliapp

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func entityTestPointer[T any](value T) *T { return &value }

func TestQ10EntityNullableDTOAndCLIDiagnostics(t *testing.T) {
	base := map[string]any{"entity_id": "entity-1", "run_id": "run-1", "flow_instance": "work/one", "entity_type": "task",
		"created_at": "2026-10-09T01:00:00Z", "updated_at": "2026-10-09T01:00:00Z", "stage_defined": nil,
		"current_state": nil, "revision": nil, "entered_state_at": nil, "lifecycle_created_at": nil, "lifecycle_updated_at": nil}
	for _, kind := range []string{"import", "no_stage", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			data := make(map[string]any, len(base))
			for k, v := range base {
				data[k] = v
			}
			full := map[string]any{"entity": data, "fields": map[string]any{"current_state": "business"}, "gates": nil, "bookkeeping": nil, "accumulated": nil, "loops": nil}
			want := "no lifecycle"
			if kind != "import" {
				data["stage_defined"], data["revision"] = kind == "unknown", 3
				data["lifecycle_created_at"], data["lifecycle_updated_at"] = base["created_at"], base["updated_at"]
				full["gates"], full["bookkeeping"], full["accumulated"], full["loops"] = map[string]bool{}, map[string]any{}, map[string]any{}, []any{}
				want = "no named stage"
				if kind == "unknown" {
					data["current_state"], data["entered_state_at"], want = "unknown", base["created_at"], "unknown"
				}
			}
			decode := func(value any) (entityFull, error) {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var out entityFull
				if err := json.Unmarshal(raw, &out); err != nil {
					return out, err
				}
				return out, validateEntityFullResult("entity.get result", out)
			}
			out, err := decode(full)
			if err != nil || entityLifecycleStageText(out.Entity) != want {
				t.Fatalf("valid %s: %+v %v", kind, out, err)
			}
			var text bytes.Buffer
			writeEntityFullResult(&text, out, entityRenderOptions{})
			if !strings.Contains(text.String(), want) || !strings.Contains(text.String(), "data updated") {
				t.Fatalf("absence/clock teaching lost: %s", text.String())
			}
			for _, key := range []string{"stage_defined", "current_state", "revision", "entered_state_at", "lifecycle_created_at", "lifecycle_updated_at"} {
				value := data[key]
				delete(data, key)
				if _, err := decode(full); err == nil {
					t.Fatalf("missing nullable %s admitted", key)
				}
				data[key] = value
			}
			if kind == "import" {
				data["current_state"] = "unknown"
				if _, err := decode(full); err == nil {
					t.Fatal("import invented lifecycle")
				}
				data["current_state"] = nil
				full["gates"] = map[string]bool{}
				if _, err := decode(full); err == nil {
					t.Fatal("import invented gates")
				}
			}
		})
	}
	for _, raw := range []string{`{"counts":{"unknown":1},"absent_count":2}`, `{"counts":{},"absent_count":0}`} {
		var out entityAggregateResult
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		if err := validateEntityAggregateResult(out); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"counts":{}}`, `{"counts":{},"absent_count":null}`, `{"counts":{},"absent_count":-1}`, `{"counts":{},"absent_count":1.5}`} {
		var out entityAggregateResult
		if err := json.Unmarshal([]byte(raw), &out); err == nil && validateEntityAggregateResult(out) == nil {
			t.Fatalf("invalid absence count admitted: %s", raw)
		}
	}
}
