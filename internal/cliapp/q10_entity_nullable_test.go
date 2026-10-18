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
			for _, key := range []string{"fields", "bookkeeping", "gates", "accumulated", "loops"} {
				value := full[key]
				delete(full, key)
				if _, err := decode(full); err == nil {
					t.Fatalf("missing full property %s admitted", key)
				}
				full[key] = value
			}
			for _, clock := range []string{"created_at", "updated_at"} {
				value := data[clock]
				data[clock] = "not-a-time"
				if _, err := decode(full); err == nil {
					t.Fatalf("malformed data clock %s admitted", clock)
				}
				data[clock] = value
			}
			data["updated_at"] = "2026-10-08T01:00:00Z"
			if _, err := decode(full); err == nil {
				t.Fatal("data update before data creation admitted")
			}
			data["updated_at"] = base["updated_at"]
			if kind == "import" {
				for key, value := range map[string]any{
					"stage_defined": false, "current_state": "unknown", "revision": 1,
					"entered_state_at": base["created_at"], "lifecycle_created_at": base["created_at"], "lifecycle_updated_at": base["updated_at"],
				} {
					data[key] = value
					if _, err := decode(full); err == nil {
						t.Fatalf("import invented lifecycle property %s", key)
					}
					data[key] = nil
				}
				for key, value := range map[string]any{
					"gates": map[string]bool{}, "bookkeeping": map[string]any{}, "accumulated": map[string]any{}, "loops": []any{},
				} {
					full[key] = value
					if _, err := decode(full); err == nil {
						t.Fatalf("import invented lifecycle property %s", key)
					}
					full[key] = nil
				}
			} else {
				for _, key := range []string{"revision", "lifecycle_created_at", "lifecycle_updated_at"} {
					value := data[key]
					data[key] = nil
					if _, err := decode(full); err == nil {
						t.Fatalf("constructed null %s admitted", key)
					}
					data[key] = value
				}
				for _, revision := range []any{0, -1, 1.5} {
					data["revision"] = revision
					if _, err := decode(full); err == nil {
						t.Fatalf("invalid constructed revision %v admitted", revision)
					}
				}
				data["revision"] = 3
				for _, key := range []string{"bookkeeping", "gates", "accumulated", "loops"} {
					value := full[key]
					full[key] = nil
					if _, err := decode(full); err == nil {
						t.Fatalf("constructed null %s admitted", key)
					}
					full[key] = value
				}
				data["lifecycle_updated_at"] = "2026-10-08T01:00:00Z"
				if _, err := decode(full); err == nil {
					t.Fatal("lifecycle update before lifecycle creation admitted")
				}
				data["lifecycle_updated_at"] = base["updated_at"]
				if kind == "no_stage" {
					for key, value := range map[string]any{"current_state": "unknown", "entered_state_at": base["created_at"]} {
						data[key] = value
						if _, err := decode(full); err == nil {
							t.Fatalf("no-stage lifecycle acquired %s", key)
						}
						data[key] = nil
					}
				} else {
					for _, key := range []string{"current_state", "entered_state_at"} {
						value := data[key]
						data[key] = nil
						if _, err := decode(full); err == nil {
							t.Fatalf("named stage missing %s admitted", key)
						}
						data[key] = value
					}
				}
			}
			if _, err := decode(full); err != nil {
				t.Fatalf("negative matrix damaged the valid %s control: %v", kind, err)
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
