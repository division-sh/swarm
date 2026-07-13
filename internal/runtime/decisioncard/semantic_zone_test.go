package decisioncard

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDecisionCardSemanticZonesCannotBypassValueAdmission(t *testing.T) {
	root := decisionCardRepoRoot(t)
	for _, zone := range []struct {
		path      string
		forbidden []string
	}{
		{
			path: "internal/runtime/decisioncard/model.go",
			forbidden: []string{
				"json.Unmarshal(", "json.NewDecoder(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/runtime/decisioncard/frozen.go",
			forbidden: []string{
				"encoding/json", "json.Unmarshal(", "json.Marshal(", "json.NewDecoder(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/apiv1/handler.go",
			forbidden: []string{
				"json.Unmarshal(", "json.NewDecoder(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/runtime/pipeline/workflow_gate_decision.go",
			forbidden: []string{
				"encoding/json", "parsePayloadMap(evt.Payload())", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/runtime/pipeline/workflow_gate_lifecycle.go",
			forbidden: []string{
				"encoding/json", "json.Marshal(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/runtime/pipeline/workflow_gate_terminal.go",
			forbidden: []string{
				"encoding/json", "json.Marshal(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/apiv1/operator_decision_cards.go",
			forbidden: []string{
				"json.Marshal(map[string]any", "json.Unmarshal(completion.Response", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/store/decision_cards.go",
			forbidden: []string{
				"json.Marshal(card.Snapshot", "json.Marshal(card.Provenance", "json.Marshal(req.Fields",
				"json.Unmarshal(snapshot", "json.Unmarshal(provenance", "json.Unmarshal(fields", "json.Unmarshal(payload",
				".UseNumber()", ".WriteJSON(",
			},
		},
	} {
		raw, err := os.ReadFile(filepath.Join(root, zone.path))
		if err != nil {
			t.Fatalf("read %s: %v", zone.path, err)
		}
		for _, forbidden := range zone.forbidden {
			if strings.Contains(string(raw), forbidden) {
				t.Errorf("%s bypasses semanticvalue admission with %q", zone.path, forbidden)
			}
		}
	}
}

func decisionCardRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve sentinel source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
