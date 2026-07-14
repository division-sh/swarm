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
		{
			path: "internal/store/proposed_effect_cards.go",
			forbidden: []string{
				"Input map[string]any", "json.Unmarshal(", "json.NewDecoder(", ".UseNumber()", ".WriteJSON(",
			},
		},
		{
			path: "internal/runtime/pipeline/activity_engine.go",
			forbidden: []string{
				"Input map[string]any", "json.Unmarshal(evt.Payload()", ".UseNumber()", ".WriteJSON(",
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

func TestRegisteredAnchorKindsStaySynchronizedWithDeclaredConsumers(t *testing.T) {
	root := decisionCardRepoRoot(t)
	wantEnum := "enum: [" + strings.Join(RegisteredAnchorKindNames(), ", ") + "]"
	spec, err := os.ReadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(spec), wantEnum) != 1 {
		t.Fatalf("platform spec anchor enum must occur exactly once as %q", wantEnum)
	}

	for _, consumer := range []struct {
		path     string
		required string
		forbid   string
	}{
		{path: "internal/apiv1/operator_mailbox.go", required: "decisioncard.IsRegisteredAnchorKind", forbid: `kind != "stage_gate"`},
		{path: "cmd/swarm/test_command.go", required: "decisioncard.IsRegisteredAnchorKind", forbid: `anchorKind != "stage_gate"`},
		{path: "cmd/swarm/control_mailbox.go", required: "decisioncard.RegisteredAnchorKindDescription", forbid: "anchor_kind must be stage_gate"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, consumer.path))
		if err != nil {
			t.Fatalf("read %s: %v", consumer.path, err)
		}
		text := string(raw)
		if !strings.Contains(text, consumer.required) {
			t.Errorf("%s does not consume the closed anchor owner %q", consumer.path, consumer.required)
		}
		if strings.Contains(text, consumer.forbid) {
			t.Errorf("%s duplicates anchor acceptance vocabulary with %q", consumer.path, consumer.forbid)
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
