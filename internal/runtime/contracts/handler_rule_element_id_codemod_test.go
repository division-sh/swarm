package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMintHandlerRuleElementIDsCoversEveryAdoptedGrammarContext(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	raw := []byte(`node:
  event_handlers:
    sequence.event:
      rules:
        - id: sequence
          condition: else
    keyed.event:
      rules:
        keyed:
          condition: else
        lookup:
          lookup:
            on: payload.kind
            entries:
              - key: service
                value: selected
            into: computed.choice
            default: fail
    single.event:
      rules: {id: single, condition: else}
    complete.event:
      on_complete:
        - id: complete
          condition: else
    join.event:
      join:
        on_complete: {advances_to: done}
        timeout: {after: 1h, advances_to: attention}
`)
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesChanged != 1 || result.IDsMinted != 7 {
		t.Fatalf("mint result = %#v, want one file and seven IDs", result)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(updated), "element_id:"); got != 7 {
		t.Fatalf("element_id count = %d, want 7\n%s", got, updated)
	}
	second, err := MintHandlerRuleElementIDs(root)
	if err != nil || second != (HandlerRuleElementIDMintResult{}) {
		t.Fatalf("idempotent mint = %#v, %v", second, err)
	}
	again, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(updated) {
		t.Fatal("idempotent mint rewrote admitted IDs")
	}
}

func TestMintHandlerRuleElementIDsCoversEveryFanOutGrammarContext(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	raw := []byte(`node:
  event_handlers:
    top.event:
      fan_out: {items_from: payload.items, as: fan_item, emit: item.ready}
    rule.event:
      rules:
        - id: selected
          condition: else
          fan_out: {items_from: payload.items, as: fan_item, emit: item.ready}
    complete.event:
      on_complete:
        - id: completed
          condition: else
          fan_out: {items_from: payload.items, as: fan_item, emit: item.ready}
`)
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesChanged != 1 || result.IDsMinted != 5 {
		t.Fatalf("fan-out mint result = %#v, want three fan-out and two containing rule IDs", result)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(updated), "element_id:"); got != 5 {
		t.Fatalf("fan-out element IDs = %d, want 5\n%s", got, updated)
	}
	second, err := MintHandlerRuleElementIDs(root)
	if err != nil || second != (HandlerRuleElementIDMintResult{}) {
		t.Fatalf("idempotent fan-out mint = %#v, %v", second, err)
	}
}

func TestMintHandlerRuleElementIDsRejectsHostileFanOutIdentityShapesWithoutWriting(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "scalar", body: "fan_out: retired-scalar", want: "fan_out must be a mapping"},
		{name: "null", body: "fan_out: null", want: "fan_out must be a mapping"},
		{name: "duplicate identity", body: "fan_out: {element_id: 00000000-0000-4000-8000-000000000451, element_id: 00000000-0000-4000-8000-000000000452, items_from: payload.items, emit: item.ready}", want: "duplicate normalized key"},
		{name: "malformed identity", body: "fan_out: {element_id: not-a-uuid, items_from: payload.items, emit: item.ready}", want: "must be one canonical non-zero lowercase UUID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nodes.yaml")
			raw := []byte("node:\n  event_handlers:\n    event:\n      " + tc.body + "\n")
			if err := os.WriteFile(path, raw, 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("fan-out hostile mint error = %v, want %q", err, tc.want)
			}
			updated, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(updated) != string(raw) {
				t.Fatalf("failed fan-out preflight changed file\n got: %s\nwant: %s", updated, raw)
			}
		})
	}
}

func TestMintHandlerRuleElementIDsRejectsReusedFanOutAliasWithoutWriting(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	raw := []byte(`template: &shared {items_from: payload.items, as: fan_item, emit: item.ready}
node:
  event_handlers:
    first.event:
      fan_out: *shared
    second.event:
      rules:
        selected:
          condition: else
          fan_out: *shared
`)
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), "YAML-ALIAS-REUSE") {
		t.Fatalf("reused fan-out alias error = %v, want YAML-ALIAS-REUSE", err)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(updated) != string(raw) {
		t.Fatalf("failed fan-out alias preflight changed file\n got: %s\nwant: %s", updated, raw)
	}
}

func TestMintHandlerRuleElementIDsPreflightsWholeTreeBeforeWriting(t *testing.T) {
	root := t.TempDir()
	goodPath := filepath.Join(root, "a", "nodes.yaml")
	badPath := filepath.Join(root, "z", "nodes.yaml")
	if err := os.MkdirAll(filepath.Dir(goodPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(badPath), 0o750); err != nil {
		t.Fatal(err)
	}
	good := []byte("node:\n  event_handlers:\n    event:\n      rules:\n        selected: {condition: else}\n")
	bad := []byte("node:\n  event_handlers:\n    event:\n      rules: retired-scalar\n")
	if err := os.WriteFile(goodPath, good, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badPath, bad, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := MintHandlerRuleElementIDs(root); err == nil {
		t.Fatal("MintHandlerRuleElementIDs succeeded with hostile scalar grammar")
	}
	for path, want := range map[string][]byte{goodPath: good, badPath: bad} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s changed after failed preflight\n got: %s\nwant: %s", path, got, want)
		}
	}
}

func TestMintHandlerRuleElementIDsPreflightsEverySingletonFormWithoutPartialWrite(t *testing.T) {
	root := t.TempDir()
	goodPath := filepath.Join(root, "a", "nodes.yaml")
	badPath := filepath.Join(root, "z", "nodes.yaml")
	if err := os.MkdirAll(filepath.Dir(goodPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(badPath), 0o750); err != nil {
		t.Fatal(err)
	}
	good := []byte(`node:
  event_handlers:
    activity.event:
      rules: {activity: {tool: notify}}
    identity_activity.event:
      rules: {element_id: 00000000-0000-4000-8000-000000000435, activity: {tool: notify}}
    presentation.event:
      rules: {id: display-only, description: presentation}
`)
	bad := []byte("node:\n  event_handlers:\n    bad.event:\n      rules: retired-scalar\n")
	for path, raw := range map[string][]byte{goodPath: good, badPath: bad} {
		if err := os.WriteFile(path, raw, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := MintHandlerRuleElementIDs(root); err == nil {
		t.Fatal("MintHandlerRuleElementIDs succeeded with hostile sibling file")
	}
	if got, err := os.ReadFile(goodPath); err != nil || string(got) != string(good) {
		t.Fatalf("valid singleton file changed after failed preflight: %v\n%s", err, got)
	}
	if err := os.Remove(badPath); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.IDsMinted != 2 || result.FilesChanged != 1 {
		t.Fatalf("singleton mint result = %#v", result)
	}
	updated, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(updated), "element_id:"); got != 3 {
		t.Fatalf("singleton element IDs = %d, want 3\n%s", got, updated)
	}
}

func TestMintHandlerRuleElementIDsRejectsAmbiguousGrammarWithoutPartialWrite(t *testing.T) {
	root := t.TempDir()
	goodPath := filepath.Join(root, "a", "nodes.yaml")
	badPath := filepath.Join(root, "z", "nodes.yaml")
	if err := os.MkdirAll(filepath.Dir(goodPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(badPath), 0o750); err != nil {
		t.Fatal(err)
	}
	good := []byte("node:\n  event_handlers:\n    event:\n      rules:\n        selected: {condition: else}\n")
	bad := []byte("node:\n  event_handlers:\n    event:\n      rules: {activity: {id: ambiguous}}\n")
	for path, raw := range map[string][]byte{goodPath: good, badPath: bad} {
		if err := os.WriteFile(path, raw, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-RULE-GRAMMAR") {
		t.Fatalf("MintHandlerRuleElementIDs error = %v, want ambiguous grammar rejection", err)
	}
	for path, want := range map[string][]byte{goodPath: good, badPath: bad} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s changed after ambiguous preflight\n got: %s\nwant: %s", path, got, want)
		}
	}
}

func TestMintHandlerRuleElementIDsTreatsGrammarFieldNamesAsKeyedLabels(t *testing.T) {
	labels := []string{
		"element_id", "id", "description", "condition", "when", "case", "range", "lookup", "validate", "compute_module",
		"else", "default", "advances_to", "emit", "emits", "action", "activity", "data_accumulation", "compute", "fan_out",
	}
	var raw strings.Builder
	raw.WriteString("node:\n  event_handlers:\n    rules.event:\n      rules:\n")
	for _, label := range labels {
		raw.WriteString("        " + label + ": {condition: else}\n")
	}
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	if err := os.WriteFile(path, []byte(raw.String()), 0o640); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	want := len(labels)
	if result.FilesChanged != 1 || result.IDsMinted != want {
		t.Fatalf("mint result = %#v, want %d keyed rows", result, want)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(updated), "{element_id:"); got != want {
		t.Fatalf("element_id count = %d, want %d\n%s", got, want, updated)
	}
	second, err := MintHandlerRuleElementIDs(root)
	if err != nil || second != (HandlerRuleElementIDMintResult{}) {
		t.Fatalf("idempotent keyed-label mint = %#v, %v", second, err)
	}
}

func TestMintHandlerRuleElementIDsResolvesAliasedRowsIndependentOfKeyLabel(t *testing.T) {
	labels := []string{
		"selected", "element_id", "id", "description", "condition", "when", "case", "range", "lookup", "validate",
		"compute_module", "else", "default", "advances_to", "emit", "emits", "action", "activity", "data_accumulation", "compute", "fan_out",
	}
	var raw strings.Builder
	raw.WriteString("templates:\n")
	for index := range labels {
		fmt.Fprintf(&raw, "  row%d: &row%d {condition: else}\n", index, index)
	}
	raw.WriteString("node:\n  event_handlers:\n")
	for index, label := range labels {
		fmt.Fprintf(&raw, "    event%d:\n      rules:\n        %q: *row%d\n", index, label, index)
	}

	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	if err := os.WriteFile(path, []byte(raw.String()), 0o640); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesChanged != 1 || result.IDsMinted != len(labels) {
		t.Fatalf("alias mint result = %#v, want %d rows", result, len(labels))
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(updated), "element_id:"); got != len(labels) {
		t.Fatalf("aliased element IDs = %d, want %d\n%s", got, len(labels), updated)
	}
	second, err := MintHandlerRuleElementIDs(root)
	if err != nil || second != (HandlerRuleElementIDMintResult{}) {
		t.Fatalf("idempotent alias mint = %#v, %v", second, err)
	}
}

func TestMintHandlerRuleElementIDsRejectsReusedAliasRowsWithoutPartialWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "keyed rules", raw: `template: &shared {condition: else}
node:
  event_handlers:
    first.event:
      rules:
        first: *shared
    second.event:
      rules:
        second: *shared
`},
		{name: "sequence rules", raw: `template: &shared {condition: else}
node:
  event_handlers:
    event:
      rules: [*shared, *shared]
`},
		{name: "handler completion", raw: `template: &shared {condition: else}
node:
  event_handlers:
    first.event:
      on_complete: [*shared]
    second.event:
      on_complete: [*shared]
`},
		{name: "join completion", raw: `template: &shared {advances_to: done}
node:
  event_handlers:
    first.event:
      join:
        on_complete: *shared
    second.event:
      join:
        on_complete: *shared
`},
		{name: "join timeout", raw: `template: &shared {after: 1h, advances_to: attention}
node:
  event_handlers:
    first.event:
      join:
        timeout: *shared
    second.event:
      join:
        timeout: *shared
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nodes.yaml")
			raw := []byte(tc.raw)
			if err := os.WriteFile(path, raw, 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), "YAML-ALIAS-REUSE") {
				t.Fatalf("mint error = %v, want reused authored alias rejection", err)
			}
			updated, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(updated) != string(raw) {
				t.Fatalf("failed alias preflight changed file\n got: %s\nwant: %s", updated, raw)
			}
		})
	}
}

func TestMintHandlerRuleElementIDsRejectsInvalidAuthoredShapesWithoutPartialWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "empty sequence row", body: "rules:\n        - {}", want: "EMPTY-AUTHORED-RULE"},
		{name: "empty keyed row", body: "rules:\n        selected: {}", want: "EMPTY-AUTHORED-RULE"},
		{name: "empty completion row", body: "on_complete:\n        - {}", want: "EMPTY-AUTHORED-RULE"},
		{name: "mapping completion", body: "on_complete:\n        selected: {condition: else}", want: "DIALECT-OC-ORDER"},
		{name: "empty keyed label", body: "rules:\n        \"\": {condition: else}", want: "label must not be empty"},
		{name: "whitespace keyed label", body: "rules:\n        \"   \": {condition: else}", want: "label must not be empty"},
		{name: "scalar keyed child", body: "rules:\n        selected: else", want: "must be a mapping"},
		{name: "null rules", body: "rules: null", want: "rules handler rule collection must not be null"},
		{name: "null completion", body: "on_complete: null", want: "on_complete handler rule collection must not be null"},
		{name: "duplicate keyed label", body: "rules:\n        selected: {condition: else}\n        selected: {condition: else}", want: "duplicate normalized key"},
		{name: "duplicate handler rules", body: "rules: [{condition: else}]\n      rules: [{condition: else}]", want: "duplicate normalized key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			goodPath := filepath.Join(root, "a", "nodes.yaml")
			badPath := filepath.Join(root, "z", "nodes.yaml")
			if err := os.MkdirAll(filepath.Dir(goodPath), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(badPath), 0o750); err != nil {
				t.Fatal(err)
			}
			good := []byte("node:\n  event_handlers:\n    event:\n      rules:\n        selected: {condition: else}\n")
			bad := []byte("node:\n  event_handlers:\n    event:\n      " + tc.body + "\n")
			if err := os.WriteFile(goodPath, good, 0o640); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(badPath, bad, 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("mint error = %v, want %q", err, tc.want)
			}
			for path, want := range map[string][]byte{goodPath: good, badPath: bad} {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(want) {
					t.Fatalf("%s changed after failed preflight", path)
				}
			}
		})
	}
}

func TestMintHandlerRuleElementIDsRejectsInvalidExistingRowBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  string
		want string
	}{
		{
			name: "duplicate element identity",
			row:  "{element_id: 00000000-0000-4000-8000-000000000413, element_id: 00000000-0000-4000-8000-000000000414, condition: else}",
			want: "duplicate normalized key",
		},
		{
			name: "retired field after identity",
			row:  "{element_id: 00000000-0000-4000-8000-000000000415, condition: else, emits: retired}",
			want: `unsupported handler rule field "emits"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			goodPath := filepath.Join(root, "a", "nodes.yaml")
			badPath := filepath.Join(root, "z", "nodes.yaml")
			if err := os.MkdirAll(filepath.Dir(goodPath), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(badPath), 0o750); err != nil {
				t.Fatal(err)
			}
			good := []byte("node:\n  event_handlers:\n    event:\n      rules:\n        selected: {condition: else}\n")
			bad := []byte("node:\n  event_handlers:\n    event:\n      rules:\n        selected: " + tc.row + "\n")
			if err := os.WriteFile(goodPath, good, 0o640); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(badPath, bad, 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := MintHandlerRuleElementIDs(root); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("mint error = %v, want %q", err, tc.want)
			}
			for path, want := range map[string][]byte{goodPath: good, badPath: bad} {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(want) {
					t.Fatalf("%s changed after failed preflight", path)
				}
			}
		})
	}
}

func TestMintHandlerRuleElementIDsDoesNotInterpretUnrelatedRulesMaps(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	raw := []byte(`rules:
  unrelated: unchanged
node:
  metadata:
    event_handlers:
      event:
        rules:
          chosen: {condition: else}
`)
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatal(err)
	}
	result, err := MintHandlerRuleElementIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.IDsMinted != 1 {
		t.Fatalf("mint result = %#v, want one handler rule", result)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(updated), "unrelated: unchanged") || strings.Count(string(updated), "element_id:") != 1 {
		t.Fatalf("unexpected rewrite:\n%s", updated)
	}
}
