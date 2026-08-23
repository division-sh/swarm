package contracts

import (
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

func TestMintHandlerRuleElementIDsRejectsInvalidExistingRowBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  string
		want string
	}{
		{
			name: "duplicate element identity",
			row:  "{element_id: 00000000-0000-4000-8000-000000000413, element_id: 00000000-0000-4000-8000-000000000414, condition: else}",
			want: "element_id may appear only once",
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
