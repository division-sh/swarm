package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func TestFiniteAccountingUnexplainedFormatterDriftRefuses(t *testing.T) {
	root := t.TempDir()
	const file = "internal/runtime/testfixtures/canonicalrouting/notify_all_children.go"
	original := []byte("package fixture\n\nvar value = true\n")
	current := []byte("package fixture\n\nvar value  = true\n")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, file)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, file), current, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, formatproof.Successors)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, formatproof.Successors), []byte(`{"version":1,"records":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	ledger := plan{Baseline: strings.Repeat("a", 40), Scope: "fixed test source", Changes: []change{{
		File: file, BeforeHash: digest([]byte("package fixture\n")), AfterHash: digest(original),
		Edits: []edit{{Offset: len("package fixture\n"), After: "\nvar value = true\n"}}, Review: "fixed historical bytes",
	}}}
	body, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ledger.json"), body, 0644); err != nil {
		t.Fatal(err)
	}
	if err := apply(root, "ledger.json", false, true); err == nil {
		t.Fatal("an unexplained current source hash must not authorize its own successor")
	}
}

func TestFiniteAccountingCopiedSourceAndHistoricalReplayAtBothCuts(t *testing.T) {
	ledgerBytes, err := os.ReadFile("finite-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var ledger plan
	if err = json.Unmarshal(ledgerBytes, &ledger); err != nil {
		t.Fatal(err)
	}
	var approved change
	for _, c := range ledger.Changes {
		if c.File == formatproof.Source {
			approved = c
		}
	}
	if approved.BeforeHash != "2fb1e9553437fc0ba0606d7612abb2f3c9db48ba30863e5531eacb4a33cf309c" || approved.AfterHash != formatproof.Origin {
		t.Fatal("reviewed before/after bytes changed")
	}
	before, err := os.ReadFile("formatproof/testdata/original.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	if digest(before) != approved.BeforeHash {
		t.Fatal("captured immutable preimage differs from the original reviewed ledger")
	}
	original, err := rewrite(approved, before)
	if err != nil {
		t.Fatal(err)
	}
	if digest(original) != formatproof.Origin {
		t.Fatal("immutable original replay changed")
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "copy")
	put := func(path string, raw []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", root}, args...)...)
		command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=fixture", "GIT_AUTHOR_EMAIL=fixture@example.test", "GIT_COMMITTER_NAME=fixture", "GIT_COMMITTER_EMAIL=fixture@example.test")
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("historical copy %v: %s: %v", args, raw, err)
		}
		return strings.TrimSpace(string(raw))
	}
	// Capture the exact hash-checked historical bytes in a private object database.
	// No CI fetch dependency or original reviewed ledger is changed by this copy.
	put(formatproof.Source, before)
	git("init", "-q")
	git("add", ".")
	git("-c", "commit.gpgsign=false", "commit", "-qm", "captured immutable input")
	ledger.Baseline = git("rev-parse", "HEAD")
	ledger.Changes = []change{approved}
	ledgerBytes, err = json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	put(formatproof.Ledger, ledgerBytes)
	history := formatproof.History{Version: 1, Records: []formatproof.Record{}}
	put(formatproof.Successors, []byte(`{"version":1,"records":[]}`))
	if err := validateFiniteAccounting(root); err == nil {
		t.Fatal("historical preimage impersonated current source")
	}
	if err := apply(root, formatproof.Ledger, false, false); err != nil {
		t.Fatalf("explicit historical application preview refused: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, formatproof.Source)); err != nil || !bytes.Equal(got, before) {
		t.Fatal("historical preview changed the original input")
	}
	if err := apply(root, formatproof.Ledger, true, false); err != nil {
		t.Fatalf("explicit historical application refused: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, formatproof.Source)); err != nil || !bytes.Equal(got, original) {
		t.Fatal("historical application did not produce the exact origin")
	}
	if err := provePlan(root, formatproof.Ledger); err != nil {
		t.Fatalf("origin historical replay refused: %v", err)
	}
	previous := original
	for index, spaces := range []int{16, 17} {
		current := bytes.Replace(original, []byte("OmitOutputPin               bool"), []byte("OmitOutputPin"+strings.Repeat(" ", spaces)+"bool"), 1)
		put(formatproof.Source, current)
		if index == 0 {
			if err := apply(root, formatproof.Ledger, false, true); err == nil {
				t.Fatal("RED control accepted unclassified source drift")
			}
		}
		history, err = history.Append(previous, current)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(history)
		if err != nil {
			t.Fatal(err)
		}
		put(formatproof.Successors, raw)
		if err := apply(root, formatproof.Ledger, false, true); err != nil {
			t.Fatalf("classified current cut refused: %v", err)
		}
		if err := apply(root, formatproof.Ledger, true, false); err != nil {
			t.Fatal(err)
		}
		if err := provePlan(root, formatproof.Ledger); err != nil {
			t.Fatalf("exact original replay plus successor endpoint: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(root, formatproof.Ledger))
		if err != nil || !bytes.Equal(got, ledgerBytes) {
			t.Fatal("successor rewrote original reviewed ledger")
		}
		literals, err := goLiteralSites(formatproof.Source, original)
		if err != nil {
			t.Fatal(err)
		}
		if len(literals) == 0 {
			t.Fatal("reviewed source lost its literal facts")
		}
		selected, err := readEntryLiteral(root, formatproof.Source, literals[0].Function, literals[0].Ordinal)
		if err != nil || selected.Body != literals[0].Body {
			t.Fatalf("canonical current reader changed its selected literal: %v", err)
		}
		actual, err := goLiteralSites(formatproof.Source, current)
		if err != nil || !reflect.DeepEqual(literals, actual) {
			t.Fatalf("complete entry/generated literal facts changed: %v", err)
		}
		if _, err := baselineEntryGoldens(root, ledger); err != nil {
			t.Fatal(err)
		}
		put(formatproof.Source, previous)
		if err := apply(root, formatproof.Ledger, false, true); err == nil {
			t.Fatal("stale copied endpoint accepted")
		}
		put(formatproof.Source, current)
		previous = current
	}
	if err := os.Remove(filepath.Join(root, formatproof.Successors)); err != nil {
		t.Fatal(err)
	}
	if err := validateFiniteAccounting(root); err == nil {
		t.Fatal("copied source without its history accepted")
	}
	// The copied source carries its historical inputs as record bytes. It adds no
	// commit/fetch selector and must keep the independent history-input owner intact.
	inputs, err := os.ReadFile(filepath.Join(repo, ".github/test-proof-history-inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string][]string
	if err = json.Unmarshal(inputs, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest["github.com/division-sh/swarm/scripts/rewrite-stages-2566"]) == 0 {
		t.Fatal("existing history owner lost its finite dependencies")
	}
	if !reflect.DeepEqual(history.Records[0].Before, string(original)) {
		t.Fatal("copied origin is not self-contained")
	}
}

func TestFiniteAccountingEmptyHistoryCannotEraseCurrentEndpoint(t *testing.T) {
	before, err := os.ReadFile("formatproof/testdata/original.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	ledgerBytes, err := os.ReadFile("finite-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var ledger plan
	if err := json.Unmarshal(ledgerBytes, &ledger); err != nil {
		t.Fatal(err)
	}
	var approved change
	for _, c := range ledger.Changes {
		if c.File == formatproof.Source {
			approved = c
		}
	}
	original, err := rewrite(approved, before)
	if err != nil || digest(original) != formatproof.Origin {
		t.Fatalf("immutable origin unavailable: %v", err)
	}
	current := bytes.Replace(original, []byte("OmitOutputPin               bool"), []byte("OmitOutputPin                bool"), 1)
	history, err := (formatproof.History{Version: 1, Records: []formatproof.Record{}}).Append(original, current)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	literals, err := goLiteralSites(formatproof.Source, original)
	if err != nil || len(literals) == 0 {
		t.Fatalf("reviewed generated source lost its literals: %v", err)
	}
	root := t.TempDir()
	put := func(path string, raw []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	put("scripts/rewrite-stages-2566/entries.json", []byte(`[]`))
	for _, test := range []struct {
		name          string
		source, chain []byte
		allowed       bool
	}{
		{"empty_origin", original, []byte(`{"version":1,"records":[]}`), true},
		{"complete_endpoint", current, complete, true},
		{"erased_history", current, []byte(`{"version":1,"records":[]}`), false},
		{"stale_endpoint", original, complete, false},
		{"preimage_is_not_current", before, []byte(`{"version":1,"records":[]}`), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			put(formatproof.Source, test.source)
			put(formatproof.Successors, test.chain)
			for name, invoke := range map[string]func() error{
				"copied_source": func() error { return validateFiniteAccounting(root) },
				"preparation":   func() error { return validateFinitePreparation(root, false, false, false, false, true) },
				"current_literal": func() error {
					_, err := readEntryLiteral(root, formatproof.Source, literals[0].Function, literals[0].Ordinal)
					return err
				},
				"retained_entries": func() error { _, err := readCurrentEntryGoldens(root); return err },
				"disk_entries":     func() error { _, err := baselineEntryGoldens(root, plan{}); return err },
				"embedded_generated": func() error {
					_, err := embeddedEntryGoldens(root, change{File: "platform-spec.yaml", Equivalence: []equivalence{{Site: `$["contract_formats"]["hello_world_example"]["schema_yaml"]`}}})
					return err
				},
			} {
				t.Run(name, func(t *testing.T) {
					err := invoke()
					if test.allowed {
						if err != nil {
							t.Fatalf("bound current source refused: %v", err)
						}
					} else if err == nil || !strings.Contains(err.Error(), "actual resolved endpoint") {
						t.Fatalf("current consumer accepted unbound source or failed for another reason: %v", err)
					}
				})
			}
		})
	}
}

func TestFiniteAccountingMalformedHistoryCannotBypassAnyCurrentConsumer(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, formatproof.Successors)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"records":[],"retire":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := change{File: formatproof.Source}
	for name, invoke := range map[string]func() error{
		"apply":            func() error { return apply(root, "missing-ledger", true, false) },
		"check":            func() error { return apply(root, "missing-ledger", false, true) },
		"replay":           func() error { return provePlan(root, "missing-ledger") },
		"disk":             func() error { _, err := baselineEntryGoldens(root, plan{}); return err },
		"retained_entries": func() error { _, err := readCurrentEntryGoldens(root); return err },
		"embedded":         func() error { _, err := embeddedEntryGoldens(root, c); return err },
		"current_literal":  func() error { _, err := readEntryLiteral(root, formatproof.Source, "missing", 1); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := invoke()
			if err == nil || !strings.Contains(err.Error(), "retire") {
				t.Fatalf("consumer bypassed the complete chain: %v", err)
			}
		})
	}
}

func TestAccountingCommandHasReadOnlyPlanningSurface(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "cmd", "swarm-test", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `args[0] == "accounting"`) {
		t.Fatal("the approved accounting command surface is absent")
	}
}
