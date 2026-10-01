package contracts

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

const r3AgentRules = "review:\n  classes: {hard: {disposition: none}}\n  rules:\n    - id: R1\n      class: hard\n      text: Review the input\n"
const r3MachineRules = "review:\n  classes: {hard: {disposition: none}}\n  inputs: {left: string, right: string}\n  rules:\n    - id: R1\n      class: hard\n      text: Compare the input\n      pin_candidate: false\n      check: {equal: {left: input.left, right: input.right}}\n"

func admitR3Rules(t *testing.T, body string) (RulesDocument, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.yaml")
	writeFixtureFile(t, path, body)
	return loadOptionalRulesDeclarations(path)
}

func TestR3PolicyLiteralPresenceAndInteriorMatrix(t *testing.T) {
	for _, tc := range []struct {
		body string
		want any
	}{
		{"null", nil}, {"''", ""}, {"0", 0}, {"false", false}, {"[]", []any{}}, {"{}", map[string]any{}},
		{"[0, false, null]", []any{0, false, nil}},
		{"{value: 1, description: user, override: false, other: kept}", map[string]any{"value": 1, "description": "user", "override": false, "other": "kept"}},
		{"'${policy.missing}'", "${policy.missing}"},
	} {
		t.Run(tc.body, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy.yaml")
			writeFixtureFile(t, path, "user: "+tc.body+"\n\"\": false\n\" spaced \": 0\n\"a.b\": null\n")
			policy, err := loadOptionalPolicyDeclarations(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(policy.Values) != 4 || !reflect.DeepEqual(policy.Values["user"].Value, tc.want) {
				t.Fatalf("literal projection=%#v want=%#v", policy, tc.want)
			}
			if v, ok := policy.Values[""]; !ok || v.Value != false {
				t.Fatal("empty exact key lost")
			}
			if v, ok := policy.Values[" spaced "]; !ok || v.Value != 0 {
				t.Fatal("spaced exact key lost")
			}
		})
	}
	for _, body := range []string{"1: x\n", "same: 1\nsame: 2\n", "root: {same: 1, same: 2}\n"} {
		path := filepath.Join(t.TempDir(), "policy.yaml")
		writeFixtureFile(t, path, body)
		if _, err := loadOptionalPolicyDeclarations(path); err == nil {
			t.Fatalf("invalid key/duplicate admitted: %s", body)
		}
	}
}

func TestR3RuleFieldPresenceMatrix(t *testing.T) {
	states := []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"}
	for _, tc := range []struct{ name, base, needle, line, scalar, sequence, mapping, admit string }{
		{"classes", r3AgentRules, "  classes: {hard: {disposition: none}}\n", "  classes: ", "hard", "[hard]", "{hard: {disposition: none}}", "mapping"},
		{"rules", r3AgentRules, "  rules:\n    - id: R1\n      class: hard\n      text: Review the input\n", "  rules: ", "R1", "[{id: R1, class: hard, text: Review}]", "{id: R1}", "sequence"},
		{"disposition", r3AgentRules, "  classes: {hard: {disposition: none}}\n", "  classes: {hard: {disposition: ", "none", "[none]", "{event: none}", "scalar"},
		{"id", r3AgentRules, "    - id: R1\n", "    - id: ", "R1", "[R1]", "{id: R1}", "scalar"},
		{"class", r3AgentRules, "      class: hard\n", "      class: ", "hard", "[hard]", "{class: hard}", "scalar"},
		{"text", r3AgentRules, "      text: Review the input\n", "      text: ", "Review", "[Review]", "{text: Review}", "scalar"},
		{"params", r3AgentRules, "      text: Review the input\n", "      params: ", "7", "[7]", "{x: 7}", "missing,empty_mapping,mapping"},
		{"inputs", r3MachineRules, "  inputs: {left: string, right: string}\n", "  inputs: ", "string", "[string]", "{left: string, right: string}", "mapping"},
		{"pin_candidate", r3MachineRules, "      pin_candidate: false\n", "      pin_candidate: ", "false", "[false]", "{flag: false}", "scalar"},
		{"check", r3MachineRules, "      check: {equal: {left: input.left, right: input.right}}\n", "      check: ", "equal", "[equal]", "{equal: {left: input.left, right: input.right}}", "mapping"},
	} {
		for _, state := range states {
			t.Run(tc.name+"/"+state, func(t *testing.T) {
				value := map[string]string{"null": "null", "empty": "''", "scalar": tc.scalar, "empty_sequence": "[]", "sequence": tc.sequence, "empty_mapping": "{}", "mapping": tc.mapping}[state]
				replacement := ""
				if state != "missing" {
					replacement = tc.line + value + "\n"
				}
				if tc.name == "disposition" {
					if state == "missing" {
						replacement = "  classes: {hard: {}}\n"
					} else {
						replacement = tc.line + value + "}}\n"
					}
				}
				if tc.name == "id" && state == "missing" {
					replacement = "    -\n"
				}
				if tc.name == "params" {
					replacement = tc.needle + replacement
				}
				body := strings.Replace(tc.base, tc.needle, replacement, 1)
				rules, err := admitR3Rules(t, body)
				want := strings.Contains(","+tc.admit+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t want=%t: %v\n%s", err == nil, want, err, body)
				}
				if err == nil && tc.name == "pin_candidate" {
					set, _ := rules.Validation("review")
					if set.Rules[0].PinCandidate == nil || *set.Rules[0].PinCandidate {
						t.Fatal("explicit false lost")
					}
				}
			})
		}
	}
}

func TestR3RuleClosedVariantsAndFiniteParams(t *testing.T) {
	for _, body := range []string{
		strings.Replace(r3AgentRules, "review:\n", "review:\n  unknown: null\n", 1),
		strings.Replace(r3AgentRules, "disposition: none", "disposition: none, unknown: null", 1),
		strings.Replace(r3AgentRules, "id: R1", "id: 7", 1),
		strings.Replace(r3AgentRules, "text: Review the input", "text: 7", 1),
		r3AgentRules + "      check: null\n", r3AgentRules + "      pin_candidate: false\n",
		strings.Replace(r3AgentRules, "review:\n", "review:\n  inputs: null\n", 1),
		strings.Replace(r3MachineRules, "      pin_candidate: false\n", "", 1),
		strings.Replace(r3MachineRules, "left: input.left", "left: ''", 1),
		strings.Replace(r3MachineRules, "{equal: {left: input.left, right: input.right}}", "{unknown: {}}", 1),
	} {
		if _, err := admitR3Rules(t, body); err == nil {
			t.Fatalf("malformed variant admitted:\n%s", body)
		}
	}
	for _, value := range []string{"null", "[]", "{}", ".inf", "-.inf", ".nan"} {
		if _, err := admitR3Rules(t, r3AgentRules+"      params: {p: "+value+"}\n"); err == nil {
			t.Fatalf("invalid param admitted %s", value)
		}
	}
	for _, value := range []string{"''", "0", "false", "1.5", "'policy.limit'"} {
		if _, err := admitR3Rules(t, r3AgentRules+"      params: {p: "+value+"}\n"); err != nil {
			t.Fatalf("inert param rejected %s: %v", value, err)
		}
	}
}

func TestR3RuleAliasMergeClosedFields(t *testing.T) {
	valid := "review: &rule\n  classes: &classes {hard: {disposition: none}}\n  rules: &rows [{id: R1, class: hard, text: Review}]\nsecond: *rule\nthird: {<<: *rule}\n"
	rules, err := admitR3Rules(t, valid)
	if err != nil || len(rules) != 3 {
		t.Fatalf("aliases: %#v, %v", rules, err)
	}
	if _, err := admitR3Rules(t, valid+"bad: {<<: *rule, unknown: null}\n"); err == nil {
		t.Fatal("merged unknown field admitted")
	}
	if _, err := admitR3Rules(t, valid+"review: *rule\n"); err == nil {
		t.Fatal("duplicate declaration admitted")
	}
}

func TestR3PolicyRulesDiskArtifactCatalogRetainedParity(t *testing.T) {
	for _, rulesBody := range []string{r3AgentRules, r3MachineRules, r3AgentRules + "      unknown: null\n"} {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: policy-rules-parity\n")
		writeFixtureFile(t, filepath.Join(root, "rules.yaml"), rulesBody)
		writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "user: {value: 1, description: data, override: false}\n\"\": null\n")
		disk, diskErr := loadOptionalRulesDeclarations(filepath.Join(root, "rules.yaml"))
		policy, err := loadOptionalPolicyDeclarations(filepath.Join(root, "policy.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := persisted.Decode()
		if err != nil {
			t.Fatal(err)
		}
		retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"artifact": artifact, "catalog": catalog, "retained": retained} {
			actual, err := loadOptionalRulesDeclarationsFromSource(source, "rules.yaml")
			if (err == nil) != (diskErr == nil) || (err == nil && !reflect.DeepEqual(actual, disk)) {
				t.Fatalf("%s rules differ: %#v %v / %#v %v", name, actual, err, disk, diskErr)
			}
			actualPolicy, err := loadOptionalPolicyDeclarationsFromSource(source, "policy.yaml")
			if err != nil || !reflect.DeepEqual(actualPolicy, policy) {
				t.Fatalf("%s literal differs", name)
			}
			if !bytes.Equal(source.LogicalBlob(), artifact.LogicalBlob()) || source.BundleHash() != artifact.BundleHash() {
				t.Fatal("source evidence changed")
			}
		}
		writeFixtureFile(t, filepath.Join(root, "rules.yaml"), rulesBody+"# exact source difference\n")
		changed, err := sourceartifact.AdmitDirectory(root)
		if err != nil || changed.BundleHash() == artifact.BundleHash() {
			t.Fatal("rule source omitted from hash")
		}
	}
}

func TestR3PolicyRulesProvenanceComposition(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: r3-provenance\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\ntask.done:\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      emit: task.done\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: business intent}\n  criteria: [review]\n  model: regular\n")
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "\" spaced \": &literal {value: 1, description: data, override: false}\nalias: *literal\n")
	writeFixtureFile(t, filepath.Join(root, "rules.yaml"), r3AgentRules)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"disk", "artifact"} {
		var bundle *WorkflowContractBundle
		if mode == "disk" {
			bundle, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
		} else {
			bundle, err = LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
		families := map[string]bool{}
		for _, entry := range bundle.EffectiveProvenance().Entries() {
			for _, family := range []string{"policy[", "rules[", "agents[", "nodes[", "events[", "schemas["} {
				if strings.HasPrefix(entry.Path, family) {
					families[family] = true
				}
			}
			if entry.Path == `policy[".:alias"].value` && (entry.Provenance.SourceLine != 2 || entry.Provenance.SourcePresence != "scalar") {
				t.Fatalf("alias introduction lost: %+v", entry)
			}
		}
		for _, family := range []string{"policy[", "rules[", "agents[", "nodes[", "events[", "schemas["} {
			if !families[family] {
				t.Fatalf("%s ledger family lost in %s", family, mode)
			}
		}
		if _, ok := bundle.EffectiveProvenance().Lookup(`policy[".: spaced "].override`); !ok {
			t.Fatal("exact literal interior provenance lost")
		}
		if _, ok := bundle.EffectiveProvenance().Lookup(`rules[".:review"].rules[0].id`); !ok {
			t.Fatal("rule field provenance lost")
		}
	}
}

func TestR3PolicyRulesDocumentWideExpansionBudget(t *testing.T) {
	for _, family := range []string{"policy", "rules"} {
		for _, mode := range []string{"alias", "merge"} {
			body := "base: &base {value: kept, description: data, override: false, nested: {a: 1, b: 2}}\n"
			if family == "rules" {
				body = "base: &base {classes: {hard: {disposition: none}}, rules: [{id: R1, class: hard, text: Review}]}\n"
			}
			for _, count := range []int{10, 10000} {
				var text strings.Builder
				text.WriteString(body)
				for i := 0; i < count; i++ {
					value := "*base"
					if mode == "merge" {
						value = "{<<: *base}"
					}
					fmt.Fprintf(&text, "row%d: %s\n", i, value)
				}
				path := filepath.Join(t.TempDir(), family+".yaml")
				writeFixtureFile(t, path, text.String())
				var err error
				if family == "policy" {
					_, err = loadOptionalPolicyDeclarations(path)
				} else {
					_, err = loadOptionalRulesDeclarations(path)
				}
				if (err == nil) != (count == 10) {
					t.Fatalf("%s/%s count=%d: %v", family, mode, count, err)
				}
			}
		}
	}
}
