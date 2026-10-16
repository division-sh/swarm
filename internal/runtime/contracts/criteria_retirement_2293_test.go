package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func Test2293Q2CriteriaGrammarIsRetiredOnPresence(t *testing.T) {
	for _, value := range []string{"null", "''", "[]", "[review]", "{}", "{review: true}"} {
		t.Run("agent/"+value, func(t *testing.T) {
			_, err := admitW5Agents(t, "worker:\n  intent: {inline: review input}\n  criteria: "+value+"\n")
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, "intent") {
				t.Fatalf("criteria presence needs a typed refusal teaching intent, got %v", err)
			}
		})
		t.Run("event/"+value, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, "result: {type: text, citation: "+value+"}")
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, "validate") {
				t.Fatalf("citation presence needs a typed refusal teaching validate, got %v", err)
			}
		})
	}
	for _, body := range []string{
		r3AgentRules,
		r3MachineRules + "    - id: R2\n      class: hard\n      text: Review without a machine check\n",
	} {
		if _, err := admitR3Rules(t, body); err == nil || !strings.Contains(err.Error(), "check") {
			t.Fatalf("agent-only or mixed rules need a machine-check teaching refusal, got %v", err)
		}
	}
}

func Test2293Q2CriteriaRetirementKeepsMachineRulesAndBusinessFields(t *testing.T) {
	for _, value := range []string{"''", "0", "false", "1.5", "'policy.limit'"} {
		doc, err := admitR3Rules(t, r3MachineRules+"      params: {limit: "+value+"}\n")
		if err != nil {
			t.Fatalf("machine parameter %s refused: %v", value, err)
		}
		set, found := doc.Validation("review")
		if !found || len(set.Rules) != 1 || set.Classes["hard"].Disposition != "none" || set.Rules[0].Check.Equal == nil || set.Rules[0].PinCandidate == nil || *set.Rules[0].PinCandidate {
			t.Fatalf("machine check, disposition or explicit false lost: %#v", set)
		}
	}
	for _, value := range []string{"null", "[]", "{}", ".inf", "-.inf", ".nan"} {
		if _, err := admitR3Rules(t, r3MachineRules+"      params: {limit: "+value+"}\n"); err == nil {
			t.Fatalf("invalid machine parameter admitted: %s", value)
		}
	}
	entry, err := admitEventCatalogEntryForTest(t, "criteria: text\ncitation: text\ndescription: text\ncheck: boolean\n")
	if err != nil || len(entry.Payload.Properties) != 4 || len(entry.Payload.Required) != 4 {
		t.Fatalf("ordinary business fields were treated as grammar: %#v, %v", entry, err)
	}
}

func Test2293Q2BundleRejectsAgentOnlyRules(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: criteria-retirement\nstages: {done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "rules.yaml"), r3AgentRules)
	repo := repoRootForContractsTest(t)
	if bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); bundle != nil || err == nil || !strings.Contains(err.Error(), "check") {
		t.Fatalf("bundle accepted a retired agent-only rule set: bundle=%v error=%v", bundle != nil, err)
	}
	writeFixtureFile(t, filepath.Join(root, "rules.yaml"), r3MachineRules)
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); err != nil {
		t.Fatalf("supported machine-rule bundle refused: %v", err)
	}
}

func Test2293Q2EventMetadataIsRetiredOnPresence(t *testing.T) {
	for _, key := range []string{"description", "pattern", "length", "range", "equal_to", "immutable", "indexed"} {
		for _, value := range []string{"null", "''", "[]", "{}", "false"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := admitEventCatalogEntryForTest(t, "result: {type: text, "+key+": "+value+"}")
				diagnostic, ok := AsLoaderDiagnostic(err)
				teaching := "YAML comment"
				if key == "indexed" {
					teaching = "state-field"
				}
				if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, teaching) {
					t.Fatalf("retired/foreign event option %s needs source refusal teaching %q: %v", key, teaching, err)
				}
			})
		}
	}
	entry, err := admitEventCatalogEntryForTest(t, "# prose is inert\nresult: {type: text} # no constraints\n")
	if err != nil || len(entry.Payload.Properties) != 1 || entry.Payload.Properties["result"].Type != "text" {
		t.Fatalf("comment or type-only declaration changed the payload: %#v error=%v", entry, err)
	}
}
