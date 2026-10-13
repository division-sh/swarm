package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCriteriaCitationAdmittedAliases(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want bool
	}{
		{"Label", true}, {"[Label]", true}, {"text", true}, {"[text]", true},
		{"Status", false}, {"[Status]", false}, {"ID", false}, {"Time", false}, {"integer", false}, {"[[Label]]", false}, {"Missing", false},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			bundle := criteriaValidationTestBundle(t)
			flow := bundle.FlowTree.ByID["validation"]
			bundle.flowTypes = map[string]TypeCatalogDocument{"validation": {Scalars: map[string]ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}, "Time": {Base: "timestamp"}}, Enums: map[string]EnumTypeDecl{"Status": {Values: []string{"hard"}, Default: "hard"}}}}
			field := flow.Events["cto.spec_vetoed"].Payload.Properties["cites"]
			field.Type = tc.ref
			flow.Events["cto.spec_vetoed"].Payload.Properties["cites"] = field
			bundle.scopedEvents["validation::cto.spec_vetoed"] = flow.Events["cto.spec_vetoed"]
			if errs := validateEventCriteriaCitationFields(bundle); (len(errs) == 0) != tc.want {
				t.Fatalf("citation %s: %v, want valid=%v", tc.ref, errs, tc.want)
			}
		})
	}
}

func TestCriteriaCitationAliasesLoadedFlow(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want bool
	}{{"Label", true}, {"[Label]", true}, {"ID", false}, {"Time", false}, {"Status", false}} {
		t.Run(tc.ref, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: citation\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "child/schema.yaml"), "name: child\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "child/types.yaml"), "scalars: {Label: text, ID: uuid, Time: timestamp}\nenums:\n  Status: {values: [hard], default: hard}\n")
			writeFixtureFile(t, filepath.Join(root, "child/rules.yaml"), "review:\n  classes: {hard: {disposition: vetoed}}\n  rules: [{id: R1, class: hard, text: Requires review.}]\n")
			writeFixtureFile(t, filepath.Join(root, "child/events.yaml"), "vetoed:\n  cites: {type: '"+tc.ref+"', citation: {criteria: review, allowed_classes: [hard]}}\n")
			repo := repoRootForContractsTest(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if (err == nil && bundle != nil) != tc.want {
				t.Fatalf("loaded citation %s: %v, want valid=%v", tc.ref, err, tc.want)
			}
			if !tc.want && !strings.Contains(err.Error(), "requires text/string or list-of-text") {
				t.Fatalf("negative did not reach citation domain check: %v", err)
			}
		})
	}
}
