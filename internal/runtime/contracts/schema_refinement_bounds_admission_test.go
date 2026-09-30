package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadRefinementBound(t *testing.T, family, kind, refinement, source string) (*WorkflowContractBundle, error) {
	t.Helper()
	root := t.TempDir()
	field := fmt.Sprintf("value: {type: %q, %s: %s}", kind, refinement, source)
	file, text := family+".yaml", ""
	switch family {
	case "receiver":
		file, text = "schema.yaml", "stages: []\ninstance_variables:\n  variables:\n    "+field+"\n"
	case "types":
		text = "types:\n  Probe:\n    " + field + "\n"
	case "entities":
		text = "probe:\n  " + field + "\n"
	case "events":
		text = "probe.ready:\n  " + field + "\n"
	default:
		t.Fatalf("unknown family %q", family)
	}
	if err := os.WriteFile(filepath.Join(root, file), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	repo := repoRootForContractsTest(t)
	return LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
}

func TestSharedRefinementBoundPresenceMatrix(t *testing.T) {
	for _, family := range []string{"receiver", "types", "entities", "events"} {
		for _, bound := range []string{"min", "max"} {
			for _, tc := range []struct {
				name, source    string
				length, numeric bool
			}{
				{"null", "null", false, false},
				{"empty", "''", false, false},
				{"text", "'1'", false, false},
				{"boolean", "false", false, false},
				{"mapping", "{}", false, false},
				{"sequence", "[]", false, false},
				{"zero", "0", true, true},
				{"integer", "3", true, true},
				{"fraction", "1.5", false, true},
				{"negative-fraction", "-0.5", false, true},
				{"negative-integer", "-1", false, true},
				{"floating-kind", "1.0", false, true},
				{"integer-overflow", "18446744073709551615", false, true},
				{"numeric-overflow", "1e999", false, false},
				{"nan", ".nan", false, false},
				{"infinity", ".inf", false, false},
				{"negative-infinity", "-.inf", false, false},
			} {
				for _, refinement := range []string{"length", "range"} {
					t.Run(family+"/"+refinement+"/"+bound+"/"+tc.name, func(t *testing.T) {
						kind, valid := "text", tc.length
						if refinement == "range" {
							kind, valid = "numeric", tc.numeric
						}
						// Event acceptance schemas additionally enforce the existing I-JSON number limit.
						if family == "events" && refinement == "range" && tc.name == "integer-overflow" {
							valid = false
						}
						bundle, err := loadRefinementBound(t, family, kind, refinement, "{"+bound+": "+tc.source+"}")
						if valid {
							if err != nil {
								t.Fatalf("valid bound rejected: %v", err)
							}
							var r SchemaRefinements
							switch family {
							case "receiver":
								r = bundle.RootSchema.InstanceVariables.Variables["value"].Refinements
							case "types":
								r = bundle.RootTypes.Types["Probe"].Fields["value"].Refinements
							case "entities":
								r = bundle.RootEntities["probe"].Fields["value"].Refinements
							case "events":
								r = bundle.Events["probe.ready"].Payload.Properties["value"].Refinements
							}
							if refinement == "length" && ((bound == "min" && r.Length.Min == nil) || (bound == "max" && r.Length.Max == nil)) {
								t.Fatal("present length bound lost")
							}
							if refinement == "range" && ((bound == "min" && r.Range.Min == nil) || (bound == "max" && r.Range.Max == nil)) {
								t.Fatal("present range bound lost")
							}
							return
						}
						if err == nil {
							t.Fatal("malformed authored bound accepted")
						}
						if tc.name != "integer-overflow" && !strings.Contains(err.Error(), refinement) {
							t.Fatalf("refinement diagnostic lost: %v", err)
						}
					})
				}
			}
		}
	}
}

func TestSharedRefinementBoundMappingAndOrdering(t *testing.T) {
	for _, family := range []string{"receiver", "types", "entities", "events"} {
		for _, refinement := range []string{"length", "range"} {
			for _, source := range []string{"{}", "null", "[]", "{min: 2, max: 1}", "{min: 0, min: 1}", "{other: 0}"} {
				t.Run(family+"/"+refinement+"/"+source, func(t *testing.T) {
					kind := "text"
					if refinement == "range" {
						kind = "numeric"
					}
					if _, err := loadRefinementBound(t, family, kind, refinement, source); err == nil {
						t.Fatal("invalid refinement mapping admitted")
					}
				})
			}
		}
		for _, kind := range []string{"text", "[text]"} {
			t.Run(family+"/zero-length/"+kind, func(t *testing.T) {
				if _, err := loadRefinementBound(t, family, kind, "length", "{min: 0, max: 0}"); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
