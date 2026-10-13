package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type r52DirectiveSuccessorPin struct {
	family, function, history, prior, successor string
}

var r52DirectiveSuccessorPins = []r52DirectiveSuccessorPin{
	{"native-api-adjacent-physical-counter", "TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay", "e8d989054244da2f565a981f2216df53f0d452b4711bc57cacfa0e0487141185", "066cc1e33cb20476729a7747bbb55eebdcb038557c26249945eb935362a5bffe", "95215bdf6e95a85244925a8e15df23dc226586cde25f329fec899ce87f71af1f"},
	{"native-run-setup-family144", "TestOperatorAgentSendDirectiveUsesCanonicalRuntimeSourceArtifact", "c0eaf89bc345b262f37e11cc05167e28265aca31d9b410fdcee8b38bdd7b9062", "26ad327def48912c3491bc904123ec5b006057f6e3b3f06df01cc0463ff8ea96", "8ebb5ec37d9696aed6360fc029546ab4f8a234241282004856659569e4951f94"},
}

// Only the two complete endpoints authorized by reviewer-b6145065880 reverse.
func reverseR52DirectiveSuccessor(row recipe, actual string, pin r52DirectiveSuccessorPin) error {
	if row.Family != pin.family || row.Function != pin.function || row.File != "internal/apiv1/operator_agent_control_test.go" {
		return fmt.Errorf("foreign directive recipe")
	}
	history := row
	history.Successor = ""
	raw, err := json.Marshal(history)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.history {
		return fmt.Errorf("directive proof history changed")
	}
	current, err := canonicalFunction(actual)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(current))) != pin.successor {
		return fmt.Errorf("unreviewed directive endpoint")
	}
	reviewed, err := canonicalFunction(row.Successor)
	if err != nil || current != reviewed {
		return fmt.Errorf("directive source differs from its successor")
	}
	roles := "runtimemanager.PersistenceRoles{LifecycleState: pg, LifecycleOutcome: pg, DirectiveOperations: pg, DirectiveTargets: pg}"
	config := ", Config: json.RawMessage(`{}`)"
	if strings.Count(current, roles) != 1 || strings.Count(current, config) != 1 {
		return fmt.Errorf("directive role/config cardinality changed")
	}
	reversed := strings.Replace(current, roles, "runtimemanager.PersistenceRoles{DirectiveOperations: pg, DirectiveTargets: pg}", 1)
	reversed = strings.Replace(reversed, config, "", 1)
	prior, err := canonicalFunction(reversed)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(prior))) != pin.prior {
		return fmt.Errorf("directive reversal changed other setup or assertions")
	}
	return nil
}

func TestR52DirectiveFixtureSuccessorsPreserveNativeSetupAndHostility(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, pin := range r52DirectiveSuccessorPins {
		t.Run(pin.function, func(t *testing.T) {
			var row *recipe
			for i := range rows {
				if rows[i].Family == pin.family && rows[i].Function == pin.function {
					if row != nil {
						t.Fatal("duplicate keyed directive recipe")
					}
					row = &rows[i]
				}
			}
			if row == nil {
				t.Fatal("authorized directive recipe missing")
			}
			source, err := os.ReadFile(filepath.Join("../../..", row.File))
			if err != nil {
				t.Fatal(err)
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, row.File, source, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			function, err := uniqueRecipeFunction(file, *row)
			if err != nil {
				t.Fatal(err)
			}
			actual := string(source[set.Position(function.Pos()).Offset:set.Position(function.End()).Offset])
			if err := reverseR52DirectiveSuccessor(*row, actual, pin); err != nil {
				t.Fatal(err)
			}
			for name, mutant := range map[string]string{
				"foreign_store":   strings.Replace(actual, "LifecycleState:      pg", "LifecycleState:      foreign", 1),
				"missing_outcome": strings.Replace(actual, "LifecycleOutcome:    pg,", "", 1),
				"late_config":     strings.Replace(actual, ", Config: json.RawMessage(`{}`)", "", 1),
				"assertion":       strings.Replace(actual, "t.Fatalf", "t.Logf", 1),
				"unrelated_body":  strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
			} {
				if mutant == actual || reverseR52DirectiveSuccessor(*row, mutant, pin) == nil {
					t.Fatalf("hostile directive delta accepted: %s", name)
				}
			}
			for _, field := range []string{"before", "after", "mechanical"} {
				mutant := *row
				switch field {
				case "before":
					mutant.Before += " changed"
				case "after":
					mutant.After += " changed"
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: "foreign"}
				}
				if reverseR52DirectiveSuccessor(mutant, actual, pin) == nil {
					t.Fatalf("historical directive metadata changed: %s", field)
				}
			}
		})
	}
}
