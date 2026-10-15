package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const selectedConfigFile1801 = "internal/serveapp/startup_creation_handoff_test.go"
const selectedConfigFunction1801 = "TestComposedStartupCreationPublicationHandoffOnBothStores"

const priorConfigSetup1801 = `cfg, err := config.Load(writeServeRuntimeTestConfig(t))
				if err != nil {
					t.Fatal(err)
				}`

const selectedConfigSetup1801 = `loadedConfig, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: repoRootForTest(), ExplicitPath: writeServeRuntimeTestConfig(t)})
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadedConfig.Config`

// USER6170905244 and reviewer-g6183746080 permit only this selected-file setup
// edit. Full reversal pins every backend, scenario and non-config statement.
func validateSelectedConfigSuccessor1801(row recipe, source string) error {
	if row.Family != "native-run-setup-family144" || row.File != selectedConfigFile1801 || row.Function != selectedConfigFunction1801 || row.Removed || row.Mechanical != nil {
		return fmt.Errorf("selected-config recipe identity changed")
	}
	if fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "513b0a521d4a7cf8a2a5ef40cad900c49e51ed87101dc7c312e988b3170792fa" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "9f399852d8ef6933ed26f428faafd1dffecdadef90d1dd242f4d4c9fa2f01574" {
		return fmt.Errorf("selected-config historical snapshots changed")
	}
	candidate, err := canonicalFunction(row.Successor)
	if err != nil {
		return fmt.Errorf("selected-config successor: %w", err)
	}
	actual, err := canonicalFunction(source)
	if err != nil || actual != candidate {
		return fmt.Errorf("selected-config current source differs")
	}
	if strings.Count(candidate, selectedConfigSetup1801) != 1 {
		return fmt.Errorf("selected-config exact setup changed")
	}
	reversed := strings.Replace(candidate, selectedConfigSetup1801, priorConfigSetup1801, 1)
	prior, err := canonicalFunction(row.After)
	if err != nil || reversed != prior {
		return fmt.Errorf("selected-config successor changed other work or phase inventory")
	}
	before, err := runSetupAssertions(row.After)
	if err != nil {
		return err
	}
	after, err := runSetupAssertions(candidate)
	if err != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
		return fmt.Errorf("selected-config assertion inventory changed")
	}
	return nil
}

func requireSelectedConfigSuccessor1801(t *testing.T, rows []recipe) {
	t.Helper()
	matched := 0
	for _, row := range rows {
		if row.File != selectedConfigFile1801 || row.Function != selectedConfigFunction1801 {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if err := validateSelectedConfigSuccessor1801(row, actual); err != nil {
			t.Fatal(err)
		}
	}
	if matched != 1 {
		t.Fatalf("selected-config recipe multiplicity=%d", matched)
	}
}

func TestSelectedConfigAdmissionSuccessorRejectsChangedHistoryAndPairedSource(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	requireSelectedConfigSuccessor1801(t, rows)
	var row recipe
	for _, candidate := range rows {
		if candidate.File == selectedConfigFile1801 && candidate.Function == selectedConfigFunction1801 {
			row = candidate
		}
	}
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	for _, field := range []string{"family", "file", "function", "before", "after", "removed", "mechanical", "missing", "source", "successor", "paired_work", "paired_assertion", "paired_phase"} {
		t.Run(field, func(t *testing.T) {
			mutant, source := row, actual
			switch field {
			case "family":
				mutant.Family += "-unlisted"
			case "file":
				mutant.File = "internal/serveapp/other_test.go"
			case "function":
				mutant.Function += "Unlisted"
			case "before":
				mutant.Before += " "
			case "after":
				mutant.After += " "
			case "removed":
				mutant.Removed = true
			case "mechanical":
				mutant.Mechanical = &mechanicalSnapshot{}
			case "missing":
				mutant.Successor = ""
			case "source":
				source = row.After
			case "successor":
				mutant.Successor = row.After
			case "paired_work":
				mutant.Successor = strings.Replace(row.Successor, "{", "{ unreviewedSetup();", 1)
				source = mutant.Successor
			case "paired_assertion":
				mutant.Successor = strings.Replace(row.Successor, "t.Fatalf(", "t.Logf(", 1)
				source = mutant.Successor
			case "paired_phase":
				mutant.Successor = strings.Replace(row.Successor, `"sqlite", "postgres"`, `"postgres", "sqlite"`, 1)
				source = mutant.Successor
			}
			if err := validateSelectedConfigSuccessor1801(mutant, source); err == nil {
				t.Fatalf("changed %s accepted", field)
			}
		})
	}
}
