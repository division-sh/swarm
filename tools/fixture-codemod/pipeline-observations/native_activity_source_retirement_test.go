package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Retain the last live successor as evidence, not as a current rewrite target.
// #642/6203629394 pins the native replacement accepted at 00e0761809.
//
//go:embed testdata/native_activity_source_retirement_642.json
var nativeActivitySourceRetirementBytes []byte

const nativeActivitySourceRetirementFile = "internal/runtime/runforkexecution/activity_fork_execution_test.go"

type nativeActivitySourceRetirement struct {
	Authorization, SourceCommit, SourceSHA256, ReplacementFunction, SuccessorSourceCommit string
	Original                                                                              recipe
}

func nativeActivityRetirementEvidence(data []byte) (nativeActivitySourceRetirement, error) {
	var pin nativeActivitySourceRetirement
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "149bb2926316d84a09c0b8d796e136ebc86f80ab185fd22ca88094de19e4025f" {
		return pin, fmt.Errorf("activity retirement history changed")
	}
	if err := json.Unmarshal(data, &pin); err != nil {
		return pin, err
	}
	return pin, nil
}

func validateNativeActivityRetirement(row recipe, source, evidence []byte) error {
	pin, err := nativeActivityRetirementEvidence(evidence)
	if err != nil {
		return err
	}
	want := pin.Original
	want.Removed, want.Successor = true, ""
	if !reflect.DeepEqual(row, want) {
		return fmt.Errorf("unlisted activity retirement or changed historical recipe")
	}
	current, err := checkpointRecipes(source, []recipe{row})
	if err != nil || len(current) != 0 {
		return fmt.Errorf("retired activity method survives or source is malformed: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), row.File, source, parser.AllErrors)
	if err != nil {
		return err
	}
	replacement, err := uniqueFunction(file, pin.ReplacementFunction)
	if err != nil || replacement.Recv != nil {
		return fmt.Errorf("native activity replacement missing or ambiguous: %v", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(source)) != pin.SourceSHA256 {
		return fmt.Errorf("activity source differs from accepted %s", pin.SourceCommit)
	}
	return nil
}

func readNativeActivityRetirementSource(root string) ([]byte, error) {
	path := filepath.Join(root, nativeActivitySourceRetirementFile)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("activity retirement requires its existing regular source file")
	}
	return os.ReadFile(path)
}

func currentNativeActivityRetirement(t *testing.T) (recipe, []byte) {
	t.Helper()
	pin, err := nativeActivityRetirementEvidence(nativeActivitySourceRetirementBytes)
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row recipe
	count := 0
	for _, candidate := range rows {
		if sameReviewedRecipeKey(candidate, pin.Original) {
			row = candidate
			count++
		}
	}
	if count != 1 {
		t.Fatalf("exact retired activity recipe count=%d, want1", count)
	}
	source, err := readNativeActivityRetirementSource("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateNativeActivityRetirement(row, source, nativeActivitySourceRetirementBytes); err != nil {
		t.Fatal(err)
	}
	return row, source
}

func TestNativeActivitySourceRetirementKeepsHistoricalTransformations(t *testing.T) {
	row, _ := currentNativeActivityRetirement(t)
	pin, _ := nativeActivityRetirementEvidence(nativeActivitySourceRetirementBytes)
	if pin.Original.Removed || pin.Original.Successor == "" || pin.Original.Mechanical != nil {
		t.Fatal("retirement lost its original live successor/provenance")
	}
	matched := 0
	for _, historical := range runSetupTransitions(t) {
		if historical.File == row.File && historical.Function == row.Function {
			if historical.Removed || historical.Before != row.Before || historical.After != row.After {
				t.Fatal("retirement rewrote or skipped its frozen family144 transformation")
			}
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("frozen activity transformation count=%d, want1", matched)
	}
	for _, pair := range [][2]string{{row.Before, row.After}, {row.After, pin.Original.Successor}} {
		historical := recipe{File: row.File, Function: row.Function, Before: pair[0], After: pair[1]}
		before := []byte("package proof\n" + pair[0])
		after, changed, err := rewriteFunction(row.File, before, historical)
		if err != nil || !changed {
			t.Fatalf("historical activity rewrite: %t/%v", changed, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, historical); err != nil || changed {
			t.Fatalf("historical activity idempotence: %t/%v", changed, err)
		}
		hostile := strings.Replace(string(before), "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte(hostile), historical); err == nil {
			t.Fatal("historical activity accepted an unreviewed transformation")
		}
	}
}

func TestNativeActivitySourceRetirementRejectsReintroductionAndForgedEvidence(t *testing.T) {
	row, source := currentNativeActivityRetirement(t)
	pin, _ := nativeActivityRetirementEvidence(nativeActivitySourceRetirementBytes)
	for name, declaration := range map[string]string{
		"original":         row.Before,
		"mechanical":       row.After,
		"successor":        pin.Original.Successor,
		"modified":         strings.Replace(pin.Original.Successor, "{", "{ unreviewedAuthority();", 1),
		"ambiguous_old":    row.Before + "\n" + row.Before,
		"ambiguous_native": "func seedSelectedContractActivitySource() {}",
		"foreign_receiver": "func (f Other) seedSource() {}",
		"unlisted_helper":  "func unlistedActivityHelper() {}",
		"malformed":        "func",
	} {
		t.Run(name, func(t *testing.T) {
			hostile := append(append([]byte(nil), source...), []byte("\n"+declaration)...)
			if err := validateNativeActivityRetirement(row, hostile, nativeActivitySourceRetirementBytes); err == nil {
				t.Fatal("retirement accepted reintroduced or malformed source")
			}
		})
	}
	for _, field := range []string{"family", "file", "function", "receiver", "before", "after", "successor", "mechanical", "removed", "live_native", "unlisted"} {
		t.Run(field, func(t *testing.T) {
			mutant := row
			switch field {
			case "family":
				mutant.Family += "-unlisted"
			case "file":
				mutant.File = "unlisted.go"
			case "function":
				mutant.Function += "Unlisted"
			case "receiver":
				mutant.Before = strings.Replace(row.Before, "activityForkFixture", "Other", 1)
				mutant.After = strings.Replace(row.After, "activityForkFixture", "Other", 1)
			case "before":
				mutant.Before = row.After
			case "after":
				mutant.After = row.Before
			case "successor":
				mutant.Successor = pin.Original.Successor
			case "mechanical":
				mutant.Mechanical = &mechanicalSnapshot{SourceCommit: pin.SourceCommit, After: row.After}
			case "removed":
				mutant.Removed = false
			case "live_native":
				mutant.Function = pin.ReplacementFunction
				mutant.Before, mutant.After = "func seedSelectedContractActivitySource() {}", "func seedSelectedContractActivitySource() {}"
			case "unlisted":
				mutant.Function = "unlistedActivityHelper"
				mutant.Before, mutant.After = "func unlistedActivityHelper() {}", "func unlistedActivityHelper() {}"
			}
			if err := validateNativeActivityRetirement(mutant, source, nativeActivitySourceRetirementBytes); err == nil {
				t.Fatal("retirement accepted changed history or an unlisted/live helper")
			}
		})
	}
	for _, field := range []string{"Before", "After", "Successor", "SourceCommit", "SourceSHA256", "Authorization", "SuccessorSourceCommit"} {
		t.Run("evidence_"+field, func(t *testing.T) {
			var evidence map[string]json.RawMessage
			if err := json.Unmarshal(nativeActivitySourceRetirementBytes, &evidence); err != nil {
				t.Fatal(err)
			}
			if field == "Before" || field == "After" || field == "Successor" {
				var original map[string]json.RawMessage
				if err := json.Unmarshal(evidence["Original"], &original); err != nil {
					t.Fatal(err)
				}
				original[field] = json.RawMessage(`"changed historical endpoint"`)
				evidence["Original"], _ = json.Marshal(original)
			} else {
				evidence[field] = json.RawMessage(`"changed retirement evidence"`)
			}
			mutant, _ := json.Marshal(evidence)
			if err := validateNativeActivityRetirement(row, source, mutant); err == nil {
				t.Fatal("changed append-only retirement evidence was accepted")
			}
		})
	}
	root := t.TempDir()
	if _, err := readNativeActivityRetirementSource(root); err == nil {
		t.Fatal("missing activity file counted as exact retirement")
	}
	path := filepath.Join(root, nativeActivitySourceRetirementFile)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing.go"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := readNativeActivityRetirementSource(root); err == nil {
		t.Fatal("nonregular activity file counted as exact retirement")
	}
}
