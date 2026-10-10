package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Check today's owner first, then give unchanged mechanical oracles their
// actual historical target. The historical target is never a source rewrite.
func historicalMechanicalRecipe(t *testing.T, row recipe) recipe {
	t.Helper()
	files, changes, err := prepareFiles("../../..", []recipe{row})
	if err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("current semantic successor diverged: %s/%v", row.Function, err)
	}
	if row.Mechanical != nil {
		row.After = row.Mechanical.After
	}
	return row
}

func mechanicalReceiptDigest(rows []recipe) (string, error) {
	var receipts []recipe
	for _, row := range rows {
		if row.Mechanical != nil {
			receipts = append(receipts, row)
		}
	}
	data, err := json.Marshal(receipts)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func TestHistoricalMechanicalReceiptsAreFiniteAndSourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Mechanical == nil {
			continue
		}
		count++
		if err := validateRecipeSnapshots(row); err != nil {
			t.Fatal(err)
		}
		historicalMechanicalRecipe(t, row)
	}
	if count != 24 {
		t.Fatalf("mechanical repair target count=%d, want24", count)
	}
	digest, err := mechanicalReceiptDigest(rows)
	if err != nil || digest != "260e72f8cbefd7e230ddfe72ddf6262166027c90c207335bb150bb000948afff" {
		t.Fatalf("source-pinned mechanical/current receipts changed: %s/%v", digest, err)
	}
}

func TestHistoricalMechanicalSnapshotsRejectInvalidAndCrossedCuts(t *testing.T) {
	row := recipe{File: "fixture.go", Function: "original", Before: "func original() { raw() }", After: "func native() { acknowledged() }", Successor: "func native() { current() }"}
	row.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("1", 40), BeforeSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))), After: "func native() { mechanical() }"}
	for _, source := range []string{row.Before, row.After, row.Mechanical.After, "func native() { foreign() }", "func native() { current() }; func native() {}", "func original() {}; func native() { current() }", "func"} {
		t.Run(source, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, row.File), []byte("package probe\n"+source), 0600); err != nil {
				t.Fatal(err)
			}
			files, changes, err := prepareFiles(root, []recipe{row})
			if err == nil && len(files) == 0 && len(changes) == 0 {
				t.Fatal("missing/ambiguous/old/foreign current semantic owner accepted")
			}
		})
	}
	if _, _, err := prepareFiles(t.TempDir(), []recipe{row}); err == nil {
		t.Fatal("missing current source accepted")
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, row.File), []byte("package probe\n"+row.Successor), 0600); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"commit", "before", "after"} {
		t.Run(change, func(t *testing.T) {
			broken := row
			mechanical := *row.Mechanical
			broken.Mechanical = &mechanical
			switch change {
			case "commit":
				mechanical.SourceCommit = "unknown"
			case "before":
				mechanical.BeforeSHA256 = strings.Repeat("0", 64)
			case "after":
				mechanical.After = "func"
			}
			if _, _, err := prepareFiles(root, []recipe{broken}); err == nil {
				t.Fatal("invalid historical proof metadata accepted")
			}
		})
	}
	if files, changes, err := prepareFiles(root, []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("exact current successor not inert: %v", err)
	}
	row.Removed, row.Successor = true, ""
	if _, _, err := prepareFiles(root, []recipe{row}); err == nil {
		t.Fatal("historical receipt permitted a surviving retired owner")
	}
}

func TestHistoricalMechanicalCurrentHandlerAssemblyIsSourcePinned(t *testing.T) {
	for _, pin := range []struct{ file, function, digest string }{
		{"internal/runtime/pipeline/workflow_handler_native_external_test.go", "openWorkflowHandlerNativeFixture", "fa6ca50cc31122fef484b3dcb892798c18adb44ad1b86413bc3906969de22847"},
		{"internal/runtime/pipeline/workflow_handler_native_external_test.go", "workflowHandlerNativeFixtureFromSelected", "79a15fe68e7ead7db5bf58ab26f04c0495429ce7a254bf9924ee59f2a3ceef86"},
		{"internal/runtime/pipeline/delivery_native_owner_external_test.go", "workflowHandlerNativeCoordinator", "21ef5b19dd26e1d1bb42825279b4b1cad5788f1ed6b96efc445fb2fdbcf256cd"},
	} {
		t.Run(pin.function, func(t *testing.T) {
			source := selectedCausalObservationBody(t, pin.file, pin.function)
			if !historicalCurrentSourcePinned(source, pin.digest) {
				t.Fatal("extracted native handler owner/cleanup/publication assembly changed")
			}
			for _, cut := range []string{"t.Helper()", "selected", "fact", "return"} {
				mutant := strings.Replace(source, cut, "foreignCut", 1)
				if mutant != source && historicalCurrentSourcePinned(mutant, pin.digest) {
					t.Fatalf("changed handler successor accepted: %s", cut)
				}
			}
		})
	}
}

func historicalCurrentSourcePinned(source, digest string) bool {
	value, err := canonicalFunction(source)
	return err == nil && fmt.Sprintf("%x", sha256.Sum256([]byte(value))) == digest
}
