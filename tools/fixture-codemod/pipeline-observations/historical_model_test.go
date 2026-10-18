package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
		current := q10PrivateTargetCurrentRecipe(t, row)
		if source, found, err := q10SharedCoordinatorCurrentSource(row); err != nil {
			t.Fatal(err)
		} else if found {
			current.Successor = source
		}
		historicalMechanicalRecipe(t, current)
	}
	if count != 24 {
		t.Fatalf("mechanical repair target count=%d, want24", count)
	}
	digest, err := mechanicalReceiptDigest(rows)
	if err != nil || digest != "d185875ba3ecbd3a2526f55212d90c45af85134246a75a326edebd827b17e5ad" {
		t.Fatalf("source-pinned mechanical/current receipts changed: %s/%v", digest, err)
	}
	// Retain the complete predecessor digest, including its old live Successors.
	// Only the two appended #2287 join-presentation receipts may reverse it.
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil || len(pins) != 113 {
		t.Fatalf("finite presentation receipt packet changed: %v", err)
	}
	prior := append([]recipe(nil), rows...)
	// Reverse only the seventy-four authorized current receipts before checking the
	// complete predecessor digest, including its prior live Successors.
	restoredSetup := 0
	for pinIndex := len(pins) - 1; pinIndex >= 39; pinIndex-- {
		pin := pins[pinIndex]
		for index, row := range prior {
			if !sameReviewedRecipeKey(row, pin.Original) {
				continue
			}
			history := row
			history.Successor = pin.Original.Successor
			body, err := reverseReviewedSuccessor2617(row.Successor, pin.Edits)
			oldBody := pin.Original.Successor
			if oldBody == "" {
				oldBody = pin.Original.After
			}
			if err != nil || body != oldBody || row.Successor != pin.Candidate || !reflect.DeepEqual(history, pin.Original) {
				t.Fatalf("#2730 setup changed historical mechanical evidence: %s/%v", row.Function, err)
			}
			prior[index] = pin.Original
			restoredSetup++
		}
	}
	if restoredSetup != 74 {
		t.Fatalf("#2730 exact setup endpoints restored=%d, want74", restoredSetup)
	}
	predecessorDigest, err := mechanicalReceiptDigest(prior)
	if err != nil || predecessorDigest != "637fcb76a19075ffc1dd93fc6a189652be2a50f2b199647d67cc0ffd58885fed" {
		t.Fatalf("#2730 changed its complete mechanical/current predecessor: %s/%v", predecessorDigest, err)
	}
	restored := 0
	for _, pin := range pins[36:39] {
		if pin.Original.Mechanical == nil {
			continue
		}
		if pin.Original.File != "internal/runtime/pipeline/workflow_join_supported_surface_external_test.go" ||
			(pin.Original.Function != "TestWorkflowJoinDurableEventBusDeliveryClaimPreservesExactDeclarationOnBothStores" &&
				pin.Original.Function != "TestWorkflowJoinScheduleOccurrencePreservesExactDeclarationThroughDurableEventBusOnBothStores") {
			t.Fatal("unlisted mechanical presentation successor")
		}
		for index, row := range prior {
			if !sameReviewedRecipeKey(row, pin.Original) {
				continue
			}
			history := row
			history.Successor = pin.Original.Successor
			body, err := reverseReviewedSuccessor2617(row.Successor, pin.Edits)
			if err != nil || body != pin.Original.Successor || row.Successor != pin.Candidate || !reflect.DeepEqual(history, pin.Original) {
				t.Fatalf("presentation successor changed historical mechanical evidence: %s/%v", row.Function, err)
			}
			prior[index] = pin.Original
			restored++
		}
	}
	if restored != 2 {
		t.Fatalf("exact mechanical presentation predecessors=%d, want2", restored)
	}
	if digest, err := mechanicalReceiptDigest(prior); err != nil || digest != "a47db59e4f7118a2c0cca2f463a68c28c32e571d9cea15892635e62d1340d7cf" {
		t.Fatalf("historical mechanical/current receipt digest changed: %s/%v", digest, err)
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
	for _, pin := range []struct{ file, function, digest, predecessor, beforeJoin string }{
		{"internal/runtime/pipeline/workflow_handler_native_external_test.go", "openWorkflowHandlerNativeFixture", "fa6ca50cc31122fef484b3dcb892798c18adb44ad1b86413bc3906969de22847", "", ""},
		{"internal/runtime/pipeline/workflow_handler_native_external_test.go", "workflowHandlerNativeFixtureFromSelected", "afe5a87932bb6b549ff9d2adc3f2fae1424c1d3d2c6b1ced3177a3218a9a13f5", "d7d11a06f23ac10d626104a7875740c93b0a0da064c5fa8011b49d37c3762ca7", "b40676927871515627d9e56bc3f921dcb735779b99038fda1ff3700fb028f02e"},
		{"internal/runtime/pipeline/delivery_native_owner_external_test.go", "workflowHandlerNativeCoordinator", "a30adb20d7f51cca29085aabb47ff1c659282e7c330a91340bf9f47f6fa6a2ef", "", "21ef5b19dd26e1d1bb42825279b4b1cad5788f1ed6b96efc445fb2fdbcf256cd"},
	} {
		t.Run(pin.function, func(t *testing.T) {
			source := selectedCausalObservationBody(t, pin.file, pin.function)
			historical, err := historicalHandlerAssemblySource(source, pin.function)
			if err != nil || !historicalCurrentSourcePinned(historical, pin.digest) {
				t.Fatal("extracted native handler owner/cleanup/publication assembly changed")
			}
			prior := historical
			if pin.beforeJoin != "" {
				var edits [][2]string
				switch pin.function {
				case "workflowHandlerNativeFixtureFromSelected":
					edits = [][2]string{{"pc, _, _ := workflowHandlerNativeCoordinator", "pc, _ := workflowHandlerNativeCoordinator"}}
				case "workflowHandlerNativeCoordinator":
					edits = [][2]string{
						{"(*pipeline.PipelineCoordinator, *runtimebus.EventBus, func(context.Context) error)", "(*pipeline.PipelineCoordinator, *runtimebus.EventBus)"},
						{"\n\tvar joinMu sync.Mutex\n\tjoined := false\n\tjoinCoordinator := func(ctx context.Context) error {\n\t\tjoinMu.Lock()\n\t\tdefer joinMu.Unlock()\n\t\tif joined {\n\t\t\treturn nil\n\t\t}\n\t\tif err := bus.WaitForQuiescence(ctx); err != nil {\n\t\t\treturn err\n\t\t}\n\t\tjoined = true\n\t\treturn nil\n\t}\n", "\n"},
						{"if err := joinCoordinator(join);", "if err := bus.WaitForQuiescence(join);"},
						{"return pc, bus, joinCoordinator", "return pc, bus"},
					}
				}
				for _, edit := range edits {
					if strings.Count(prior, edit[0]) != 1 {
						t.Fatal("native handler join continuation differs from its finite setup")
					}
					prior = strings.Replace(prior, edit[0], edit[1], 1)
				}
				if !historicalCurrentSourcePinned(prior, pin.beforeJoin) {
					t.Fatal("native handler join changed its prior owner/cleanup/publication assembly")
				}
			}
			if pin.predecessor != "" {
				const setup = "\n\tctx = originalPipelineFixtureContext(t, ctx, selected)\n"
				if strings.Count(prior, setup) != 1 || !historicalCurrentSourcePinned(strings.Replace(prior, setup, "\n", 1), pin.predecessor) {
					t.Fatal("original process setup changed historical handler assembly")
				}
			}
			for _, cut := range []string{"t.Helper()", "selected", "fact", "return"} {
				mutant := strings.Replace(source, cut, "foreignCut", 1)
				restored, err := historicalHandlerAssemblySource(mutant, pin.function)
				if mutant != source && err == nil && historicalCurrentSourcePinned(restored, pin.digest) {
					t.Fatalf("changed handler successor accepted: %s", cut)
				}
			}
			if pin.function == "workflowHandlerNativeFixtureFromSelected" {
				for _, cut := range []string{"prepare native handler construction: %w", "commit native handler construction: %w"} {
					for _, replacement := range []string{"foreign construction: %w", strings.Replace(cut, "%w", "%v", 1)} {
						mutant := strings.Replace(source, cut, replacement, 1)
						restored, err := historicalHandlerAssemblySource(mutant, pin.function)
						if mutant == source || err == nil && historicalCurrentSourcePinned(restored, pin.digest) {
							t.Fatalf("lost construction phase/error cause accepted: %s", replacement)
						}
					}
				}
				if _, err := historicalHandlerAssemblySource(historical, pin.function); err == nil {
					t.Fatal("historical unannotated errors accepted as the current handler assembly")
				}
			}
		})
	}
}

func historicalHandlerAssemblySource(source, name string) (string, error) {
	if name != "workflowHandlerNativeFixtureFromSelected" {
		return source, nil
	}
	for _, phase := range []string{"prepare", "commit"} {
		current := "return fmt.Errorf(\"" + phase + " native handler construction: %w\", err)"
		if strings.Count(source, current) != 1 {
			return "", fmt.Errorf("missing exact %s construction error cause", phase)
		}
		source = strings.Replace(source, current, "return err", 1)
	}
	return source, nil
}

func historicalCurrentSourcePinned(source, digest string) bool {
	value, err := canonicalFunction(source)
	return err == nil && fmt.Sprintf("%x", sha256.Sum256([]byte(value))) == digest
}
