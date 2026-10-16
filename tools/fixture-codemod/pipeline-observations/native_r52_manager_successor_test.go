package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type r52ManagerSuccessorPin struct {
	function, predecessorHash, successorHash string
}

var r52ManagerSuccessorPins = []r52ManagerSuccessorPin{
	{"newTestAgentManagerWithOptions", "8f8a9c9af2d39103fab22dbce3a7164c0ac66c52840fa364200da3b64bbf98e8", "14a8702f17c80cdcf0281d26b2f9bc2d7c71669f9e582510cc3f52c71ee8c787"},
	{"TestManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim", "deb91ca2a9aae65f5bea5ac738e9983e50abc5317270150a7ce33c81e95fdb4a", "db08b641eafb4b3bfb86c64db701fd4b6ac5818e1c73ddc974d6c9109e4ad36f"},
	{"TestRunningManagerInterventionFailureSettlesClaimBeforeShutdownAndRecovery", "8f360d2a059091ab1765dca8765dcc38ba4cb6fc85fa4c82115766096bc35ef9", "d8806c423573b60e3edef61538c3ba04d3e452336d4d4d6ea51790f6bb52c1cf"},
	{"TestRunningManagerInterventionSettlementFailureShutsDownAndRecoversClaim", "da1087ca5fa073a6917ccbc427161f79581ef3caa9285f11847e7d6743da45ee", "d7dd1fa5d58e18cf4cf2ccbd60084a894be1e7dc4144ad3c7df59140f41af0c0"},
	{"TestSourceSetTransitionKeepsRealEventBusDeliveryPendingUntilAggregateRelease", "116bd275b573f69bcc1de20d6cbe62553ce113094c9158b8430f460aa1e91eeb", "a6d12ab42dc82c82bb04634781a0acb4f8af5d93f693b90e1562258160ee9f7d"},
	{"TestSourceSetTransitionRetainsDequeuedDeliveryAndRouteAcrossManagerCancellation", "7698f818c86f0a8132b4dc588d2679d6186b9dc3291e066c81a9931806d70cac", "d3e2eb8e2ea3620b5bc76ad300d1a790624f49cdd1aa9992b88051bade6fa854"},
	{"TestClaimedAttemptExecutorDoesNotInheritLaneAuthorityThroughEventBusDescendant", "9232da05a43c2e37892eb3802e7059abd2aa0392b57917c09a81808415e3bdd8", "5638657a55268d5e56b49fcfc4467a8e6ea9990faffe4415aba9f004c4e6d456"},
	{"TestRunningManagerDeliveryCarrierDispositionMatrix", "8de5812d9b6e7b959de149b479041af6ee5ac0deef858dfb296453d9bffe58ae", "767c7f2732e3b862f25649e0f98013b1368afcacb375966e6948cbb0ecb43c66"},
}

// This reverses only the eight complete bodies authorized at 5848841d3.
func reverseR52ManagerSuccessor(row recipe, actual string) (string, error) {
	var pin *r52ManagerSuccessorPin
	for i := range r52ManagerSuccessorPins {
		if r52ManagerSuccessorPins[i].function == row.Function {
			pin = &r52ManagerSuccessorPins[i]
		}
	}
	if pin == nil || row.Successor == "" {
		return "", fmt.Errorf("unapproved R5.2 successor %s", row.Function)
	}
	history := row
	history.Successor = ""
	raw, err := json.Marshal(history)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.predecessorHash {
		return "", fmt.Errorf("R5.2 predecessor metadata changed: %s", row.Function)
	}
	current, err := canonicalFunction(actual)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(current))) != pin.successorHash {
		return "", fmt.Errorf("R5.2 complete successor changed: %s", row.Function)
	}
	reviewed, err := canonicalFunction(row.Successor)
	if err != nil || current != reviewed {
		return "", fmt.Errorf("R5.2 source differs from its successor: %s", row.Function)
	}
	var restored string
	if row.Function == "newTestAgentManagerWithOptions" {
		model := "\tunitLifecycle := opts.LifecycleStore == nil\n\tif unitLifecycle {\n\t\topts.LifecycleStore = &ephemeralLifecyclePersistenceProbe{newLifecyclePersistenceProbe()}\n\t}\n"
		marker := "\tif opts.SessionResetter == nil && opts.Sessions != nil {"
		projection := "\tif opts.SessionLifecycle == nil && opts.Sessions != nil {\n\t\topts.SessionLifecycle, _ = opts.Sessions.(sessions.LifecycleProjection)\n\t}\n"
		if strings.Count(current, model) != 1 || strings.Count(current, marker) != 1 {
			return "", fmt.Errorf("R5.2 constructor owner cuts changed")
		}
		restored = strings.Replace(current, model, "", 1)
		if strings.Count(restored, "if unitLifecycle {") != 1 {
			return "", fmt.Errorf("R5.2 constructor finalizer cut changed")
		}
		restored = strings.Replace(restored, "if unitLifecycle {", "if opts.LifecycleStore == nil {", 1)
		restored = strings.Replace(restored, marker, projection+marker, 1)
	} else {
		file, err := parser.ParseFile(token.NewFileSet(), "successor.go", "package proof\n"+current, parser.AllErrors)
		if err != nil {
			return "", err
		}
		calls := 0
		valid := true
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || method.Sel.Name != "spawnAgentInternal" {
				return true
			}
			calls++
			if len(call.Args) != 3 {
				valid = false
				return true
			}
			flag, ok := call.Args[2].(*ast.Ident)
			if !ok || flag.Name != "true" {
				valid = false
				return true
			}
			flag.Name = "false"
			return true
		})
		if calls != 1 || !valid {
			return "", fmt.Errorf("R5.2 initial registration is not one exact true literal")
		}
		var output bytes.Buffer
		if err := format.Node(&output, token.NewFileSet(), file.Decls[0]); err != nil {
			return "", err
		}
		restored = output.String()
	}
	before, err := canonicalFunction(row.After)
	if err != nil {
		return "", err
	}
	reversed, err := canonicalFunction(restored)
	if err != nil || reversed != before {
		return "", fmt.Errorf("R5.2 reversal changed other setup, assertions or temporal cuts: %s", row.Function)
	}
	return reversed, nil
}

func TestR52ManagerFixtureSuccessorsPreserveNativeSetupAndHostility(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, pin := range r52ManagerSuccessorPins {
		t.Run(pin.function, func(t *testing.T) {
			var selected *recipe
			for i := range rows {
				if rows[i].Function == pin.function {
					if selected != nil {
						t.Fatal("ambiguous finite recipe")
					}
					selected = &rows[i]
				}
			}
			if selected == nil {
				t.Fatal("approved recipe missing")
			}
			source, err := os.ReadFile(filepath.Join("../../..", selected.File))
			if err != nil {
				t.Fatal(err)
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, selected.File, source, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			function, err := uniqueRecipeFunction(file, *selected)
			if err != nil {
				t.Fatal(err)
			}
			actual := repair2698HistoricalBody(t, selected.File, function.Name.Name)
			if _, err := reverseR52ManagerSuccessor(*selected, actual); err != nil {
				t.Fatal(err)
			}
			mutants := map[string]string{
				"unknown_body":           strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
				"assertion":              strings.Replace(actual, "t.Fatal", "t.Log", 1),
				"temporal_cut":           strings.Replace(actual, "context.Background()", "context.TODO()", 1),
				"initial_receiver":       strings.Replace(actual, ".spawnAgentInternal", ".unreviewedRegistration", 1),
				"duplicate_registration": strings.Replace(actual, "{", "{ am.spawnAgentInternal(ctx, record, true);", 1),
				"delivery_owner":         strings.Replace(actual, "stores[0]", "stores[1]", 1),
				"raw_fallback":           strings.Replace(actual, "{", "{ opts.DeliveryStore = newManagerDeliveryTestStore(t);", 1),
			}
			if pin.function != "newTestAgentManagerWithOptions" {
				changed, err := parser.ParseFile(token.NewFileSet(), "hostile.go", "package proof\n"+actual, parser.AllErrors)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				ast.Inspect(changed, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					method, ok := call.Fun.(*ast.SelectorExpr)
					if ok && method.Sel.Name == "spawnAgentInternal" && len(call.Args) == 3 {
						count++
						call.Args[1] = ast.NewIdent("foreignRecord")
					}
					return true
				})
				if count != 1 {
					t.Fatalf("hostile argument cut changed: %d", count)
				}
				var output bytes.Buffer
				if err := format.Node(&output, token.NewFileSet(), changed.Decls[0]); err != nil {
					t.Fatal(err)
				}
				mutants["initial_arguments"] = output.String()
			}
			for name, mutant := range mutants {
				if mutant == actual {
					continue
				}
				if _, err := reverseR52ManagerSuccessor(*selected, mutant); err == nil {
					t.Errorf("accepted hostile %s", name)
				}
			}
			for _, field := range []string{"Before", "After", "File", "Family", "Removed", "Mechanical"} {
				mutant := *selected
				switch field {
				case "Before":
					mutant.Before += " "
				case "After":
					mutant.After += " "
				case "File":
					mutant.File += ".foreign"
				case "Family":
					mutant.Family += "-foreign"
				case "Removed":
					mutant.Removed = true
				case "Mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("a", 40)}
				}
				if _, err := reverseR52ManagerSuccessor(mutant, actual); err == nil {
					t.Errorf("accepted changed historical %s", field)
				}
			}
		})
	}
}
