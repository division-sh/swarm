package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Exact five-row composition authorized by reviewer-b6148187122.
//
//go:embed testdata/shared_coordinator_composition_2617.json
var sharedCoordinatorCompositionBytes2617 []byte

//go:embed testdata/reviewed_successors_2617.json
var priorSharedCoordinatorBytes2617 []byte

type sharedCoordinatorCompositionPin2617 struct {
	Family, File, Function   string
	Master, PriorC, Combined recipe
}

func sharedCoordinatorComposition2617(row recipe) (sharedCoordinatorCompositionPin2617, bool, error) {
	var pins []sharedCoordinatorCompositionPin2617
	if fmt.Sprintf("%x", sha256.Sum256(sharedCoordinatorCompositionBytes2617)) != "5b0d2cb6a802d3d5ff6f1bd9c408f523ccbbca354529ff41b1943bb6b17fc53f" {
		return sharedCoordinatorCompositionPin2617{}, false, fmt.Errorf("combined coordinator evidence changed")
	}
	if err := json.Unmarshal(sharedCoordinatorCompositionBytes2617, &pins); err != nil {
		return sharedCoordinatorCompositionPin2617{}, false, err
	}
	if len(pins) != 5 {
		return sharedCoordinatorCompositionPin2617{}, false, fmt.Errorf("combined coordinator row count changed")
	}
	for _, pin := range pins {
		if row.Family == pin.Family && row.File == pin.File && row.Function == pin.Function {
			return pin, true, nil
		}
	}
	return sharedCoordinatorCompositionPin2617{}, false, nil
}

func validateSharedCoordinatorComposition2617(row recipe, actual string, directive r52DirectiveSuccessorPin, pin sharedCoordinatorCompositionPin2617) error {
	if !reflect.DeepEqual(row, pin.Combined) {
		return fmt.Errorf("combined coordinator history or endpoint changed")
	}
	if err := reverseR52OriginalDirectiveSuccessor(pin.Master, pin.Master.Successor, directive); err != nil {
		return fmt.Errorf("original lifecycle endpoint: %w", err)
	}
	var prior reviewedSuccessorPacket2617
	// The separately pinned C packet is immutable, not refreshed by this merge.
	if fmt.Sprintf("%x", sha256.Sum256(priorSharedCoordinatorBytes2617)) != "ee6bdfc2cdf99e3257d92cfe2899219c2628b46eeaecff5e50d551c6387088a7" {
		return fmt.Errorf("prior C composition evidence changed")
	}
	if err := json.Unmarshal(priorSharedCoordinatorBytes2617, &prior); err != nil {
		return err
	}
	priorBody, err := reviewedSharedCoordinatorBody2617(pin.PriorC, prior)
	if err != nil {
		return err
	}
	current, err := canonicalFunction(actual)
	if err != nil {
		return err
	}
	combined, err := canonicalFunction(pin.Combined.Successor)
	if err != nil || current != combined {
		return fmt.Errorf("combined source differs from its exact successor")
	}
	call := "externalRuntimeTestManagerLifecycleRoles(bus, pg)"
	if strings.Count(current, call) != 1 {
		return fmt.Errorf("combined lifecycle role cardinality changed")
	}
	reversed := strings.Replace(current, call, "externalRuntimeTestManagerBusRoles(bus)", 1)
	priorCanonical, err := canonicalFunction(priorBody)
	if err != nil || reversed != priorCanonical {
		return fmt.Errorf("combined reversal changed C setup or assertions")
	}
	return nil
}

func reviewedSharedCoordinatorCurrentBody2617(row recipe, actual string, prior reviewedSuccessorPacket2617) (string, error) {
	_, composed, err := sharedCoordinatorComposition2617(row)
	if err != nil {
		return "", err
	}
	if composed {
		for _, pin := range r52DirectiveSuccessorPins {
			if pin.family == row.Family && pin.function == row.Function {
				if err := reverseR52DirectiveSuccessor(row, actual, pin); err != nil {
					return "", err
				}
				return row.Successor, nil
			}
		}
		return "", fmt.Errorf("combined lifecycle endpoint is unlisted")
	}
	if original, err := fixtureSetupCurrentEndpoint2617(row, actual); err == nil {
		if _, err := reviewedSharedCoordinatorBody2617(original, prior); err != nil {
			return "", err
		}
		return row.Successor, nil
	}
	return reviewedSharedCoordinatorBody2617(row, prior)
}

func TestSharedCoordinatorComposition2617PreservesBothEndpointsAndRefusesHostilePairs(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	prior := loadReviewedSuccessors2617(t)
	matched := 0
	for _, row := range rows {
		pin, found, err := sharedCoordinatorComposition2617(row)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			continue
		}
		matched++
		t.Run(row.Function, func(t *testing.T) {
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			if _, err := reviewedSharedCoordinatorCurrentBody2617(row, actual, prior); err != nil {
				t.Fatal(err)
			}
			master, candidate := pin.Master, pin.Combined
			master.Successor, candidate.Successor = "", ""
			if !reflect.DeepEqual(master, candidate) {
				t.Fatal("historical fields changed during composition")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("combined current endpoint is not inert: %v", err)
			}
			for _, cut := range []string{"externalRuntimeTestManagerLifecycleRoles(bus, pg)", "SourceArtifactFact", "InstanceKey", "t.Fatalf"} {
				mutant := row
				mutant.Successor = strings.Replace(actual, cut, "unreviewedCut", 1)
				if mutant.Successor == actual {
					continue
				}
				if _, err := reviewedSharedCoordinatorCurrentBody2617(mutant, mutant.Successor, prior); err == nil {
					t.Fatalf("matching hostile source/successor accepted: %s", cut)
				}
			}
			for _, field := range []string{"history", "unlisted", "missing_successor"} {
				mutant := row
				switch field {
				case "history":
					mutant.Before += " changed"
				case "unlisted":
					mutant.Function += "Unlisted"
				case "missing_successor":
					mutant.Successor = ""
				}
				if _, err := reviewedSharedCoordinatorCurrentBody2617(mutant, actual, prior); err == nil {
					t.Fatalf("combined mutation accepted: %s", field)
				}
			}
		})
	}
	if matched != 5 {
		t.Fatalf("combined endpoint count=%d, want5", matched)
	}
}
