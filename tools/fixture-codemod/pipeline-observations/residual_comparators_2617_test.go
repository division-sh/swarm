package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type residualComparatorPin2617 struct {
	Family, File, Function, BeforeSHA256, AfterSHA256, SuccessorSHA256 string
	HistorySHA256                                                      string
	LostCuts                                                           []string
}

type historicalComparatorEndpoint2617 struct {
	Source, File, Function, SHA256, Body string
}

type residualComparatorPacket2617 struct {
	Source, Authorization, MapSHA256 string
	Rows                             []residualComparatorPin2617
	SelectedAgentInputHistorical     historicalComparatorEndpoint2617
}

func loadResidualComparators2617(t *testing.T) residualComparatorPacket2617 {
	t.Helper()
	raw, err := os.ReadFile("testdata/residual_comparators_2617.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "f6d134d4d736e95c1b216498b77984c6488ff92f56ce655cb83c12f69bb6a8d3" {
		t.Fatal("finite residual comparator evidence changed")
	}
	var packet residualComparatorPacket2617
	if err := json.Unmarshal(raw, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.Source != "abf99ee69a296145238c4757abe321e9711c6df4" ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2617#issuecomment-6150546076" ||
		packet.MapSHA256 != "34e301112fcc1800e93ff97357c3f067f5156059c30109923536caae3233950f" || len(packet.Rows) != 13 {
		t.Fatal("wrong finite comparator source/authorization/row set")
	}
	return packet
}

func residualComparatorMatches2617(row recipe, actual string, corpus []recipe, packet residualComparatorPacket2617) bool {
	for _, pin := range packet.Rows {
		if row.Family != pin.Family || row.File != pin.File || row.Function != pin.Function {
			continue
		}
		count := 0
		for _, candidate := range corpus {
			if sameReviewedRecipeKey(candidate, row) {
				count++
			}
		}
		history := row
		history.Successor = ""
		raw, err := json.Marshal(history)
		return count == 1 && err == nil && fmt.Sprintf("%x", sha256.Sum256(raw)) == pin.HistorySHA256 &&
			row.Successor != "" && historicalCurrentSourcePinned(row.Before, pin.BeforeSHA256) &&
			historicalCurrentSourcePinned(row.After, pin.AfterSHA256) &&
			historicalCurrentSourcePinned(row.Successor, pin.SuccessorSHA256) &&
			historicalCurrentSourcePinned(actual, pin.SuccessorSHA256)
	}
	// Unchanged callers keep their exact After comparison, not a successor rule.
	return row.Successor == "" && canonicalComparatorEqual2617(actual, row.After)
}

func canonicalComparatorEqual2617(left, right string) bool {
	a, err := canonicalFunction(left)
	b, otherErr := canonicalFunction(right)
	return err == nil && otherErr == nil && a == b
}

func residualComparatorCurrent2617(t *testing.T, row recipe, actual string) bool {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	return residualComparatorMatches2617(row, actual, rows, loadResidualComparators2617(t))
}

func assertSelectedAgentInputHistoricalEndpoint2617(t *testing.T) {
	t.Helper()
	historical := loadResidualComparators2617(t).SelectedAgentInputHistorical
	if historical.Source != "134a521a818fdaedaf0b5f359696584b93aae5bb" ||
		historical.File != "internal/runtime/runforkexecution/constructed_agent_input_execution_test.go" ||
		historical.Function != "proveSelectedConstructedAgentInput" ||
		historical.SHA256 != "49bf1fb2c3444f00ea8a75d3e1352aae24aa300a687c96a1f2a249220ea14d7f" ||
		!historicalCurrentSourcePinned(historical.Body, historical.SHA256) {
		t.Fatal("lost the named historical134a521a8 selected-input endpoint")
	}
}

func TestResidualComparators2617PreserveHistoryAndRejectUnlistedCurrentPairs(t *testing.T) {
	packet := loadResidualComparators2617(t)
	assertSelectedAgentInputHistoricalEndpoint2617(t)
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, pin := range packet.Rows {
		t.Run(pin.Function, func(t *testing.T) {
			index := -1
			for i, row := range rows {
				if row.Family == pin.Family && row.File == pin.File && row.Function == pin.Function {
					if index != -1 {
						t.Fatal("duplicate reviewed residual key")
					}
					index = i
				}
			}
			if index == -1 {
				t.Fatal("missing reviewed residual key")
			}
			row := rows[index]
			name := row.Function
			if row.Family == "native-pipeline-delivery-exact-join-semantic" || row.Family == "native-pipeline-delivery-bus-semantic" {
				name = nativePipelineRecipeReplacementName(t, row)
			}
			actual := selectedCausalObservationBody(t, row.File, name)
			if !residualComparatorMatches2617(row, actual, rows, packet) {
				t.Fatal("exact historical/current residual endpoint refused")
			}
			missing := append(append([]recipe{}, rows[:index]...), rows[index+1:]...)
			duplicate := append(append([]recipe{}, rows...), row)
			if residualComparatorMatches2617(row, actual, missing, packet) || residualComparatorMatches2617(row, actual, duplicate, packet) {
				t.Fatal("missing/duplicate ledger identity accepted")
			}
			for _, field := range []string{"family", "file", "function", "before", "after", "mechanical", "removed", "successor"} {
				mutant := row
				switch field {
				case "family":
					mutant.Family += "-unlisted"
				case "file":
					mutant.File = "unlisted.go"
				case "function":
					mutant.Function += "Unlisted"
				case "before":
					mutant.Before = row.After
				case "after":
					mutant.After = row.Before
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("0", 40), After: row.After}
				case "removed":
					mutant.Removed = true
				case "successor":
					mutant.Successor = ""
				}
				changedRows := append([]recipe{}, rows...)
				changedRows[index] = mutant
				if residualComparatorMatches2617(mutant, actual, changedRows, packet) {
					t.Fatalf("changed residual %s accepted", field)
				}
			}
			for _, cut := range pin.LostCuts {
				changed := strings.Replace(actual, cut, "unreviewedCut", 1)
				if changed == actual {
					t.Fatalf("missing row-specific lost cut: %s", cut)
				}
				mutant := row
				mutant.Successor = changed
				changedRows := append([]recipe{}, rows...)
				changedRows[index] = mutant
				if residualComparatorMatches2617(row, changed, rows, packet) || residualComparatorMatches2617(mutant, changed, changedRows, packet) {
					t.Fatalf("changed actual or self-authorizing pair accepted: %s", cut)
				}
			}
			if row.Function == "proveSelectedConstructedAgentInput" && residualComparatorMatches2617(row, packet.SelectedAgentInputHistorical.Body, rows, packet) {
				t.Fatal("historical selected-input endpoint accepted interchangeably as current")
			}
		})
	}
	unchanged := recipe{Family: "unchanged", File: "unchanged.go", Function: "fixture", Before: "func fixture() { old() }", After: "func fixture() { native() }"}
	if !residualComparatorMatches2617(unchanged, unchanged.After, []recipe{unchanged}, packet) {
		t.Fatal("unchanged caller lost exact After comparison")
	}
	unchanged.Successor = "func fixture() { unreviewed() }"
	if residualComparatorMatches2617(unchanged, unchanged.Successor, []recipe{unchanged}, packet) {
		t.Fatal("unlisted exact source/Successor pair gained currentness authority")
	}
}
