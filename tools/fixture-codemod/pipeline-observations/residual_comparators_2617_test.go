package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
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
	row, actual, err := embeddedSpecHistoricalPair1801(row, actual)
	if err != nil {
		return false
	}
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
	if _, err := fixtureSetupCurrentEndpoint2617(row, actual); err == nil {
		return true
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	return residualComparatorMatches2617(row, actual, rows, loadResidualComparators2617(t))
}
