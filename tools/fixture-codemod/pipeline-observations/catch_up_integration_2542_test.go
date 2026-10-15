package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func catchUp2542RecipeKey(row recipe) string {
	return fmt.Sprintf("%s\t%s\t%s\t%x", row.Family, row.File, row.Function, sha256.Sum256([]byte(row.Before)))
}

func catchUp2542RecipeDigest(row recipe) string {
	raw, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	digest, err := selectedreadrole.CatchUp2542JSONDigest(raw)
	if err != nil {
		panic(err)
	}
	return digest
}

func catchUp2542ReadRecipes(ref string) ([]recipe, error) {
	raw, err := exec.Command("git", "-C", "../../..", "show", ref+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil {
		return nil, err
	}
	var rows []recipe
	err = json.Unmarshal(raw, &rows)
	return rows, err
}

func catchUp2542HistoricalRows(t *testing.T, rows []recipe, master bool) []recipe {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = selectedreadrole.CatchUp2542HistoricalRecipes("../../..", raw, master)
	if err != nil {
		t.Fatal(err)
	}
	var prior []recipe
	if err := json.Unmarshal(raw, &prior); err != nil {
		t.Fatal(err)
	}
	return prior
}

var catchUp2542PriorRows sync.Map

// Restore only the exact authenticated pair for the old master comparator.
// Its immutable packets and hostile controls remain unchanged.
func catchUp2542PriorMasterPair(row recipe, actual string) (recipe, string, error) {
	return catchUp2542PriorPair(row, actual, true)
}

func catchUp2542PriorLanePair(row recipe, actual string) (recipe, string, error) {
	return catchUp2542PriorPair(row, actual, false)
}

func requireCatchUp2542MasterPair(t *testing.T, row recipe, actual string) (recipe, string) {
	t.Helper()
	prior, source, err := catchUp2542PriorMasterPair(row, actual)
	if err != nil {
		t.Fatal(err)
	}
	return prior, source
}

func catchUp2542PriorPair(row recipe, actual string, master bool) (recipe, string, error) {
	packet, err := selectedreadrole.CatchUp2542Receipts()
	if err != nil {
		return recipe{}, "", err
	}
	for _, pin := range packet.Rows {
		ref, priorDigest := packet.Master, pin.MasterSHA256
		if !master {
			ref, priorDigest = packet.Lane, pin.LaneSHA256
		}
		if pin.Key != catchUp2542RecipeKey(row) || pin.ComposedSHA256 != catchUp2542RecipeDigest(row) || priorDigest == pin.ComposedSHA256 {
			continue
		}
		endpoint := row.Successor
		if endpoint == "" {
			endpoint = row.After
		}
		want, err := canonicalFunction(endpoint)
		got, sourceErr := canonicalFunction(actual)
		if err != nil || sourceErr != nil || got != want {
			return recipe{}, "", fmt.Errorf("changed catch-up current source pair: %s", row.Function)
		}
		cached, found := catchUp2542PriorRows.Load(ref)
		if !found {
			rows, err := catchUp2542ReadRecipes(ref)
			if err != nil {
				return recipe{}, "", err
			}
			indexed := map[string]recipe{}
			for _, old := range rows {
				indexed[catchUp2542RecipeKey(old)] = old
			}
			cached = indexed
			catchUp2542PriorRows.Store(ref, cached)
		}
		prior, found := cached.(map[string]recipe)[pin.Key]
		if !found || catchUp2542RecipeDigest(prior) != priorDigest {
			return recipe{}, "", fmt.Errorf("catch-up master predecessor differs: %s", row.Function)
		}
		endpoint = prior.Successor
		if endpoint == "" {
			endpoint = prior.After
		}
		return prior, endpoint, nil
	}
	return row, actual, nil
}

func TestCatchUp2542RecipeCompositionPreservesBothSignedInventories(t *testing.T) {
	packet, err := selectedreadrole.CatchUp2542Receipts()
	if err != nil {
		t.Fatal(err)
	}
	if packet.Base != "26701e7b187579220dffe72e01debd2e89cd2675" || packet.Master != "19dc188f6d57985a3360a619ea57b890b3c148dd" || packet.Lane != "dec6f1a009400d230b0bb92c83c0997ae35b2938" ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/2542#issuecomment-6194296317" || len(packet.Rows) != 407 || len(packet.Files) != 229 {
		t.Fatal("finite integration boundary changed")
	}
	var current []recipe
	if err := json.Unmarshal(recipeBytes, &current); err != nil {
		t.Fatal(err)
	}
	master := catchUp2542HistoricalRows(t, current, true)
	lane := catchUp2542HistoricalRows(t, current, false)
	if len(master) != 893 || len(lane) != 1856 || len(current) != len(lane) {
		t.Fatal("integration lost or appended reviewed rows")
	}
	for index, row := range current {
		old := lane[index]
		row.Successor, row.Removed = old.Successor, old.Removed
		if !reflect.DeepEqual(row, old) {
			t.Fatalf("immutable lane receipt/order changed: %s", row.Function)
		}
		if index < len(master) {
			oldMaster := master[index]
			oldMaster.Successor, oldMaster.Removed = old.Successor, old.Removed
			if !reflect.DeepEqual(oldMaster, old) {
				t.Fatalf("immutable master receipt/order changed: %s", row.Function)
			}
		}
	}
	assertCatchUp2542RowMap(t, packet, current, master, lane)
	assertCatchUp2542CurrentFiles(t, packet)
	if files, changes, err := prepareFiles("../../..", current); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("complete current endpoint packet is not inert: %v", err)
	}
}

func assertCatchUp2542RowMap(t *testing.T, packet selectedreadrole.CatchUp2542Packet, current, master, lane []recipe) {
	t.Helper()
	base, err := catchUp2542ReadRecipes(packet.Base)
	if err != nil {
		t.Fatal(err)
	}
	indexes := []map[string]recipe{}
	for _, rows := range [][]recipe{base, master, lane, current} {
		indexed := map[string]recipe{}
		for _, row := range rows {
			key := catchUp2542RecipeKey(row)
			if _, exists := indexed[key]; exists {
				t.Fatalf("duplicate signed recipe key: %s", key)
			}
			indexed[key] = row
		}
		indexes = append(indexes, indexed)
	}
	counts := map[string]int{}
	seen := map[string]bool{}
	for _, pin := range packet.Rows {
		if seen[pin.Key] {
			t.Fatalf("duplicate finite intersection: %s", pin.Key)
		}
		seen[pin.Key] = true
		want := []string{pin.BaseSHA256, pin.MasterSHA256, pin.LaneSHA256, pin.ComposedSHA256}
		for index, rows := range indexes {
			if catchUp2542RecipeDigest(rows[pin.Key]) != want[index] {
				t.Fatalf("intersection lost signed endpoint %d: %s", index, pin.Key)
			}
		}
		counts[pin.Disposition]++
	}
	if !reflect.DeepEqual(counts, map[string]int{
		"double-changed": 3, "double-changed/composed-source": 94, "master-retired-lane-successor": 4,
		"lane-only": 110, "lane-only/composed-source": 2, "master-only": 110,
		"lane-added/composed-source": 79, "lane-added/exact-retired-helper": 4, "identical/composed-source": 1,
	}) {
		t.Fatalf("finite intersection/retirement classes changed: %v", counts)
	}
}

func assertCatchUp2542CurrentFiles(t *testing.T, packet selectedreadrole.CatchUp2542Packet) {
	t.Helper()
	seen := map[string]bool{}
	for _, file := range packet.Files {
		if seen[file.File] {
			t.Fatalf("duplicate integration source: %s", file.File)
		}
		seen[file.File] = true
		raw, err := os.ReadFile(filepath.Join("../../..", file.File))
		if file.ComposedSHA256 == "absent" {
			if !os.IsNotExist(err) {
				t.Fatalf("retired integration source survived: %s: %v", file.File, err)
			}
			continue
		}
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != file.ComposedSHA256 {
			t.Fatalf("composed source differs from finite packet: %s: %v", file.File, err)
		}
	}
}

func TestCatchUp2542CurrentPairsKeepHistoricalChecksBoundToExactLiveSource(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, master := range []bool{false, true} {
		checked := 0
		for _, row := range rows {
			if row.Removed || !assertCatchUp2542CurrentPairHostility(t, row, master) {
				continue
			}
			checked++
			if checked == 3 {
				break
			}
		}
		if checked != 3 {
			t.Fatal("lost both-predecessor current-pair counterexamples")
		}
	}
}

func assertCatchUp2542CurrentPairHostility(t *testing.T, row recipe, master bool) bool {
	t.Helper()
	endpoint := row.Successor
	if endpoint == "" {
		endpoint = row.After
	}
	prior, previous, err := catchUp2542PriorPair(row, endpoint, master)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(prior, row) {
		return false
	}
	if previous == endpoint {
		t.Fatal("changed receipt did not recover its exact predecessor")
	}
	for _, source := range []string{previous, strings.Replace(endpoint, "{", "{ unreviewed();", 1)} {
		if _, _, err := catchUp2542PriorPair(row, source, master); err == nil {
			t.Fatal("historical or mutated source gained current-pair authority")
		}
	}
	for _, field := range []string{"before", "after", "endpoint", "identity", "mechanical"} {
		mutant := mutateCatchUp2542Receipt(row, field)
		got, source, err := catchUp2542PriorPair(mutant, endpoint, master)
		if err != nil || !reflect.DeepEqual(got, mutant) || source != endpoint {
			t.Fatal("finite inverse erased unreviewed receipt metadata:", field, err)
		}
	}
	return true
}

func mutateCatchUp2542Receipt(row recipe, field string) recipe {
	switch field {
	case "before":
		row.Before += " changed"
	case "after":
		row.After += " changed"
	case "endpoint":
		row.Successor = "changed"
	case "identity":
		row.Function += "Unlisted"
	case "mechanical":
		row.Mechanical = &mechanicalSnapshot{SourceCommit: "foreign"}
	}
	return row
}

func TestCatchUp2542RecipeCompositionRejectsChangedHistoryCurrentnessAndMultiplicity(t *testing.T) {
	for _, mutation := range []string{"missing", "extra", "history", "endpoint", "mechanical", "retirement", "order"} {
		t.Run(mutation, func(t *testing.T) {
			var rows []map[string]any
			if err := json.Unmarshal(recipeBytes, &rows); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "missing":
				rows = rows[:len(rows)-1]
			case "extra":
				rows = append(rows, rows[0])
			case "history":
				rows[0]["Before"] = "unreviewed"
			case "endpoint":
				rows[0]["Successor"] = "unreviewed"
			case "mechanical":
				rows[0]["Mechanical"] = map[string]any{"SourceCommit": strings.Repeat("0", 40)}
			case "retirement":
				rows[0]["Removed"] = true
			case "order":
				rows[0], rows[1] = rows[1], rows[0]
			}
			raw, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			for _, master := range []bool{false, true} {
				if _, err := selectedreadrole.CatchUp2542HistoricalRecipes("../../..", raw, master); err == nil {
					t.Fatal("unreviewed history supplied a predecessor view")
				}
			}
		})
	}
}
