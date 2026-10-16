package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
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
