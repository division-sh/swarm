package main

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const catchUp2672Master = "3a6de3d3b224fb74003c6da3377a8b85d1fd3b9e"

func catchUp2672Endpoint(name string) bool {
	switch name {
	case "TestTier12RuntimeFork_SelectedContractForkExecutionFixture", "runLifecycleRootForkPolicy",
		"TestHumanTaskRealRequesterTopologyBothStores", "TestReceiverCompositionForkBothStores",
		"Test2376ForkSourceAndPinsFollowSourceRunBothStores":
		return true
	default:
		return false
	}
}

// The original master views remain authoritative for their old receipts. Only
// these five incoming guards need #2672's pinned, reviewed predecessor.
func requireCatchUp2672MasterPair(t *testing.T, row recipe, actual string) (recipe, string) {
	t.Helper()
	prior, source, err := catchUp2672MasterPair(row, actual)
	if err != nil {
		t.Fatal(err)
	}
	return prior, source
}

func catchUp2672MasterPair(row recipe, actual string) (recipe, string, error) {
	if !catchUp2672Endpoint(row.Function) {
		return catchUp2542PriorMasterPair(row, actual)
	}
	packet, err := selectedreadrole.CatchUp2542Receipts()
	if err != nil {
		return recipe{}, "", err
	}
	matched := false
	for _, pin := range packet.Rows {
		if pin.Key == catchUp2542RecipeKey(row) && pin.ComposedSHA256 == catchUp2542RecipeDigest(row) {
			matched = true
		}
	}
	current, err := canonicalFunction(row.Successor)
	got, sourceErr := canonicalFunction(actual)
	if !matched || err != nil || sourceErr != nil || current != got {
		return recipe{}, "", fmt.Errorf("fork continuation requires the exact composed recipe/source pair")
	}
	rows, err := catchUp2542ReadRecipes(catchUp2672Master)
	if err != nil {
		return recipe{}, "", err
	}
	for _, prior := range rows {
		if catchUp2542RecipeKey(prior) != catchUp2542RecipeKey(row) {
			continue
		}
		immutable := row
		immutable.Successor, immutable.Removed = prior.Successor, prior.Removed
		if !reflect.DeepEqual(immutable, prior) {
			return recipe{}, "", fmt.Errorf("fork continuation changed immutable evidence")
		}
		return prior, prior.Successor, nil
	}
	return recipe{}, "", fmt.Errorf("fork continuation lost its exact reviewed predecessor")
}
