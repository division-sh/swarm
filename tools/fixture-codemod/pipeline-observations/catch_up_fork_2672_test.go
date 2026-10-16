package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const catchUp2672Prior = "501c2b53446f187e4ad77a024129d0907bb12979"
const catchUp2672Master = "3a6de3d3b224fb74003c6da3377a8b85d1fd3b9e"
const catchUp2672PriorPacketSHA = "bc17ae7749fd5a4c3f4661d9c3d7b3e8a46f518f279559362bbc9498f0c7a01e"

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

func catchUp2672ComposedEndpoint(name string) bool {
	return catchUp2672Endpoint(name) || name == "TestExecuteSelectedContractRunForkBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint"
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

func assertCatchUp2672RecipeAdvances(t *testing.T, before, after string, count int, allowed func(string) bool) {
	t.Helper()
	old, err := catchUp2542ReadRecipes(before)
	if err != nil {
		t.Fatal(err)
	}
	var current []recipe
	if after == "" {
		err = json.Unmarshal(recipeBytes, &current)
	} else {
		current, err = catchUp2542ReadRecipes(after)
	}
	if err != nil || len(old) != len(current) {
		t.Fatal("fork integration lost recipe inventory:", err)
	}
	changed := 0
	for index, row := range current {
		previous := old[index]
		if reflect.DeepEqual(row, previous) {
			continue
		}
		changed++
		row.Successor = previous.Successor
		if !allowed(row.Function) || !reflect.DeepEqual(row, previous) {
			t.Fatalf("unlisted fork endpoint or historical change: %s", row.Function)
		}
	}
	if changed != count {
		t.Fatalf("fork endpoint advances=%d want=%d", changed, count)
	}
}

func assertCatchUp2672PacketAdvances(t *testing.T, before, after selectedreadrole.CatchUp2542Packet) {
	t.Helper()
	prior := map[string]selectedreadrole.CatchUp2542Row{}
	for _, row := range before.Rows {
		prior[row.Key] = row
	}
	added := 0
	for _, row := range after.Rows {
		old, found := prior[row.Key]
		name := strings.Split(row.Key, "\t")[2]
		if !found {
			added++
			if !catchUp2672ComposedEndpoint(name) {
				t.Fatal("unlisted appended source correspondence:", name)
			}
			continue
		}
		if row.ComposedSHA256 != old.ComposedSHA256 && !catchUp2672ComposedEndpoint(name) {
			t.Fatal("unrelated composed endpoint changed:", name)
		}
		row.ComposedSHA256, row.SourceSHA256, row.Disposition = old.ComposedSHA256, old.SourceSHA256, old.Disposition
		if row != old {
			t.Fatal("immutable predecessor correspondence changed:", name)
		}
		delete(prior, row.Key)
	}
	if len(prior) != 0 || added != 2 {
		t.Fatal("fork continuation dropped or appended unlisted correspondences")
	}
	assertCatchUp2672FileAdvances(t, before.Files, after.Files)
}

func assertCatchUp2672FileAdvances(t *testing.T, before, after []selectedreadrole.CatchUp2542File) {
	t.Helper()
	prior := map[string]selectedreadrole.CatchUp2542File{}
	for _, file := range before {
		prior[file.File] = file
	}
	changed := 0
	for _, file := range after {
		old, found := prior[file.File]
		if !found {
			t.Fatal("fork continuation added an unlisted whole-file source")
		}
		if file.ComposedSHA256 != old.ComposedSHA256 {
			changed++
			if !catchUp2672SourceFile(file.File) {
				t.Fatal("unrelated whole-file pin refreshed:", file.File)
			}
		}
		file.ComposedSHA256 = old.ComposedSHA256
		if file != old {
			t.Fatal("immutable source-file predecessor changed:", file.File)
		}
		delete(prior, file.File)
	}
	if changed != 7 || len(prior) != 0 {
		t.Fatalf("fork whole-file intersections=%d; missing=%v", changed, prior)
	}
}

func catchUp2672SourceFile(path string) bool {
	switch path {
	case "internal/apiv1/operator_event_publish_test.go", "internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go",
		"internal/serveapp/lifecycle_transition_fork_test.go", "internal/serveapp/mailbox_human_topology_test.go",
		"internal/serveapp/receiver_fork_test.go", "internal/serveapp/source_admission_fork_proof_test.go",
		"internal/runtime/runforkexecution/execution_test.go":
		return true
	default:
		return false
	}
}
