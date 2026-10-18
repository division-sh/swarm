package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os/exec"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

//go:embed testdata/capability_completion_successor_2707.json
var capabilityCompletionSuccessor2707 []byte

type reviewedSuccessorEdit struct {
	Start          int
	Current, Prior []string
}

func reverseReviewedSuccessor2617(source string, edits []reviewedSuccessorEdit) (string, error) {
	lines := strings.SplitAfter(source, "\n")
	end := len(lines)
	for index := len(edits) - 1; index >= 0; index-- {
		edit := edits[index]
		stop := edit.Start + len(edit.Current)
		if edit.Start < 0 || stop > end || !reflect.DeepEqual(lines[edit.Start:stop], edit.Current) {
			return "", fmt.Errorf("changed or overlapping finite composition edit %d", index)
		}
		lines = append(append(append([]string{}, lines[:edit.Start]...), edit.Prior...), lines[stop:]...)
		end = edit.Start
	}
	return strings.Join(lines, ""), nil
}

func capabilityCompletionPair2698(root string, row recipe, actual, candidate string) (recipe, string, error) {
	before, after, err := selectedreadrole.CompletionRebase2698Images(root)
	if err != nil {
		return recipe{}, "", err
	}
	priorBody, err := selectedreadrole.Body(before, selectedreadrole.CompletionRebase2698Function)
	if err != nil {
		return recipe{}, "", err
	}
	current, err := selectedreadrole.Body(after, selectedreadrole.CompletionRebase2698Function)
	got, sourceErr := canonicalFunction(actual)
	want, recipeErr := canonicalFunction(row.Successor)
	if err != nil || sourceErr != nil || recipeErr != nil || got != current || want != current {
		return recipe{}, "", fmt.Errorf("completion rebase requires its exact actual recipe/source pair")
	}
	raw, err := exec.Command("git", "-C", root, "show", selectedreadrole.CompletionRebase2698Before+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil {
		return recipe{}, "", err
	}
	var rows []recipe
	if err := json.Unmarshal(raw, &rows); err != nil {
		return recipe{}, "", err
	}
	matched := 0
	for _, prior := range rows {
		if prior.File != row.File || prior.Function != row.Function {
			continue
		}
		matched++
		immutable := row
		immutable.Successor = prior.Successor
		body, err := canonicalFunction(prior.Successor)
		if err != nil || body != priorBody || !reflect.DeepEqual(immutable, prior) {
			return recipe{}, "", fmt.Errorf("completion rebase changed its signed typed-reader predecessor")
		}
	}
	typed, err := capabilityCompletionTyped2698(candidate)
	if matched != 1 || err != nil {
		return recipe{}, "", fmt.Errorf("completion rebase lost its unique predecessor: %v", err)
	}
	typed, err = canonicalFunction(typed)
	if err != nil || typed != current {
		return recipe{}, "", fmt.Errorf("completion rebase is not the exact common descendant")
	}
	row.Successor = candidate
	return row, candidate, nil
}

func capabilityCompletionTyped2698(source string) (string, error) {
	for _, edit := range []struct {
		before, after string
		count         int
	}{
		{"waitForkReceiverSourceCompletion(t, rt, seed.RunID)", "waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, seed.RunID)", 1},
		{"requireReceiverPublicReadback(t, rt, seed.RunID)", "requireReceiverPublicReadback(t, rt.Endpoint, rt.ReceiverStateReader, seed.RunID)", 1},
		{"snapshotForkReceiverApplication(t, rt)", "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", 2},
		{"forkReceiverCapabilityRefusalSnapshotDelta(before, after)", "forkReceiverCapabilityRefusalSelectedSnapshotDelta(before, after)", 1},
	} {
		if strings.Count(source, edit.before) != edit.count {
			return "", fmt.Errorf("completion typed-reader composition lost an exact call: %s", edit.before)
		}
		source = strings.ReplaceAll(source, edit.before, edit.after)
	}
	return source, nil
}

func completionRebase2698Predecessor(root string, row recipe, actual string) (string, error) {
	if fmt.Sprintf("%x", sha256.Sum256(capabilityCompletionSuccessor2707)) != "4b531e52419e81158f1faed4ad8c3c63f367d30d13a3ddb79fd62302a61bb9ff" {
		return "", fmt.Errorf("capability completion history changed")
	}
	var pin struct {
		SourceCommit, Review string
		Original             recipe
		Candidate            string
		Edits                []reviewedSuccessorEdit
	}
	if err := json.Unmarshal(capabilityCompletionSuccessor2707, &pin); err != nil {
		return "", err
	}
	if pin.SourceCommit != "80a7850bb8b6108b46eb4a7b3dc1e1594cf543cb" || pin.Review != "https://github.com/division-sh/swarm/pull/2707#issuecomment-6213567352" ||
		pin.Original.Family != "native-served-delivery-quiescence-caller" || pin.Original.File != "internal/serveapp/fork_receiver_ownership_test.go" ||
		pin.Original.Function != "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores" || len(pin.Edits) != 2 {
		return "", fmt.Errorf("unlisted capability completion history")
	}
	row, actual, err := capabilityCompletionPair2698(root, row, actual, pin.Candidate)
	if err != nil {
		return "", err
	}
	history := row
	history.Successor = pin.Original.Successor
	if !reflect.DeepEqual(history, pin.Original) || row.Successor != pin.Candidate {
		return "", fmt.Errorf("capability completion successor crossed its exact predecessor")
	}
	current, err := canonicalFunction(actual)
	want, wantErr := canonicalFunction(pin.Candidate)
	if err != nil || wantErr != nil || current != want {
		return "", fmt.Errorf("capability completion current source changed")
	}
	previous, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || previous != pin.Original.Successor {
		return "", fmt.Errorf("capability completion does not restore its exact predecessor: %v", err)
	}
	return previous, nil
}

// Adapt only an authenticated current source for in-memory idempotence/type
// checking. Immutable recipe bytes and every historical endpoint stay intact.
func repair2698CurrentRecipes(root, path string, source []byte, rows []recipe) ([]recipe, error) {
	prior, err := selectedreadrole.Repair2698HistoricalSource(root, path, source)
	if err != nil || bytes.Equal(prior, source) {
		return rows, err
	}
	var originals []recipe
	if err := json.Unmarshal(recipeBytes, &originals); err != nil {
		return nil, err
	}
	var historical []recipe
	for _, row := range originals {
		if row.File == path {
			historical = append(historical, row)
		}
	}
	historical, err = checkpointRecipes(prior, historical)
	if err != nil {
		return nil, err
	}
	immutable := map[string][]recipe{}
	for _, row := range historical {
		immutable[row.Function] = append(immutable[row.Function], row)
	}
	set := token.NewFileSet()
	oldFile, err := parser.ParseFile(set, path, prior, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	currentSet := token.NewFileSet()
	currentFile, err := parser.ParseFile(currentSet, path, source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	current := append([]recipe(nil), rows...)
	for index, row := range current {
		if !consumeRepair2698Recipe(immutable, row) {
			return nil, fmt.Errorf("native repair recipe changed history: %s/%s", path, row.Function)
		}
		if row.Removed {
			continue
		}
		fn, err := uniqueRecipeFunction(oldFile, row)
		if err != nil {
			return nil, err
		}
		before, err := canonicalFunction(string(prior[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]))
		expected, parseErr := completionRebase2698RecipeEndpoint(root, row, source, originals)
		if err != nil || parseErr != nil || before != expected {
			return nil, fmt.Errorf("native repair is not its exact prior recipe endpoint: %s/%s (source: %v; recipe: %v)", path, row.Function, err, parseErr)
		}
		actualFunction, err := uniqueRecipeFunction(currentFile, row)
		if err != nil {
			return nil, err
		}
		actual, err := canonicalFunction(string(source[currentSet.Position(actualFunction.Pos()).Offset:currentSet.Position(actualFunction.End()).Offset]))
		if err != nil {
			return nil, err
		}
		if row.Successor != "" {
			current[index].Successor = actual
		} else {
			current[index].After = actual
		}
	}
	return current, nil
}

func completionRebase2698RecipeEndpoint(root string, row recipe, source []byte, originals []recipe) (string, error) {
	endpoint := row.Successor
	if endpoint == "" {
		endpoint = row.After
	}
	if row.File != selectedreadrole.CompletionRebase2698Path || row.Function != selectedreadrole.CompletionRebase2698Function {
		return canonicalFunction(endpoint)
	}
	original, err := completionRebase2698OriginalRecipe(row, originals)
	if err != nil {
		return "", err
	}
	actual, err := selectedreadrole.Body(source, row.Function)
	if err != nil {
		return "", err
	}
	previous, err := completionRebase2698Predecessor(root, original, actual)
	if err != nil {
		return "", err
	}
	typed, err := capabilityCompletionTyped2698(previous)
	if err != nil {
		return "", err
	}
	return canonicalFunction(typed)
}

func completionRebase2698OriginalRecipe(row recipe, originals []recipe) (recipe, error) {
	var original recipe
	matched := 0
	for _, candidate := range originals {
		if candidate.File == row.File && candidate.Function == row.Function {
			original = candidate
			matched++
		}
	}
	checkpoint := original
	checkpoint.Before, checkpoint.After, checkpoint.Successor = original.After, original.Successor, ""
	if matched != 1 || !reflect.DeepEqual(checkpoint, row) {
		return recipe{}, fmt.Errorf("completion rebase lost its exact checkpointed recipe")
	}
	return original, nil
}

// Method names can repeat under different exact receivers. Compare the complete
// receipt and consume one occurrence rather than collapsing them by name.
func consumeRepair2698Recipe(rows map[string][]recipe, row recipe) bool {
	for index, original := range rows[row.Function] {
		if reflect.DeepEqual(original, row) {
			rows[row.Function] = append(rows[row.Function][:index], rows[row.Function][index+1:]...)
			return true
		}
	}
	return false
}
