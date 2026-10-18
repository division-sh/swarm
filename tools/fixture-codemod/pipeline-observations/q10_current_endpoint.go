package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// The historical recipes stay immutable. This fixed packet extends only their
// exact current endpoints, using the same reversal owner as the earlier cuts.
//
//go:embed testdata/q10_current_endpoints.json
var q10CurrentEndpointBytes []byte

type reviewedSuccessorEdit struct {
	Start          int
	Current, Prior []string
}

type fixtureSetupSuccessor2617 struct {
	Original  recipe
	Candidate string
	Edits     []reviewedSuccessorEdit
}

type q10CurrentEndpointPacket struct {
	Source, Scope string
	Rows          []fixtureSetupSuccessor2617
}

func decodeQ10CurrentEndpoints(raw []byte) (q10CurrentEndpointPacket, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "7e26bea7902fd5e502a33c2513303bc3cddf7b42aca9f4086c0b9fd6cd1b02a1" {
		return q10CurrentEndpointPacket{}, fmt.Errorf("Q10 finite endpoint evidence changed")
	}
	var packet q10CurrentEndpointPacket
	if err := json.Unmarshal(raw, &packet); err != nil {
		return q10CurrentEndpointPacket{}, err
	}
	if packet.Source != "6abe659c42a5cbb02742956a17b606631f702887" ||
		packet.Scope != "https://github.com/division-sh/swarm/issues/2593#issuecomment-6192639877" || len(packet.Rows) != 43 {
		return q10CurrentEndpointPacket{}, fmt.Errorf("wrong Q10 source, scope or finite endpoint set")
	}
	return packet, nil
}

var q10CurrentEndpoints = sync.OnceValues(func() (q10CurrentEndpointPacket, error) {
	return decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
})

func q10CurrentEndpointRecipe(row recipe, pin fixtureSetupSuccessor2617) (recipe, error) {
	if !reflect.DeepEqual(row, pin.Original) {
		return q10FixtureCustodyCurrentRecipe(row, pin)
	}
	if row.Removed || len(pin.Edits) == 0 {
		return recipe{}, fmt.Errorf("Q10 endpoint changed original history or identity")
	}
	reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil {
		return recipe{}, fmt.Errorf("Q10 endpoint failed exact historical reversal: %w", err)
	}
	if err := validateRecipeSnapshots(row); err != nil {
		return recipe{}, err
	}
	prior := row.Successor
	if prior == "" {
		prior = row.After
	}
	want, err := canonicalFunction(prior)
	if err != nil {
		return recipe{}, err
	}
	current, err := canonicalFunction(pin.Candidate)
	if err != nil || current == want {
		return recipe{}, fmt.Errorf("Q10 endpoint is not a distinct complete function")
	}
	if reversed != want {
		return recipe{}, fmt.Errorf("Q10 endpoint failed exact historical reversal")
	}
	row.Successor = pin.Candidate
	// Append the reviewed missing-companion correction without rewriting the
	// immutable endpoint packet or any historical recipe.
	if row.Family == "native-invalid-lifecycle-target" {
		const old = "if after.Presence != WorkflowTargetPersistenceCompleteFieldless ||"
		const current = "if after.Presence != WorkflowTargetPersistenceLifecycleOnly ||"
		if row.File != "internal/runtime/pipeline/delivery_target_application_test.go" ||
			row.Function != "TestDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutation" || strings.Count(row.Successor, old) != 1 {
			return recipe{}, fmt.Errorf("unreviewed Q10 missing-companion successor")
		}
		row.Successor = strings.Replace(row.Successor, old, current, 1)
	}
	return row, nil
}

// Compose the independently pinned original-custody setup with Q10's exact
// header-backed read/write changes; neither historical endpoint is rewritten.
func q10FixtureCustodyCurrentRecipe(row recipe, pin fixtureSetupSuccessor2617) (recipe, error) {
	switch row.File + "/" + row.Function {
	case "internal/runtime/final_flow_instance_authoring_runtime_test.go/TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance",
		"internal/runtime/runforkexecution/activity_fork_execution_test.go/seedSource",
		"internal/runtime/runforkexecution/execution_test.go/seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource":
	default:
		return recipe{}, fmt.Errorf("Q10 endpoint changed original history or identity")
	}
	if _, err := q10CurrentEndpointRecipe(pin.Original, pin); err != nil {
		return recipe{}, err
	}
	var setup []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &setup); err != nil {
		return recipe{}, err
	}
	var prior recipe
	for _, endpoint := range setup {
		if sameReviewedRecipeKey(row, endpoint.Original) {
			prior = endpoint.Original
			prior.Successor = endpoint.Candidate
		}
	}
	current := prior
	var err error
	current.Successor, err = q10FixtureSetupCurrentSource(prior)
	if err != nil {
		return recipe{}, err
	}
	original, err := fixtureSetupCurrentEndpoint2617(prior, current.Successor)
	if err != nil || !reflect.DeepEqual(original, pin.Original) ||
		(!reflect.DeepEqual(row, prior) && !reflect.DeepEqual(row, current)) {
		return recipe{}, fmt.Errorf("Q10 custody composition crossed its complete source/history pair: %v", err)
	}
	return current, nil
}

func q10CurrentRecipe(row recipe) (recipe, error) {
	packet, err := q10CurrentEndpoints()
	if err != nil {
		return recipe{}, err
	}
	for _, pin := range packet.Rows {
		if reflect.DeepEqual(row, pin.Original) {
			return q10CurrentEndpointRecipe(row, pin)
		}
		if sameReviewedRecipeKey(row, pin.Original) {
			switch row.File + "/" + row.Function {
			case "internal/runtime/final_flow_instance_authoring_runtime_test.go/TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance",
				"internal/runtime/runforkexecution/activity_fork_execution_test.go/seedSource",
				"internal/runtime/runforkexecution/execution_test.go/seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource":
				if current, err := q10FixtureCustodyCurrentRecipe(row, pin); err == nil {
					return current, nil
				}
			}
		}
	}
	if row.File == "internal/serveapp/receiver_restart_test.go" && row.Function == "TestReceiverCompositionRestartBothStores" {
		for _, pin := range packet.Rows {
			if pin.Original.File == row.File && pin.Original.Function == row.Function {
				return q10EmbeddedSpecCurrentRecipe(row, pin)
			}
		}
		return recipe{}, fmt.Errorf("missing exact Q10 restart composition")
	}
	return row, nil
}

func sameReviewedRecipeKey(left, right recipe) bool {
	return left.Family == right.Family && left.File == right.File && left.Function == right.Function
}

// reviewer-e6248012287 composes only this complete delivered predecessor.
func q10EmbeddedSpecCurrentRecipe(row recipe, pin fixtureSetupSuccessor2617) (recipe, error) {
	if pin.Original.Family != "native-served-delivery-quiescence-owner" ||
		pin.Original.File != "internal/serveapp/receiver_restart_test.go" ||
		pin.Original.Function != "TestReceiverCompositionRestartBothStores" {
		return recipe{}, fmt.Errorf("unlisted Q10 embedded-spec composition")
	}
	embedded, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		return recipe{}, err
	}
	for _, prior := range embedded.Rows {
		if !reflect.DeepEqual(prior.Original, pin.Original) {
			continue
		}
		delivered := prior.Original
		delivered.Successor = prior.Candidate
		current, err := q10CurrentEndpointRecipe(pin.Original, pin)
		if err != nil {
			return recipe{}, err
		}
		current.Successor, err = embeddedSpecFieldDeletion1801(current.Successor)
		if err != nil {
			return recipe{}, err
		}
		if !reflect.DeepEqual(row, delivered) && !reflect.DeepEqual(row, current) {
			return recipe{}, fmt.Errorf("Q10 restart changed complete delivered row or history")
		}
		return current, nil
	}
	return recipe{}, fmt.Errorf("Q10 restart has no exact embedded-spec predecessor")
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
