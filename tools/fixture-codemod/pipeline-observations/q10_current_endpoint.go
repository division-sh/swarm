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
	if !reflect.DeepEqual(row, pin.Original) || row.Removed || len(pin.Edits) == 0 {
		return recipe{}, fmt.Errorf("Q10 endpoint changed original history or identity")
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
	reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || reversed != want {
		return recipe{}, fmt.Errorf("Q10 endpoint failed exact historical reversal: %v", err)
	}
	row.Successor = pin.Candidate
	return row, nil
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
	}
	return row, nil
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
