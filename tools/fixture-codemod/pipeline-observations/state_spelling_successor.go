package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
)

// #1745/6207972014 changes authored spellings, not historical owner evidence.
//
//go:embed state-spelling-successors-1745.json
var stateSpellingSuccessors1745 []byte

type stateSpellingSuccessor1745 struct {
	Original  recipe
	Candidate string
}

func stateSpellingEndpoint1745(row recipe) (recipe, error) {
	var pins []stateSpellingSuccessor1745
	if err := json.Unmarshal(stateSpellingSuccessors1745, &pins); err != nil {
		return recipe{}, err
	}
	for _, pin := range pins {
		if row.File != pin.Original.File || row.Function != pin.Original.Function || row.Family != pin.Original.Family {
			continue
		}
		if !reflect.DeepEqual(row, pin.Original) {
			return recipe{}, fmt.Errorf("state spelling successor history changed: %s", row.Function)
		}
		row.Before = row.After
		if row.Successor != "" {
			row.Before = row.Successor
		}
		row.After, row.Successor = pin.Candidate, ""
		return row, nil
	}
	return row, nil
}

// Historical comparators consume only the exact predecessor of an admitted
// spelling successor. The packet's independent test checks today's real body.
func stateSpellingHistoricalBody1745(path, name, actual string) string {
	var pins []stateSpellingSuccessor1745
	if json.Unmarshal(stateSpellingSuccessors1745, &pins) != nil {
		return actual
	}
	for _, pin := range pins {
		if path != pin.Original.File {
			continue
		}
		candidate, err := canonicalFunction(pin.Candidate)
		current, currentErr := canonicalFunction(actual)
		if err != nil || currentErr != nil || candidate != current {
			continue
		}
		prior := pin.Original.Successor
		if prior == "" {
			prior = pin.Original.After
		}
		return prior
	}
	return actual
}
