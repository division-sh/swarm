package main

import (
	"encoding/json"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
)

func composeChannelSettlementHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := channelsettlement.ReadPins(".")
	must(err)
	must(channelsettlement.ValidateCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := channelsettlement.PriorHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("channel settlement earlier reconciliation changed its approved predecessor")
		}
		raw, err := channelsettlement.ComposeHistory(prior, pins, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}
