package main

import (
	"fmt"
	"os"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
)

func generateHistory(root string, write bool) {
	pins, err := channelsettlement.ReadPins(root)
	must(err)
	must(channelsettlement.ValidateCurrent(root, pins))
	for _, recipes := range []bool{true, false} {
		prior, err := channelsettlement.PriorHistory(root, recipes)
		must(err)
		current, err := channelsettlement.ComposeHistory(prior, pins, recipes)
		must(err)
		path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
		if recipes {
			path = "tools/fixture-codemod/pipeline-observations/recipes.json"
		}
		fmt.Printf("channel history %s bytes=%d\n", path, len(current))
		if write {
			must(os.WriteFile(path, current, 0644))
		}
	}
}
