package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
)

//go:embed family145-event-name-preimages.json
var family145EventNameBytes []byte

func family145EventNameCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := eventname.Decode(family145EventNameBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return eventname.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
