package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
)

//go:embed family145-channel-event-count-preimages.json
var family145ChannelEventCountBytes []byte

func family145ChannelEventCountCallerInverse(function, current string) string {
	pins, err := channeleventcount.Decode(family145ChannelEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channeleventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
