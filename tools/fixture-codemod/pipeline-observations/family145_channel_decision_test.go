package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeldecision"
)

//go:embed family145-channel-decision-preimages.json
var family145ChannelDecisionBytes []byte

func family145ChannelDecisionCallerInverse(function, current string) string {
	current = family145ChannelEventCountCallerInverse(function, current)
	pins, err := channeldecision.Decode(family145ChannelDecisionBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channeldecision.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
