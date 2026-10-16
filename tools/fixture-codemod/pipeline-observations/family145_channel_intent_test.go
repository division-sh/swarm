package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
)

//go:embed family145-channel-intent-preimages.json
var family145ChannelIntentBytes []byte

func family145ChannelIntentCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := channelintent.Decode(family145ChannelIntentBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channelintent.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
