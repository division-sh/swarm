package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forknotice"
)

//go:embed family145-fork-notice-preimages.json
var family145ForkNoticeBytes []byte

func family145ForkNoticeCallerInverse(function, current string) string {
	pins, err := forknotice.Decode(family145ForkNoticeBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return forknotice.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
