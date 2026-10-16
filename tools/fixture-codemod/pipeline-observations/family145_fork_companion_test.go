package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forkcompanion"
)

//go:embed family145-fork-companion-preimages.json
var family145ForkCompanionBytes []byte

func family145ForkCompanionCallerInverse(function, current string) string {
	current = family145RequestKeyCallerInverse(function, current)
	current = family145ForkNoticeCallerInverse(function, current)
	pins, err := forkcompanion.Decode(family145ForkCompanionBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return forkcompanion.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
