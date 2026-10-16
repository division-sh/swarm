package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-run-physical-preimages.json
var runPhysicalBytes []byte

var runPhysicalRows = func() []runphysical.Pin {
	pins, err := runphysical.Decode(runPhysicalBytes)
	if err != nil {
		panic(err)
	}
	return pins
}()

func family145RunPhysicalCallerInverse(function, current string) string {
	for _, pin := range runPhysicalRows {
		if pin.Function == function {
			return runphysical.Previous(runPhysicalRows, pin.File, function, current)
		}
	}
	return current
}
