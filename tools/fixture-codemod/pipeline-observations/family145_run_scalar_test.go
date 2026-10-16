package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
)

//go:embed family145-run-scalar-preimages.json
var family145RunScalarBytes []byte

func family145RunScalarCallerInverse(function, current string) string {
	current = family145DeliveryStatusCallerInverse(function, current)
	pins, err := runscalar.Decode(family145RunScalarBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return runscalar.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
