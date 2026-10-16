package main

import (
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
)

func family145PublicA2EndpointCallerInverse(function, current string) string {
	for _, pin := range publicendpoint.Pins() {
		if pin.Function == function {
			return publicendpoint.Previous(pin.File, function, current)
		}
	}
	return current
}
