package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicjoin"
)

//go:embed family145-public-join-preimages.json
var family145PublicJoinBytes []byte

func family145PublicJoinCallerInverse(function, current string) string {
	pins, err := publicjoin.Decode(family145PublicJoinBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return publicjoin.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
