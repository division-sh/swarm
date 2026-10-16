package main

import (
	_ "embed"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/receiverreadback"
)

//go:embed family145-receiver-public-readback-preimages.json
var family145ReceiverPublicReadbackBytes []byte

func family145ReceiverReadbackCallerInverse(function, current string) string {
	pins, err := receiverreadback.Decode(family145ReceiverPublicReadbackBytes)
	if err != nil {
		panic(err)
	}
	return receiverreadback.Previous(pins, function, current)
}
