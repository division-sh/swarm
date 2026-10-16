package main

import (
	_ "embed"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedeventcount"
)

//go:embed family145-served-event-count-preimages.json
var family145ServedEventCountBytes []byte

func servedEventCountPins(t *testing.T) []servedeventcount.Pin {
	t.Helper()
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ServedEventCountPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	return servedeventcount.Previous(servedEventCountPins(t), file, function, current)
}

func family145ServedEventCountCallerInverse(function, current string) string {
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return servedeventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}
