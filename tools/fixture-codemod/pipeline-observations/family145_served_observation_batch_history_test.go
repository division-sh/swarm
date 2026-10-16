package main

import (
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func servedObservationBatchPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.ReadBatchPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := servedsettlement.ValidateBatchCurrentSource("../../..", pins); err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedObservationEarlierSource(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedSetupEarlierSource(t, file, function, current)
	for _, pin := range servedObservationInventoryPins {
		if pin.File == file && pin.Function == function {
			previous, err := servedsettlement.EarlierSource(servedObservationBatchPins(t), file, function, current)
			if err != nil {
				t.Fatal(err)
			}
			return previous
		}
	}
	return current
}
