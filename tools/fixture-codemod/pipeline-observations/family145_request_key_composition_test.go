package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

// Source correspondence only; these inverses never alter executable fixtures.
func family145RequestKeyCurrentPrevious(file, function, current string) string {
	current = runphysical.Previous(requestKeySuccessorPins(), file, function, current)
	return family145RunScalarCallerInverse(function, current)
}

func requestKeySuccessorPins() []runphysical.Pin {
	pins, err := publicrollback.Decode(publicRollbackBytes)
	if err != nil {
		panic(err)
	}
	physical, err := runphysical.Decode(runPhysicalBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range physical {
		if pin.File == "internal/serveapp/constructed_decision_deferral_test.go" && pin.Function == "TestServedFieldlessGateDeferralBothStores" {
			pins = append(pins, pin)
		}
	}
	if fmt.Sprintf("%x", sha256.Sum256(stopWitnessBytes)) != stopWitnessDigest {
		panic("exact stop manifest changed")
	}
	var stops []stopWitnessPin
	if err := json.Unmarshal(stopWitnessBytes, &stops); err != nil {
		panic(err)
	}
	for _, pin := range stops {
		if (pin.File == "internal/serveapp/fan_out_stop_failure_test.go" && pin.Function == "TestIssue2394StopFailureEnvelopeBothStores") || (pin.File == "internal/serveapp/fan_out_stop_http_test.go" && pin.Function == "TestIssue2394ServedStopTwentyBothStores") {
			pins = append(pins, runphysical.Pin{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After, SHA256: pin.SHA256})
		}
	}
	if len(pins) != 6 {
		panic("request-key successor inventory changed")
	}
	return pins
}
