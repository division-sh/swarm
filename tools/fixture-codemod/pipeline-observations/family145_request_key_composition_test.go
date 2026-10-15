package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestFamily145RequestKeySuccessorCompositionIsFinite(t *testing.T) {
	for _, pin := range requestKeySuccessorPins() {
		t.Run(pin.Function, func(t *testing.T) {
			proveRequestKeySuccessorComposition(t, pin)
		})
	}
}

func proveRequestKeySuccessorComposition(t *testing.T, pin runphysical.Pin) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
	if err != nil || servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, raw, pin.Function)) != pin.After {
		t.Fatal("successor source changed:", err)
	}
	if family145RequestKeyCurrentPrevious(pin.File, pin.Function, pin.After) != pin.Before {
		t.Fatal("exact successor inverse refused")
	}
	matches := 0
	for _, historical := range requestKeyPins(t) {
		if historical.File == pin.File && historical.Function == pin.Function {
			matches++
			if historical.After != pin.Before {
				t.Fatal("successor lost frozen request-key endpoint")
			}
		}
	}
	if matches != 1 {
		t.Fatal("missing/duplicate historical row:", matches)
	}
	for _, key := range [][2]string{{"foreign.go", pin.Function}, {pin.File, "foreignFunction"}} {
		if family145RequestKeyCurrentPrevious(key[0], key[1], pin.After) != pin.After {
			t.Fatal("foreign key received inverse")
		}
	}
	for _, cut := range []string{"reader", "ReceiverStateReader", "InspectSnapshot", "eventID", "runID", "pending", "t.Fatal", " != ", "context.Background()"} {
		if strings.Contains(pin.After, cut) {
			mutant := strings.Replace(pin.After, cut, "unreviewedSourceCut", 1)
			if family145RequestKeyCurrentPrevious(pin.File, pin.Function, mutant) != mutant {
				t.Fatal("changed successor received historical credit:", cut)
			}
		}
	}
}
