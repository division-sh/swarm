package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func servedSetupHistoryPins() []runphysical.Pin {
	pins, err := servedsettlement.ReadSetupPins(".")
	must(err)
	must(servedsettlement.ValidateSetupCurrentSource(".", pins))
	for _, pin := range pins {
		servedSetupComparison[pin.File+"/"+pin.Function] = pin
	}
	return pins
}

func servedSetupPrior(path string) []byte {
	raw, err := exec.Command("git", "show", servedsettlement.SetupPredecessor+":"+path).Output()
	must(err)
	return raw
}

func reconcileServedSetup() {
	if len(os.Args) != 2 && (len(os.Args) != 3 || os.Args[2] != "-write") {
		panic("usage: reconcile-family145 -served-setup [-write]")
	}
	pins := servedSetupHistoryPins()
	for _, input := range []struct {
		path    string
		recipes bool
	}{{ledger, true}, {transitionLedger, false}} {
		raw, err := servedsettlement.ComposeSetupHistory(servedSetupPrior(input.path), pins, input.recipes)
		must(err)
		var rows []map[string]any
		must(json.Unmarshal(raw, &rows))
		fmt.Printf("served setup history %s rows=%d\n", input.path, len(rows))
		if len(os.Args) == 3 {
			must(os.WriteFile(input.path, append(raw, '\n'), 0644))
		}
	}
}

func composeServedSetupHistory(rows []recipe, changes []transition, pins []runphysical.Pin) ([]recipe, []transition) {
	for _, input := range []struct {
		path    string
		value   any
		recipes bool
		out     any
	}{{ledger, rows, true, &rows}, {transitionLedger, changes, false, &changes}} {
		prior := servedSetupPrior(input.path)
		candidate, err := json.Marshal(input.value)
		must(err)
		var expected, actual []map[string]any
		must(json.Unmarshal(prior, &expected))
		must(json.Unmarshal(candidate, &actual))
		if !reflect.DeepEqual(actual, expected) {
			panic("served setup earlier regeneration changed approved predecessor: " + input.path)
		}
		raw, err := servedsettlement.ComposeSetupHistory(prior, pins, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}
