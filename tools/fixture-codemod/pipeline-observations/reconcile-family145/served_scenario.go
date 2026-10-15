package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

var servedScenarioFiles = []string{
	"internal/serveapp/test_command_runtime_test.go",
	"internal/serveapp/root_publication_ack_loss_test.go",
	"internal/runtime/template_instance_delivery_test.go",
	"internal/store/testsql/event.go",
}

func captureServedScenario() {
	var pins []transition
	for _, path := range servedScenarioFiles {
		raw, err := exec.Command("git", "show", selectedreadrole.ServedScenarioPredecessor+":"+path).Output()
		must(err)
		prior := functions(raw)
		raw, err = os.ReadFile(path)
		must(err)
		current := functions(raw)
		keys := make([]string, 0, len(prior))
		for key := range prior {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			if prior[key] != current[key] {
				pins = append(pins, transition{File: path, Function: functionName(prior[key]), Before: prior[key], After: current[key], Removed: current[key] == ""})
				fmt.Printf("served-scenario\t%s\t%s\n", path, key)
			}
		}
	}
	writeJSON(selectedreadrole.ServedScenarioPath, pins)
	raw, err := os.ReadFile(selectedreadrole.ServedScenarioPath)
	must(err)
	canonical, err := json.Marshal(pins)
	must(err)
	fmt.Printf("served scenario declarations=%d file=%x canonical=%x\n", len(pins), sha256.Sum256(raw), sha256.Sum256(canonical))
}

func composeServedScenarioHistory() {
	for _, recipes := range []bool{true, false} {
		prior, err := selectedreadrole.PriorServedScenarioHistory(".", recipes)
		must(err)
		candidate, err := selectedreadrole.ComposeServedScenarioHistory(".", prior, recipes)
		must(err)
		path := transitionLedger
		if recipes {
			path = ledger
			var rows []recipe
			must(json.Unmarshal(candidate, &rows))
			writeJSON(path, rows)
		} else {
			var rows []transition
			must(json.Unmarshal(candidate, &rows))
			writeJSON(path, rows)
		}
	}
}
