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

var durableResourceFiles = []string{
	"internal/store/durable_data_selected_store_test.go",
	"internal/apiv1/operator_data_run_lifecycle_test.go",
	"internal/apiv1/operator_data_show_selected_test.go",
	"internal/apiv1/operator_data_show_corruption_test.go",
	"internal/apiv1/operator_data_test.go",
	"internal/apiv1/operator_finite_run_start_test.go",
	"internal/apiv1/operator_deployment_run_start_test.go",
	"internal/apiv1/reviewer_2566_early_test.go",
}

func captureDurableResource() {
	var pins []transition
	for _, path := range durableResourceFiles {
		raw, err := exec.Command("git", "show", selectedreadrole.DurableResourcePredecessor+":"+path).Output()
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
			}
		}
	}
	writeJSON(selectedreadrole.DurableResourcePath, pins)
	raw, err := os.ReadFile(selectedreadrole.DurableResourcePath)
	must(err)
	canonical, err := json.Marshal(pins)
	must(err)
	fmt.Printf("durable resource declarations=%d file=%x canonical=%x\n", len(pins), sha256.Sum256(raw), sha256.Sum256(canonical))
}

func composeDurableResourceHistory() {
	for _, recipes := range []bool{true, false} {
		prior, err := selectedreadrole.PriorDurableResourceHistory(".", recipes)
		must(err)
		candidate, err := selectedreadrole.ComposeDurableResourceHistory(".", prior, recipes)
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
