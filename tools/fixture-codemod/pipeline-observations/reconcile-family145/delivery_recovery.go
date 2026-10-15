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

var deliveryRecoveryFiles = []string{
	"internal/runtime/conformance/delivery_lifecycle_conformance_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
}

func captureDeliveryRecovery() {
	var pins []transition
	for _, path := range deliveryRecoveryFiles {
		raw, err := exec.Command("git", "show", selectedreadrole.DeliveryRecoveryPredecessor+":"+path).Output()
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
	writeJSON(selectedreadrole.DeliveryRecoveryPath, pins)
	raw, err := os.ReadFile(selectedreadrole.DeliveryRecoveryPath)
	must(err)
	canonical, err := json.Marshal(pins)
	must(err)
	fmt.Printf("delivery recovery declarations=%d file=%x canonical=%x\n", len(pins), sha256.Sum256(raw), sha256.Sum256(canonical))
}

func composeDeliveryRecoveryHistory() {
	for _, recipes := range []bool{true, false} {
		prior, err := selectedreadrole.PriorDeliveryRecoveryHistory(".", recipes)
		must(err)
		candidate, err := selectedreadrole.ComposeDeliveryRecoveryHistory(".", prior, recipes)
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
