package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
)

const deploymentResourcePredecessor = "1c4cb4917703cda30faa17de2f093332e7a38c36"
const deploymentResourceManifest = "tools/fixture-codemod/pipeline-observations/family145-deployment-resource-preimages.json"

var deploymentResourceFiles = []string{
	"internal/runtime/conformance/data_text_file_journey_2456_test.go",
	"internal/runtime/conformance/data_text_file_support_2456_test.go",
	"internal/runtime/conformance/deployment_run_start_servedparity_test.go",
	"internal/runtime/conformance/fan_out_resource_journey_test.go",
	"internal/runtime/conformance/fan_out_resource_keyless_multiplicity_test.go",
	"internal/runtime/conformance/fan_out_resource_multi_pin_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_activation_crash_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_empty_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_external_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_initial_race_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_last_row_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_pending_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_pre_receipt_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_preissue_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_stale_claim_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_test.go",
	"internal/runtime/conformance/fan_out_resource_source_proof_test.go",
}

func captureDeploymentResource() {
	var pins []transition
	for _, path := range deploymentResourceFiles {
		raw, err := exec.Command("git", "show", deploymentResourcePredecessor+":"+path).Output()
		must(err)
		prior := functions(raw)
		raw, err = os.ReadFile(path)
		must(err)
		current := functions(raw)
		keys := make([]string, 0, len(prior))
		for name := range prior {
			keys = append(keys, name)
		}
		slices.Sort(keys)
		for _, name := range keys {
			if prior[name] == current[name] {
				continue
			}
			pins = append(pins, transition{File: path, Function: functionName(prior[name]), Before: prior[name], After: current[name], Removed: current[name] == ""})
		}
	}
	canonical, err := json.Marshal(pins)
	must(err)
	writeJSON(deploymentResourceManifest, pins)
	raw, err := os.ReadFile(deploymentResourceManifest)
	must(err)
	fmt.Printf("deployment resource declarations=%d canonical=%x file=%x\n", len(pins), sha256.Sum256(canonical), sha256.Sum256(raw))
}
