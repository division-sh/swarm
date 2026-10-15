package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"os"
	"os/exec"
	"slices"
)

const agentSnapshotPredecessor = "11d5e9ef4b0cd967d25448bba87ccb86d39ffac3"
const agentSnapshotFile = "internal/apiv1/agent_snapshot_atomicity_test.go"
const agentSnapshotManifest = "tools/fixture-codemod/pipeline-observations/family145-agent-snapshot-preimages.json"

func captureAgentSnapshotCommand(args []string) bool {
	if len(args) != 2 || args[1] != "-agent-snapshot-capture" {
		return false
	}
	captureAgentSnapshot()
	return true
}

func captureAgentSnapshot() {
	raw, err := exec.Command("git", "show", agentSnapshotPredecessor+":"+agentSnapshotFile).Output()
	must(err)
	prior := functions(raw)
	raw, err = os.ReadFile(agentSnapshotFile)
	must(err)
	current := functions(raw)
	keys := make([]string, 0, len(prior))
	for key := range prior {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var pins []transition
	for _, key := range keys {
		if prior[key] == current[key] {
			continue
		}
		pins = append(pins, transition{File: agentSnapshotFile, Function: functionName(prior[key]), Before: prior[key], After: current[key], Removed: current[key] == ""})
	}
	canonical, err := json.Marshal(pins)
	must(err)
	writeJSON(agentSnapshotManifest, pins)
	manifest, err := os.ReadFile(agentSnapshotManifest)
	must(err)
	fmt.Printf("agent snapshot declarations=%d canonical=%x file=%x source=%x\n", len(pins), sha256.Sum256(canonical), sha256.Sum256(manifest), sha256.Sum256(raw))
}

func agentSnapshotEarlierFunctions(path string, current map[string]string) map[string]string {
	if path != selectedreadrole.AgentSnapshotFile {
		return current
	}
	pins, err := selectedreadrole.ReadAgentSnapshotPins(".")
	must(err)
	must(selectedreadrole.ValidateAgentSnapshotCurrent(".", pins))
	prior, err := exec.Command("git", "show", selectedreadrole.AgentSnapshotPredecessor+":"+path).Output()
	must(err)
	return functions(prior)
}

func composeAgentSnapshotHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		candidate, err := json.Marshal(input.value)
		must(err)
		raw, err := selectedreadrole.ComposeAgentSnapshotHistory(".", candidate, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}
