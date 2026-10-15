package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
)

const standingIngressPredecessor = "366ef25c98c0ec629354038246e7a30b214d7971"
const standingIngressManifest = "tools/fixture-codemod/pipeline-observations/family145-standing-ingress-control-preimages.json"

var standingIngressFiles = []string{
	"internal/serveapp/fan_out_stop_http_test.go",
	"internal/serveapp/lifecycle_emitter_admission_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/semantic_numeric_ingress_test.go",
	"internal/serveapp/source_admission_fork_proof_test.go",
	"internal/serveapp/standing_ingress_dormancy_supported_surface_test.go",
	"internal/serveapp/standing_ingress_frozen_admission_test.go",
	"internal/serveapp/standing_ingress_supported_surface_test.go",
}

func captureStandingIngress() {
	var pins []transition
	for _, path := range standingIngressFiles {
		raw, err := exec.Command("git", "show", standingIngressPredecessor+":"+path).Output()
		must(err)
		prior := functions(raw)
		raw, err = os.ReadFile(path)
		must(err)
		current := functions(raw)
		for name, body := range current {
			if prior[name] == "" {
				fmt.Printf("standing new helper %s/%s sha256=%x\n", path, name, sha256.Sum256([]byte(body)))
			}
		}
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
	writeJSON(standingIngressManifest, pins)
	raw, err := os.ReadFile(standingIngressManifest)
	must(err)
	fmt.Printf("standing ingress declarations=%d canonical=%x file=%x\n", len(pins), sha256.Sum256(canonical), sha256.Sum256(raw))
}
