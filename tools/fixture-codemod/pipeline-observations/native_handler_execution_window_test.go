package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise the real committed follow-up cut, not an earlier setup/planner error.
// A Go overlay keeps the deliberately wrong production change out of the tree.
func TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal", "runtime", "pipeline", "engine_bridge.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	anchor := "\t}\n\tdiagnostics := &pipelineEmissionPlan{}"
	if strings.Count(string(source), anchor) != 1 {
		t.Fatal("committed follow-up mutation cut is missing or ambiguous")
	}
	mutant := strings.Replace(string(source), anchor, "\t}\n\tif !preview && collectDiagnosticEmissions && result.Committed && len(followUp.Emissions) > 0 {\n\t\tif earlyErr := pc.transferCommittedHandlerFollowUp(ctx, followUp, nil); earlyErr != nil {\n\t\t\treturn contractHandlerExecutionResult{}, earlyErr\n\t\t}\n\t\tfmt.Println(\"native-handler-early-dispatch-mutant: success\")\n\t}\n\tdiagnostics := &pipelineEmissionPlan{}", 1)
	work := t.TempDir()
	mutantPath := filepath.Join(work, "engine_bridge.go")
	if err := os.WriteFile(mutantPath, []byte(mutant), 0600); err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{path: mutantPath}})
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(work, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-overlay", overlayPath, "-race", "-count=1", "-timeout=3m", "-json", "./internal/runtime/pipeline", "-run", "^TestExecuteNodeContractHandlerDefersCommittedEmissions$")
	command.Dir = root
	output, runErr := command.CombinedOutput()
	if runErr == nil || ctx.Err() != nil {
		t.Fatalf("early-dispatch mutant was not a bounded failing run: %v/%v\n%s", runErr, ctx.Err(), output)
	}
	assertNativeHandlerEarlyDispatchFailure(t, output)
	t.Logf("both-store real early-dispatch mutation evidence:\n%s", output)
}

func assertNativeHandlerEarlyDispatchFailure(t *testing.T, output []byte) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output))
	failed, details := map[string]bool{}, map[string]string{}
	for {
		var record struct{ Action, Test, Output string }
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid mutant execution evidence: %v\n%s", err, output)
		}
		failed[record.Test] = failed[record.Test] || record.Action == "fail"
		details[record.Test] += record.Output
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			name := "TestExecuteNodeContractHandlerDefersCommittedEmissions/" + backend
			if !failed[name] || !strings.Contains(details[name], "native-handler-early-dispatch-mutant: success") || !strings.Contains(details[name], "bus published count = 1, want 0 before deferred dispatch") {
				t.Fatalf("mutant did not fail at the observed real dispatch cut:\n%s", output)
			}
		})
	}
}
