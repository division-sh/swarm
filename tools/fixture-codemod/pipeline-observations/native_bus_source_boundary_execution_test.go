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

func TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range [][4]string{
		{"claim", "outbox.go", "d.bus.pipelineObligations.ClaimEvent(ctx, intent.Event.ID(), runtimepipelineobligation.PurposeRecovery)", "d.bus.pipelineObligations.ClaimEvent(context.Background(), intent.Event.ID(), runtimepipelineobligation.PurposeRecovery)"},
		{"settle", "pipeline_publication_claim.go", "eb.pipelineObligations.Settle(ctx, claim, disposition)", "eb.pipelineObligations.Settle(context.Background(), claim, disposition)"},
	} {
		t.Run(fault[0], func(t *testing.T) {
			path := filepath.Join(root, "internal", "runtime", "bus", fault[1])
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(source), fault[2]) != 1 {
				t.Fatal("exact production source-fact handoff is missing or ambiguous")
			}
			work := t.TempDir()
			mutantPath := filepath.Join(work, fault[1])
			if err := os.WriteFile(mutantPath, []byte(strings.Replace(string(source), fault[2], fault[3], 1)), 0600); err != nil {
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
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, "go", "test", "-overlay", overlayPath, "-count=1", "-timeout=2m", "-json", "./internal/runtime/bus", "-run", "^TestPostCommitDispatchPreservesBusOwnedSourceFactThroughClaimAndSettlement$")
			command.Dir = root
			output, runErr := command.CombinedOutput()
			if runErr == nil || ctx.Err() != nil {
				t.Fatalf("lost-source mutant was not an exact bounded failure: %v/%v\n%s", runErr, ctx.Err(), output)
			}
			assertNativeBusSourceBoundaryFault(t, output, fault[0])
			t.Logf("lost production %s source fact evidence:\n%s", fault[0], output)
		})
	}
}

func assertNativeBusSourceBoundaryFault(t *testing.T, output []byte, action string) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output))
	failed, details := false, ""
	for {
		var record struct{ Action, Test, Output string }
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid source-fault evidence: %v\n%s", err, output)
		}
		if record.Test == "TestPostCommitDispatchPreservesBusOwnedSourceFactThroughClaimAndSettlement" {
			failed = failed || record.Action == "fail"
			details += record.Output
		}
	}
	if !failed || !strings.Contains(details, "post-commit "+action+" lost the exact immutable bus source") {
		t.Fatalf("mutant did not fail at the actual %s handoff:\n%s", action, output)
	}
}
