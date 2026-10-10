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

func TestNativeTimerSuccessorObserverRejectsRepublicationBothStores(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal/runtime/pipeline/workflow_compiled_lifecycle_evidence_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []struct {
		name, anchor, variants, refusal string
	}{
		{"duplicate", "\t\t\t\tctx = restartedCtx\n", "advance|emit_only|loop_emit_and_self_advance", "successor publications=1"},
		{"stale_generation", "\t\t\t\t\tif recognized, fired, err := restarted.handleWorkflowStageTimerFire(ctx, accepted);", "loop_emit_and_self_advance", "stale same-stage occurrence changed lifecycle"},
	} {
		t.Run(cut.name, func(t *testing.T) {
			if strings.Count(string(source), cut.anchor) != 1 {
				t.Fatal("successor observation cut is missing or ambiguous")
			}
			injected := "\n\t\t\t\tif err := successorBus.DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: accepted}}); err != nil { t.Fatalf(\"successor dispatch injection failed: %v\", err) }\n\t\t\t\tt.Log(\"native-timer-successor-dispatch-mutant: success\")\n"
			placement := injected + cut.anchor
			if cut.name == "duplicate" {
				placement = cut.anchor + injected
			}
			mutant := strings.Replace(string(source), cut.anchor, placement, 1)
			work := t.TempDir()
			mutantPath := filepath.Join(work, "timer.go")
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
			selector := "^TestPipelineCompiledTimerTransitionEvidenceOnBothStores$/(sqlite|postgres)/^(" + cut.variants + ")$"
			command := exec.CommandContext(ctx, "go", "test", "-overlay", overlayPath, "-race", "-count=1", "-timeout=3m", "-json", "./internal/runtime/pipeline", "-run", selector)
			command.Dir = root
			output, runErr := command.CombinedOutput()
			if runErr == nil || ctx.Err() != nil {
				t.Fatalf("successor mutant was not a bounded failing execution: %v/%v\n%s", runErr, ctx.Err(), output)
			}
			assertNativeTimerSuccessorMutationRefused(t, output, cut.variants, cut.refusal)
			t.Logf("successor dispatch negative-control evidence:\n%s", output)
		})
	}
}

func assertNativeTimerSuccessorMutationRefused(t *testing.T, output []byte, variants, refusal string) {
	t.Helper()
	failed, details := map[string]bool{}, map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var record struct{ Action, Test, Output string }
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid successor execution evidence: %v\n%s", err, output)
		}
		failed[record.Test] = failed[record.Test] || record.Action == "fail"
		details[record.Test] += record.Output
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range strings.Split(variants, "|") {
			name := "TestPipelineCompiledTimerTransitionEvidenceOnBothStores/" + backend + "/" + variant
			if !failed[name] || !strings.Contains(details[name], "native-timer-successor-dispatch-mutant: success") || !strings.Contains(details[name], refusal) {
				t.Fatalf("successor mutant escaped or failed before the real observation cut: %s\n%s", name, output)
			}
		}
	}
}
