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

func TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal/runtime/bus/recovery_read_postgres_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	anchor := "\t\t\treleaseReader()\n\t\t\twaitOriginSQLLock(t, f.store)"
	if strings.Count(string(source), anchor) != 1 {
		t.Fatal("origin SQL failure cut is missing or ambiguous")
	}
	mutant := strings.Replace(string(source), anchor, anchor+"\n\t\t\tt.Fatal(\"injected origin observation failure\")", 1)
	work := t.TempDir()
	mutantPath := filepath.Join(work, "recovery_read_postgres_test.go")
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-overlay", overlayPath, "-race", "-count=1", "-timeout=2m", "-json", "./internal/runtime/bus", "-run", "^TestContinuationOriginReadPostgresCancellationCausality$")
	command.Dir = root
	output, runErr := command.CombinedOutput()
	if runErr == nil || ctx.Err() != nil {
		t.Fatalf("assertion cut was not a bounded failing run: %v/%v\n%s", runErr, ctx.Err(), output)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	failed, details := map[string]bool{}, map[string]string{}
	for {
		var record struct{ Action, Test, Output string }
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid origin failure proof: %v\n%s", err, output)
		}
		failed[record.Test] = failed[record.Test] || record.Action == "fail"
		details[record.Test] += record.Output
	}
	for _, name := range []string{"unowned_read_native_57014_control", "coordinator_drains_origin_read"} {
		key := "TestContinuationOriginReadPostgresCancellationCausality/" + name
		if !failed[key] || !strings.Contains(details[key], "injected origin observation failure") || !strings.Contains(details[key], "origin proof cleanup joined") {
			t.Fatalf("actual blocked SQL/runtime failure did not join %s:\n%s", name, output)
		}
	}
	t.Logf("real blocked-SQL assertion-failure cleanup evidence:\n%s", output)
}
