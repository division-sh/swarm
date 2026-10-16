package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// Read-only batch enumeration reuses the existing canonical declaration parser.
// It neither authorizes new history nor rewrites source or ledger records.
func TestEntityToolBatchHistoryIntersectionsAtSignedPredecessor(t *testing.T) {
	const predecessor = "6a82aaca9326883cbaa296730da773396d9a8801"
	root := filepath.Join("..", "..", "..", "..")
	var recipes []recipe
	raw, err := os.ReadFile(filepath.Join(root, ledger))
	if err != nil || json.Unmarshal(raw, &recipes) != nil {
		t.Fatalf("read immutable recipes: %v", err)
	}
	paths := []string{
		"internal/runtime/tools/executor_entity_test.go",
		"internal/runtime/tools/executor_sqlite_persistence_test.go",
		"internal/runtime/tools/entity_sparse_mutation_test.go",
		"internal/runtime/tools/entity_field_ack_response_test.go",
		"internal/runtime/tools/entity_create_retirement_test.go",
		"internal/runtime/tools/ask_human_ack_response_test.go",
		"internal/serveapp/mailbox_completion_test.go",
		"internal/serveapp/mailbox_completion_process_test.go",
		"internal/serveapp/gate_completion_diagnostic_test.go",
	}
	var changes []transition
	for _, path := range paths {
		command := exec.Command("git", "show", predecessor+":"+path)
		command.Dir = root
		before, err := command.Output()
		if err != nil {
			t.Fatal(err)
		}
		after, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		old, current := functions(before), functions(after)
		keys := make([]string, 0, len(old))
		for key := range old {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if old[key] == current[key] {
				continue
			}
			change := transition{File: path, Function: functionName(old[key]), Before: old[key], After: current[key], Removed: current[key] == ""}
			changes = append(changes, change)
			intersections := 0
			for _, row := range recipes {
				if row.File == path && row.Function == change.Function {
					intersections++
				}
			}
			t.Logf("ENTITY_TOOL_SCOPE %s/%s removed=%t history=%d before=%x after=%x", path, change.Function, change.Removed, intersections, sha256.Sum256([]byte(change.Before)), sha256.Sum256([]byte(change.After)))
		}
	}
	data, err := json.Marshal(changes)
	if err != nil || len(changes) == 0 {
		t.Fatalf("empty/unreadable batch: %v", err)
	}
	t.Logf("ENTITY_TOOL_COMPLETE count=%d digest=%s", len(changes), fmt.Sprintf("%x", sha256.Sum256(data)))
	t.Logf("ENTITY_TOOL_PIN_DATA %s", data)
}
