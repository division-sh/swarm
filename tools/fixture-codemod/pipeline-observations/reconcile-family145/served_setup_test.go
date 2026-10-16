package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func TestServedSetupGeneratedTransitionDigestMatchesFiniteOutput(t *testing.T) {
	raw, err := os.ReadFile("../family145-transitions.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = servedSetupOriginalTransitionOutput(raw)
	if err != nil {
		t.Fatal(err)
	}
	var rows []transition
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(encoded))
	if len(rows) != 1060 || digest != targetDigest {
		t.Fatalf("finite generated transition inventory=%d digest=%s", len(rows), digest)
	}
}

func servedSetupOriginalTransitionOutput(current []byte) ([]byte, error) {
	const root = "../../../.."
	const path = "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	trusted, err := exec.Command("git", "-C", root, "show", selectedreadrole.Repair2698Before+":"+path).Output()
	if err != nil || !bytes.Equal(current, trusted) {
		return nil, fmt.Errorf("complete current transition endpoint changed: %w", err)
	}
	// #2698/6213366699: the unchanged digest pins the complete 1,060-row
	// channel-process predecessor, not the stale 973-row count in this guard.
	return selectedreadrole.BeforeChannelProcessHistory(root, current, false)
}

func TestServedSetupGeneratedTransitionRejectsLaterRowFieldOrderAndInventoryChanges(t *testing.T) {
	raw, err := os.ReadFile("../family145-transitions.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1194 {
		t.Fatal("complete later endpoint changed:", err)
	}
	for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Foreign"} {
		t.Run(field, func(t *testing.T) {
			var changed []map[string]any
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			changed[len(changed)-1][field] = "foreign later transition"
			candidate, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := servedSetupOriginalTransitionOutput(candidate); err == nil {
				t.Fatal("later transition field disappeared during historical inversion")
			}
		})
	}
	for _, changed := range [][]map[string]any{rows[:len(rows)-1], append(append([]map[string]any(nil), rows...), rows[len(rows)-1])} {
		candidate, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := servedSetupOriginalTransitionOutput(candidate); err == nil {
			t.Fatal("later missing/duplicate transition disappeared during historical inversion")
		}
	}
	rows[len(rows)-1], rows[len(rows)-2] = rows[len(rows)-2], rows[len(rows)-1]
	candidate, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := servedSetupOriginalTransitionOutput(candidate); err == nil {
		t.Fatal("later transition order disappeared during historical inversion")
	}
}
