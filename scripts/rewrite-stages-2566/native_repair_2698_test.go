package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestFiniteNativeRepairPreservesExactLedgerAndBothSourceCuts(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts/rewrite-stages-2566/finite-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger plan
	if err := json.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, c := range ledger.Changes {
		if _, listed := finiteNativeRepairPins[c.File]; !listed {
			continue
		}
		checked++
		t.Run(c.File, func(t *testing.T) { proveFiniteNativeRepair(t, root, c) })
	}
	if checked != 3 {
		t.Fatalf("native source repair changed its three-row inventory: %d", checked)
	}
}

func proveFiniteNativeRepair(t *testing.T, root string, c change) {
	t.Helper()
	old, err := exec.Command("git", "-C", root, "show", finiteNativeRepairBefore+":"+c.File).Output()
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(root, c.File))
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{old, current} {
		output, handled, err := finiteNativeRepairSource(root, c, input)
		if err != nil || !handled || !bytes.Equal(output, current) {
			t.Fatal("exact historical/current source correspondence failed:", err)
		}
	}
	for _, input := range [][]byte{append(bytes.Clone(old), '\n'), append(bytes.Clone(current), '\n')} {
		if _, _, err := finiteNativeRepairSource(root, c, input); err == nil {
			t.Fatal("changed old/current source admitted")
		}
	}
	for _, mutation := range []string{"before", "after", "review", "entry"} {
		changed := c
		switch mutation {
		case "before":
			changed.BeforeHash += "0"
		case "after":
			changed.AfterHash += "0"
		case "review":
			changed.Review += "foreign"
		case "entry":
			changed.EntrySourceRevision += "foreign"
		}
		if _, _, err := finiteNativeRepairSource(root, changed, current); err == nil {
			t.Fatal("changed reviewed row accepted:", mutation)
		}
	}
	foreign := c
	foreign.File += ".foreign"
	if _, handled, err := finiteNativeRepairSource(root, foreign, current); err != nil || handled {
		t.Fatal("foreign source acquired native correspondence")
	}
	if _, _, err := finiteNativeRepairOutput(root, c, current); err == nil {
		t.Fatal("successor was consumed as the historical origin")
	}
}
