package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// The one-time whole-corpus receipt is independent of permanent entry goldens.
// It does not freeze later legitimate changes to unrelated fixture bytes.
func provePlan(root, ledger string) error {
	if err := apply(root, ledger, false, true); err != nil {
		return err
	}
	if !filepath.IsAbs(ledger) {
		ledger = filepath.Join(root, ledger)
	}
	body, err := os.ReadFile(ledger)
	if err != nil {
		return err
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	for _, c := range p.Changes {
		command := exec.Command("git", "show", p.Baseline+":"+c.File)
		command.Dir = root
		before, err := command.Output()
		if err != nil {
			return fmt.Errorf("%s: immutable baseline input unavailable: %w", c.File, err)
		}
		output, err := rewrite(c, before)
		if err != nil {
			return err
		}
		current, err := os.ReadFile(filepath.Join(root, c.File))
		if err != nil || !bytes.Equal(current, output) {
			return fmt.Errorf("%s: output differs from the reviewed tree", c.File)
		}
		second, err := rewrite(c, output)
		if err != nil || !bytes.Equal(second, output) {
			return fmt.Errorf("%s: replay is not byte-idempotent", c.File)
		}
	}
	fmt.Printf("%d corpus files replayed exactly from %s and byte-idempotent; no inferred inputs\n", len(p.Changes), p.Baseline)
	return nil
}
