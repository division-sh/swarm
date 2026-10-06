package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Capture records reviewed edits, not inferred migration decisions. Existing
// entry/end decisions remain intact; fragments are explicitly distinguished.
func refreshLedger(root, ledger, baseline, reviewedRevision string) error {
	ledgerPath := ledger
	if !filepath.IsAbs(ledger) {
		ledger = filepath.Join(root, ledger)
	}
	body, err := os.ReadFile(ledger)
	if err != nil {
		return err
	}
	if reviewedRevision != "" {
		command := exec.Command("git", "show", reviewedRevision+":"+filepath.ToSlash(ledgerPath))
		command.Dir = root
		body, err = command.Output()
		if err != nil {
			return fmt.Errorf("read independently reviewed ledger: %w", err)
		}
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	command := exec.Command("git", "rev-parse", "--verify", baseline+"^{commit}")
	command.Dir = root
	sha, err := command.Output()
	if err != nil {
		return err
	}
	p.Baseline = strings.TrimSpace(string(sha))
	command = exec.Command("git", "diff", "--name-only", "-z", "--diff-filter=M", p.Baseline, "--")
	command.Dir = root
	names, err := command.Output()
	if err != nil {
		return err
	}
	existing := map[string]change{}
	for _, c := range p.Changes {
		existing[c.File] = c
	}
	var changes []change
	for _, name := range strings.Split(strings.TrimSuffix(string(names), "\x00"), "\x00") {
		c, positive := existing[name]
		candidate := positive || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "internal/runtime/testfixtures/")
		if !candidate || (!positive && strings.HasPrefix(name, "scripts/")) {
			continue
		}
		command = exec.Command("git", "show", p.Baseline+":"+name)
		command.Dir = root
		before, err := command.Output()
		if err != nil {
			return err
		}
		after, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		text := string(before)
		yamlStage := strings.Contains(text, "stages:") && (strings.Contains(text, "initial:") || strings.Contains(text, "terminal:"))
		typedStage := (strings.Contains(text, "FlowStageDeclaration") || strings.Contains(text, "StageGraphNodeView")) && (strings.Contains(text, "Initial:") || strings.Contains(text, "Terminal:"))
		if !positive && !yamlStage && !typedStage {
			continue
		}
		command = exec.Command("git", "diff", "--no-ext-diff", "--no-color", "--unified=0", p.Baseline, "--", name)
		command.Dir = root
		diff, err := command.Output()
		if err != nil {
			return err
		}
		c.File, c.BeforeHash, c.AfterHash = name, digest(before), digest(after)
		c.Edits, err = captureLineEdits(before, after, string(diff))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if !positive {
			c.Review = "generated/partial source or source-admission oracle; no standalone whole-file entry claim; preserve branch assertions and qualify the enclosing named test family"
		}
		if _, err := rewrite(c, before); err != nil {
			return fmt.Errorf("%s: captured edit reproduction: %w", name, err)
		}
		changes = append(changes, c)
	}
	if len(changes) < len(existing) {
		return fmt.Errorf("capture lost a reviewed source: existing=%d captured=%d", len(existing), len(changes))
	}
	p.Changes = changes
	p.Scope = "reviewed positive entry/end equivalence plus explicitly classified generated/partial source and negative-oracle cut; runtime closure requires the separate execution proof matrix"
	body, err = json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(ledger, append(body, '\n'), 0644); err != nil {
		return err
	}
	fmt.Printf("%d exact corpus files captured against %s; entry decisions preserved\n", len(changes), p.Baseline)
	return nil
}

func captureLineEdits(before, after []byte, diff string) ([]edit, error) {
	header := regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)
	span := func(body []byte, startText, countText string) (int, int, error) {
		start, _ := strconv.Atoi(startText)
		count := 1
		if countText != "" {
			count, _ = strconv.Atoi(countText)
		}
		if count != 0 {
			start--
		}
		lines := strings.SplitAfter(string(body), "\n")
		if start < 0 || start+count > len(lines) {
			return 0, 0, fmt.Errorf("invalid diff line range")
		}
		offset := len(strings.Join(lines[:start], ""))
		return offset, offset + len(strings.Join(lines[start:start+count], "")), nil
	}
	var edits []edit
	for _, line := range strings.Split(diff, "\n") {
		matches := header.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		start, end, err := span(before, matches[1], matches[2])
		if err != nil {
			return nil, err
		}
		newStart, newEnd, err := span(after, matches[3], matches[4])
		if err != nil {
			return nil, err
		}
		edits = append(edits, edit{Offset: start, Before: string(before[start:end]), After: string(after[newStart:newEnd])})
	}
	return edits, nil
}
