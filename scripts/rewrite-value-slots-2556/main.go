// Finite, one-shot rewrite of the reviewed #2556 corpus. Not a runtime reader.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type change struct {
	File       string     `json:"file"`
	BeforeHash string     `json:"before_hash"`
	AfterHash  string     `json:"after_hash"`
	Edits      []lineEdit `json:"edits"`
	Slots      []string   `json:"slots"`
}

type lineEdit struct {
	Start  int      `json:"start"`
	Remove []string `json:"remove,omitempty"`
	Add    []string `json:"add,omitempty"`
}

func digest(source []byte) string { return fmt.Sprintf("%x", sha256.Sum256(source)) }

func main() {
	root := flag.String("root", ".", "explicit checkout root")
	ledger := flag.String("ledger", "scripts/rewrite-value-slots-2556/intent.json", "finite reviewed rewrite ledger")
	capture := flag.String("capture", "", "freeze reviewed corpus/generator/oracle edits against an exact base commit")
	write := flag.Bool("write", false, "apply reviewed edits")
	check := flag.Bool("check", false, "require reviewed output already present")
	flag.Parse()
	var err error
	if *capture != "" {
		err = captureCorpus(*root, *ledger, *capture)
	} else {
		err = apply(*root, *ledger, *write, *check)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Capture records decisions already reviewed in the patch. It neither infers
// business intent nor parses a second executable grammar.
func captureCorpus(root, ledger, base string) error {
	data, err := os.ReadFile(filepath.Join(root, ledger))
	if err != nil {
		return err
	}
	var previous []change
	if err := json.Unmarshal(data, &previous); err != nil {
		return err
	}
	slots := map[string][]string{}
	for _, c := range previous {
		slots[c.File] = append(slots[c.File], c.Slots...)
	}
	command := exec.Command("git", "diff", "--name-only", "--diff-filter=M", base)
	command.Dir = root
	names, err := command.Output()
	if err != nil {
		return err
	}
	var plan []change
	for _, name := range strings.Fields(string(names)) {
		if len(slots[name]) == 0 && !strings.HasSuffix(name, "_test.go") && !strings.HasPrefix(name, "internal/runtime/testfixtures/canonicalrouting/") {
			continue
		}
		command := exec.Command("git", "show", base+":"+name)
		command.Dir = root
		before, err := command.Output()
		if err != nil {
			return fmt.Errorf("%s: no committed corpus source at %s: %w", name, base, err)
		}
		after, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if strings.HasSuffix(name, ".go") {
			slots[name] = append(slots[name], "reviewed generator, partial replacement anchors and typed/negative oracles migrate together; no data-file grammar change")
		}
		command = exec.Command("git", "diff", "--no-ext-diff", "--unified=0", base, "--", name)
		command.Dir = root
		patch, err := command.Output()
		if err != nil {
			return err
		}
		edits, err := parseEdits(patch)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		unique := map[string]bool{}
		var reviewed []string
		for _, slot := range slots[name] {
			if !unique[slot] {
				reviewed = append(reviewed, slot)
				unique[slot] = true
			}
		}
		plan = append(plan, change{File: name, BeforeHash: digest(before), AfterHash: digest(after), Edits: edits, Slots: reviewed})
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].File < plan[j].File })
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, ledger), append(encoded, '\n'), 0644)
}

func parseEdits(patch []byte) ([]lineEdit, error) {
	pattern := regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@`)
	var edits []lineEdit
	for _, line := range strings.Split(strings.TrimSuffix(string(patch), "\n"), "\n") {
		if match := pattern.FindStringSubmatch(line); match != nil {
			start, _ := strconv.Atoi(match[1])
			if match[2] != "0" {
				start--
			}
			edits = append(edits, lineEdit{Start: start})
			continue
		}
		if len(edits) == 0 {
			continue
		}
		switch {
		case strings.HasPrefix(line, "-"):
			edits[len(edits)-1].Remove = append(edits[len(edits)-1].Remove, line[1:])
		case strings.HasPrefix(line, "+"):
			edits[len(edits)-1].Add = append(edits[len(edits)-1].Add, line[1:])
		case strings.HasPrefix(line, "\\"):
			return nil, fmt.Errorf("non-newline corpus source is not supported")
		}
	}
	if len(edits) == 0 {
		return nil, fmt.Errorf("missing reviewed line edits")
	}
	return edits, nil
}

func apply(root, ledger string, write, check bool) error {
	if !filepath.IsAbs(ledger) {
		ledger = filepath.Join(root, ledger)
	}
	data, err := os.ReadFile(ledger)
	if err != nil {
		return err
	}
	var plan []change
	if err := json.Unmarshal(data, &plan); err != nil {
		return err
	}
	outputs, err := prepareOutputs(root, plan, check)
	if err != nil {
		return err
	}
	return writeOutputs(root, plan, outputs, write)
}

func prepareOutputs(root string, plan []change, check bool) (map[string][]byte, error) {
	outputs := map[string][]byte{}
	for _, c := range plan {
		if filepath.IsAbs(c.File) || filepath.Clean(c.File) != c.File || strings.HasPrefix(c.File, "..") {
			return nil, fmt.Errorf("invalid ledger path %q", c.File)
		}
		if _, duplicate := outputs[c.File]; duplicate {
			return nil, fmt.Errorf("duplicate reviewed file %s", c.File)
		}
		data, err := os.ReadFile(filepath.Join(root, c.File))
		if err != nil {
			return nil, err
		}
		output, err := rewriteFile(c.File, data, []change{c})
		if err != nil {
			return nil, err
		}
		if bytes.Equal(output, data) {
			outputs[c.File] = nil
			continue
		}
		if check {
			return nil, fmt.Errorf("unapplied reviewed changes in %s", c.File)
		}
		outputs[c.File] = output
	}
	return outputs, nil
}

func writeOutputs(root string, plan []change, outputs map[string][]byte, write bool) error {
	// Refuse the entire plan before writing any output if any anchor drifted.
	changed := 0
	for _, c := range plan {
		if output := outputs[c.File]; output != nil {
			changed++
			fmt.Println(c.File)
			if write {
				info, err := os.Stat(filepath.Join(root, c.File))
				if err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(root, c.File), output, info.Mode().Perm()); err != nil {
					return err
				}
			}
		}
	}
	fmt.Printf("%d files changed\n", changed)
	return nil
}

func rewriteFile(name string, source []byte, plan []change) ([]byte, error) {
	if len(plan) != 1 {
		return nil, fmt.Errorf("%s: ambiguous file plan", name)
	}
	c := plan[0]
	if digest(source) == c.AfterHash {
		return source, nil
	}
	if digest(source) != c.BeforeHash {
		return nil, fmt.Errorf("%s: reviewed source drift", name)
	}
	lines := strings.Split(strings.TrimSuffix(string(source), "\n"), "\n")
	for i := len(c.Edits) - 1; i >= 0; i-- {
		e := c.Edits[i]
		end := e.Start + len(e.Remove)
		if e.Start < 0 || end > len(lines) || strings.Join(lines[e.Start:end], "\n") != strings.Join(e.Remove, "\n") {
			return nil, fmt.Errorf("%s: reviewed anchor drift", name)
		}
		lines = append(append(append([]string{}, lines[:e.Start]...), e.Add...), lines[end:]...)
	}
	output := []byte(strings.Join(lines, "\n") + "\n")
	if digest(output) != c.AfterHash {
		return nil, fmt.Errorf("%s: reviewed output mismatch", name)
	}
	return output, nil
}
