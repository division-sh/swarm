package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"golang.org/x/tools/imports"
)

func migrateProcessRead(write, capture bool) {
	root, err := os.Getwd()
	must(err)
	var pins []runphysical.Pin
	if capture {
		paths := make([]string, 0, len(selectedreadrole.ProcessFunctions))
		for path := range selectedreadrole.ProcessFunctions {
			paths = append(paths, path)
		}
		slices.Sort(paths)
		originals := map[string][]byte{}
		for _, path := range paths {
			original, err := exec.Command("git", "show", selectedreadrole.ProcessPredecessor+":"+path).Output()
			must(err)
			originals[path] = original
			current, err := os.ReadFile(path)
			must(err)
			for _, name := range selectedreadrole.ProcessFunctions[path] {
				before, err := selectedreadrole.ProcessBody(original, name)
				must(err)
				after, err := selectedreadrole.ProcessBody(current, name)
				must(err)
				if before == "" || (before == after && name != "waitForMockConnectorDecisionCard") {
					panic("process-read capture lost its changed signed input: " + name)
				}
				pins = append(pins, runphysical.Pin{File: path, Function: name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before)))})
			}
		}
		validateCandidate(root, originals)
	} else {
		pins, err = selectedreadrole.ReadProcessPins(root)
		must(err)
	}
	outputs := map[string][]byte{}
	for _, pin := range pins {
		raw := outputs[pin.File]
		if raw == nil {
			raw, err = os.ReadFile(pin.File)
			must(err)
		}
		outputs[pin.File], err = processReadCandidate(pin, raw)
		must(err)
	}
	for path, raw := range outputs {
		outputs[path], err = imports.Process(filepath.Join(root, path), raw, nil)
		must(err)
	}
	validateCandidate(root, outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("process-read declarations=%d files=%d sha256=%x\n", len(pins), len(outputs), sha256.Sum256(raw))
	if write {
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(selectedreadrole.ProcessPath, raw, 0644))
	}
}

func processReadCandidate(pin runphysical.Pin, raw []byte) ([]byte, error) {
	current, err := selectedreadrole.ProcessBody(raw, pin.Function)
	if err != nil {
		return nil, err
	}
	if current == pin.After {
		return raw, nil
	}
	if current != pin.Before {
		return nil, fmt.Errorf("process-read candidate escaped its exact source pair: %s", pin.Function)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, pin.File, raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	fn, err := uniqueDestination(file, pin.Function)
	if err != nil {
		return nil, err
	}
	return servedsettlement.Apply(raw, []servedsettlement.Splice{{Start: set.Position(fn.Pos()).Offset, End: set.Position(fn.End()).Offset, After: pin.After}})
}
