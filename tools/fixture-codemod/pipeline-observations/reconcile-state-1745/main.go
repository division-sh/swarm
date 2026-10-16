// Command reconcile-state-1745 appends exact current-source spelling evidence.
// It never changes the historical recipe ledger or its Before/After receipts.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const directory = "tools/fixture-codemod/pipeline-observations"

type recipe struct {
	File, Function, Family, After, Successor string
	Removed                                  bool
}

type successor struct {
	Original  json.RawMessage
	Candidate string
}

func canonical(source string) (string, string) {
	file, err := parser.ParseFile(token.NewFileSet(), "endpoint.go", "package endpoint\n"+source, 0)
	if err != nil || len(file.Decls) != 1 {
		panic(fmt.Sprintf("invalid endpoint: %v", err))
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		panic("endpoint is not a function")
	}
	var body bytes.Buffer
	if err := format.Node(&body, token.NewFileSet(), fn); err != nil {
		panic(err)
	}
	return fn.Name.Name, body.String()
}

func main() {
	body, err := os.ReadFile(filepath.Join(directory, "recipes.json"))
	if err != nil {
		panic(err)
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		panic(err)
	}
	var pins []successor
	for _, raw := range rows {
		var row recipe
		if err := json.Unmarshal(raw, &row); err != nil {
			panic(err)
		}
		if row.Removed {
			continue
		}
		prior := row.Successor
		if prior == "" {
			prior = row.After
		}
		name, prior := canonical(prior)
		candidate := strings.ReplaceAll(prior, "entities.yaml", "state.yaml")
		candidate = strings.ReplaceAll(candidate, `\n  key:`, `\n  _key:`)
		candidate = strings.ReplaceAll(candidate, "\n  key:", "\n  _key:")
		_, candidate = canonical(candidate)
		if candidate == prior {
			continue
		}
		source, err := os.ReadFile(row.File)
		if err != nil {
			panic(err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, row.File, source, 0)
		if err != nil {
			panic(err)
		}
		actual, count := "", 0
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == name {
				count++
				_, actual = canonical(string(source[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]))
			}
		}
		if count != 1 || actual != candidate {
			panic(fmt.Sprintf("source differs beyond the approved spelling: %s/%s (matches %d)", row.File, name, count))
		}
		pins = append(pins, successor{Original: raw, Candidate: candidate})
	}
	output, err := json.MarshalIndent(pins, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "state-spelling-successors-1745.json"), append(output, '\n'), 0600); err != nil {
		panic(err)
	}
	fmt.Printf("appended %d exact state/metadata spelling successors; historical ledger unchanged\n", len(pins))
	appendStageSpellingEvidence()
}

func appendStageSpellingEvidence() {
	const ledger = "scripts/rewrite-stages-2566/finite-fixtures.json"
	body, err := os.ReadFile(ledger)
	if err != nil {
		panic(err)
	}
	var plan struct {
		Changes []struct {
			File      string
			AfterHash string `json:"after_hash"`
		}
	}
	if err := json.Unmarshal(body, &plan); err != nil {
		panic(err)
	}
	type endpoint struct{ File, PriorSHA256, CandidateSHA256 string }
	var endpoints []endpoint
	for _, row := range plan.Changes {
		prior, err := exec.Command("git", "show", "9beb615299e14741752ea6c92128b8e6036e9ee6:"+row.File).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != row.AfterHash {
			panic(fmt.Sprintf("stage spelling predecessor differs: %s %v", row.File, err))
		}
		next := strings.ReplaceAll(string(prior), "entities.yaml", "state.yaml")
		next = strings.ReplaceAll(next, `\n  key:`, `\n  _key:`)
		next = strings.ReplaceAll(next, "\n  key:", "\n  _key:")
		candidate := []byte(next)
		if strings.HasSuffix(row.File, ".go") {
			candidate, err = format.Source(candidate)
			if err != nil {
				panic(err)
			}
		}
		actual, err := os.ReadFile(row.File)
		if err != nil || !bytes.Equal(actual, candidate) {
			panic(fmt.Sprintf("stage source differs beyond spelling: %s %v", row.File, err))
		}
		if bytes.Equal(prior, candidate) {
			continue
		}
		endpoints = append(endpoints, endpoint{row.File, row.AfterHash, fmt.Sprintf("%x", sha256.Sum256(candidate))})
	}
	output, err := json.MarshalIndent(endpoints, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("scripts/rewrite-stages-2566/state-spelling-1745.json", append(output, '\n'), 0600); err != nil {
		panic(err)
	}
	fmt.Printf("appended %d exact stage-fixture spelling endpoints; original stage ledger unchanged\n", len(endpoints))
}
