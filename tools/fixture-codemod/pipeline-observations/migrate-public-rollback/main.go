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

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-compose-history" {
		composeHistory()
		return
	}
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-public-rollback [-write]")
	}
	path := publicrollback.Rules[0].File
	old, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
	must(err)
	current, err := os.ReadFile(path)
	must(err)
	var pins []publicrollback.Pin
	for _, rule := range publicrollback.Rules {
		_, before := declaration(old, rule.Function)
		after, err := runphysical.Transform(rule, before)
		must(err)
		_, candidate := declaration(current, rule.Function)
		if candidate != before && candidate != after {
			panic("public rollback escaped exact predecessor/output: " + rule.Function)
		}
		rawBefore, _ := declaration(old, rule.Function)
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		current = replace(current, rule.Function, rawAfter)
		pins = append(pins, publicrollback.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("bodies=%d cuts=7 callers=2 sha256=%x\n", len(pins), sha256.Sum256(raw))
	if write {
		_, err := publicrollback.Decode(raw)
		must(err)
		must(os.WriteFile(path, current, 0644))
		must(os.WriteFile(publicrollback.Path, raw, 0644))
	}
}

func composeHistory() {
	raw, err := os.ReadFile(publicrollback.Path)
	must(err)
	pins, err := publicrollback.Decode(raw)
	must(err)
	for _, path := range []string{"tools/fixture-codemod/pipeline-observations/recipes.json", "tools/fixture-codemod/pipeline-observations/family145-transitions.json"} {
		original, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
		must(err)
		var rows []json.RawMessage
		must(json.Unmarshal(original, &rows))
		seen := make(map[string]int)
		for i, raw := range rows {
			rows[i] = composeHistoryRow(raw, path, pins, seen)
		}
		if len(seen) != 3 {
			panic("rollback history missing exact declarations")
		}
		for _, count := range seen {
			if count != 1 {
				panic("ambiguous rollback historical row")
			}
		}
		out, err := json.MarshalIndent(rows, "", "  ")
		must(err)
		out = append(out, '\n')
		current, err := os.ReadFile(path)
		must(err)
		if !bytes.Equal(current, original) && !bytes.Equal(current, out) {
			panic("unreviewed history outside exact rollback Successors")
		}
		must(os.WriteFile(path, out, 0644))
		fmt.Printf("composed only three rollback endpoints: %s\n", path)
	}
}

func composeHistoryRow(raw json.RawMessage, path string, pins []publicrollback.Pin, seen map[string]int) json.RawMessage {
	var row map[string]json.RawMessage
	must(json.Unmarshal(raw, &row))
	var file, function, after string
	must(json.Unmarshal(row["File"], &file))
	must(json.Unmarshal(row["Function"], &function))
	must(json.Unmarshal(row["After"], &after))
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		seen[file+"/"+function]++
		if after != pin.Before || row["Successor"] != nil || row["Removed"] != nil {
			panic("rollback history lost unique unchanged predecessor")
		}
		current, err := os.ReadFile(file)
		must(err)
		_, body := declaration(current, function)
		if body != pin.After {
			panic("rollback current endpoint changed")
		}
		key := "After"
		if path == "tools/fixture-codemod/pipeline-observations/recipes.json" {
			key = "Successor"
		}
		row[key], err = json.Marshal(pin.After)
		must(err)
		raw, err = json.Marshal(row)
		must(err)
	}
	return raw
}

func declaration(raw []byte, name string) (string, string) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return string(raw[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset]), requestkey.Node(fn)
		}
	}
	panic("missing public rollback declaration: " + name)
}

func replace(raw []byte, name, after string) []byte {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
			out, err := format.Source(append(append(append([]byte{}, raw[:start]...), after...), raw[end:]...))
			must(err)
			return out
		}
	}
	panic("missing public rollback destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
