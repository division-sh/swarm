package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type goRewriteInput struct {
	Path        string        `json:"path"`
	Declaration string        `json:"declaration"`
	Ordinal     int           `json:"ordinal"`
	Before      string        `json:"before"`
	After       string        `json:"after"`
	Counts      rewriteCounts `json:"counts"`
}

type goSourceLiteral struct {
	declaration string
	ordinal     int
	start, end  int
	text        string
}

func rewriteGoNodeLiteral(text string) (string, rewriteCounts, error) {
	body, err := strconv.Unquote(text)
	if err != nil {
		return "", rewriteCounts{}, err
	}
	if strings.TrimRight(body, " \t") != body {
		return "", rewriteCounts{}, fmt.Errorf("incomplete source fragment; disposition required")
	}
	output, counts, err := rewriteNodeSource([]byte(body))
	if err != nil {
		return "", rewriteCounts{}, err
	}
	if strings.HasPrefix(text, "`") {
		return "`" + string(output) + "`", counts, nil
	}
	return strconv.Quote(string(output)), counts, nil
}

func goSourceLiterals(path string, source []byte) ([]goSourceLiteral, error) {
	if filepath.Ext(path) != ".go" {
		return nil, fmt.Errorf("%s is not a Go source file", path)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, 0)
	if err != nil {
		return nil, err
	}
	var literals []goSourceLiteral
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ordinal := 0
		ast.Inspect(function.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			literals = append(literals, goSourceLiteral{
				declaration: function.Name.Name, ordinal: ordinal,
				start: fset.Position(literal.Pos()).Offset, end: fset.Position(literal.End()).Offset,
				text: literal.Value,
			})
			ordinal++
			return true
		})
	}
	return literals, nil
}

func prepareGoInputs(paths []string) ([]goRewriteInput, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("select complete Go fixture sources explicitly")
	}
	inputs := []goRewriteInput{}
	for _, selection := range paths {
		path, declaration, _ := strings.Cut(selection, ":")
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		literals, err := goSourceLiterals(path, source)
		if err != nil {
			return nil, err
		}
		found := declaration == ""
		for _, literal := range literals {
			if declaration != "" && literal.declaration != declaration {
				continue
			}
			found = true
			body, err := strconv.Unquote(literal.text)
			if err != nil {
				return nil, err
			}
			if !strings.Contains(body, "\n  event_handlers:") {
				continue
			}
			after, counts, err := rewriteGoNodeLiteral(literal.text)
			if err != nil {
				return nil, fmt.Errorf("%s:%s literal %d: %w", path, literal.declaration, literal.ordinal, err)
			}
			inputs = append(inputs, goRewriteInput{Path: filepath.ToSlash(path), Declaration: literal.declaration, Ordinal: literal.ordinal, Before: literal.text, After: after, Counts: counts})
		}
		if !found {
			return nil, fmt.Errorf("%s: selected declaration %q has no string literals", path, declaration)
		}
	}
	return inputs, nil
}

func applyGoInputs(path string, write bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var inputs []goRewriteInput
	if err := json.Unmarshal(data, &inputs); err != nil {
		return err
	}
	if len(inputs) == 0 {
		return fmt.Errorf("empty Go rewrite selection")
	}
	byPath := map[string][]goRewriteInput{}
	for _, input := range inputs {
		after, counts, err := rewriteGoNodeLiteral(input.Before)
		if err != nil || after != input.After || counts != input.Counts || !counts.changed() {
			return fmt.Errorf("%s:%s literal %d: reviewed rewrite does not replay: %v", input.Path, input.Declaration, input.Ordinal, err)
		}
		byPath[input.Path] = append(byPath[input.Path], input)
	}
	type plannedFile struct {
		path   string
		output []byte
		mode   os.FileMode
	}
	var plan []plannedFile
	for path, selected := range byPath {
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		literals, err := goSourceLiterals(path, source)
		if err != nil {
			return err
		}
		type edit struct {
			start, end int
			after      string
		}
		var edits []edit
		seen := map[int]bool{}
		for _, input := range selected {
			found := false
			for _, literal := range literals {
				if input.Declaration != literal.declaration || input.Ordinal != literal.ordinal {
					continue
				}
				if seen[literal.start] || literal.text != input.Before && literal.text != input.After {
					return fmt.Errorf("%s:%s literal %d: duplicate selection or reviewed input changed", path, input.Declaration, input.Ordinal)
				}
				seen[literal.start], found = true, true
				edits = append(edits, edit{literal.start, literal.end, input.After})
				break
			}
			if !found {
				return fmt.Errorf("%s:%s literal %d: selected source is missing", path, input.Declaration, input.Ordinal)
			}
		}
		sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
		for _, edit := range edits {
			source = append(append(append([]byte{}, source[:edit.start]...), edit.after...), source[edit.end:]...)
		}
		output, err := format.Source(source)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		plan = append(plan, plannedFile{path, output, info.Mode().Perm()})
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].path < plan[j].path })
	for _, file := range plan {
		if write {
			if err := os.WriteFile(file.path, file.output, file.mode); err != nil {
				return err
			}
		}
		fmt.Printf("%s: explicit Go literal rewrite write=%v\n", file.path, write)
	}
	return nil
}
