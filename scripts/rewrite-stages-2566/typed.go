package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Preparation for the inventoried Go fixtures only. Final application is by
// reviewed hashes/edits; this never parses executable YAML or changes data fields.
func prepareTyped(root string) error {
	command := exec.Command("git", "ls-files", "-z", "--", "*.go")
	command.Dir = root
	names, err := command.Output()
	if err != nil {
		return err
	}
	changed := 0
	for _, name := range strings.Split(strings.TrimSuffix(string(names), "\x00"), "\x00") {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, name, body, 0)
		if err != nil {
			return err
		}
		output, count, err := rewriteTypedStages(body, positions, file)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if count == 0 {
			continue
		}
		output, err = format.Source(output)
		if err != nil {
			return fmt.Errorf("%s: prepared typed fixture corrupt: %w", name, err)
		}
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(root, name), output, info.Mode().Perm()); err != nil {
			return err
		}
		changed++
		fmt.Printf("%s: %d typed marker fields; entry ordering/assertions remain review-owned\n", name, count)
	}
	fmt.Printf("%d typed fixture files prepared\n", changed)
	return nil
}

func rewriteTypedStages(body []byte, positions *token.FileSet, file *ast.File) ([]byte, int, error) {
	var edits []edit
	seen := map[*ast.CompositeLit]bool{}
	var typeName func(ast.Expr) string
	typeName = func(expression ast.Expr) string {
		switch typed := expression.(type) {
		case *ast.Ident:
			return typed.Name
		case *ast.SelectorExpr:
			return typed.Sel.Name
		case *ast.ArrayType:
			return typeName(typed.Elt)
		}
		return ""
	}
	add := func(literal *ast.CompositeLit) {
		if seen[literal] {
			return
		}
		seen[literal] = true
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok {
				continue
			}
			start := positions.Position(field.Pos()).Offset
			end := positions.Position(field.End()).Offset
			switch key.Name {
			case "Terminal":
				edits = append(edits, edit{Offset: start, Before: "Terminal", After: "Final"})
			case "Initial":
				for end < len(body) && (body[end] == ' ' || body[end] == '\t') {
					end++
				}
				if end < len(body) && body[end] == ',' {
					end++
				} else {
					for start > 0 && (body[start-1] == ' ' || body[start-1] == '\t') {
						start--
					}
					if start > 0 && body[start-1] == ',' {
						start--
					}
				}
				edits = append(edits, edit{Offset: start, Before: string(body[start:end])})
			}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || typeName(literal.Type) != "FlowStageDeclaration" {
			return true
		}
		if _, array := literal.Type.(*ast.ArrayType); array {
			for _, element := range literal.Elts {
				if child, ok := element.(*ast.CompositeLit); ok {
					add(child)
				}
			}
		} else {
			add(literal)
		}
		return true
	})
	sort.Slice(edits, func(i, j int) bool { return edits[i].Offset < edits[j].Offset })
	c := change{File: positions.Position(file.Pos()).Filename, BeforeHash: digest(body), Edits: edits}
	// Derive the prepared output hash, then use the same checked finite applier.
	output := append([]byte(nil), body...)
	last := len(body)
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		if e.Offset+len(e.Before) > last || string(body[e.Offset:e.Offset+len(e.Before)]) != e.Before {
			return nil, 0, fmt.Errorf("ambiguous typed field edit")
		}
		output = append(append(append([]byte(nil), output[:e.Offset]...), []byte(e.After)...), output[e.Offset+len(e.Before):]...)
		last = e.Offset
	}
	c.AfterHash = digest(output)
	output, err := rewrite(c, body)
	return output, len(edits), err
}
