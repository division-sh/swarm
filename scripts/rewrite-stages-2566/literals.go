package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// This one-time preparation visits only complete YAML documents in Go strings.
// Exact application remains the finite hash-checked ledger, not this discovery.
func prepareLiterals(root string) error {
	command := exec.Command("git", "ls-files", "-z", "--", "*.go")
	command.Dir = root
	names, err := command.Output()
	if err != nil {
		return err
	}
	for _, name := range strings.Split(strings.TrimSuffix(string(names), "\x00"), "\x00") {
		if strings.HasPrefix(name, "scripts/") || strings.HasSuffix(name, "workflow_final_stage_contract_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, name, body, 0)
		if err != nil {
			return err
		}
		var edits []edit
		var failed error
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING || failed != nil {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || (!strings.Contains(value, "initial:") && !strings.Contains(value, "terminal:")) {
				return true
			}
			output, err := prepareStageLiteral(value)
			if err != nil {
				failed = fmt.Errorf("%s: %w", positions.Position(literal.Pos()), err)
				return false
			}
			if output == value {
				return true
			}
			replacement := strconv.Quote(output)
			if strings.HasPrefix(literal.Value, "`") && !strings.Contains(output, "`") {
				replacement = "`" + output + "`"
			}
			edits = append(edits, edit{Offset: positions.Position(literal.Pos()).Offset, Before: literal.Value, After: replacement})
			return true
		})
		if failed != nil {
			return failed
		}
		if len(edits) == 0 {
			continue
		}
		output := append([]byte(nil), body...)
		for i := len(edits) - 1; i >= 0; i-- {
			e := edits[i]
			output = append(append(append([]byte(nil), output[:e.Offset]...), e.After...), output[e.Offset+len(e.Before):]...)
		}
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(root, name), output, info.Mode().Perm()); err != nil {
			return err
		}
		fmt.Printf("%s: %d complete stage literals prepared; fragments still require review\n", name, len(edits))
	}
	return nil
}

func prepareStageLiteral(body string) (string, error) {
	snapshot, err := yamlsource.Load([]byte(body))
	if err != nil {
		return body, nil // A partial generator is not inferred to be a schema.
	}
	root := snapshot.Document("fixture").Root()
	stages, err := root.Lookup("stages")
	if err != nil || stages.Presence == yamlsource.PresenceMissing {
		return body, nil
	}
	declarations, err := stages.Value.Mapping()
	if err != nil {
		return body, nil
	}
	lines := strings.SplitAfter(body, "\n")
	offset := func(location yamlsource.Location) int {
		position := location.Column - 1
		for i := 0; i < location.Line-1; i++ {
			position += len(lines[i])
		}
		return position
	}
	var edits []edit
	for i, stage := range declarations {
		fields, err := stage.Value.Mapping()
		if err != nil {
			continue
		}
		for _, field := range fields {
			if field.Name != "initial" && field.Name != "terminal" {
				continue
			}
			value, err := field.Value.Scalar()
			start := offset(field.KeyLocation)
			if err != nil || value.Tag != "!!bool" || value.Style != 0 || value.Alias != "" || field.FromMerge || !strings.HasPrefix(body[start:], field.Name+":") {
				return "", fmt.Errorf("explicit review required for non-simple stage marker at %s", field.KeyLocation)
			}
			if field.Name == "terminal" {
				edits = append(edits, edit{Offset: start, Before: "terminal", After: "final"})
				continue
			}
			if i != 0 && strings.EqualFold(value.Value, "true") {
				return "", fmt.Errorf("entry %s is not first; review declaration movement", stage.Name)
			}
			end := offset(value.Location) + len(value.Value)
			if body[offset(stage.Value.Location())] == '{' {
				for end < len(body) && body[end] == ' ' {
					end++
				}
				if end < len(body) && body[end] == ',' {
					end++
				} else {
					for start > 0 && body[start-1] == ' ' {
						start--
					}
					if start > 0 && body[start-1] == ',' {
						start--
					}
				}
				edits = append(edits, edit{Offset: start, Before: body[start:end]})
			} else if len(fields) == 1 {
				edits = append(edits, edit{Offset: start, Before: body[start:end], After: "{}"})
			} else {
				start -= field.KeyLocation.Column - 1
				end = start + len(lines[field.KeyLocation.Line-1])
				edits = append(edits, edit{Offset: start, Before: body[start:end]})
			}
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].Offset < edits[j].Offset })
	output := body
	last := len(body)
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		if e.Offset+len(e.Before) > last {
			return "", fmt.Errorf("overlapping stage edits require review")
		}
		output = output[:e.Offset] + e.After + output[e.Offset+len(e.Before):]
		last = e.Offset
	}
	return output, nil
}
