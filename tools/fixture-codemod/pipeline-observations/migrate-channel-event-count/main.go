package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

type pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-channel-event-count [-write]")
	}
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", channeleventcount.Predecessor, "--", "internal/serveapp").Output()
	must(err)
	var pins []pin
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || !strings.HasPrefix(filepath.Base(path), "channel") || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		raw, err := exec.Command("git", "show", channeleventcount.Predecessor+":"+path).Output()
		if err != nil {
			return err
		}
		pins = append(pins, sourcePins(path, raw)...)
		return nil
	}))
	count := 0
	for _, pin := range pins {
		count += pin.Calls
	}
	if count != channeleventcount.Calls || len(pins) != channeleventcount.Bodies {
		panic(fmt.Sprintf("channel event-count inventory=%d/%d", count, len(pins)))
	}
	outputs := prepare(pins)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d bodies=%d manifest_sha256=%x\n", count, len(pins), sha256.Sum256(raw))
	if write {
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != channeleventcount.Digest {
			panic("channel event-count historical source escaped its complete rule")
		}
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(channeleventcount.Path, raw, 0644))
	}
}

func sourcePins(path string, raw []byte) []pin {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	must(err)
	var pins []pin
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		before := requestkey.Node(fn)
		after, calls, err := channeleventcount.Transform(before)
		must(err)
		if before != after {
			pins = append(pins, pin{path, fn.Name.Name, before, after, fmt.Sprintf("%x", sha256.Sum256([]byte(before))), calls})
		}
	}
	return pins
}

func prepare(pins []pin) map[string][]byte {
	grouped := map[string][]pin{}
	for _, pin := range pins {
		grouped[pin.File] = append(grouped[pin.File], pin)
	}
	outputs := map[string][]byte{}
	for path, rows := range grouped {
		raw, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
		must(err)
		var edits []edit
		for _, row := range rows {
			edits = append(edits, currentEdit(fset, file, path, row))
		}
		slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
		for _, e := range edits {
			raw = append(append(append([]byte(nil), raw[:e.start]...), e.after...), raw[e.end:]...)
		}
		outputs[path] = raw
	}
	return outputs
}

func currentEdit(fset *token.FileSet, file *ast.File, path string, pin pin) edit {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != pin.Function || fn.Recv != nil {
			continue
		}
		current := requestkey.Node(fn)
		if current != pin.Before && current != pin.After {
			panic("channel event-count source escaped exact predecessor/successor: " + path + "/" + pin.Function)
		}
		return edit{fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset, pin.After}
	}
	panic("required channel event-count source disappeared: " + path + "/" + pin.Function)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
