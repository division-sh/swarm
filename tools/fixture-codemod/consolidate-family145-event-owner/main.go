// A finite, source-pinned relocation into the existing closed event fixture owner.
package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path"
	"sort"
	"strconv"
)

const directory = "internal/store/internal/runtimepersistence/"
const owner = directory + "test_event_support.go"
const ownerBefore = "4c77f69b1ff74d396c21841a892f69b7b2f94d18a2daea1f9f28debb3183efc0"

type source struct {
	file, digest string
	names        []string
}

var sources = []source{
	{"test_generic_schedule_publication_storage.go", "19a9ffa9adda511b52b0538636b929c4fe32056471e06e6d0b84003602bc90f0", []string{"GenericSchedulePublicationModes", "ReadGenericSchedulePublicationModesForTest", "ReadGenericScheduleDistinctPublicationModesForTest", "CountGenericSchedulePublicationsForTest"}},
	{"test_numeric_registration_observation.go", "a067559aca7d07f2a905db01e002768e116c1f2083f35167ff9d7a01df873115", []string{"NumericRegistrationStorage", "ReadNumericRegistrationStorageForTest"}},
	{"test_run_named_event_identity.go", "9b1e777fb51716b26799077080ed3627b6657e7576c3d67bb2da2ee05ae957e1", []string{"ReadRunNamedEventIdentityStorageForTest", "ReadRunNamedEventIdentitiesStorageForTest"}},
	{"test_source_revision_diagnostic_storage.go", "16886ff386d0da99c1faffab89928e2717700e723f8927741e117e47730ea1e8", []string{"ReadRunLifecycleProjectionLogPayloadsForTest"}},
	{"test_runtime_fixture_diagnostics.go", "b77ae3b67d50ceb9483aaf78653fa93e2a3c9f1e356e0436434622072f43e543", []string{"ReadLifecycleProjectionLogPayloadsForTest"}},
	{"test_selected_fork_business_storage.go", "ab7c98bb86aa791b247f97f4ca0d7b5341ad372be7856b6c1043ce4039e1d8d9", []string{"CountRunNamedEventStorageForTest"}},
	{"test_publication_observation.go", "f9e349f7e12277eedda16d3b331242db5a2fb90bfa2636bcf104206acd0f8231", []string{"PublicationEventProjection", "PublicationEventIdentityStorage", "OperatorReferenceStorage", "RootTemplatePublicationStorage", "RunStartStorage", "ReadPostgresRootTemplatePublicationForTest", "ReadPostgresRunStartStorageForTest", "ReadPublicationEventProjectionForTest", "ReadPublicationEventIdentityStorageForTest", "ReadOperatorReferenceStorageForTest"}},
	{"test_replay_target_route_fault.go", "fe59aada943596fb8d8b17a20af8349223fec57531c8e3aed72b5c51863e04ac", []string{"CorruptReplayEventTargetRouteForTest", "RestoreReplayEventTargetRouteForTest"}},
}

func main() {
	write := flag.Bool("write", false, "write only after all finite inputs and declaration correspondence pass")
	flag.Parse()
	if flag.NArg() != 0 {
		panic("usage: consolidate-family145-event-owner [-write]")
	}
	data := pinned(owner, ownerBefore)
	outputs := map[string][]byte{}
	declarations := map[string]string{}
	for _, input := range sources {
		raw := pinned(directory+input.file, input.digest)
		remaining, moved := relocate(raw, input.names)
		for _, name := range input.names {
			if _, duplicate := declarations[name]; duplicate {
				panic("duplicate relocation: " + name)
			}
			declarations[name] = moved[name]
			data = append(data, []byte("\n"+moved[name]+"\n")...)
			fmt.Printf("%s\t%s\t%x\n", input.file, name, sha256.Sum256([]byte(moved[name])))
		}
		outputs[directory+input.file] = remaining
	}
	data = bytes.Replace(data, []byte("import (\n"), []byte("import (\n\t\"errors\"\n\t\"strings\"\n\t\"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle\"\n"), 1)
	formatted, err := format.Source(data)
	must(err)
	_, movedAgain := relocate(formatted, sortedKeys(declarations))
	for name, before := range declarations {
		if movedAgain[name] != before {
			panic("relocation changed declaration: " + name)
		}
	}
	outputs[owner] = formatted
	if *write {
		for _, file := range sortedKeys(outputs) {
			if len(outputs[file]) == 0 {
				must(os.Remove(file))
			} else {
				must(os.WriteFile(file, outputs[file], 0644))
			}
		}
	}
}

func pinned(file, digest string) []byte {
	raw, err := os.ReadFile(file)
	must(err)
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != digest {
		panic("unreviewed input: " + file)
	}
	return raw
}

func relocate(raw []byte, names []string) ([]byte, map[string]string) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.ParseComments|parser.AllErrors)
	must(err)
	wanted := map[string]bool{}
	for _, name := range names {
		wanted[name] = true
	}
	type cut struct{ first, last int }
	var cuts []cut
	moved := map[string]string{}
	for _, decl := range file.Decls {
		var name string
		first := decl.Pos()
		switch node := decl.(type) {
		case *ast.FuncDecl:
			if node.Recv == nil {
				name = node.Name.Name
			}
			if node.Doc != nil {
				first = node.Doc.Pos()
			}
		case *ast.GenDecl:
			if node.Tok == token.TYPE && len(node.Specs) == 1 {
				name = node.Specs[0].(*ast.TypeSpec).Name.Name
			}
			if node.Doc != nil {
				first = node.Doc.Pos()
			}
		}
		if !wanted[name] {
			continue
		}
		if _, duplicate := moved[name]; duplicate {
			panic("ambiguous declaration: " + name)
		}
		start, end := fset.Position(first).Offset, fset.Position(decl.End()).Offset
		moved[name] = string(bytes.TrimSpace(raw[start:end]))
		cuts = append(cuts, cut{start, end})
	}
	if len(moved) != len(wanted) {
		panic("incomplete finite relocation")
	}
	for i := len(cuts) - 1; i >= 0; i-- {
		raw = append(append([]byte{}, raw[:cuts[i].first]...), raw[cuts[i].last:]...)
	}
	return pruneImports(raw), moved
}

func pruneImports(raw []byte) []byte {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "remaining.go", raw, parser.ParseComments|parser.AllErrors)
	must(err)
	used := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if qualifier, ok := selector.X.(*ast.Ident); ok {
				used[qualifier.Name] = true
			}
		}
		return true
	})
	var kept []ast.Decl
	for _, decl := range file.Decls {
		imports, ok := decl.(*ast.GenDecl)
		if ok && imports.Tok == token.IMPORT {
			var specs []ast.Spec
			for _, spec := range imports.Specs {
				imported := spec.(*ast.ImportSpec)
				location, err := strconv.Unquote(imported.Path.Value)
				must(err)
				name := path.Base(location)
				if imported.Name != nil {
					name = imported.Name.Name
				}
				if name == "_" || name == "." {
					panic("unexpected side-effect import")
				}
				if used[name] {
					specs = append(specs, spec)
				}
			}
			imports.Specs = specs
			if len(specs) == 0 {
				continue
			}
		}
		kept = append(kept, decl)
	}
	if len(kept) == 0 {
		return nil
	}
	file.Decls = kept
	var out bytes.Buffer
	must(format.Node(&out, fset, file))
	return out.Bytes()
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
