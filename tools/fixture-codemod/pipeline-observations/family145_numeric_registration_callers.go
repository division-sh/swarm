package main

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
)

var family145NumericRegistrationSources = []struct{ name, digest string }{
	{"empty_handler_construction_test.go", "bdd515f4a8d3cbc6ec718b317a19e99049d81ebfd93b0ffdf8fe71d9413975f8"},
	{"fan_out_expired_evaluation_conformance_test.go", "cc875363caa0d7c83836b39108d2dd2beedefc5020d78b383c2548fc60815ead"},
	{"fan_out_shared_serving_conformance_test.go", "a25758db86a88d5ed7c677dbdcf1529e7291560e5ef4c6ccc05adb5fcda3c3d4"},
	{"fan_out_serving_matrix_helpers_test.go", "c9478f1b2f4fb59374fffe0d4b7d3000e9ea40fc545b568a534e7ee32aee0dd8"},
	{"fan_out_supplemental_helpers_test.go", "9830651afc3a18f0b4f70679535b71220a612b888aaf455752e5b8892a6341fd"},
	{"notify_all_children_runtime_conformance_test.go", "104d156c18e3e338f2e58bd88dc31b5f94d3d1f2f1cdc4d307dee9df1d25225e"},
}

func migrateFamily145NumericRegistrationCallers(write bool) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	var files []pendingFile
	var calls int
	for _, input := range family145NumericRegistrationSources {
		path := filepath.Join(root, "internal/runtime/conformance", input.name)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != input.digest {
			return fmt.Errorf("numeric registration source/binding changed: %s; no files written", input.name)
		}
		after, count, err := rewriteFamily145NumericRegistrationCalls(input.name, data)
		if err != nil {
			return err
		}
		calls += count
		files = append(files, pendingFile{path: path, data: after})
	}
	if calls != 8 {
		return fmt.Errorf("numeric registration inventory changed: calls=%d; no files written", calls)
	}
	if err := applyPendingFiles(root, files, write); err != nil {
		return err
	}
	fmt.Printf("family145-numeric-registration: files=%d calls=%d write=%t\n", len(files), calls, write)
	return nil
}

func rewriteFamily145NumericRegistrationCalls(name string, data []byte) ([]byte, int, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, name, data, parser.ParseComments)
	if err != nil {
		return nil, 0, err
	}
	var edits []family145SourceEdit
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || err != nil {
			return err == nil
		}
		fn, ok := call.Fun.(*ast.Ident)
		if !ok || fn.Name != "loadNotifyAllChildrenNumericRegistrations" {
			return true
		}
		if len(call.Args) != 5 || family145NodeString(call.Args[0]) != "t" {
			err = fmt.Errorf("unreviewed numeric registration call shape in %s", name)
			return false
		}
		binding := family145NodeString(call.Args[2]) + "/" + family145NodeString(call.Args[3])
		switch binding {
		case "selected/db", "f.selected/f.db":
		default:
			err = fmt.Errorf("unreviewed numeric registration owner/pool %s in %s", binding, name)
			return false
		}
		edits = append(edits, family145SourceEdit{start: set.Position(call.Args[3].Pos()).Offset, end: set.Position(call.Args[4].Pos()).Offset})
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	after, err := applyFamily145SourceEdits(name, data, edits)
	return after, len(edits), err
}
