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

// The shared owner repair precedes this finite propagation. These digests pin
// that exact intermediate source, including the unchanged caller bindings.
var family145NotifyRuntimeSources = []struct{ name, digest string }{
	{"empty_handler_construction_test.go", "03789f0bee50de71e0c06cafa86ec656e0f0977de8d0ac7cd652808922da2844"},
	{"fan_out_b17_mixed_dependency_test.go", "e1958a2a8ee56e38026f55a1d30516630222971662cf3f470855b60be01a1b3c"},
	{"fan_out_expired_evaluation_conformance_test.go", "f45a2b948efa64884cd933cdc9ec7a356e488f36929a6fc652e76ca15e2efaaa"},
	{"fan_out_nested_serving_helpers_test.go", "4501d19d7180c4cf7ade519a9f6a16b44ee6caca4399bbfe21a72a8d1fed64e4"},
	{"fan_out_resource_selected_fork_test.go", "f6ea32d406b6039205efb19d7a707b34a3efe9954ad1be8f047686c18cd54323"},
	{"fan_out_resource_source_proof_test.go", "7cfb6a0a913381e41536b3ffe18d2b9a40b7512227369e2e52f0657b59a0f2f4"},
	{"fan_out_semantic_proof_helpers_test.go", "307a6cae5709babe5363ec76231ed27397519973aec3d13cc4617cd7d80fd836"},
	{"fan_out_serving_generation_replacement_test.go", "d921a0b4bf2b5197ec73eadbca72c5545abecd44d31ca33099b48ae8c1d6731f"},
	{"fan_out_serving_matrix_helpers_test.go", "ac8cd863244083defe7a872a2c2ba02460bb1d70c284c56ba3ad4d2cb710d70f"},
	{"fan_out_shared_serving_conformance_test.go", "408ba6e60ce646fea7145e5b2308882a6bb8bc03ab3e6dee5d73328704a5017c"},
	{"fan_out_supplemental_helpers_test.go", "39850d5e178e62058e8ccea674bb99639019d07189c41cbbd20cb9c61dded1be"},
	{"notify_all_children_runtime_conformance_test.go", "49bd2786f1dc14562a76aac0e2f5eaf5e27cd173aacf60075a14a66c09c3523f"},
}

func migrateFamily145NotifyRuntimeCallers(write bool) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	var files []pendingFile
	var calls int
	for _, input := range family145NotifyRuntimeSources {
		path := filepath.Join(root, "internal/runtime/conformance", input.name)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != input.digest {
			return fmt.Errorf("notify runtime source/binding changed: %s; no files written", input.name)
		}
		after, count, err := rewriteFamily145NotifyRuntimeCalls(input.name, data)
		if err != nil {
			return err
		}
		calls += count
		files = append(files, pendingFile{path: path, data: after})
	}
	if calls != 24 {
		return fmt.Errorf("notify runtime inventory changed: calls=%d; no files written", calls)
	}
	if err := applyPendingFiles(root, files, write); err != nil {
		return err
	}
	fmt.Printf("family145-notify-runtime: files=%d calls=%d write=%t\n", len(files), calls, write)
	return nil
}

func rewriteFamily145NotifyRuntimeCalls(name string, data []byte) ([]byte, int, error) {
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
		if !ok || fn.Name != "newNotifyAllChildrenRuntime" {
			return true
		}
		if len(call.Args) < 5 || family145NodeString(call.Args[0]) != "t" {
			err = fmt.Errorf("unreviewed notify runtime call shape in %s", name)
			return false
		}
		binding := family145NodeString(call.Args[1]) + "/" + family145NodeString(call.Args[2])
		switch binding {
		case "selected/db", "backend/db", "f.selected/f.db":
		default:
			err = fmt.Errorf("unreviewed notify runtime pool/owner binding %s in %s", binding, name)
			return false
		}
		edits = append(edits, family145SourceEdit{start: set.Position(call.Args[2].Pos()).Offset, end: set.Position(call.Args[3].Pos()).Offset})
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	after, err := applyFamily145SourceEdits(name, data, edits)
	return after, len(edits), err
}
