package scenariodocument

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestScenarioConsumersCannotRestoreIndependentDecoders(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	paths := []string{"internal/cliapp/test_command.go"}
	entries, err := os.ReadDir(filepath.Join(root, "internal/runtime/scenarioderivation"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			paths = append(paths, "internal/runtime/scenarioderivation/"+entry.Name())
		}
	}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			t.Fatal(err)
		}
		if err := checkScenarioConsumer(path, raw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestScenarioOwnershipSentinelRejectsDecoderMutations(t *testing.T) {
	for _, row := range []struct{ path, source string }{
		{"internal/cliapp/test_command.go", `package cliapp; import y "gopkg.in/yaml.v3"; func hidden(raw []byte) { _ = y.Unmarshal(raw, nil) }`},
		{"internal/cliapp/test_command.go", `package cliapp; import "encoding/json"; func hidden(raw []byte) { _ = json.Unmarshal(raw, nil) }`},
		{"internal/cliapp/test_command.go", `package cliapp; import "encoding/json"; func hidden(v any) { _, _ = json.Marshal(v) }`},
		{"internal/runtime/scenarioderivation/declaration.go", `package scenarioderivation; import "encoding/json"; func hidden(raw []byte) { _ = json.Unmarshal(raw, nil) }`},
		{"internal/runtime/scenarioderivation/nested.go", `package scenarioderivation; import "encoding/json"; func hidden(v any) { _, _ = json.Marshal(v) }`},
		{"internal/cliapp/test_command.go", `package cliapp; func yamlNodeValue() {}`},
	} {
		if err := checkScenarioConsumer(row.path, []byte(row.source)); err == nil {
			t.Fatalf("decoder bypass accepted: %s", row.source)
		}
	}
	if err := checkScenarioConsumer("internal/cliapp/api_client.go", []byte(`package cliapp; import "encoding/json"; func transport(raw []byte) { _ = json.Unmarshal(raw,nil) }`)); err != nil {
		t.Fatalf("typed API transport conflated with scenario grammar: %v", err)
	}
}

func checkScenarioConsumer(path string, raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		return err
	}
	jsonAliases := map[string]bool{}
	for _, spec := range file.Imports {
		importPath, _ := strconv.Unquote(spec.Path.Value)
		if importPath == "gopkg.in/yaml.v3" {
			return fmt.Errorf("%s restores raw scenario YAML decoding", path)
		}
		if importPath == "encoding/json" {
			name := "json"
			if spec.Name != nil {
				name = spec.Name.Name
			}
			jsonAliases[name] = true
		}
	}
	retired := map[string]bool{"mappingNode": true, "yamlNodeValue": true, "parseScenarioStep": true, "parseScenarioSetup": true, "parseScenarioInvalid": true, "scenarioCELValue": true, "cloneDeclarationMap": true, "cloneObject": true}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && retired[fn.Name.Name] {
			return fmt.Errorf("%s restores retired interpreter %s", path, fn.Name.Name)
		}
	}
	if path != "internal/cliapp/test_command.go" && !strings.Contains(path, "scenarioderivation/") {
		return nil
	}
	var bypass error
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if !ok || !jsonAliases[owner.Name] {
			return true
		}
		switch selector.Sel.Name {
		case "Marshal", "Unmarshal", "NewDecoder":
			bypass = fmt.Errorf("%s restores an unchecked scenario clone/decoder: %s", path, selector.Sel.Name)
		}
		return true
	})
	return bypass
}
