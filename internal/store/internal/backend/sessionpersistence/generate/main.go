// Command generate seals the pinned SDK's typed stores without exporting its
// SQL-capable implementation. It copies signatures, never SDK implementation.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	raw, err := exec.Command("go", "list", "-m", "-json", "go.mau.fi/whatsmeow").Output()
	if err != nil {
		panic(err)
	}
	var module struct{ Dir string }
	if err := json.Unmarshal(raw, &module); err != nil {
		panic(err)
	}
	fset := token.NewFileSet()
	source, err := parser.ParseFile(fset, filepath.Join(module.Dir, "store", "store.go"), nil, 0)
	if err != nil {
		panic(err)
	}
	interfaces := sdkInterfaces(source)
	methods := sdkMethods(interfaces)
	var output strings.Builder
	output.WriteString("// Code generated from the exact pinned SDK interface signatures; DO NOT EDIT.\npackage sessionpersistence\nimport(\"context\";\"time\";\"go.mau.fi/whatsmeow/store\";\"go.mau.fi/whatsmeow/types\";\"go.mau.fi/whatsmeow/util/keys\")\n")
	names := make([]string, 0, len(methods))
	for name := range methods {
		names = append(names, name)
	}
	sort.Strings(names)
	lids := make(map[string]bool)
	for _, method := range interfaces["LIDStore"].Methods.List {
		lids[method.Names[0].Name] = true
	}
	for _, name := range names {
		writeMethod(&output, fset, name, methods[name], lids[name])
	}
	formatted, err := format.Source([]byte(output.String()))
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("sdk_storage_generated.go", formatted, 0644); err != nil {
		panic(err)
	}
}

func sdkInterfaces(source *ast.File) map[string]*ast.InterfaceType {
	interfaces := make(map[string]*ast.InterfaceType)
	for _, decl := range source.Decls {
		if group, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range group.Specs {
				if spec, ok := spec.(*ast.TypeSpec); ok {
					if body, ok := spec.Type.(*ast.InterfaceType); ok {
						interfaces[spec.Name.Name] = body
					}
				}
			}
		}
	}
	return interfaces
}

func sdkMethods(interfaces map[string]*ast.InterfaceType) map[string]*ast.FuncType {
	methods := make(map[string]*ast.FuncType)
	var collect func(string)
	collect = func(name string) {
		for _, method := range interfaces[name].Methods.List {
			if len(method.Names) == 0 {
				collect(method.Type.(*ast.Ident).Name)
			} else {
				methods[method.Names[0].Name] = method.Type.(*ast.FuncType)
			}
		}
	}
	collect("AllStores")
	return methods
}

func writeMethod(output *strings.Builder, fset *token.FileSet, name string, method *ast.FuncType, lid bool) {
	text := func(expr ast.Expr) string {
		var out bytes.Buffer
		if err := printer.Fprint(&out, fset, expr); err != nil {
			panic(err)
		}
		value := out.String()
		for _, name := range []string{"AppStateSyncKey", "AppStateMutationMAC", "ContactEntry", "RedactedPhoneEntry", "MessageSecretInsert", "PrivacyToken", "BufferedEvent", "LIDMapping"} {
			value = strings.ReplaceAll(value, name, "store."+name)
		}
		return value
	}
	var params, args, results []string
	for _, field := range method.Params.List {
		for _, param := range field.Names {
			params = append(params, param.Name+" "+text(field.Type))
			argument := param.Name
			if _, ok := field.Type.(*ast.Ellipsis); ok {
				argument += "..."
			}
			args = append(args, argument)
		}
	}
	for _, field := range method.Results.List {
		results = append(results, text(field.Type))
	}
	result := strings.Join(results, ", ")
	if len(results) > 1 {
		result = "(" + result + ")"
	}
	owner := "s.session"
	receiver := "sdkStorage"
	if lid {
		owner = "s.lids"
		receiver = "sdkLIDStorage"
	}
	fmt.Fprintf(output, "func(s *%s) %s(%s) %s { return %s.%s(%s) }\n", receiver, name, strings.Join(params, ", "), result, owner, name, strings.Join(args, ", "))
}
