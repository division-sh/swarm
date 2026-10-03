package providertriggers

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestTriggerBodyAdmissionOwnership(t *testing.T) {
	if findings := triggerBodyOwnerFindings(t, nil); len(findings) != 0 {
		t.Fatal(strings.Join(findings, "\n"))
	}
}

func TestTriggerBodyOwnerGuardRejectsDecoderAndCarrierRoundtrips(t *testing.T) {
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"NewCatalogSnapshot", "readback", "compilePackAdmission"} {
		t.Run(owner, func(t *testing.T) {
			path := filepath.Join(root, "admission.go")
			if owner != "compilePackAdmission" {
				path = filepath.Join(root, "providertriggers.go")
			}
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			source := strings.Replace(string(body), "import (", "import (\n probeYAML \"gopkg.in/yaml.v3\"", 1)
			files := token.NewFileSet()
			file, err := parser.ParseFile(files, path, source, 0)
			if err != nil {
				t.Fatal(err)
			}
			position := 0
			for _, declaration := range file.Decls {
				if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == owner {
					position = files.Position(fn.Body.Lbrace).Offset + 1
				}
			}
			if position == 0 {
				t.Fatalf("missing carrier owner: %s", owner)
			}
			injection := "\nvar raw []byte; _, _ = ParseManifest(raw); _ = probeYAML.Unmarshal(raw, new(any)); _, _ = probeYAML.Marshal(new(any))\n"
			source = source[:position] + injection + source[position:]
			findings := strings.Join(triggerBodyOwnerFindings(t, map[string][]byte{path: []byte(source)}), "\n")
			for _, want := range []string{owner, "competing BODY admission", "competing YAML codec"} {
				if !strings.Contains(findings, want) {
					t.Fatalf("guard missed %q: %s", want, findings)
				}
			}
		})
	}
}

func triggerBodyOwnerFindings(t testing.TB, overlay map[string][]byte) []string {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Dir: ".", Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}, ".")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("BODY guard requires compiler-resolved owners: %v", err)
	}
	allowed := map[string]string{
		"ParseManifest": "parseManifestStrict", "parseManifestStrict": "parseManifestAt",
		"loadPackBody": "parseManifestAt", "StampPackEnvelope": "parseManifestStrict",
	}
	var findings []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					var object types.Object
					switch callee := call.Fun.(type) {
					case *ast.Ident:
						object = pkg.TypesInfo.Uses[callee]
					case *ast.SelectorExpr:
						object = pkg.TypesInfo.Uses[callee.Sel]
					}
					if object == nil || object.Pkg() == nil {
						return true
					}
					name, path := object.Name(), object.Pkg().Path()
					if path == "gopkg.in/yaml.v3" {
						findings = append(findings, fmt.Sprintf("%s competing YAML codec %s", fn.Name.Name, name))
					}
					if path == pkg.PkgPath && (name == "ParseManifest" || name == "parseManifestStrict" || name == "parseManifestAt") && allowed[fn.Name.Name] != name {
						findings = append(findings, fmt.Sprintf("%s competing BODY admission %s", fn.Name.Name, name))
					}
					if path == "github.com/division-sh/swarm/internal/yamlsource" && name == "Load" && fn.Name.Name != "parseManifestAt" {
						findings = append(findings, fmt.Sprintf("%s competing lexical BODY source", fn.Name.Name))
					}
					return true
				})
			}
		}
	}
	return findings
}
