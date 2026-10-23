package packadmission

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

func TestS1ProviderDirectoryAdmissionCompilerOwner(t *testing.T) {
	if findings := providerDirectoryOwnerFindings(t, nil); len(findings) != 0 {
		t.Fatal(strings.Join(findings, "\n"))
	}
}

func TestS1ProviderDirectoryAdmissionGuardRejectsCarrierAndSameNameMethod(t *testing.T) {
	path, err := filepath.Abs("provider.go")
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	injection := `
_, _ = providertriggers.AdmitBody(packartifact.ProviderBody{})
_, _ = providerconnectors.AdmitBody(packartifact.ProviderBody{})
_, _ = packs.CompileProviderChannel(nil, packartifact.AdmittedProvider{}, packs.TriggerPackDescriptor{}, packs.ConnectorPackDescriptor{})
`
	for _, owner := range []string{"ChannelPlan", "ConnectorDescriptor", "same-name method"} {
		t.Run(owner, func(t *testing.T) {
			source := string(body)
			if owner == "same-name method" {
				source += "\nfunc (p ProviderDefinition) AdmitProviderDirectory() {" + injection + "}\n"
			} else {
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
					t.Fatalf("missing carrier owner %s", owner)
				}
				source = source[:position] + injection + source[position:]
			}
			findings := strings.Join(providerDirectoryOwnerFindings(t, map[string][]byte{path: []byte(source)}), "\n")
			for _, want := range []string{"providertriggers.AdmitBody", "providerconnectors.AdmitBody", "packs.CompileProviderChannel"} {
				if !strings.Contains(findings, want) {
					t.Fatalf("guard missed %s: %s", want, findings)
				}
			}
		})
	}
}

func providerDirectoryOwnerFindings(t testing.TB, overlay map[string][]byte) []string {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Dir: ".", Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}, ".")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("directory guard requires compiler-resolved admission: %v", err)
	}
	var findings []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				if fn, ok := declaration.(*ast.FuncDecl); ok {
					findings = append(findings, providerDirectoryFunctionFindings(pkg, fn)...)
				}
			}
		}
	}
	return findings
}

func providerDirectoryFunctionFindings(pkg *packages.Package, fn *ast.FuncDecl) []string {
	if fn.Body == nil || (fn.Name.Name == "AdmitProviderDirectory" && fn.Recv == nil) {
		return nil
	}
	var findings []string
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
		path, name := object.Pkg().Path(), object.Name()
		provider := path == "github.com/division-sh/swarm/internal/providertriggers" || path == "github.com/division-sh/swarm/internal/providerconnectors"
		compiler := path == "github.com/division-sh/swarm/internal/packs" && (name == "CompileProviderChannel" || name == "AdmitChannelBody" || name == "NewProviderBodyIdentity")
		if (provider && (name == "AdmitBody" || name == "NewCatalogSnapshotFromAdmittedBodies")) || compiler {
			findings = append(findings, fmt.Sprintf("%s competing provider admission %s.%s", fn.Name.Name, object.Pkg().Name(), name))
		}
		return true
	})
	return findings
}
