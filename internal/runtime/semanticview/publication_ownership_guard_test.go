package semanticview_test

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestPublicationConsumersCannotRestoreLegacyNameProjection(t *testing.T) {
	if findings := publicationProjectionFindings(t, nil); len(findings) != 0 {
		t.Fatalf("competing publication interpretation: %v", findings)
	}
}

func TestPublicationProjectionGuardRejectsAliasesAndExistingFileBypasses(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	overlay := map[string][]byte{}
	for _, pkg := range []string{"engine", "pipeline", "manager", "bus", "tools"} {
		overlay[filepath.Join(root, "internal/runtime", pkg, "publication_hostile.go")] = []byte(`package ` + pkg + `
import disguised "github.com/division-sh/swarm/internal/runtime/core/identity"
func hostilePublicationName(path, event string) string {
    alternate := disguised.ExternalizeEventForFlow
    return alternate(path, []string{event}, event)
}
func hostileLeafAdmission(event string) string { return disguised.EventLeafName(event) }
`)
	}
	existing := filepath.Join(root, "internal/runtime/tools/emit.go")
	data, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	overlay[existing] = append(data, []byte(`
func hostileExistingEmitOwner(event string) string { return runtimeidentity.EventLeafName(event) }
`)...)
	findings := publicationProjectionFindings(t, overlay)
	foundExisting := false
	for _, finding := range findings {
		if strings.Contains(finding, ".hostileExistingEmitOwner:") {
			foundExisting = true
		}
	}
	if !foundExisting {
		t.Errorf("guard missed illegal conversion in existing owner: %v", findings)
	}
	for _, pkg := range []string{"engine", "pipeline", "manager", "bus", "tools"} {
		for _, name := range []string{"hostilePublicationName", "hostileLeafAdmission"} {
			want := "github.com/division-sh/swarm/internal/runtime/" + pkg + "." + name
			found := false
			for _, finding := range findings {
				if strings.HasPrefix(finding, want+":") {
					found = true
				}
			}
			if !found {
				t.Errorf("guard missed %s: %v", want, findings)
			}
		}
	}
}

func publicationProjectionFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Dir: agentNameGuardRepoRoot(t), Tests: false, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedCompiledGoFiles,
	}, "./internal/runtime/engine", "./internal/runtime/pipeline", "./internal/runtime/bus", "./internal/runtime/manager", "./internal/runtime/tools")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("publication guard must typecheck every consumer")
	}
	var findings []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					used, ok := pkg.TypesInfo.Uses[id].(*types.Func)
					if !ok || used.Pkg() == nil || used.Pkg().Path() != "github.com/division-sh/swarm/internal/runtime/core/identity" {
						return true
					}
					if used.Name() != "ExternalizeEventForFlow" && used.Name() != "EventLeafName" {
						return true
					}
					// Tool spelling is a representation, never declaration/schema authority.
					if used.Name() == "EventLeafName" && pkg.PkgPath == "github.com/division-sh/swarm/internal/runtime/tools" && pkg.TypesInfo.Defs[fn.Name] == pkg.Types.Scope().Lookup("localEmitEventType") {
						return true
					}
					findings = append(findings, pkg.PkgPath+"."+fn.Name.Name+":"+used.Name())
					return true
				})
			}
		}
	}
	sort.Strings(findings)
	return findings
}

func TestPublicationProjectionGuardRejectsSameNamedReceiverMethod(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	overlay := map[string][]byte{
		filepath.Join(root, "internal/runtime/tools/publication_hostile_receiver.go"): []byte(`package tools
import disguise "github.com/division-sh/swarm/internal/runtime/core/identity"
type unrelatedPublicationReceiver struct{}
func (unrelatedPublicationReceiver) localEmitEventType(event string) string {
	return disguise.EventLeafName(event)
}
`),
	}
	findings := publicationProjectionFindings(t, overlay)
	for _, finding := range findings {
		if strings.Contains(finding, ".localEmitEventType:EventLeafName") {
			return
		}
	}
	t.Fatalf("guard allowed a receiver method to borrow the exact function exception: %v", findings)
}
