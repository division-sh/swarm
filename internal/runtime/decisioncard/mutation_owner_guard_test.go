package decisioncard_test

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestMutationRefusalHasNoConsumerLocalClassifier(t *testing.T) {
	if findings := mutationRefusalFindings(t, nil); len(findings) != 0 {
		t.Fatalf("consumer-local refusal owner: %v", findings)
	}
}

func TestMutationRefusalGuardRejectsAliasesAndSameNamedMethods(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	overlay := map[string][]byte{
		filepath.Join(root, "internal/runtime/pipeline/refusal_hostile.go"): []byte(`package pipeline
import dc "github.com/division-sh/swarm/internal/runtime/decisioncard"
func classifierWithArbitraryReceiver(nothingToDoWithCard dc.Card) error {
  if nothingToDoWithCard.Status != dc.StatusPending { return dc.ErrAlreadyTerminal }
  return nil
}
`),
		filepath.Join(root, "internal/apiv1/refusal_hostile.go"): []byte(`package apiv1
import dc "github.com/division-sh/swarm/internal/runtime/decisioncard"
type impostorClassifier struct{}
func (impostorClassifier) decisionCardAPIError() error {
  alias := dc.ErrSuperseded
  return alias
}
`),
	}
	findings := mutationRefusalFindings(t, overlay)
	if len(findings) != 2 || !strings.Contains(strings.Join(findings, ";"), "classifierWithArbitraryReceiver") || !strings.Contains(strings.Join(findings, ";"), "impostorClassifier") {
		t.Fatalf("missed hostile classifiers: %v", findings)
	}
}

func mutationRefusalFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedCompiledGoFiles,
	}, "./internal/runtime/pipeline", "./internal/store/internal/backend/decisionpersistence", "./internal/store/internal/backend/pipelinepersistence", "./internal/apiv1")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("refusal owner census requires complete typechecking: %v", err)
	}
	const base = "github.com/division-sh/swarm/internal/"
	var findings []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
				signature := owner.Type().(*types.Signature)
				allowed := signature.Recv() == nil && ((pkg.PkgPath == base+"apiv1" && owner.Name() == "decisionCardAPIError") ||
					(pkg.PkgPath == base+"store/internal/backend/decisionpersistence" && owner.Name() == "requireActiveDecisionCardRun"))
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					obj, ok := pkg.TypesInfo.Uses[id].(*types.Const)
					if !ok || obj.Pkg() == nil || obj.Pkg().Path() != base+"runtime/decisioncard" || (obj.Name() != "ErrAlreadyTerminal" && obj.Name() != "ErrSuperseded") {
						return true
					}
					if !allowed {
						findings = append(findings, owner.FullName()+":"+obj.Name())
					}
					return true
				})
			}
		}
	}
	return findings
}
