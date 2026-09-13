package semanticview_test

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestProspectivePublicationOnlyExactMutationCanMintAndDischarge(t *testing.T) {
	if findings := prospectivePublicationFindings(t, nil); len(findings) != 0 {
		t.Fatalf("competing prospective authority: %v", findings)
	}
}

func TestProspectivePublicationGuardRejectsAliasesAndImpersonatedOwner(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	overlay := map[string][]byte{
		filepath.Join(root, "internal/runtime/pipeline/prospective_hostile.go"): []byte(`package pipeline
import c "github.com/division-sh/swarm/internal/runtime/correlation"
func hostileMint(s WorkflowEngineStateRecord, fact c.SourceArtifactFact) {
  alias := prepareWorkflowPublicationState
  _, _ = alias(s, WorkflowLifecycleMutationPlan{}, "review", fact)
}
`),
		filepath.Join(root, "internal/store/internal/backend/pipelinepersistence/prospective_hostile.go"): []byte(`package pipelinepersistence
import b "github.com/division-sh/swarm/internal/runtime/bus"
import p "github.com/division-sh/swarm/internal/runtime/pipeline"
type impostorMutationOwner struct{}
func (impostorMutationOwner) commitWorkflowEngineMutation(plan b.EnginePublicationPlan, state p.WorkflowEngineStateRecord) {
  alias := plan.PublicationCommandForMutation
  _, _ = alias(state, p.WorkflowLifecycleMutationPlan{})
}
`),
	}
	findings := prospectivePublicationFindings(t, overlay)
	if len(findings) != 2 || !strings.Contains(strings.Join(findings, ";"), "hostileMint") || !strings.Contains(strings.Join(findings, ";"), "impostorMutationOwner") {
		t.Fatalf("guard missed aliased mint/discharge: %v", findings)
	}
}

func prospectivePublicationFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Dir: agentNameGuardRepoRoot(t), Tests: false, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedCompiledGoFiles,
	}, "./internal/runtime/pipeline", "./internal/runtime/bus", "./internal/store/internal/backend/pipelinepersistence", "./internal/store/internal/backend/eventpersistence", "./internal/store/internal/backend/runforkpersistence")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("prospective publication guard requires complete typechecking")
	}
	const base = "github.com/division-sh/swarm/internal/"
	allowed := map[string]string{
		base + "runtime/pipeline.prepareWorkflowPublicationState":                base + "runtime/pipeline.pipelineEngineMutationOwner.CommitEngineMutation",
		base + "runtime/bus.EnginePublicationPlan.PublicationCommandForMutation": base + "store/internal/backend/pipelinepersistence.commitWorkflowEngineMutation",
	}
	key := func(fn *types.Func) string {
		if fn == nil || fn.Pkg() == nil {
			return ""
		}
		out := fn.Pkg().Path() + "."
		if receiver := fn.Type().(*types.Signature).Recv(); receiver != nil {
			typ := types.Unalias(receiver.Type())
			if pointer, ok := typ.(*types.Pointer); ok {
				typ = types.Unalias(pointer.Elem())
			}
			if named, ok := typ.(*types.Named); ok {
				out += named.Obj().Name() + "."
			}
		}
		return out + fn.Name()
	}
	var findings []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner, _ := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					id, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					callee, _ := pkg.TypesInfo.Uses[id].(*types.Func)
					if expected, guarded := allowed[key(callee)]; guarded && key(owner) != expected {
						findings = append(findings, key(owner)+":"+key(callee))
					}
					return true
				})
			}
		}
	}
	return findings
}
