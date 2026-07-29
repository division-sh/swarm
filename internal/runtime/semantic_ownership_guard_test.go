package runtime_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestRegisteredToolSemanticReconstructionIsAbsent(t *testing.T) {
	forbiddenTypes := map[string]struct{}{
		"RegisteredTool":   {},
		"SourceCapability": {},
	}
	forbiddenFunctions := map[string]struct{}{
		"registeredToolFromContract": {},
		"BaseSemanticSource":         {},
		"CloneEventCatalogEntry":     {},
		"CloneEventCatalogEntries":   {},
	}
	inspectProductionGo(t, func(path string, file *ast.File) {
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					if _, forbidden := forbiddenTypes[typeSpec.Name.Name]; forbidden {
						t.Errorf("%s declares retired semantic owner %s", path, typeSpec.Name.Name)
					}
				}
			case *ast.FuncDecl:
				if _, forbidden := forbiddenFunctions[typed.Name.Name]; forbidden {
					t.Errorf("%s declares retired semantic reconstruction function %s", path, typed.Name.Name)
				}
			}
		}
	})
}

func TestSchemaToolPlanPackagesRejectFreeSemanticStringsAndConsumerNormalization(t *testing.T) {
	closed := map[string]reflect.Type{
		"ToolInputSchema":               reflect.TypeOf(runtimecontracts.ToolInputSchema{}),
		"ToolSchemaEntry":               reflect.TypeOf(runtimecontracts.ToolSchemaEntry{}),
		"SatisfactionPlan":              reflect.TypeOf(packs.SatisfactionPlan{}),
		"OutboundBindingPlan":           reflect.TypeOf(packs.OutboundBindingPlan{}),
		"PrivateActivityTargetIdentity": reflect.TypeOf(packs.PrivateActivityTargetIdentity{}),
		"ExecutionTool":                 reflect.TypeOf(runtimetools.ExecutionTool{}),
		"ChannelActivityTarget":         reflect.TypeOf(runtimepipeline.ChannelActivityTarget{}),
		"PlanGeneration":                reflect.TypeOf(plangeneration.Generation{}),
		"TriggerCatalogGeneration":      reflect.TypeOf(triggergeneration.Generation{}),
		"SemanticCapabilities":          reflect.TypeOf(semanticview.Capabilities{}),
	}
	for name, typ := range closed {
		for index := 0; index < typ.NumField(); index++ {
			if field := typ.Field(index); field.IsExported() {
				t.Errorf("%s exposes semantic field %s", name, field.Name)
			}
		}
	}

	inspectProductionGo(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok {
				return true
			}
			for _, name := range field.Names {
				if name.Name == "PlanGeneration" && isStringType(field.Type) {
					t.Errorf("%s declares free-string PlanGeneration authority", path)
				}
			}
			return true
		})
	})
}

func inspectProductionGo(t *testing.T, inspect func(string, *ast.File)) {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		inspect(filepath.ToSlash(relative), file)
		return nil
	})
	if err != nil {
		t.Fatalf("inspect production Go: %v", err)
	}
}

func isStringType(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "string"
}
