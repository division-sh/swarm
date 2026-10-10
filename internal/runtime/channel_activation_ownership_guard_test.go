package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestChannelActivationExecutableReaderCensus(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	want := []string{
		"internal/channelonboarding/model.go",
		"internal/channelonboarding/publication.go",
		"internal/cliapp/verify_deployment.go",
		"internal/runtime/channel_activation_admission.go",
		"internal/runtime/channelactivation/owner.go",
		"internal/runtime/context_manager.go",
		"internal/runtime/engine/types.go",
		"internal/runtime/pipeline/activity_engine.go",
		"internal/runtime/pipeline/coordinator.go",
		"internal/runtime/publicingress/readiness.go",
		"internal/runtime/publicingress/registration.go",
		"internal/runtime/runtime.go",
		"internal/runtime/tools/channel_runtime.go",
		"internal/runtime/workflow_validation.go",
		"internal/serveapp/channel_onboarding.go",
		"internal/serveapp/main.go",
		"internal/serveapp/public_ingress.go",
	}
	got := map[string]struct{}{}
	forbidden := map[string]struct{}{
		"ChannelOutboundBindings":      {},
		"DeclaredChannelBindings":      {},
		"channelOperations":            {},
		"compileChannelOperations":     {},
		"channelActivityTools":         {},
		"ChannelActivityTools":         {},
		"compiledChannelActivityTools": {},
	}
	err := checkoutsource.WalkDir(repoRoot, filepath.Join(repoRoot, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		ast.Inspect(parsed, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			switch identifier.Name {
			case "ChannelActivationPublication", "ChannelActivationGeneration":
				got[rel] = struct{}{}
			}
			if _, retired := forbidden[identifier.Name]; retired {
				t.Errorf("retired channel activation interpreter %s survives in %s", identifier.Name, rel)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	gotList := make([]string, 0, len(got))
	for path := range got {
		gotList = append(gotList, path)
	}
	sort.Strings(gotList)
	if strings.Join(gotList, "\n") != strings.Join(want, "\n") {
		t.Fatalf("channel activation executable reader census changed:\ngot:\n%s\nwant:\n%s", strings.Join(gotList, "\n"), strings.Join(want, "\n"))
	}
}

func TestLearnedActivationConsumersKeepCompleteResponsibilityProjection(t *testing.T) {
	for path, function := range map[string]string{
		"channel_activation_admission.go":   "validateLearnedChannelPublication",
		"../serveapp/channel_onboarding.go": "compileServeLearnedChannelActivations",
	} {
		t.Run(function, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			found, calls := false, 0
			for _, declaration := range parsed.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Name.Name != function {
					continue
				}
				found = true
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if call, ok := node.(*ast.CallExpr); ok {
						if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "AdmissionResponsibility" {
							calls++
						}
					}
					if literal, ok := node.(*ast.CompositeLit); ok {
						if selector, ok := literal.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "AdmissionResponsibility" {
							t.Error("activation consumer reconstructed an incomplete responsibility")
						}
					}
					return true
				})
			}
			if !found || calls != 1 {
				t.Fatal("activation consumer stopped using the complete canonical projection", found, calls)
			}
		})
	}
}

func TestEffectiveSourceHasNoChannelDeploymentInterpreter(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	raw, err := os.ReadFile(filepath.Join(repoRoot, "internal", "runtime", "effective_source.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, forbidden := range []string{"OutboundBindingPlan", "ChannelActivationPublication", "WithChannelRuntimeToolProjection", "WithRuntimeTools"} {
		if strings.Contains(source, forbidden) {
			t.Errorf("effective source retained mutable channel deployment interpreter %q", forbidden)
		}
	}
}

func TestStructuralReadersDoNotConstructChannelActivation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	for _, relative := range []string{"internal/cliapp/verify_runtime.go", "internal/cliapp/test_command.go"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"NewDeclaredOnlyChannelActivationPublication", "LoadBundlePackRuntime"} {
			if strings.Contains(string(raw), forbidden) {
				t.Errorf("pre-execution source reader %s regained deployment activation via %s", relative, forbidden)
			}
		}
	}
}

func TestChannelActivationExecutionConsumersUseCanonicalOwner(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	checks := map[string][]string{
		"internal/runtime/mcp/gateway.go":              {"AcquireToolDefinitionsForActorInContext", "acquireTurnPresentation"},
		"internal/runtime/mcp/context.go":              {"BindPresentation", "revokePresentation", "Presentation.Close"},
		"internal/runtime/tools/channel_runtime.go":    {"PresentationFromContext", "ValidatePresentation", "BorrowRuntimeOperation"},
		"internal/runtime/pipeline/coordinator.go":     {"AcquireActivityOperation", "BorrowActivityOperation"},
		"internal/runtime/pipeline/activity_engine.go": {"ChannelActivationGeneration", "WithoutExecutionLease"},
		"internal/runtime/tools/executor.go":           {"AcquireToolDefinitionsForActorInContext", "AcquirePresentationForContext"},
		"internal/runtime/context_manager.go":          {"ReplaceChannelActivationsContext", "AcquireChannelActivationPublication"},
		"internal/serveapp/public_ingress.go":          {"AcquireChannelActivationPublication"},
		"internal/serveapp/channel_onboarding.go":      {"NewChannelActivationPublication", "AcquireChannelActivationPublication"},
		"internal/serveapp/main.go":                    {"NewDeclaredOnlyChannelActivationPublication", "prepareServeRuntimeContexts", "releaseRuntimeContexts"},
	}
	for relative, required := range checks {
		raw, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range required {
			if !strings.Contains(string(raw), token) {
				t.Errorf("canonical channel activation consumer %s no longer uses %s", relative, token)
			}
		}
	}
}

func TestUnleasedToolDefinitionReadersDoNotProjectChannelActivations(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	path := filepath.Join(repoRoot, "internal", "runtime", "tools", "executor.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil || (function.Name.Name != "ToolDefinitionsForActor" && function.Name.Name != "ToolDefinitionsForActorInContext") {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && (identifier.Name == "channelActivations" || identifier.Name == "AcquirePresentation") {
				t.Errorf("unleased %s regained channel activation reader %q", function.Name.Name, identifier.Name)
			}
			return true
		})
	}
}
