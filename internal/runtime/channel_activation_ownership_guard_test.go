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
)

func TestChannelActivationExecutableReaderCensus(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	want := []string{
		"internal/channelonboarding/model.go",
		"internal/channelonboarding/publication.go",
		"internal/cliapp/verify_runtime.go",
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
		"internal/serveapp/builder_project_supervisor.go",
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
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
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

func TestChannelActivationExecutionConsumersUseCanonicalOwner(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	checks := map[string][]string{
		"internal/runtime/mcp/gateway.go":              {"AcquireToolDefinitionsForActorInContext"},
		"internal/runtime/tools/channel_runtime.go":    {"channelActivationPresentationFromContext", "RuntimeOperation"},
		"internal/runtime/pipeline/coordinator.go":     {"AcquireActivityOperation"},
		"internal/runtime/pipeline/activity_engine.go": {"ChannelActivationGeneration"},
		"internal/runtime/tools/executor.go":           {"AcquireToolDefinitionsForActorInContext", "AcquirePresentation"},
		"internal/runtime/context_manager.go":          {"ReplaceChannelActivationsContext", "AcquireChannelActivationPublication"},
		"internal/serveapp/public_ingress.go":          {"AcquireChannelActivationPublication"},
		"internal/serveapp/channel_onboarding.go":      {"NewChannelActivationPublication", "AcquireChannelActivationPublication"},
		"internal/serveapp/main.go":                    {"NewDeclaredOnlyChannelActivationPublication"},
		"internal/cliapp/verify_runtime.go":            {"NewDeclaredOnlyChannelActivationPublication"},
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
