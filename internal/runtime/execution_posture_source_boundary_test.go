package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestProcessExecutionPostureOwnsProductionLiveAuthorityLiterals(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	allowed := map[string]bool{
		"internal/runtime/agents/agent_llm.go":                                    true,
		"internal/runtime/effects/authority.go":                                   true,
		"internal/runtime/executionposture/posture.go":                            true,
		"internal/runtime/llm/selection/profile.go":                               true,
		"internal/store/internal/backend/agentpersistence/projection.go":          true,
		"internal/store/internal/backend/pipelinepersistence/standing_service.go": true,
	}
	// Exact session owners: three read-only mode comparisons, the process-posture
	// admission call, and the original logout owner's fenced effect/context pair.
	namedSessionSites := map[string]int{
		"internal/runtime/pipeline/channel_native_activity.go::withNativeChannelActivityLaunch":          1,
		"internal/runtime/pipeline/channel_session_activity.go::prepareSessionActivityIntent":            1,
		"internal/runtime/pipeline/channel_session_activity.go::validateSessionActivityRequestSelection": 1,
		"internal/serveapp/channel_logout.go::PrepareSessionLogout":                                      1,
		"internal/sessionprovider/runtime_logout_unix.go::admitSessionLogout":                            2,
	}
	seenSessionSites := map[string]int{}
	skippedPrefixes := []string{
		"internal/events/eventtest/",
		"internal/runtime/effects/effecttest/",
		"internal/store/eventfixture/",
	}
	var violations []string
	for _, root := range []string{"internal", "cmd"} {
		err := checkoutsource.WalkDir(repoRoot, filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			for _, prefix := range skippedPrefixes {
				if strings.HasPrefix(rel, prefix) {
					return nil
				}
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			aliases := map[string]string{}
			for _, imported := range parsed.Imports {
				importPath, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if importPath != "github.com/division-sh/swarm/internal/runtime/executionmode" &&
					importPath != "github.com/division-sh/swarm/internal/runtime/effects" {
					continue
				}
				name := filepath.Base(importPath)
				if imported.Name != nil {
					name = imported.Name.Name
				}
				aliases[name] = importPath
			}
			comparisons := map[*ast.SelectorExpr]bool{}
			sessionSites := map[*ast.SelectorExpr]bool{}
			for _, declaration := range parsed.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || namedSessionSites[rel+"::"+fn.Name.Name] == 0 {
					continue
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					qualifier, ok := selector.X.(*ast.Ident)
					if ok && (aliases[qualifier.Name] == "github.com/division-sh/swarm/internal/runtime/executionmode" && selector.Sel.Name == "Live" ||
						aliases[qualifier.Name] == "github.com/division-sh/swarm/internal/runtime/effects" && selector.Sel.Name == "ExecutionModeLive") {
						sessionSites[selector] = true
						seenSessionSites[rel+"::"+fn.Name.Name]++
					}
					return true
				})
			}
			if rel == "internal/runtime/runfork/preparation.go" {
				// The persisted preparation decoder tests whether a probe is required.
				// Only equality operands in this predicate are exempt, never constructors.
				for _, declaration := range parsed.Decls {
					fn, ok := declaration.(*ast.FuncDecl)
					if !ok || fn.Name.Name != "RequiresProbe" {
						continue
					}
					ast.Inspect(fn.Body, func(node ast.Node) bool {
						comparison, ok := node.(*ast.BinaryExpr)
						if ok && comparison.Op == token.EQL {
							if selector, ok := comparison.Y.(*ast.SelectorExpr); ok {
								comparisons[selector] = true
							}
						}
						return true
					})
				}
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				qualifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				importPath := aliases[qualifier.Name]
				isLive := importPath == "github.com/division-sh/swarm/internal/runtime/executionmode" && selector.Sel.Name == "Live"
				isEffectsLive := importPath == "github.com/division-sh/swarm/internal/runtime/effects" && selector.Sel.Name == "ExecutionModeLive"
				if (isLive || isEffectsLive) && !allowed[rel] && !comparisons[selector] && !sessionSites[selector] {
					violations = append(violations, rel+":"+selector.Sel.Name)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan production source under %s: %v", root, err)
		}
	}
	for owner, want := range namedSessionSites {
		if got := seenSessionSites[owner]; got != want {
			t.Errorf("session live-authority owner %s has %d sites, want exactly %d", owner, got, want)
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("production source minted live authority outside the process-posture owner/comparison allowlist:\n%s", strings.Join(violations, "\n"))
	}
}

func TestValidatedRuntimeDepsDoesNotProjectProfileExecutionMode(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "runtime.go", nil, 0)
	if err != nil {
		t.Fatalf("parse runtime.go: %v", err)
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		typeSpec, ok := node.(*ast.TypeSpec)
		if !ok || typeSpec.Name.Name != "validatedRuntimeDeps" {
			return true
		}
		structure, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			t.Fatal("validatedRuntimeDeps is not a struct")
		}
		for _, field := range structure.Fields.List {
			for _, name := range field.Names {
				if name.Name == "ExecutionMode" {
					t.Fatal("validatedRuntimeDeps retained profile-derived ExecutionMode authority")
				}
			}
		}
		return false
	})
}
