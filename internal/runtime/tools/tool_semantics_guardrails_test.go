package tools

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestHITLSourceBoundaryRetiresOldInterpreters(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file")
	}
	runtimeRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), ".."))
	allowedAgentMessageFiles := map[string]struct{}{
		"effective_tool_verification.go": {},
		"hitl_tools.go":                  {},
		"registry.go":                    {},
		"tool_capability_policy.go":      {},
	}

	err := filepath.WalkDir(runtimeRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.BasicLit:
				if typed.Kind != token.STRING {
					return true
				}
				literal, unquoteErr := strconv.Unquote(typed.Value)
				if unquoteErr != nil {
					t.Errorf("unquote %s: %v", path, unquoteErr)
					return true
				}
				if literal == "mailbox_send" || literal == "human_task_request" {
					t.Errorf("%s restores retired HITL tool literal %q", path, literal)
				}
				if literal == "agent_message" {
					if _, allowed := allowedAgentMessageFiles[filepath.Base(path)]; !allowed {
						t.Errorf("%s restores agent_message outside the fail-closed owner", path)
					}
				}
			case *ast.FuncDecl:
				if strings.EqualFold(typed.Name.Name, "execAgentMessage") {
					t.Errorf("%s restores executable agent_message interpreter %s", path, typed.Name.Name)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan runtime sources: %v", err)
	}
}

func TestNormalizeNativeToolNameCanonicalAliases(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":                                   "",
		"bash":                               "bash",
		"Bash":                               "bash",
		"web_search":                         "web_search",
		"WebFetch":                           "web_search",
		"WebSearch":                          "web_search",
		"Read":                               "read_file",
		"read_file":                          "read_file",
		"Write":                              "write_file",
		"Edit":                               "write_file",
		"mcp__runtime-tools__read_file":      "read_file",
		"mcp__runtime-tools__write_file":     "write_file",
		"mcp__runtime-tools__emit_scan_done": "emit_scan_done",
	}

	for raw, want := range tests {
		if got := normalizeNativeToolName(raw); got != want {
			t.Fatalf("normalizeNativeToolName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestRuntimeAndValidatorNormalizationStayAligned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		tool  string
		input any
	}{
		{
			name:  "read_file_alias_path",
			tool:  "read_file",
			input: map[string]any{"file_path": "/tmp/a.txt"},
		},
		{
			name:  "write_file_alias_path",
			tool:  "write_file",
			input: map[string]any{"file_path": "/tmp/a.txt", "content": "x"},
		},
		{
			name: "ask_human_explicit_deadline",
			tool: "ask_human",
			input: map[string]any{
				"entity_id":   "entity-1",
				"deadline_at": "2026-01-02T03:04:05Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runtimeNormalized := normalizeRuntimeToolInput(tt.tool, tt.input)
			validatorNormalized := validatorNormalizeRuntimeToolInput(tt.tool, tt.input)
			if !reflect.DeepEqual(runtimeNormalized, validatorNormalized) {
				t.Fatalf("normalization mismatch for %s\nruntime:   %#v\nvalidator: %#v", tt.tool, runtimeNormalized, validatorNormalized)
			}
		})
	}
}

func TestToolDefinitionsForActor_IncludesEnabledNativeTools(t *testing.T) {
	t.Parallel()

	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		ModelRuntimes: staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analysis-agent",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	foundRead := false
	foundWrite := false
	for _, name := range names {
		if name == "read_file" {
			foundRead = true
		}
		if name == "write_file" {
			foundWrite = true
		}
	}
	if !foundRead || !foundWrite {
		t.Fatalf("expected native file tools in actor definitions, got %v", names)
	}
}
