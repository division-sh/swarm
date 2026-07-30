package mcp

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestActorCatalogHasNoParallelDefinitionProducers(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve guardrail source path")
	}
	runtimeDir := filepath.Dir(filepath.Dir(currentFile))
	files := []string{
		filepath.Join(runtimeDir, "mcp", "gateway.go"),
		filepath.Join(runtimeDir, "mcp_hooks.go"),
		filepath.Join(runtimeDir, "agents", "agent_llm.go"),
		filepath.Join(runtimeDir, "tools", "executor.go"),
	}
	forbidden := []string{
		"EmitTools" + "ForActor:",
		"Emit" + "Tools:",
		"EmitSchema" + "ForTool:",
		"runtimeGateway" + "EmitSchemaForTool",
		"composeConversation" + "Tools",
		"emitTool" + "Definitions",
		"merge" + "Tools",
		"NewLLM" + "AgentWithOptions",
		"func NewLLM" + "Agent(",
		"func (e *Executor) Tool" + "Definitions(",
	}

	for _, path := range files {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, symbol := range forbidden {
			if strings.Contains(string(content), symbol) {
				t.Errorf("%s still contains retired catalog producer %q", path, symbol)
			}
		}
	}
}
