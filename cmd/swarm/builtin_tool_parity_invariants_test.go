package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestBuiltinToolParityInvariant_SupportedSurfacesShareRuntimeToolTruth_V2(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	cases := []struct {
		name               string
		configuredTool     string
		permissions        []string
		wantToolResolution bool
	}{
		{
			name:               "builtin runtime tool stays accepted across verify and boot",
			configuredTool:     "schedule",
			permissions:        []string{"schedule"},
			wantToolResolution: false,
		},
		{
			name:               "truly missing tool still warns across boot surfaces",
			configuredTool:     "missing_tool",
			wantToolResolution: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := testWorkflowValidationBundle()
			bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
				"agent-1": {
					ID:          "agent-1",
					Tools:       []string{tc.configuredTool},
					Permissions: tc.permissions,
				},
			}
			source := semanticview.Wrap(bundle)

			directReport := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			assertToolResolutionWarning(t, directReport.Warnings(), tc.configuredTool, tc.wantToolResolution)

			result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil))
			if runtimeErr != nil {
				t.Fatalf("ValidateWorkflowContractSurface: %v", runtimeErr)
			}
			assertToolResolutionWarning(t, result.BootReport.Warnings(), tc.configuredTool, tc.wantToolResolution)

			if err := verifyBundle(context.Background(), source); err != nil {
				t.Fatalf("verifyBundle: %v", err)
			}

			assertBootProgressUsesRuntimeToolInventory(t, source)
		})
	}
}

func assertToolResolutionWarning(t *testing.T, warnings []runtimebootverify.Finding, toolID string, want bool) {
	t.Helper()
	found := false
	for _, warning := range warnings {
		if strings.TrimSpace(warning.CheckID) != "tool_resolution" {
			continue
		}
		if strings.Contains(warning.Message, toolID) {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("tool_resolution warning mismatch for %q: found=%v want=%v warnings=%#v", toolID, found, want, warnings)
	}
}

func assertBootProgressUsesRuntimeToolInventory(t *testing.T, source semanticview.Source) {
	t.Helper()

	wantTools := len(runtimetools.RuntimeAvailableToolNamesForSource(source))
	if wantTools == 0 {
		t.Fatal("runtime tool inventory unexpectedly empty")
	}

	out := serveBootBundleLoadDetail("sha256:test", source)
	if !strings.Contains(out, fmt.Sprintf("tools=%d", wantTools)) {
		t.Fatalf("boot progress detail missing runtime tool count %d:\n%s", wantTools, out)
	}
	if strings.Contains(out, "tools=0") {
		t.Fatalf("boot progress detail still reports zero tools:\n%s", out)
	}
}
