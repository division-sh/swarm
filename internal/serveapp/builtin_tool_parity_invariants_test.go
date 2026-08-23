package serveapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestBuiltinToolParityInvariant_SupportedSurfacesShareRuntimeToolTruth_V2(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	cases := []struct {
		name           string
		configuredTool string
		permissions    []string
		wantReject     bool
	}{
		{
			name:           "builtin runtime tool stays accepted across verify and boot",
			configuredTool: "schedule",
			permissions:    []string{"schedule"},
		},
		{
			name:           "truly missing tool rejects across boot surfaces",
			configuredTool: "missing_tool",
			wantReject:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			intent, err := runtimeagentintent.Resolve(
				runtimeagentintent.SourceInline,
				"inline",
				"agents.yaml#agents.agent-1.intent",
				"Exercise the configured runtime tool.",
			)
			if err != nil {
				t.Fatalf("resolve test agent intent: %v", err)
			}
			bundle := testWorkflowValidationBundle()
			bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
				"agent-1": {
					ID:             "agent-1",
					Type:           "worker",
					Role:           "worker",
					Model:          "regular",
					ResolvedIntent: intent,
					Subscriptions:  []string{"platform.run.started"},
					Tools:          []string{tc.configuredTool},
					Permissions:    tc.permissions,
				},
			}
			source := semanticviewtest.WrapRootAgents(bundle)

			directReport := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			assertToolResolutionFinding(t, directReport.Errors(), tc.configuredTool, tc.wantReject)

			result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
			if tc.wantReject {
				if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), tc.configuredTool) {
					t.Fatalf("ValidateWorkflowContractSurface error = %v, want tool rejection", runtimeErr)
				}
			} else if runtimeErr != nil {
				t.Fatalf("ValidateWorkflowContractSurface: %v", runtimeErr)
			}
			assertToolResolutionFinding(t, result.BootReport.Errors(), tc.configuredTool, tc.wantReject)

			verifyErr := cliapp.VerifyBundle(context.Background(), source, executionposture.Live)
			if tc.wantReject {
				if verifyErr == nil || !strings.Contains(verifyErr.Error(), tc.configuredTool) {
					t.Fatalf("cliapp.VerifyBundle error = %v, want tool rejection", verifyErr)
				}
				return
			}
			if verifyErr != nil {
				t.Fatalf("cliapp.VerifyBundle: %v", verifyErr)
			}

			assertBootProgressUsesRuntimeToolInventory(t, source)
		})
	}
}

func assertToolResolutionFinding(t *testing.T, findings []runtimebootverify.Finding, toolID string, want bool) {
	t.Helper()
	found := false
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != "tool_resolution" {
			continue
		}
		if strings.Contains(finding.Message, toolID) {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("tool_resolution finding mismatch for %q: found=%v want=%v findings=%#v", toolID, found, want, findings)
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
