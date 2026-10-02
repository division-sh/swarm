package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestVerifyAgentMessageRetirementSupportedCLI(t *testing.T) {
	config := writeTestVerifyRuntimeConfig(t)
	for _, scope := range []string{"root", "project", "flow"} {
		for _, mutation := range []string{"positive", "raw_tool", "mcp_tool", "direct_permission", "selected_bundle", "unused_bundle", "unused_tool_permission"} {
			t.Run(scope+"/"+mutation, func(t *testing.T) {
				root := canonicalrouting.CopyExample(t, canonicalrouting.PolicyRules)
				dir := root
				original, err := os.ReadFile(filepath.Join(dir, "agents.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				agent := string(original)
				if scope != "root" {
					root = templateflowpilot.Write(t, templateflowpilot.Options{})
					dir = root
					agent = "worker:\n  model: regular\n  intent: {inline: Review declared work.}\n  subscriptions: [account.requested]\n"
					if scope == "flow" {
						dir = filepath.Join(root, "account")
						agent = "worker:\n  model: regular\n  intent: {inline: Review declared work.}\n  subscriptions: [account.ready]\n"
					}
				}
				switch mutation {
				case "raw_tool":
					agent += "  tools: [agent_message]\n"
				case "mcp_tool":
					agent += "  tools: [mcp__runtime-tools__agent_message]\n"
				case "direct_permission":
					agent += "  permissions: [message_flow, message_peers]\n"
				case "selected_bundle", "unused_bundle":
					if mutation == "selected_bundle" {
						agent += "  permissions_bundle: operators\n"
					}
					policyPath := filepath.Join(dir, "policy.yaml")
					policy, err := os.ReadFile(policyPath)
					if err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
					writeDescribeTestFile(t, policyPath, string(policy)+"\npermission_bundles:\n  operators:\n    permissions: [message_flow, message_peers]\n")
				case "unused_tool_permission":
					writeDescribeTestFile(t, filepath.Join(dir, "tools.yaml"), "unused:\n  description: Forbidden permission extension.\n  handler_type: platform_builtin\n  permission: message_flow\n")
				}
				writeDescribeTestFile(t, filepath.Join(dir, "agents.yaml"), agent)
				var stdout, stderr bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", config, "--json"}, &stdout, &stderr, defaultRootCommandOptions())
				var result verifyCommandResult
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
					t.Fatalf("verify JSON: %v code=%d stdout=%s stderr=%s", err, code, &stdout, &stderr)
				}
				if mutation == "positive" {
					if code != 0 || !result.OK || !result.ProductionValid {
						t.Fatalf("positive declared-event fixture failed: code=%d errors=%+v stderr=%s", code, result.Errors, &stderr)
					}
					return
				}
				if code == 0 || result.OK {
					t.Fatalf("retirement bypass: code=%d errors=%+v", code, result.Errors)
				}
				findings := append(result.Errors, result.Warnings...)
				for _, check := range []string{"agent_permission_validation", "tool_resolution", "platform_tool_usage_hints"} {
					found := false
					for _, finding := range findings {
						if finding.CheckID == check && strings.Contains(finding.Message, "RETIRED: agent_message is unsupported") {
							found = true
							if !strings.Contains(finding.Remediation, "cannot be enabled") || !strings.Contains(finding.Message, "declared typed workflow events") {
								t.Fatalf("misleading %s diagnostic: %+v", check, finding)
							}
						}
					}
					if !found {
						t.Fatalf("missing %s diagnostic: %+v", check, findings)
					}
				}
			})
		}
	}
}
