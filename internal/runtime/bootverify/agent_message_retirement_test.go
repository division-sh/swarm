package bootverify

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
)

func loadMessageRetirementScope(t *testing.T, scope, agents, policy, tool string) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: retirement-proof\nstages: []\n")
	dir := root
	if scope != "root" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\nstages: []\n")
		if scope == "flow" {
			dir = filepath.Join(root, "child")
		}
	}
	writeOptionalBootverifyFixtureFile(t, filepath.Join(dir, "agents.yaml"), agents)
	writeOptionalBootverifyFixtureFile(t, filepath.Join(dir, "policy.yaml"), policy)
	writeOptionalBootverifyFixtureFile(t, filepath.Join(dir, "tools.yaml"), tool)
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load real %s scope: %v", scope, err)
	}
	if scope != "root" && len(bundle.FlowViews()) == 0 {
		t.Fatal("fixture did not load the child scope")
	}
	return semanticview.Wrap(bundle)
}

func requireMessageRetirement(t *testing.T, source semanticview.Source, token string) {
	t.Helper()
	token = toolidentity.CanonicalName(token)
	errs := tools.ValidateHITLIdentityLifecycleReferences(source)
	if len(errs) == 0 || !strings.Contains(fmt.Sprint(errs), token) || !strings.Contains(fmt.Sprint(errs), "RETIRED: agent_message is unsupported") {
		t.Fatalf("retirement admission errors = %v", errs)
	}
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.HardInvalidities(), "tool_resolution", token) {
		t.Fatalf("full boot did not reject %s: %+v", token, report)
	}
	for _, check := range []string{"agent_permission_validation", "tool_resolution", "platform_tool_usage_hints"} {
		found := false
		for _, finding := range report.Findings {
			if finding.CheckID == check && strings.Contains(finding.Message, token) {
				found = true
				if !strings.Contains(finding.Remediation, "cannot be enabled") {
					t.Fatalf("%s still teaches enablement: %+v", check, finding)
				}
			}
		}
		if !found {
			t.Fatalf("missing %s retirement projection: %+v", check, report.Findings)
		}
	}
}

func TestAgentMessageRetirementSourceAdmissionMatrix(t *testing.T) {
	for _, scope := range []string{"root", "project", "flow"} {
		for _, name := range []string{"agent_message", "mcp__runtime-tools__agent_message"} {
			for _, site := range []string{"configured", "unused_definition", "unselected_bundle"} {
				t.Run(scope+"/"+name+"/"+site, func(t *testing.T) {
					var agents, policy, tool string
					switch site {
					case "configured":
						agents = "worker:\n  model: regular\n  intent: {inline: Coordinate declared events.}\n  tools: [" + name + "]\n"
					case "unused_definition":
						tool = name + ":\n  description: Forbidden unused definition.\n  handler_type: platform_builtin\n"
					case "unselected_bundle":
						policy = "permission_bundles:\n  unused:\n    permissions: [" + name + "]\n"
					}
					requireMessageRetirement(t, loadMessageRetirementScope(t, scope, agents, policy, tool), name)
				})
			}
		}
	}
}

func TestAgentMessageReservedPermissionsRetiredAcrossScopes(t *testing.T) {
	for _, scope := range []string{"root", "project", "flow"} {
		for _, name := range []string{"message_flow", "message_peers"} {
			for _, site := range []string{"direct", "selected_bundle", "combined", "unselected_bundle", "unused_tool_permission", "merged_tool_permission"} {
				t.Run(scope+"/"+name+"/"+site, func(t *testing.T) {
					agents := "worker:\n  model: regular\n  intent: {inline: Coordinate declared events.}\n"
					var policy, tool string
					switch site {
					case "direct":
						agents += "  permissions: [" + name + "]\n"
					case "selected_bundle", "combined", "unselected_bundle":
						policy = "permission_bundles:\n  operators:\n    permissions: [" + name + "]\n"
						if site == "unselected_bundle" {
							agents = ""
						} else {
							agents += "  permissions_bundle: operators\n"
							if site == "combined" {
								agents += "  permissions: [ask_human]\n"
							}
						}
					case "unused_tool_permission", "merged_tool_permission":
						agents = ""
						tool = "custom_lookup:\n  description: Unused workflow extension.\n  handler_type: platform_builtin\n"
						if site == "merged_tool_permission" {
							tool += "  <<: {permission: " + name + "}\n"
						} else {
							tool += "  permission: " + name + "\n"
						}
					}
					source := loadMessageRetirementScope(t, scope, agents, policy, tool)
					requireMessageRetirement(t, source, name)
					for _, declaration := range semanticview.AgentDeclarations(source) {
						if _, err := tools.ResolveAgentPermissions(source, declaration.OwnerFlowID, declaration.Entry); err == nil {
							t.Fatal("permission expansion accepted retired grant")
						}
					}
				})
			}
		}
	}
}

func TestAgentMessageRetirementPreservesWorkflowPermissionExtensions(t *testing.T) {
	for _, scope := range []string{"root", "project", "flow"} {
		t.Run(scope, func(t *testing.T) {
			source := loadMessageRetirementScope(t, scope,
				"worker:\n  model: regular\n  intent: {inline: Coordinate declared events.}\n  permissions_bundle: operators\n  permissions: [custom_access, ask_human]\n",
				"permission_bundles:\n  operators:\n    permissions: [custom_access, ask_human]\n",
				"message_flow:\n  description: Permission namespace is not the tool namespace.\n  handler_type: platform_builtin\n  permission: custom_access\n")
			if _, errs := tools.ValidateAgentPermissions(source); len(errs) != 0 {
				t.Fatalf("valid workflow extension rejected: %v", errs)
			}
			for _, declaration := range semanticview.AgentDeclarations(source) {
				perms, err := tools.ResolveAgentPermissions(source, declaration.OwnerFlowID, declaration.Entry)
				if err != nil || fmt.Sprint(perms) != "[custom_access ask_human]" {
					t.Fatalf("extension/dedup/exact ask grant = %v, %v", perms, err)
				}
			}
		})
	}
}

func TestAgentMessageRetirementShadowedAncestorDeclarationsStillReject(t *testing.T) {
	for _, site := range []string{"bundle", "tool_permission"} {
		t.Run(site, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: retirement-project\nstages: []\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\nstages: []\n")
			if site == "bundle" {
				writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "permission_bundles:\n  operators:\n    permissions: [message_peers]\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "child", "policy.yaml"), "permission_bundles:\n  operators:\n    permissions: [ask_human]\n")
			} else {
				writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "lookup:\n  description: Ancestor retired permission.\n  handler_type: platform_builtin\n  permission: message_peers\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "child", "tools.yaml"), "lookup:\n  description: Child ordinary permission.\n  handler_type: platform_builtin\n  permission: custom_access\n")
			}
			repo := repoRootForBootverifyTest(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			requireMessageRetirement(t, semanticview.Wrap(bundle), "message_peers")
		})
	}
}
