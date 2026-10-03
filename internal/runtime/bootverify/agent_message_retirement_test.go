package bootverify

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
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
	if len(errs) == 0 || !strings.Contains(fmt.Sprint(errs), token) || !strings.Contains(fmt.Sprint(errs), "is unsupported") {
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

func TestPermissionBundleMalformedLoadedScopes(t *testing.T) {
	for _, scope := range []string{"root", "project", "flow"} {
		for _, tc := range []struct{ name, policy string }{
			{"null_root", "permission_bundles: null\n"},
			{"scalar_root", "permission_bundles: message_flow\n"},
			{"list_root", "permission_bundles: [message_flow]\n"},
			{"null_bundle", "permission_bundles: {operators: null}\n"},
			{"scalar_bundle", "permission_bundles: {operators: message_flow}\n"},
			{"list_bundle", "permission_bundles: {operators: [message_flow]}\n"},
			{"missing_permissions", "permission_bundles: {operators: {}}\n"},
			{"null_permissions", "permission_bundles: {operators: {permissions: null}}\n"},
			{"scalar_permissions", "permission_bundles: {operators: {permissions: message_flow}}\n"},
			{"mixed_integer", "permission_bundles: {operators: {permissions: [message_flow, 7]}}\n"},
			{"mixed_null", "permission_bundles: {operators: {permissions: [message_peers, null]}}\n"},
		} {
			for _, site := range []string{"zero_agent", "unselected", "selected", "combined"} {
				t.Run(scope+"/"+tc.name+"/"+site, func(t *testing.T) {
					agents := ""
					if site != "zero_agent" {
						agents = "worker:\n  model: regular\n  intent: {inline: Coordinate declared events.}\n"
					}
					if site == "selected" || site == "combined" {
						agents += "  permissions_bundle: operators\n"
					}
					if site == "combined" {
						agents += "  permissions: [ask_human]\n"
					}
					source := loadMessageRetirementScope(t, scope, agents, tc.policy, "")
					if errs := tools.ValidateHITLIdentityLifecycleReferences(source); len(errs) == 0 || !strings.Contains(fmt.Sprint(errs), "permission_bundles") {
						t.Fatalf("malformed source declaration omitted: %v", errs)
					}
					if report := Run(context.Background(), source, Options{}); !reportContains(report.HardInvalidities(), "agent_permission_validation", "permission_bundles") {
						t.Fatalf("shape error was not a hard failure: %+v", report)
					}
				})
			}
		}
		for _, name := range []string{"message_flow", "message_peers", "agent_message", "mailbox_send", "human_task_request", "agent_hire", "ask_human"} {
			for _, shape := range []string{"[%s, 7]", "[%s, null]", "%s"} {
				t.Run(scope+"/unused/"+name+"/"+shape, func(t *testing.T) {
					policy := "permission_bundles:\n  operators:\n    permissions: " + fmt.Sprintf(shape, name) + "\n"
					source := loadMessageRetirementScope(t, scope, "", policy, "")
					if errs := tools.ValidateHITLIdentityLifecycleReferences(source); len(errs) == 0 {
						t.Fatal("malformed unused sibling declaration accepted")
					}
				})
			}
		}
	}
}

func TestPermissionWarningsConsumeCanonicalScopedResolver(t *testing.T) {
	for _, scope := range []string{"root", "project", "flow"} {
		for _, site := range []string{"direct", "selected", "combined", "malformed_unused", "valid", "missing_required"} {
			t.Run(scope+"/"+site, func(t *testing.T) {
				agents := "worker:\n  model: regular\n  intent: {inline: Coordinate declared events.}\n  tools: [lookup]\n"
				policy := "permission_bundles:\n  operators:\n    permissions: [message_flow]\n"
				switch site {
				case "direct":
					agents += "  permissions: [message_peers]\n"
				case "selected", "combined":
					agents += "  permissions_bundle: operators\n"
					if site == "combined" {
						agents += "  permissions: [message_peers]\n"
					}
				case "malformed_unused":
					policy = "permission_bundles: {operators: {permissions: [ask_human, null]}}\n"
					agents += "  permissions: [ask_human]\n"
				case "valid", "missing_required":
					policy = "permission_bundles: {operators: {permissions: [ask_human]}}\n"
					agents += "  permissions_bundle: operators\n"
					if site == "valid" {
						agents += "  permissions: [custom_access, ask_human, custom_access]\n"
					}
				}
				source := loadMessageRetirementScope(t, scope, agents, policy,
					"lookup:\n  description: Scoped permission control.\n  handler_type: platform_builtin\n  permission: custom_access\n")
				declarations := semanticview.AgentDeclarations(source)
				if len(declarations) != 1 {
					t.Fatalf("declarations = %d", len(declarations))
				}
				declaration := declarations[0]
				plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
				if err != nil {
					t.Fatal(err)
				}
				_, canonicalErr := tools.ResolveAgentPermissions(source, plan.OwnerFlowID, declaration.Entry)
				warnings := mergedAgentPermissionWarnings(source)
				if site == "valid" {
					if canonicalErr != nil || len(warnings) != 0 {
						t.Fatalf("valid scoped grants disagree: %v, %+v", canonicalErr, warnings)
					}
				} else if site == "missing_required" {
					if canonicalErr != nil || len(warnings) != 1 || !strings.Contains(warnings[0].Message, `missing permission "custom_access"`) {
						t.Fatalf("derived missing-permission warning lost: %v, %+v", canonicalErr, warnings)
					}
				} else if canonicalErr == nil || len(warnings) != 1 || !strings.Contains(warnings[0].Message, canonicalErr.Error()) {
					t.Fatalf("diagnostic bypasses canonical error: %v, %+v", canonicalErr, warnings)
				}
			})
		}
	}
}

func TestPermissionBundleMalformedShadowedAncestors(t *testing.T) {
	for _, malformedOwner := range []string{"root", "child"} {
		t.Run(malformedOwner, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: project\nstages: []\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\nstages: []\n")
			valid := "permission_bundles: {operators: {permissions: [ask_human]}}\n"
			malformed := "permission_bundles: {operators: {permissions: [message_flow, 7]}}\n"
			rootPolicy, childPolicy := valid, malformed
			if malformedOwner == "root" {
				rootPolicy, childPolicy = malformed, valid
			}
			writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), rootPolicy)
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "policy.yaml"), childPolicy)
			repo := repoRootForBootverifyTest(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if errs := tools.ValidateHITLIdentityLifecycleReferences(semanticview.Wrap(bundle)); len(errs) == 0 || !strings.Contains(fmt.Sprint(errs), "permission_bundles.operators.permissions") {
				t.Fatalf("malformed %s disappeared behind a scoped override: %v", malformedOwner, errs)
			}
		})
	}
}

func TestPermissionWarningResolverOwnership(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	canonicalCall := false
	err := checkoutsource.WalkDir(repo, filepath.Join(repo, "internal", "runtime", "bootverify"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if literal, ok := node.(*ast.BasicLit); ok && (literal.Value == `"permission_bundles"` || literal.Value == "`permission_bundles`") {
				t.Errorf("%s reparses permission bundles outside the canonical owner", path)
			}
			function, ok := node.(*ast.FuncDecl)
			if !ok || function.Name.Name != "agentPermissionWarnings" {
				return true
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "ResolveAgentPermissions" {
						if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == "runtimetools" {
							canonicalCall = true
						}
					}
				}
				return true
			})
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !canonicalCall {
		t.Fatal("permission warnings do not consume tools.ResolveAgentPermissions")
	}
}
