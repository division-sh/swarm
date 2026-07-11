package effects

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type primitiveOwner string

const (
	ownerManagedAgent        primitiveOwner = "managed_agent_attempt"
	ownerRuntimeDependency   primitiveOwner = "runtime_dependency"
	ownerPipelineActivity    primitiveOwner = "pipeline_activity"
	ownerNotification        primitiveOwner = "notification"
	ownerOperatorInfra       primitiveOwner = "operator_infrastructure"
	ownerCredentialLifecycle primitiveOwner = "credential_lifecycle"
	ownerDiagnostic          primitiveOwner = "diagnostic"
	ownerComputeSandbox      primitiveOwner = "compute_sandbox"
	ownerBuildTest           primitiveOwner = "build_test_infrastructure"
)

// sourcePrimitiveOwners is an exact source-derived ledger. Keys include the
// enclosing function and per-function ordinal so adding, moving, or removing a
// launch/write primitive makes this test fail until ownership is reclassified.
var sourcePrimitiveOwners = map[string]primitiveOwner{
	"internal/runtime/contracts/bundle_build.go:BuildBundleMaterialization:filesystem_write:1":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleInputs:filesystem_write:1":                               ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleSourceInputs:filesystem_write:1":                         ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:writeDeterministicJSONFile:filesystem_write:1":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:materializeBundleCatalogRuntimeFiles:filesystem_write:1": ownerRuntimeDependency,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:1":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:2":                                            ownerCredentialLifecycle,
	"internal/runtime/engine/helpers.go:executionConditionEnv:http_do:1":                                                  ownerPipelineActivity,
	"internal/runtime/llm/api_runtime.go:sendRequest:http_do:1":                                                           ownerManagedAgent,
	"internal/runtime/llm/cli_runtime_process.go:buildCommand:process_launch:1":                                           ownerManagedAgent,
	"internal/runtime/llm/cli_tool_result_relay.go:runWorkspaceCommand:process_launch:1":                                  ownerManagedAgent,
	"internal/runtime/llm/openai_compatible_runtime.go:sendRequest:http_do:1":                                             ownerManagedAgent,
	"internal/runtime/llm/openai_responses_runtime.go:sendRequest:http_do:1":                                              ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:exchange:http_do:1":                                                     ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:exchangeGitHubAppInstallation:http_do:1":                                ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:1":                                         ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:2":                                         ownerCredentialLifecycle,
	"internal/runtime/mcp/client.go:callHTTPServerWithCredentialKeyResolver:http_do:1":                                    ownerManagedAgent,
	"internal/runtime/mcp/client.go:newStdioRPCClient:process_launch:1":                                                   ownerRuntimeDependency,
	"internal/runtime/mcp/client.go:Call:stdio_write:1":                                                                   ownerManagedAgent,
	"internal/runtime/pipeline/activity_engine.go:executePreparedActivityHTTPTool:http_do:1":                              ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:runArtifactGit:process_launch:1":                                          ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoWritableDirectory:filesystem_write:1":                 ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:writeArtifactRepoFiles:filesystem_write:1":                                ownerPipelineActivity,
	"internal/runtime/pipeline/generic_test_module.go:init:http_do:1":                                                     ownerBuildTest,
	"internal/runtime/pythonmodule/runtime.go:materializedArtifactDir:http_do:1":                                          ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:runHarness:filesystem_write:1":                                              ownerComputeSandbox,
	"internal/runtime/runtime.go:startSystemNodesAndWaitForSubscriptions:http_do:1":                                       ownerRuntimeDependency,
	"internal/runtime/runtime_claude_startup.go:startupCallMCP:http_do:1":                                                 ownerRuntimeDependency,
	"internal/runtime/sessions/heartbeat.go:StartLeaseHeartbeatWithErrorHandler:http_do:1":                                ownerRuntimeDependency,
	"internal/runtime/testfixtures/fanoutpinroute/fixture.go:writeFile:filesystem_write:1":                                ownerBuildTest,
	"internal/runtime/testfixtures/finalflowinstanceauthoring/fixture.go:writeFile:filesystem_write:1":                    ownerBuildTest,
	"internal/runtime/testfixtures/sealedpackage/fixture.go:writeFile:filesystem_write:1":                                 ownerBuildTest,
	"internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:writeFile:filesystem_write:1":                     ownerBuildTest,
	"internal/runtime/testfixtures/templatefanin/fixture.go:writeFile:filesystem_write:1":                                 ownerBuildTest,
	"internal/runtime/testfixtures/templateflowpilot/fixture.go:writeFile:filesystem_write:1":                             ownerBuildTest,
	"internal/runtime/testfixtures/templatereply/fixture.go:writeFile:filesystem_write:1":                                 ownerBuildTest,
	"internal/runtime/testfixtures/templateselectexisting/fixture.go:writeFile:filesystem_write:1":                        ownerBuildTest,
	"internal/runtime/testfixtures/templateselectorcreate/fixture.go:writeFile:filesystem_write:1":                        ownerBuildTest,
	"internal/runtime/tools/executor_http.go:execHTTPRequestOnce:http_do:1":                                               ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:doNormalizedSearch:http_do:1":                                              ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:1":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:2":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1":                                      ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:2":                                      ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:1":                             ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:2":                             ownerManagedAgent,
	"internal/runtime/workflowexpr/data_expression.go:dataExpressionEnvForContext:http_do:1":                              ownerPipelineActivity,
	"internal/runtime/workflowexpr/data_expression.go:dataExpressionEnvForContext:http_do:2":                              ownerPipelineActivity,
	"internal/runtime/workspace/manager.go:RunDocker:process_launch:1":                                                    ownerRuntimeDependency,
}

func TestDirectPrimitiveOwnershipManifestIsTotal(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	actual, err := collectDirectPrimitives(root)
	if err != nil {
		t.Fatalf("collect direct primitives: %v", err)
	}
	missing := make([]string, 0)
	stale := make([]string, 0)
	for key := range actual {
		if _, ok := sourcePrimitiveOwners[key]; !ok {
			missing = append(missing, key)
		}
	}
	for key := range sourcePrimitiveOwners {
		if _, ok := actual[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) != 0 || len(stale) != 0 {
		t.Fatalf("direct primitive ownership manifest drift\nmissing:\n%s\nstale:\n%s", strings.Join(missing, "\n"), strings.Join(stale, "\n"))
	}
}

func TestManagedEffectRegistrationsAreCompleteAndLive(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	seen := map[string]struct{}{}
	for _, registration := range Registrations() {
		if registration.Kind == "" || registration.Class == "" || strings.TrimSpace(registration.Adapter) == "" ||
			strings.TrimSpace(registration.Transport) == "" || strings.TrimSpace(registration.LaunchSite) == "" ||
			strings.TrimSpace(registration.LaunchObserved) == "" || strings.TrimSpace(registration.OutcomeMapping) == "" ||
			strings.TrimSpace(registration.CanonicalEvidence) == "" || strings.TrimSpace(registration.SettlementRecovery) == "" ||
			strings.TrimSpace(registration.Proof) == "" {
			t.Fatalf("incomplete effect registration: %#v", registration)
		}
		if _, ok := seen[registration.Adapter]; ok {
			t.Fatalf("duplicate effect adapter registration %q", registration.Adapter)
		}
		seen[registration.Adapter] = struct{}{}
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(registration.LaunchSite)))
		if err != nil {
			t.Fatalf("read launch site for %s: %v", registration.Adapter, err)
		}
		if !strings.Contains(string(raw), fmt.Sprintf("\"%s\"", registration.Adapter)) {
			t.Fatalf("launch site %s does not consume adapter %s", registration.LaunchSite, registration.Adapter)
		}
	}
}

func collectDirectPrimitives(root string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	base := filepath.Join(root, "internal", "runtime")
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			counts := map[string]int{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				primitive := directPrimitive(call)
				if primitive == "" {
					return true
				}
				counts[primitive]++
				key := fmt.Sprintf("%s:%s:%s:%d", filepath.ToSlash(rel), fn.Name.Name, primitive, counts[primitive])
				out[key] = struct{}{}
				return true
			})
		}
		return nil
	})
	return out, err
}

func directPrimitive(call *ast.CallExpr) string {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	root := selectorRoot(selector.X)
	switch {
	case selector.Sel.Name == "Do":
		return "http_do"
	case root == "exec" && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext"):
		return "process_launch"
	case root == "os" && (selector.Sel.Name == "WriteFile" || selector.Sel.Name == "Create" || selector.Sel.Name == "CreateTemp" || selector.Sel.Name == "Rename"):
		return "filesystem_write"
	case selector.Sel.Name == "Write" && strings.Contains(strings.ToLower(selectorPath(selector.X)), "stdin"):
		return "stdio_write"
	default:
		return ""
	}
}

func selectorPath(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		prefix := selectorPath(value.X)
		if prefix == "" {
			return value.Sel.Name
		}
		return prefix + "." + value.Sel.Name
	case *ast.ParenExpr:
		return selectorPath(value.X)
	default:
		return ""
	}
}

func selectorRoot(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return selectorRoot(value.X)
	case *ast.ParenExpr:
		return selectorRoot(value.X)
	default:
		return ""
	}
}
