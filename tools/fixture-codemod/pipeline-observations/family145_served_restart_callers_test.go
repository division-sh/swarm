package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

var family145ServedRestartCallerInventory = []string{
	"a2_join_public_projection_agreement_test.go/TestA2JoinPublicProjectionAgreementBothStores\tlifecycleRestartHarness(t, backend, root)",
	"a2_map_recipe_surface_test.go/TestA2MapRecipeSupportedSurfaceBothStores\tlifecycleRestartHarness(t, backend, root)",
	"a2_portfolio_arrival_admission_test.go/TestA2PortfolioPublicIndependentArrivalAdmissionBothStores\tlifecycleRestartHarness(t, backend, root)",
	"a2_portfolio_supported_surface_test.go/TestA2PortfolioSupportedFiniteJoinSurfaceBothStores\tlifecycleRestartHarness(t, backend, root)",
	"a2_portfolio_supported_surface_test.go/TestA2PortfolioSupportedStreamSurfaceBothStores\tlifecycleRestartHarness(t, backend, root)",
	"connection_edge_policy_test.go/TestEdgeOwnedConnectionPoliciesSharingInputBothStores\tlifecycleRestartHarness(t, backend, root)",
	"connection_edge_policy_test.go/TestEdgeOwnedMixedConnectionProjectionsBothStores\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_one_second_preservation_test.go/TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_original_reporter_http_test.go/TestIssue2394ServedReporterTransactionCensusBothStores\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_original_reporter_http_test.go/runIssue2394OriginalReporterHTTP\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_served_incident_test.go/TestIssue2394ServedFanOutFailureIncidentBothStores\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_supported_surface_test.go/TestIssue2394HeldReporterConsumerKeepsRunUnreadyBothStores\tlifecycleRestartHarness(t, backend, root)",
	"fan_out_supported_surface_test.go/TestIssue2394ServedFanOutSupportedSurfacesBothStores\tlifecycleRestartHarness(t, backend, root)",
	"lifecycle_transition_gate_restart_test.go/proveServedCompiledGateOutcomeRestart\tlifecycleRestartHarness(t, backend, root)",
	"lifecycle_transition_node_recovery_test.go/TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmits))",
	"lifecycle_transition_restart_test.go/TestServedCompiledGateFrozenTransitionEvidenceOnBothStores\tlifecycleRestartHarness(t, backend, sourceA)",
	"lifecycle_transition_restart_test.go/TestServedCompiledTransitionRestartOnBothStores\tlifecycleRestartHarness(t, backend, root)",
	"lifecycle_transition_template_test.go/startLifecycleTemplateRuntime\tlifecycleRestartHarness(t, store, root, 3*time.Minute)",
	"publication_activity_test.go/TestServedGeneratedActivityPublicationBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationActivity(t, mode, provider.URL, approval))",
	"publication_sites_test.go/TestServedPublicationDirectRestartBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationDirectTextSite(t, mode))",
	"publication_state_result_test.go/TestServedStateResultPublicationBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationStateResult(t, mode))",
	"receiver_agent_collision_test.go/TestServedReceiverAgentBusinessNamespaceBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyReceiverAgentCollision(t))",
	"receiver_initialization_ingress_test.go/TestReceiverInitializationPublicProviderSchemaIngressBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyProviderReceiverInitialization(t))",
	"receiver_initialization_test.go/TestServedTypedReceiverInitializationBothStores\tlifecycleRestartHarness(t, backend, canonicalrouting.CopyServedReceiverInitialization(t))",
	"source_admission_fork_proof_test.go/Test2376ForkSourceAndPinsFollowSourceRunBothStores\tlifecycleRestartHarness(t, backend, rootC)",
	"source_admission_fork_proof_test.go/Test2376RootManifestRejectsBeforePublicationBothStores\tlifecycleRestartHarness(t, backend, root)",
	"source_admission_fork_proof_test.go/Test2376VerifyServeArtifactParityBothStores\tlifecycleRestartHarness(t, backend, root)",
}

func family145ServedRestartCallers(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("../../../internal/serveapp/*_test.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("owned serving source enumeration: %v", err)
	}
	var actual []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if ok && name.Name == "lifecycleRestartHarness" {
					actual = append(actual, filepath.Base(path)+"/"+fn.Name.Name+"\t"+family145NodeString(call))
				}
				return true
			})
		}
	}
	sort.Strings(actual)
	return actual
}

func family145ServedRestartCallerInventoryError(actual []string) error {
	if len(actual) != 27 || !slices.Equal(actual, family145ServedRestartCallerInventory) {
		return fmt.Errorf("changed serving construction caller/source/readiness inventory: %d/27\n%s", len(actual), strings.Join(actual, "\n"))
	}
	return nil
}
