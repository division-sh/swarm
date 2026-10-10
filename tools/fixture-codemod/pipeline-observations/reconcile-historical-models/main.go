package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const approved = "b57c87cbab1247e9521036146ac4e121661dd4a2"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string    `json:"Successor,omitempty"`
	Removed                               bool      `json:"Removed,omitempty"`
	Mechanical                            *snapshot `json:"Mechanical,omitempty"`
}
type snapshot struct{ SourceCommit, BeforeSHA256, After string }

func main() {
	rows := readFile()
	base := readCommit(approved)
	if len(rows) != 754 || len(base) != len(rows) {
		panic("finite recipe inventory changed")
	}
	for i := range rows {
		current := rows[i]
		current.Mechanical = nil
		if current != base[i] {
			panic("approved recipe pins changed: " + current.Function)
		}
	}
	allowed := sources()
	cache := map[string][]recipe{}
	count := 0
	for i := range rows {
		row := &rows[i]
		commit, ok := allowed[row.File+"/"+row.Function]
		if !ok {
			if row.Mechanical != nil {
				panic("unreviewed mechanical target: " + row.Function)
			}
			continue
		}
		delete(allowed, row.File+"/"+row.Function)
		if cache[commit] == nil {
			cache[commit] = readCommit(commit)
		}
		old := find(cache[commit], row.File, row.Function)
		if old.Before != row.Before || old.After == row.After {
			panic("mechanical provenance changed: " + row.Function)
		}
		row.Mechanical = &snapshot{commit, fmt.Sprintf("%x", sha256.Sum256([]byte(old.Before))), old.After}
		count++
		fmt.Printf("%s\t%s\t%s\n", commit, row.File, row.Function)
	}
	if count != 24 || len(allowed) != 0 {
		panic(fmt.Sprintf("mechanical target inventory=%d/%d, want24/0", count, len(allowed)))
	}
	if len(os.Args) > 1 && os.Args[1] == "-write" {
		b, err := json.MarshalIndent(rows, "", "  ")
		must(err)
		must(os.WriteFile(ledger, append(b, '\n'), 0644))
	}
}

func sources() map[string]string {
	result := map[string]string{}
	add := func(file, commit string, names ...string) {
		for _, name := range names {
			result[file+"/"+name] = commit
		}
	}
	add("internal/apiv1/operator_event_publish_test.go", "b0d3a3a5ba3c6f3de18d7cf6222fddd6354b195b",
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication", "TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence")
	add("internal/runtime/bus/event_identity_dispatch_surface_test.go", "faad5ecdbc9a1838726f20ffca7a552d18bfabf9", "newCompleteEventDispatchFixtureWithOrigin")
	add("internal/runtime/bus/eventbus_publish_test.go", "d696aace365835aa154fe321e9d92f23cd128a94", "TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode")
	add("internal/runtime/bus/scalar_template_instance_store_parity_test.go", "d133e52564cf983075d62f013d09c5616fb30922", "TestScalarTemplateInstanceResolutionPersistsAndReplaysOnSQLiteAndPostgres")
	add("internal/runtime/pipeline/workflow_handler_native_external_test.go", "237b7bf4ba695b80e73e2e4d1c8e831226ec3f5a", "workflowHandlerNativeFixture")
	add("internal/runtime/pipeline/a2_activation_join_seam_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9", "TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores")
	add("internal/runtime/pipeline/delivery_target_declared_key_supported_surface_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9", "TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores")
	add("internal/runtime/pipeline/receiver_future_appearance_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9", "TestReceiverCompositionActivationReuseAndConflictBothStores")
	add("internal/runtime/pipeline/workflow_join_constructor_identity_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9", "TestWorkflowJoinConstructedDescendantAdmissionOnBothStores")
	add("internal/runtime/pipeline/workflow_join_supported_surface_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9",
		"TestWorkflowJoinDurableEventBusDeliveryClaimPreservesExactDeclarationOnBothStores", "TestWorkflowJoinScheduleOccurrencePreservesExactDeclarationThroughDurableEventBusOnBothStores")
	add("internal/runtime/pipeline/workflow_timer_contention_external_test.go", "1292e358bdc06865c1ac02c8934ffd28763b19b9", "verifyWorkflowTimerPublishedOccurrenceRecovery")
	add("internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "6044e9311875ca7c16230f91a919e3d495c6a829",
		"selectedContractExecutionOwnerForCatalogTest", "selectedContractExecutionOwnerForCatalogHarness")
	add("internal/runtime/artifact_action_result_delivery_test.go", "56a8e9a7610685b60b18b4fbb379f804351a69f0", "TestRuleResultEventsFlowThroughDurableCallbackDelivery")
	add("internal/runtime/final_flow_instance_authoring_runtime_test.go", "56a8e9a7610685b60b18b4fbb379f804351a69f0", "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance")
	add("internal/runtime/node_delivery_startup_recovery_test.go", "56a8e9a7610685b60b18b4fbb379f804351a69f0",
		"TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore", "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity", "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries")
	add("internal/runtime/template_flow_pilot_runtime_test.go", "56a8e9a7610685b60b18b4fbb379f804351a69f0", "TestTemplateFlowPilotRuntime_ParentConnectCreatesTemplateInstanceAndPersistedDeliveryRoute")
	add("internal/runtime/template_instance_delivery_test.go", "56a8e9a7610685b60b18b4fbb379f804351a69f0",
		"TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately", "TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution", "TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute")
	return result
}
func find(rows []recipe, file, function string) recipe {
	var result recipe
	count := 0
	for _, row := range rows {
		if row.File == file && row.Function == function {
			result = row
			count++
		}
	}
	if count != 1 {
		panic("historical identity missing or ambiguous: " + file + "/" + function)
	}
	return result
}
func readFile() []recipe { b, e := os.ReadFile(ledger); must(e); return decode(b) }
func readCommit(commit string) []recipe {
	b, e := exec.Command("git", "show", commit+":"+ledger).Output()
	must(e)
	return decode(b)
}
func decode(b []byte) []recipe { var r []recipe; must(json.Unmarshal(b, &r)); return r }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
