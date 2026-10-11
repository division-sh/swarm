package main

import (
	"encoding/json"
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeHandlerCoordinatorRequiresExplicitFactWithoutChangingTimerWorkloads(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-handler-source-coordinator" {
			continue
		}
		count++
		before, after := projectionShapeFunction(t, row.Before), projectionShapeFunction(t, row.After)
		if row.Function == "newTimerReplayCoordinator" {
			validate := projectionShapeStatement(t, `if err:=fact.Validate();err!=nil{t.Fatal(err)}`)
			if formattedNativeReadNode(after.Body.List[1]) != formattedNativeReadNode(validate) {
				t.Fatal("coordinator did not fail closed before binding")
			}
			after.Body.List = append(after.Body.List[:1], after.Body.List[2:]...)
			ast.Inspect(before.Body, func(node ast.Node) bool {
				assignment, ok := node.(*ast.AssignStmt)
				if ok && formattedNativeReadNode(assignment) == "options.SourceArtifactFact = authorActivityTestSourceArtifactFact" {
					assignment.Rhs[0] = ast.NewIdent("fact")
				}
				return true
			})
		} else {
			ast.Inspect(before.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && formattedNativeReadNode(call.Fun) == "newTimerReplayCoordinator" {
					call.Args = append(call.Args, ast.NewIdent("authorActivityTestSourceArtifactFact"))
				}
				return true
			})
		}
		if formattedNativeReadNode(before.Body) != formattedNativeReadNode(after.Body) {
			t.Fatalf("timer workload/owner ports changed: %s", row.Function)
		}
	}
	if count != 3 {
		t.Fatal("coordinator and both existing callers must be covered")
	}
}

func TestNativeHandlerFixtureConsumesRealPublicationAndExactArtifactOwners(t *testing.T) {
	paths := map[string][]string{
		"internal/runtime/pipeline/workflow_handler_native_external_test.go": {
			"sourceartifactfixture.FactFor(bundle.SourceArtifact)", "Artifact: bundle.SourceArtifact",
			"BundleHash: fact.BundleHash()", "testAuthorActivityContextForSource(t, context.Background(), fact)",
			"workflowHandlerNativeCoordinator(t, selected, options, fact)",
			"flowactivationfixture.Command(ctx, instance", "CommitFlowInstanceActivation(ctx, command)",
			"storetest.CommitSemanticEventWithRoutes", "TransactionWorkflowMutation", "TransactionDeliveryClaim",
		},
		"internal/runtime/pipeline/delivery_native_owner_external_test.go": {
			"SourceArtifactFact: fact", "newTimerReplayCoordinator(t, bus, selected, options, fact)",
			"options.WorkOwner = pipelineExternalTestWorkOwnerForSource(t, fact)",
		},
		"internal/runtime/pipeline/workflow_handler_native_fixture_test.go": {
			"pc.deliveryStore.Snapshot(ctx, id)", "pc.deliveryStore.ClaimDelivery(ctx, snapshot.Authority, event, route)",
			"deliverylifecycle.StartClaimHeartbeatFromClaim", "result.Renewal", "heartbeat.Stop",
		},
	}
	for path, required := range paths {
		source, err := os.ReadFile(filepath.Join("..", "..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		for _, fragment := range required {
			if !strings.Contains(strings.Join(strings.Fields(string(source)), " "), strings.Join(strings.Fields(fragment), " ")) {
				t.Fatalf("native owner/fact obligation missing: %s %s", path, fragment)
			}
		}
		for _, old := range []string{"pipelineTestPublicationPlan", "newDurablePipelineCoordinatorForTest", "seedExactOnceEvent", "withClaimedWorkflowNodePublicationForTest", "configurePipelineTestDeliveryOwner"} {
			if strings.Contains(string(source), old) {
				t.Fatalf("native family retained obsolete authority: %s", old)
			}
		}
	}
}
