package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

var family145DescriptorReadinessCalls = map[string][]string{
	"TestFlowActivationAttemptAdmissionBothStores":                                          {"seedExactFlowInstanceDescriptorOwner(t, selected,", "storetest.ReadDescriptorReadinessStampStorage(ctx, selected, runID, path)"},
	"TestFlowActivationAttemptBatchRetirementBothStores":                                    {"seedExactFlowInstanceDescriptorOwner(t, selected,"},
	"TestFlowActivationSourceSetRebindBothStores":                                           {"seedExactFlowInstanceDescriptorOwner(t, selected,", "flowReadinessStampSnapshot(t, ctx, selected, runID, path)"},
	"flowReadinessStampSnapshot":                                                            {"storetest.ReadDescriptorReadinessStampStorage(ctx, selected, runID, path)"},
	"TestActiveFlowInstanceDescriptorAuthorityPreservesRoutesOnInvalidProvenanceBothStores": {"seedFlowInstanceDescriptorAuthorityCase("},
	"TestActiveFlowInstanceDescriptorAuthorityScopesCensusToExactRunBothStores":             {"seedExactFlowInstanceDescriptorOwner(t, selected,"},
	"TestDynamicFlowRuntimeReadinessForSourceScopesInSQLBothStores":                         {"seedStaticFlowInstanceRouteWithoutReadiness(t, store,", "store.ReplaceFlowInstanceRouteRecords(ctx, standing, nil)", "storetest.RemoveDescriptorReadiness(ctx, store, runB, \"account/b\")"},
	"TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores":                           {"seedExactFlowInstanceDescriptorOwner(t, selected,", "storetest.SetDescriptorReadinessConcurrentPlan(ctx, selected, changed)"},
	"TestDynamicFlowRuntimeReadinessPlanBatchIsAtomicBothStores":                            {"seedExactFlowInstanceDescriptorOwner(t, selected,", "last := requests[1].Expected", "storetest.InstallDescriptorReadinessBatchUpdateFailure(ctx, selected, last.RunID, last.Identity.InstancePath)", "cleanup(context.Background())"},
	"TestDynamicFlowRuntimeReadinessProjectionClassifiesSourceTransitionsBothStores":        {"seedExactFlowInstanceDescriptorOwner(t, selected,", "RunID: row.runID, Source: current", "setReadinessPlanRaw(t, selected,"},
	"TestDynamicFlowRuntimeReadinessRejectsABAObservationBothStores":                        {"seedExactFlowInstanceDescriptorOwner(t, selected,"},
	"seedExactFlowInstanceDescriptorOwner":                                                  {"commitDescriptorReadinessComponent(t, ctx, selected,"},
	"seedFlowInstanceDescriptorAuthorityCase":                                               {"commitDescriptorReadinessComponent(t, currentCtx, selected,", "storetest.RemoveDescriptorReadiness(ctx, selected, runID, instancePath)", "selected.ReviseRunSource(ctx,"},
	"seedStaticFlowInstanceRouteWithoutReadiness":                                           {"flowactivationfixture.Command(ctx,", "selected.CommitFlowInstanceActivation(ctx, command)", "!committed.Acknowledged || !committed.Created", "storetest.RemoveDescriptorReadiness(ctx, selected, runID, instancePath)", "selected.ReplaceFlowInstanceRouteRecords(ctx, owner,"},
	"setReadinessCoordinate":                                                                {"storetest.RetirePlannedReadiness(t, ctx, selected, row.Plan, row.AttemptOrdinal, value)", "storetest.SetDescriptorReadinessCreationClock(ctx, selected,", "storetest.SetWorkflowProjectionActiveTerminatedTimestamp(ctx, selected,"},
	"setReadinessInstanceStatus":                                                            {"storetest.SetWorkflowProjectionDraining(context.Background(), selected,"},
	"setReadinessPlanRaw":                                                                   {"storetest.SetDescriptorReadinessMalformedPlan(context.Background(), selected,"},
	"TestFlowRouteTopologyAcknowledgedFaultCompletesProcessFollowupBothStores":              {"seedFlowInstanceDescriptorAuthorityCase(t, ctx, selected,"},
	"TestActiveWorkflowRouteRecoveryUsesExactRunOwnerOnBothStores":                          {"constructWorkflowRecoveryComponent(t, ctx, selected,", "storetest.AddAmbiguousWorkflowProjectionFields(ctx, selected, currentRunID, currentEntityID)"},
	"TestEntityStateSchemaRequiresExplicitNonblankEntityContractOnBothStores":               {"storetest.InsertDescriptorEntityStateWithoutType(ctx, selected, runID)", "storetest.InsertDescriptorEntityStateWithBlankType(ctx, selected, runID)"},
}

func family145DescriptorReadinessProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	// Native construction adds acknowledgment and exact-owner setup checks.
	// Every original behavioral condition and refusal assertion must survive,
	// in order; only delegated SQL/serialization error gates are transport.
	index := 0
	for _, assertion := range got {
		if index < len(want) && want[index] == assertion {
			index++
		}
	}
	if index != len(want) {
		return fmt.Errorf("descriptor/readiness lost behavioral assertion %d: %s", index, want[index])
	}
	for _, call := range family145DescriptorReadinessCalls[function] {
		if !strings.Contains(after, call) {
			return fmt.Errorf("descriptor/readiness lost exact owner/cut: %s", call)
		}
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func family145DescriptorBusinessAssertions(source string) ([]string, error) {
	// The exact caller propagation removes only the DB/dialect carrier. The
	// physical five-column snapshot and its complete-row comparison remain.
	source = strings.ReplaceAll(source, "flowReadinessStampSnapshot(t, ctx, db, sqlite, runID, path)", "flowReadinessStampSnapshot(t, ctx, selected, runID, path)")
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var result []string
	ast.Inspect(file, func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		condition := family145NodeString(conditional.Cond)
		// Exact simple error gates are delegated to the canonical selected owner.
		// Expected refusals (err == nil), cardinalities, clocks and identities stay.
		if condition == "err != nil" || condition == "marshalErr != nil" {
			return false
		}
		for _, stmt := range conditional.Body.List {
			expression, ok := stmt.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := expression.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			owner, ok := selector.X.(*ast.Ident)
			if ok && owner.Name == "t" && slices.Contains([]string{"Fatal", "Fatalf", "Error", "Errorf", "Fail", "FailNow"}, selector.Sel.Name) {
				result = append(result, condition+" => "+family145NodeString(call))
			}
		}
		return true
	})
	return result, nil
}

func family145DescriptorReadinessEscape(data []byte) error {
	if err := family145SupplementalEscape(data); err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), "descriptor.go", data, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		member, ok := node.(*ast.SelectorExpr)
		if ok && slices.Contains([]string{"Exec", "ExecContext", "Begin", "BeginTx", "Database"}, member.Sel.Name) {
			err = fmt.Errorf("raw member %s", member.Sel.Name)
		}
		return true
	})
	return err
}
