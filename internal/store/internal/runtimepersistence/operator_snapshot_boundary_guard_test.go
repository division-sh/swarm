package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

var operatorSnapshotFixtureScopes = map[string]string{
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_constraints.go": `captureOperatorSnapshotConstraintsTx restoreOperatorSnapshotConstraintsTx`,
	"internal/store/storetest/operator_snapshot_cut.go":                                      `OperatorSnapshotCut InitializeOperatorSnapshotReadBarrier OperatorSnapshotTerminalization OperatorSnapshotActivation OperatorSnapshotSessionReplacement OperatorSnapshotDeliverySettlement OperatorSnapshotDeliveryFactsSettlement OperatorSnapshotTurnReplacement OperatorSnapshotDeliveryCorruption OperatorSnapshotEventCorruption PrepareOperatorSnapshotReads ReadOperatorSnapshotAcrossCut ApplyOperatorAgentTerminalized CorruptOperatorAgentSubscriptions CorruptOperatorAgentConfig CorruptOperatorSessionState CorruptOperatorTurnBlocks CorruptOperatorDeliveryRoute CorruptOperatorSnapshotEvent`,
	"internal/store/storetest/operator_snapshot_corruption.go":                               `RequireMalformedOperatorAgentRoute RequireUnknownOperatorAgentStatus`,
	"internal/store/internal/runtimepersistence/test_operator_snapshot_cut.go":               `OperatorSnapshotCut OperatorSnapshotTerminalization OperatorSnapshotActivation OperatorSnapshotSessionReplacement OperatorSnapshotDeliverySettlement OperatorSnapshotDeliveryFactsSettlement OperatorSnapshotTurnReplacement OperatorSnapshotDeliveryCorruption OperatorSnapshotEventCorruption validate apply hold restore ApplyOperatorAgentTerminalizedForTest CorruptOperatorAgentSubscriptionsForTest CorruptOperatorAgentConfigForTest CorruptOperatorSessionStateForTest CorruptOperatorTurnBlocksForTest CorruptOperatorDeliveryRouteForTest requireOperatorSnapshotReadArguments`,
	"internal/store/internal/runtimepersistence/test_operator_snapshot_corruption.go":        `operatorAgentSnapshotFault captureOperatorAgentSnapshotFault install restore cleanup RequireMalformedOperatorAgentRouteForTest RequireUnknownOperatorAgentStatusForTest requireOperatorAgentSnapshotFault`,
	"internal/store/internal/runtimepersistence/test_operator_snapshot_read_barrier.go":      `InitializeOperatorSnapshotReadBarrierForTest PrepareOperatorSnapshotReadsForTest ReadOperatorSnapshotAcrossCutForTest readOperatorSnapshotSQLiteCut readOperatorSnapshotPostgresCut`,
	"internal/store/internal/runtimepersistence/test_event_support.go":                       `corruptOperatorSnapshotEventTx CorruptOperatorSnapshotEventForTest`,
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_corruption.go":  `OperatorSnapshotCorruption CaptureOperatorSnapshotRouteCorruptionTx CaptureOperatorSnapshotStatusCorruptionTx captureOperatorSnapshotCorruptionTx readRow InstallTx RestoreTx replaceValue VerifyEnforcementTx requireOperatorSnapshotSingleFieldDelta`,
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_mutations.go":   `SetOperatorSnapshotAgentActiveTx CorruptOperatorSnapshotSubscriptionsTx CorruptOperatorSnapshotConfigTx`,
	"internal/store/internal/backend/llmpersistence/test_operator_snapshot_barrier.go":       `HoldOperatorSessionSnapshotTx RestoreOperatorSessionSnapshotTx HoldOperatorTurnSnapshotTx RestoreOperatorTurnSnapshotTx`,
	"internal/store/internal/backend/llmpersistence/test_operator_snapshot_mutations.go":     `ReplaceOperatorSnapshotSessionTx ReplaceOperatorSnapshotTurnSummaryTx CorruptOperatorSnapshotTurnBlocksTx replaceOperatorSnapshotTurnBlocksTx CorruptOperatorSnapshotSessionStateTx`,
	"internal/store/internal/backend/delivery/lifecycle.go":                                  `SettleOperatorSnapshotDeliveryTx CorruptOperatorSnapshotDeliveryRouteTx HoldOperatorDeliverySnapshotTx RestoreOperatorDeliverySnapshotTx`,
	"internal/store/internal/backend/eventrecord/postgres/adapter.go":                        `CorruptOperatorSnapshotEventPayloadTx`,
	"internal/store/internal/backend/eventrecord/sqlite/adapter.go":                          `CorruptOperatorSnapshotEventPayloadTx`,
	"internal/store/internal/backend/sqlite/transaction.go":                                  `runTransactionOnceOutcome runTransactionWithConnectionScope prepareTransactionConnectionScope cleanupTransactionConnectionScope transactionConnectionScope`,
	"internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go":               `runAgentSnapshotCorruption RunAgentSnapshotRouteCorruptionForTest RunAgentSnapshotStatusCorruptionForTest`,
}

func isOperatorSnapshotFixtureReference(name string) bool {
	for _, marker := range []string{"OperatorSnapshot", "OperatorAgentSnapshot", "operatorAgentSnapshot", "OperatorSessionSnapshot", "OperatorTurnSnapshot", "OperatorDeliverySnapshot"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	for _, prefix := range []string{"RequireMalformedOperatorAgentRoute", "RequireUnknownOperatorAgentStatus", "CorruptOperatorAgent", "ApplyOperatorAgentTerminalized", "CorruptOperatorSessionState", "CorruptOperatorTurnBlocks", "CorruptOperatorDeliveryRoute", "RunAgentSnapshot"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return name == "transactionConnectionScope" || name == "runTransactionWithConnectionScope" || name == "prepareTransactionConnectionScope" || name == "cleanupTransactionConnectionScope"
}

func operatorSnapshotConsumerViolations(path string, source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	allowed := " " + operatorSnapshotFixtureScopes[path] + " "
	var violations []string
	for _, declaration := range file.Decls {
		scope := "package initializer"
		var definition *ast.Ident
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			scope, definition = node.Name.Name, node.Name
		case *ast.GenDecl:
			if node.Tok == token.TYPE && len(node.Specs) == 1 {
				scope = node.Specs[0].(*ast.TypeSpec).Name.Name
			}
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			id, ok := node.(*ast.Ident)
			if !ok || id == definition || !isOperatorSnapshotFixtureReference(id.Name) {
				return true
			}
			if !strings.Contains(allowed, " "+scope+" ") {
				violations = append(violations, fmt.Sprintf("%s/%s consumes fixture-only %s", path, scope, id.Name))
			}
			return true
		})
	}
	return violations, nil
}

func TestOperatorSnapshotFixtureAuthorityHasOnlyClosedConsumers(t *testing.T) {
	root := eventBoundaryRepositoryRoot(t)
	for _, dir := range []string{"internal", "cmd"} {
		err := checkoutsource.WalkDir(root, filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			violations, err := operatorSnapshotConsumerViolations(filepath.ToSlash(relative), raw)
			if err != nil {
				return err
			}
			for _, violation := range violations {
				t.Error(violation)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOperatorSnapshotFixtureConsumerGuardRejectsProductionAndInitializerEscapes(t *testing.T) {
	for _, body := range []string{
		`func serve(){ storetest.OperatorSnapshotTerminalization(identity) }`,
		`func serve(){ backend.RunAgentSnapshotRouteCorruptionForTest(ctx,callback) }`,
		`func serve(){ backend.runTransactionWithConnectionScope(ctx,nil,0,scope,callback) }`,
		`var escape=storetest.OperatorSnapshotSessionReplacement`,
		`var escape=func(){ storetest.RequireMalformedOperatorAgentRoute(t,ctx,store,identity) }()`,
		`func serve(cut storetest.OperatorSnapshotCut){ use(cut) }`,
	} {
		for _, path := range []string{"internal/serveapp/probe.go", "internal/store/internal/backend/delivery/lifecycle.go", "internal/store/internal/runtimepersistence/test_event_support.go"} {
			violations, err := operatorSnapshotConsumerViolations(path, []byte("package probe\n"+body))
			if err != nil || len(violations) == 0 {
				t.Fatalf("production/initializer fixture escape passed: %s: %s: %v", path, body, err)
			}
		}
	}
}
