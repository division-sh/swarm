package main

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

var family145ForkReceiverJoinedCallers = map[string]int{
	"requireForkReceiverDelivery":                                                          1,
	"runForkReceiverOwnershipJourney":                                                      4,
	"TestSelectedForkIndependentReceiverBusinessMutationBothStores":                        3,
	"requireForkReceiverExecution":                                                         1,
	"requireSupplementalForkAcquisitionSettlement":                                         1,
	"TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores": 3,
	"TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores":                          1,
	"TestRunForkHistoricalIdentityPublicExecutionBothStores":                               1,
}

func family145ForkReceiverJoinedCallerInverse(function, after string) (string, error) {
	const native = "readWorkspaceProofReceiverRows(t, rt.Runtime.Bus.Store(), "
	want := family145ForkReceiverJoinedCallers[function]
	if want == 0 || strings.Count(after, native) != want || strings.Contains(after, "readForkReceiverRows(") {
		return "", fmt.Errorf("receiver inventory lost its original live bus owner/call count: %s", function)
	}
	return strings.ReplaceAll(after, native, "readForkReceiverRows(t, rt, "), nil
}

func family145ForkReceiverJoinedOwnedInventory(t *testing.T) map[string]int {
	t.Helper()
	setupPins := servedSetupPins(t)
	root := filepath.Join("..", "..", "..")
	counts := map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if function.Name.Name == "readForkReceiverRows" || function.Name.Name == "servedEventPublishEntityState" {
				t.Fatal("retired duplicate receiver owner returned")
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			family145ForkReceiverJoinedFunctionCalls(t, servedSetupComparisonDeclaration(t, setupPins, filepath.ToSlash(rel), function), counts)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

func family145ForkReceiverJoinedFunctionCalls(t *testing.T, function *ast.FuncDecl, counts map[string]int) {
	t.Helper()
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := family145NodeString(call.Fun)
		family145LifecycleBusinessRecordCall(t, function.Name.Name, call, counts)
		family145MailboxCompletionRecordCall(t, function.Name.Name, call, counts)
		family145WriterStageRecordCall(t, function.Name.Name, call, counts)
		family145LifecycleReadersRecordCall(t, function.Name.Name, call, counts)
		family145ForkCompletionRecordCall(t, function.Name.Name, call, counts)
		if name == "readServedForkRecipientSourceDomain" {
			if family145ForkSourceDomainCallers[function.Name.Name] == 0 || len(call.Args) != 3 || family145NodeString(call.Args[1]) != "rt.Runtime.Bus.Store()" {
				t.Fatal("source-domain call escaped its finite live selected bus owner")
			}
			counts["source-domain/"+function.Name.Name]++
		}
		if name == "requireReceiverSourceDomainSnapshot" {
			if function.Name.Name != "requireTypedReceiverInitializationCases" || len(call.Args) != 3 || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
				t.Fatal("release source-domain caller escaped its exact existing inspection owner")
			}
			counts["source-domain/"+function.Name.Name]++
		}
		if name == "readForkReceiverRows" {
			t.Fatal("retired raw receiver caller returned")
		}
		if name == "readForkReceiverProducerEvidence" {
			if family145ForkProducerCallers[function.Name.Name] == 0 || len(call.Args) != 4 || family145NodeString(call.Args[1]) != "rt.Runtime.Bus.Store()" {
				t.Fatal("producer call escaped its finite live selected bus owner")
			}
			counts["producer/"+function.Name.Name]++
		}
		if family145ForkReceiverJoinedCallers[function.Name.Name] != 0 && name == "readWorkspaceProofReceiverRows" {
			if len(call.Args) != 3 || family145NodeString(call.Args[1]) != "rt.Runtime.Bus.Store()" {
				t.Fatal("receiver call escaped its actual selected bus owner")
			}
			counts[function.Name.Name]++
		}
		return true
	})
}
