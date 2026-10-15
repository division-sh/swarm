package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
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

func family145ForkReceiverJoinedCallerProofError(function, before, after string) error {
	after = family145ForkCompletionInverse(function, after)
	if family145ForkProducerCallers[function] != 0 {
		return family145ForkProducerCallerProofError(function, before, after)
	}
	var err error
	after, err = family145ForkSourceDomainCallerInverse(function, after)
	if err != nil {
		return err
	}
	normalized, err := family145ForkReceiverJoinedCallerInverse(function, after)
	if err != nil {
		return err
	}
	if family145ServedSnapshotCallers[function] != 0 {
		normalized = strings.ReplaceAll(normalized, "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", "snapshotForkReceiverApplication(t, rt)")
	}
	if normalized != before {
		return fmt.Errorf("receiver caller changed a run/path/field/identity/assertion/cleanup cut: %s", function)
	}
	return nil
}

func family145ForkReceiverJoinedCheckCaller(t *testing.T, file, function, before, after string) {
	t.Helper()
	if err := family145ForkReceiverJoinedCallerProofError(function, before, after); err != nil {
		t.Fatal(err)
	}
	actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(file), function)
	if actual != after {
		t.Fatalf("receiver caller escaped its source-pinned rewrite: %s", function)
	}
	for _, cut := range []string{"rt.Runtime.Bus.Store()", "readWorkspaceProofReceiverRows(t,", "t.Fatal"} {
		if !strings.Contains(after, cut) || family145ForkReceiverJoinedCallerProofError(function, before, strings.Replace(after, cut, "foreignReceiverOwner()", 1)) == nil {
			t.Fatalf("receiver owner/call/assertion mutation survived: %s/%s", function, cut)
		}
	}
}

func TestFamily145ForkReceiverJoinedInventoryRetiresWholeDuplicateOwnerAndCallerChain(t *testing.T) {
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool
	}
	if err := json.Unmarshal(servedSetupEarlierHistory(t, family145TransitionBytes, false), &rows); err != nil {
		t.Fatal(err)
	}
	seen, retired := map[string]bool{}, 0
	for _, row := range rows {
		if row.Function == "readForkReceiverRows" {
			if !row.Removed || row.After != "" {
				t.Fatal("duplicate receiver query/decoder survived")
			}
			retired++
			family145ForkReceiverJoinedOriginalOracle(t, row.Before)
			continue
		}
		if family145ForkReceiverJoinedCallers[row.Function] == 0 {
			continue
		}
		seen[row.Function] = true
		family145ForkReceiverJoinedCheckCaller(t, row.File, row.Function, row.Before, row.After)
	}
	if retired != 1 || len(seen) != len(family145ForkReceiverJoinedCallers) {
		t.Fatalf("retired receiver owner=%d/1 callers=%d/%d", retired, len(seen), len(family145ForkReceiverJoinedCallers))
	}
	counts := family145ForkReceiverJoinedOwnedInventory(t)
	for function, want := range family145ForkReceiverJoinedCallers {
		if counts[function] != want {
			t.Fatalf("receiver owned caller count %s=%d/%d", function, counts[function], want)
		}
	}
}

func family145ForkReceiverJoinedQuery(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "query.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(value, "SELECT ") {
			queries = append(queries, strings.ReplaceAll(strings.Join(strings.Fields(value), " "), ", ", ","))
		}
		return true
	})
	if len(queries) != 1 {
		t.Fatalf("exact receiver query count=%d/1", len(queries))
	}
	return queries[0]
}

func family145ForkReceiverJoinedOriginalOracle(t *testing.T, before string) {
	t.Helper()
	query := family145ConversationForkOwnerSource(t, "../store/internal/backend/pipelinepersistence/workflow_instance_read.go", "ReadReceiverJoinedInventoryStorageTx")
	if family145ForkReceiverJoinedQuery(t, before) != family145ForkReceiverJoinedQuery(t, query) {
		t.Fatal("existing joined physical owner changed columns/join/run predicate/order")
	}
	decoder := family145ConversationForkOwnerSource(t, "../serveapp/served_workspace_proof_test.go", "workspaceProofReceiverRowsByFlow")
	const expected = `func workspaceProofReceiverRowsByFlow(rows []storetest.ReceiverJoinedInventoryStorageRow) (map[string]forkReceiverRow, error) {
	out := map[string]forkReceiverRow{}
	for _, stored := range rows {
		row := forkReceiverRow{ID: stored.EntityID, Flow: stored.FlowInstance, Type: stored.EntityType, State: stored.CurrentState}
		if err := json.Unmarshal([]byte(stored.Fields), &row.Fields); err != nil {
			return nil, err
		}
		if _, duplicate := out[row.Flow]; duplicate {
			return nil, fmt.Errorf("fixture has duplicate owner rows for %s", row.Flow)
		}
		out[row.Flow] = row
	}
	return out, nil
}`
	if decoder != expected {
		t.Fatal("receiver decoder changed full fields, JSON number/null semantics or duplicate refusal")
	}
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
