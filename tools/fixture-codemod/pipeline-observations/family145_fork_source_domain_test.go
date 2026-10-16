package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var family145ForkSourceDomainCallers = map[string]int{
	"requireTypedReceiverInitializationCases":                                              2,
	"TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores": 4,
	"TestServedForkLoopGenerationStateEffectBothStores":                                    4,
	"TestRunForkHistoricalIdentityPublicExecutionBothStores":                               4,
	"requireForkReceiverPostRevisionPolicyRefusal":                                         2,
	"TestServedForkRootSourceIngressBothStores":                                            2,
	"requireServedForkConsumerAuthority":                                                   1,
	"TestServedForkConnectedRecipientAuthorityOnBothStores":                                4,
	"runForkReceiverOwnershipJourney":                                                      8,
	"TestServedForkSourceCompletionWithoutForkBothStores":                                  3,
	"TestSelectedForkIndependentReceiverBusinessMutationBothStores":                        4,
	"TestServedForkAccumulatorRetainedGenerationBothStores":                                2,
	"proveActivitySemanticResultExecutionParity":                                           2,
}

func family145ForkSourceDomainCallerInverse(function, after string) (string, error) {
	want := family145ForkSourceDomainCallers[function]
	if want == 0 {
		return after, nil
	}
	native := "readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), "
	if function == "requireTypedReceiverInitializationCases" {
		native = "requireReceiverSourceDomainSnapshot(t, rt.ReceiverStateReader, "
	}
	if strings.Count(after, native) != want || strings.Contains(after, "readServedForkRecipientSourceDomain(t, rt, ") {
		return "", fmt.Errorf("source-domain caller lost its original selected owner/cardinality: %s", function)
	}
	return strings.ReplaceAll(after, native, "readServedForkRecipientSourceDomain(t, rt, "), nil
}

func family145ForkSourceDomainReaderProofError(before, after string) error {
	const expected = `func readServedForkRecipientSourceDomain(t *testing.T, owner runtimebus.EventStore, runID string) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkSourceDomain(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}`
	if after != expected || !strings.Contains(before, "rt.DB.Query(query, runID)") || !strings.Contains(before, "rows.Scan(pointers...)") || !strings.Contains(before, "rows.Err()") || !strings.Contains(before, "rows.Close()") {
		return fmt.Errorf("source-domain witness escaped its fixed existing owner or lost read-error refusal")
	}
	return nil
}

func family145ForkSourceDomainCallerProofError(function, before, after string) error {
	after = family145WriterStageCallerInverse(function, after)
	after = family145MailboxCompletionCallerInverse(after)
	normalized, err := family145ForkSourceDomainCallerInverse(function, after)
	if err != nil {
		return err
	}
	if family145ForkProducerCallers[function] != 0 {
		normalized, err = family145ForkProducerCallerInverse(function, normalized)
		if err != nil {
			return err
		}
	}
	if family145ForkReceiverJoinedCallers[function] != 0 {
		normalized, err = family145ForkReceiverJoinedCallerInverse(function, normalized)
		if err != nil {
			return err
		}
	}
	if family145ServedSnapshotCallers[function] != 0 {
		normalized = strings.ReplaceAll(normalized, "snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)", "snapshotForkReceiverApplication(t, rt)")
	}
	if function == "TestServedForkSourceCompletionWithoutForkBothStores" {
		normalized = family145ForkSourceCompletionInverse(normalized)
	}
	if normalized != before {
		return fmt.Errorf("source-domain caller lost an original scope/row/assertion/temporal/cleanup cut: %s", function)
	}
	return nil
}

func family145ForkSourceCompletionInverse(after string) string {
	const schemaRead = "columnsRows, err := rt.DB.Query(`SELECT * FROM event_deliveries WHERE 1=0`)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tcolumns, err := columnsRows.Columns()\n\t\t\tcolumnsRows.Close()\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}"
	normalized := strings.Replace(after, `columns := settled["event_deliveries"].Columns`, schemaRead, 1)
	for _, guard := range []string{
		"\n\t\t\tif !reflect.DeepEqual(early[\"event_deliveries\"].Columns, columns) {\n\t\t\t\tt.Fatal(\"no-fork observation changed delivery columns\")\n\t\t\t}",
		"\n\t\t\t\tif len(beforeRow) != len(columns) || len(afterRow) != len(columns) {\n\t\t\t\t\tt.Fatal(\"no-fork observation changed delivery row cardinality\")\n\t\t\t\t}",
		"\n\t\t\tif !reflect.DeepEqual(early[\"event_receipts\"].Columns, settled[\"event_receipts\"].Columns) {\n\t\t\t\tt.Fatal(\"no-fork observation changed receipt columns\")\n\t\t\t}",
	} {
		normalized = strings.Replace(normalized, guard, "", 1)
	}
	normalized = strings.NewReplacer(
		`early["event_deliveries"].Rows`, `early["event_deliveries"]`,
		`settled["event_deliveries"].Rows`, `settled["event_deliveries"]`,
		`early["event_receipts"].Rows`, `early["event_receipts"]`,
		`settled["event_receipts"].Rows`, `settled["event_receipts"]`,
		`[]storetest.SelectedForkSnapshotCell`, `[]any`,
		`beforeRow[handoff].Type != "<nil>"`, `beforeRow[handoff] != nil`,
		`afterRow[handoff].Type == "<nil>"`, `afterRow[handoff] == nil`,
	).Replace(normalized)
	return normalized
}

func family145ForkSourceDomainQueries(source string) (map[string]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "inventory.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	queries := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		pair, ok := node.(*ast.KeyValueExpr)
		if !ok || err != nil {
			return true
		}
		key, keyOK := pair.Key.(*ast.BasicLit)
		value, valueOK := pair.Value.(*ast.BasicLit)
		if !keyOK || !valueOK || key.Kind != token.STRING || value.Kind != token.STRING {
			return true
		}
		var table, query string
		table, err = strconv.Unquote(key.Value)
		if err == nil {
			query, err = strconv.Unquote(value.Value)
		}
		if err != nil {
			return false
		}
		if _, duplicate := queries[table]; duplicate {
			err = fmt.Errorf("duplicate source-domain table %s", table)
			return false
		}
		queries[table] = query
		return true
	})
	return queries, err
}

func TestFamily145ForkSourceDomainConsumesExactSevenTableOwnerAndAll42Calls(t *testing.T) {
	family145ForkSourceDomainCheckReleaseInspection(t)
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(servedSetupEarlierHistory(t, family145TransitionBytes, false), &rows); err != nil {
		t.Fatal(err)
	}
	seen, helpers := map[string]bool{}, 0
	for _, row := range rows {
		if row.Function == "readServedForkRecipientSourceDomain" {
			helpers++
			family145ForkSourceDomainCheckReader(t, row.Before, row.After)
			for _, pair := range [][2]string{{"run_id=$1", "run_id=$2"}, {"SELECT * FROM events", "SELECT event_id FROM events"}} {
				hostile := strings.Replace(row.Before, pair[0], pair[1], 1)
				finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
				if hostile == row.Before {
					t.Fatal("source-domain hostile predicate/column control lost its source")
				}
				if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
					t.Fatal("source-domain transform accepted an altered predicate/physical projection")
				}
			}
		} else if family145ForkSourceDomainCallers[row.Function] != 0 {
			seen[row.Function] = true
			family145ForkSourceDomainCheckCaller(t, row.Function, row.Before, row.After)
		} else {
			continue
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(row.File), row.Function)
		if actual != row.After {
			t.Fatalf("source-domain escaped its finite source: %s", row.Function)
		}
	}
	if helpers != 1 || len(seen) != len(family145ForkSourceDomainCallers) {
		t.Fatalf("source-domain helper=%d/1 callers=%d/%d", helpers, len(seen), len(family145ForkSourceDomainCallers))
	}
	counts := family145ForkReceiverJoinedOwnedInventory(t)
	for function, want := range family145ForkSourceDomainCallers {
		if counts["source-domain/"+function] != want {
			t.Fatalf("source-domain owned call count %s=%d/%d", function, counts["source-domain/"+function], want)
		}
	}
}

func family145ForkSourceDomainCheckReader(t *testing.T, before, after string) {
	t.Helper()
	if err := family145ForkSourceDomainReaderProofError(before, after); err != nil {
		t.Fatal(err)
	}
	original, err := family145ForkSourceDomainQueries(before)
	if err != nil || len(original) != 7 {
		t.Fatalf("original source-domain query inventory=%d/7: %v", len(original), err)
	}
	private := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_selected_source_domain.go", "ReadSelectedForkSourceDomainForTest")
	delivery := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", "ReadSourceDeliveryStorageTables")
	queries, err := family145ForkSourceDomainQueries(private)
	if err != nil {
		t.Fatal(err)
	}
	deliveryQueries, err := family145ForkSourceDomainQueries(delivery)
	if err != nil {
		t.Fatal(err)
	}
	for table, query := range deliveryQueries {
		if _, duplicate := queries[table]; duplicate {
			t.Fatal("source-domain and delivery owners overlap")
		}
		queries[table] = query
	}
	if !reflect.DeepEqual(original, queries) {
		t.Fatalf("source-domain owner lost exact tables/predicates/physical columns: %v / %v", original, queries)
	}
	for _, cut := range []string{"context.Background()", "owner, runID", "if err != nil", "t.Fatal(err)", "return snapshot"} {
		if family145ForkSourceDomainReaderProofError(before, strings.Replace(after, cut, "foreignSourceDomain()", 1)) == nil {
			t.Fatalf("lost original owner/scope/error/complete-row snapshot accepted: %s", cut)
		}
	}
}

func family145ForkSourceDomainCheckCaller(t *testing.T, function, before, after string) {
	t.Helper()
	if err := family145ForkSourceDomainCallerProofError(function, before, after); err != nil {
		t.Fatal(err)
	}
	owner := "readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), "
	if function == "requireTypedReceiverInitializationCases" {
		owner = "requireReceiverSourceDomainSnapshot(t, rt.ReceiverStateReader, "
	}
	cuts := []string{owner, "t.Fatal"}
	if function == "TestServedForkSourceCompletionWithoutForkBothStores" {
		cuts = append(cuts, "waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, seed.RunID)",
			`columns := settled["event_deliveries"].Columns`, `handoff < 0`,
			`len(early["event_deliveries"].Rows) != len(settled["event_deliveries"].Rows)`,
			`beforeRow[handoff].Type != "<nil>"`, `afterRow[handoff].Type == "<nil>"`,
			`beforeRow[handoff] = afterRow[handoff]`, `!reflect.DeepEqual(beforeRow, afterRow)`,
			`if !found`, `nonPipeline != 0`, `table != "event_receipts" && table != "event_deliveries"`,
			`!reflect.DeepEqual(settled, after)`, `len(beforeRow) != len(columns)`,
			`!reflect.DeepEqual(early["event_receipts"].Columns, settled["event_receipts"].Columns)`)
	}
	for _, cut := range cuts {
		if !strings.Contains(after, cut) || family145ForkSourceDomainCallerProofError(function, before, strings.Replace(after, cut, "foreignSourceDomainCut()", 1)) == nil {
			t.Fatalf("source-domain owner/assertion/temporal mutation survived: %s/%s", function, cut)
		}
	}
}

func family145ForkSourceDomainCheckReleaseInspection(t *testing.T) {
	t.Helper()
	const expected = `func requireReceiverSourceDomainSnapshot(t testing.TB, reader receiverProofStateReader, runID string) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	var snapshot map[string]storetest.SelectedForkStorageTableSnapshot
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		snapshot, err = storetest.ReadSelectedForkSourceDomain(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}`
	actual := family145ConversationForkOwnerSource(t, "../serveapp/receiver_state_readback_test.go", "requireReceiverSourceDomainSnapshot")
	if actual != expected {
		t.Fatal("release source-domain witness escaped the existing read-only inspection scope/original owner")
	}
	for _, cut := range []string{"reader.InspectSnapshot", "ctx, reader, runID", "return err", "if err != nil", "t.Fatal(err)"} {
		if strings.Replace(actual, cut, "foreignInspectionOwner()", 1) == expected {
			t.Fatalf("release inspection owner/scope/error mutation survived: %s", cut)
		}
	}
}
