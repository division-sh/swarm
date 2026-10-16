package main

import (
	_ "embed"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func family145WriterStageCallerInverse(function, after string) string {
	after = family145ServedDebugCallerInverse(function, after)
	after = family145ForkCompletionInverse(function, after)
	after = family145LifecycleReadersInverse(after)
	after = strings.NewReplacer(
		"requireServedEventPublishEntityState(t, rt.ReceiverStateReader, ", "requireServedEventPublishEntityState(t, rt.DB, ",
		"requireServedEventPublishEntityState(t, recovered.ReceiverStateReader, ", "requireServedEventPublishEntityState(t, recovered.DB, ",
		"requireServedEventPublishEntityState(t, selectedA.ReceiverStateReader, ", "requireServedEventPublishEntityState(t, selectedA.DB, ",
	).Replace(after)
	if function == "TestServedMailboxCompletionProcessBoundariesBothStores" {
		after = strings.ReplaceAll(after, "requireServedEventPublishEntityState(t, rt.DB, ", "requireServedEventPublishEntityState(t, db, ")
	}
	db := "db"
	if function == "TestServePostgresLossAndRestartFromDurableState" || function == "TestServePostgresLostCommitResponseRecoversDurablePublication" {
		db = "observer"
	}
	after = strings.ReplaceAll(after, "requireServedEventPublishEntityState(t, reader, ", "requireServedEventPublishEntityState(t, "+db+", ")
	after = family145WriterStageHelperRoleInverse(after)
	after = family145WriterStageCaptureInverse(function, after)
	file, err := parser.ParseFile(token.NewFileSet(), "inverse.go", "package proof\n"+after, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return after
	}
	return family145NodeString(file.Decls[0])
}

func family145WriterStageHelperRoleInverse(after string) string {
	for _, helper := range []string{"runServedDynamicAutoEmitProof", "runServedEventPublishFollowUpProof", "runServedEventPublishTargetRouteProof", "runServedEventPublishActiveLoadProof"} {
		for _, db := range []string{"db", "servedDB"} {
			after = strings.ReplaceAll(after, helper+"(t, endpoint, "+db+", reader, ", helper+"(t, endpoint, "+db+", ")
		}
	}
	return strings.NewReplacer(
		"db *sql.DB, reader receiverProofStateReader, backend,", "db *sql.DB, backend,",
		"db *sql.DB, reader receiverProofStateReader, backend string", "db *sql.DB, backend string",
		"\tdb *sql.DB,\n\treader receiverProofStateReader,", "\tdb *sql.DB,",
	).Replace(after)
}

func family145WriterStageCaptureInverse(function, after string) string {
	const rootCapture = "\n\tvar reader receiverProofStateReader\n\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { reader = requireReceiverProofStateReader(t, p.deps.EventStore) })"
	const indentedCapture = "\n\t\t\tvar reader receiverProofStateReader\n\t\t\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\t\treader = requireReceiverProofStateReader(t, p.deps.EventStore)\n\t\t\t})"
	after = strings.Replace(after, rootCapture, "", 1)
	after = strings.Replace(after, indentedCapture, "", 1)
	const unindentedCapture = "\n\tvar reader receiverProofStateReader\n\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\treader = requireReceiverProofStateReader(t, p.deps.EventStore)\n\t})"
	after = strings.Replace(after, unindentedCapture, "", 1)
	if function == "TestReceiverCompositionMixedAgentBothStores" {
		after = strings.Replace(after, "\n\t\t\tvar reader receiverProofStateReader", "", 1)
		after = strings.Replace(after, "\n\t\t\t\treader = requireReceiverProofStateReader(t, p.deps.EventStore)", "", 1)
	}
	if function == "TestAudit2277StoppedRunReadinessRestart" {
		after = strings.Replace(after, "\n\t\t\tvar reader receiverProofStateReader", "", 1)
		after = strings.Replace(after, "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\t\tdb, _, _ = selectedRuntimeStoreForTest(t, p)\n\t\t\t\treader = requireReceiverProofStateReader(t, p.deps.EventStore)\n\t\t\t})", "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })", 1)
	}
	if function == "TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores" {
		after = family145WriterStageProcessInspectorInverse(after)
	}
	return after
}

func family145WriterStageProcessInspectorInverse(after string) string {
	return strings.NewReplacer(
		"var cfg, storeMode, backendName, location string", "var cfg, storeMode, backendName string",
		"\n\t\t\t\tlocation = path", "",
		"\n\t\t\t\tlocation = dsn", "",
		"\n\t\t\tinspection, err := storetest.OpenReleaseProcessReadOnlyInspection(backendName, location)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tt.Cleanup(func() { _ = inspection.Close() })\n\t\t\treader := requireReceiverProofStateReader(t, inspection)", "",
		"\n\t\t\tinspection, err := storetest.OpenReleaseProcessReadOnlyInspection(backendName, location)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tt.Cleanup(func() {\n\t\t\t\t_ = inspection.Close()\n\t\t\t})\n\t\t\treader := requireReceiverProofStateReader(t, inspection)", "",
	).Replace(after)
}

func family145WriterStageRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	if family145NodeString(call.Fun) != "requireServedEventPublishEntityState" {
		return
	}
	if len(call.Args) != 6 {
		t.Fatal("writer-stage observation changed helper arity")
	}
	owner := family145NodeString(call.Args[1])
	if owner != "rt.ReceiverStateReader" && owner != "recovered.ReceiverStateReader" && owner != "selectedA.ReceiverStateReader" && owner != "reader" {
		t.Fatal("writer-stage caller retained raw or foreign authority")
	}
	counts["writer-stage/"+function]++
}

func TestFamily145WriterStageRetainsExactBothDialectHeaderQueries(t *testing.T) {
	var rows []struct {
		Function, Before string
		Removed          bool
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original string
	for _, row := range rows {
		if row.Function == "servedEventPublishEntityState" && row.Removed {
			original = row.Before
		}
	}
	if original == "" {
		t.Fatal("raw stage reader has not been retired in the finite codemod")
	}
	physical := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/test_issue2564_evidence.go", "observeWriterStage")
	want, got := family145WriterStageNormalizedQueries(t, original), family145WriterStageNormalizedQueries(t, physical)
	if len(want) != 2 || len(got) != 2 || !reflect.DeepEqual(want, got) {
		t.Fatal("writer-stage owner changed exact entity/earliest state/run/order/LIMIT query semantics")
	}
	for _, pair := range [][2]string{{"ORDER BY created_at,entity_id", "ORDER BY created_at DESC,entity_id DESC"}, {"LIMIT 1", "LIMIT 2"}, {"WHERE run_id=$1", "WHERE run_id=$2"}, {"current_state=$2", "current_state='active'"}} {
		hostile := strings.Replace(physical, pair[0], pair[1], 1)
		if hostile == physical || reflect.DeepEqual(want, family145WriterStageNormalizedQueries(t, hostile)) {
			t.Fatalf("writer-stage query rule accepted foreign physical policy: %s", pair[0])
		}
	}
}

func family145WriterStageNormalizedQueries(t *testing.T, source string) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, query := range family145A2ClaimLiteralQueries(t, source) {
		query = strings.NewReplacer("::uuid", "", "::text", "").Replace(query)
		query = strings.Replace(query, "?", "$1", 1)
		query = strings.Replace(query, "?", "$2", 1)
		seen[strings.Join(strings.Fields(query), "")] = true
	}
	var queries []string
	for query := range seen {
		queries = append(queries, query)
	}
	slices.Sort(queries)
	return queries
}
