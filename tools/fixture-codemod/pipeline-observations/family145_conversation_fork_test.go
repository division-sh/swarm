package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var family145ConversationForkCuts = map[string][]string{
	"newConversationForkCommitFixture": {
		"forkCommitNativeReopen", "storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx)",
		"storetest.StartPostgresRuntimeStoreWithReopen(t)", "selected, reopen.sqlite = s, open", "selected, reopen.postgres = s, open", "storetest.RequireRun(t, ctx, selected, runFixture)",
		"storetest.UpsertStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{", "storetest.SeedConversationForkSource(t, ctx, selected,", "storetest.PersistManagedAgentTurnFixture(t, ctx,",
	},
	"requireForkCommitCounts": {
		"conversationForkCommitCounts(t, selected)", `map[string]int{"conversation_forks": forks, "api_idempotency": completions}`, "if got != want",
	},
	"TestConversationForkCommitBeforeAPICompletionProbe": {
		"beforeForks, beforeCompletions := conversationForkCommitCounts(t, selected)", "forks, completions := conversationForkCommitCounts(t, selected)",
		"firstOwner := &forkCommitOmissionProbeOwner{ConversationForkLifecycleStore: selected, omit: omit}", "retryOwner := &forkCommitOmissionProbeOwner{ConversationForkLifecycleStore: selected}", "secondID != firstOwner.ids[0]",
	},
	"TestConversationForkAtomicCreationControls": {
		"storetest.InstallConversationForkCompletionInsertFailure(context.Background(), selected)", "defer func()", "storetest.RemoveConversationForkCompletionInsertFailure(context.Background(), selected)",
		"storetest.ReadConversationForkCompletionExpiryStorage(context.Background(), selected)", "raw := expiry.SQLiteText", `time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", raw)`, "expiresAt = expiry.PostgresTime",
		"const workers = 8", "close(start)", "wg.Wait()", "requireForkCommitCounts(t, selected, 0, 0)", "requireForkCommitCounts(t, selected, 1, 1)", "requireForkCommitCounts(t, selected, 5, 3)", "requireForkCommitCounts(t, selected, 6, 1)",
	},
	"TestConversationForkLostHTTPResponseReplays": {
		"_, selected, reopen, now, sessionID, turnID := newConversationForkCommitFixture(t, backend)", "reconstructed = reopen.sqlite()", "reconstructed = reopen.postgres()", "if reconstructed == selected",
		"requireForkCommitCounts(t, reconstructed, 1, 1)", "client := &http.Client{Timeout: 10 * time.Second}", "first := <-settled", "server.Close()",
		"w.(http.Hijacker).Hijack()", "conn.Close()", "retryHandler := makeHandler(reconstructed)", "json.NewDecoder(response.Body).Decode(&retry)", "requireForkCommitCounts(t, selected, 1, 1)",
	},
}

func family145ConversationForkProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145ConversationForkCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("conversation fork lost original native/observation/temporal cut: %s", cut)
		}
	}
	return nil
}

func assertFamily145ConversationForkPhysicalSQL(t *testing.T, original map[string]string) {
	t.Helper()
	for _, owner := range []struct{ file, function, table string }{
		{"internal/backend/runforkpersistence/conversation_fork_fixture_observation.go", "CountConversationForkStorageTx", "conversation_forks"},
		{"internal/apiidempotency/conversation_fork_fixture.go", "CountConversationForkCompletionStorageTx", "api_idempotency"},
	} {
		if !strings.Contains(original["requireForkCommitCounts"], `"SELECT count(*) FROM " + table`) || !strings.Contains(original["requireForkCommitCounts"], `"`+owner.table+`"`) {
			t.Fatal("original global fork/completion cardinality disappeared")
		}
		source := family145ConversationForkOwnerSource(t, owner.file, owner.function)
		queries := family145ConnectorSQL(t, "package proof\n"+source)
		want := family145ConnectorSQL(t, "package proof; const query = `SELECT count(*) FROM "+owner.table+"`")
		if !reflect.DeepEqual(queries, want) {
			t.Fatalf("fixed physical fork/completion count narrowed or changed: %v/%v", queries, want)
		}
	}
	owner := family145ConversationForkOwnerSource(t, "internal/apiidempotency/conversation_fork_fixture.go", "ReadConversationForkCompletionExpiryStorageTx")
	queries := family145ConnectorSQL(t, "package proof\n"+owner)
	before := family145ConnectorSQL(t, "package proof\n"+original["TestConversationForkAtomicCreationControls"])
	if len(queries) != 1 || len(before) != 2 || queries[0] != before[0] || queries[0] != before[1] || !strings.Contains(owner, "row.Scan(&out.PostgresTime)") || !strings.Contains(owner, "row.Scan(&out.SQLiteText)") {
		t.Fatal("native completion expiry predicate/order/limit/representation changed")
	}
	for _, cut := range []struct{ function, keyword string }{
		{"InstallConversationForkCompletionInsertFailureTx", "CREATE"},
		{"RemoveConversationForkCompletionInsertFailureTx", "DROP"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/apiidempotency/conversation_fork_fixture.go", cut.function)
		before := family145GateRecoveryStatements(t, "package proof\n"+original["TestConversationForkAtomicCreationControls"], cut.keyword)
		after := family145GateRecoveryStatements(t, "package proof\n"+owner, cut.keyword)
		if len(before) != 3 || !reflect.DeepEqual(before, after) {
			t.Fatalf("actual completion fault table/phase/payload/cleanup changed: %v/%v", before, after)
		}
	}
}

func family145ConversationForkOwnerSource(t *testing.T, path, function string) string {
	t.Helper()
	data, err := os.ReadFile("../../../internal/store/" + path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == function {
			return servedSetupEarlierSource(t, filepath.ToSlash(filepath.Clean(filepath.Join("internal/store", path))), function, family145NodeString(fn))
		}
	}
	t.Fatalf("missing original row-family owner port: %s", function)
	return ""
}

func assertFamily145ConversationForkIndependentCountCuts(t *testing.T) {
	t.Helper()
	source := family145ConversationForkOwnerSource(t, "../apiv1/conversation_fork_atomic_creation_test.go", "conversationForkCommitCounts")
	first := "storetest.CountConversationForkStorage(context.Background(), selected)"
	second := "storetest.CountConversationForkCompletionStorage(context.Background(), selected)"
	if strings.Count(source, first) != 1 || strings.Count(source, second) != 1 || strings.Index(source, first) >= strings.Index(source, second) {
		t.Fatal("original independent cardinality cuts were collapsed/reordered")
	}
}
