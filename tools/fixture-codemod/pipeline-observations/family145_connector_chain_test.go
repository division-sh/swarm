package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestFamily145ConnectorReadQueriesRetainExactHistoricalPredicates(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string][]string{}
	for _, row := range rows {
		original[row.Function] = family145ConnectorSQL(t, "package proof\n"+row.Before)
	}
	for _, row := range []struct{ file, function, predecessor string }{
		{"eventrecord/connector_storage_observation.go", "ReadConnectorNormalizedProviderEventIDTx", "loadNormalizedProviderEventID"},
		{"eventrecord/connector_storage_observation.go", "CountConnectorNamedSourceEventTx", "countSlackManagedConnectorFailureEventsForSource"},
		{"eventrecord/connector_storage_observation.go", "CountConnectorNamedEventIdentityTx", "slackManagedConnectorEventExists"},
		{"eventrecord/connector_storage_observation.go", "ReadConnectorEventChronologyTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "CountTemplateNodeDeliveryTx", "countTelegramConnectorSupportedSurfaceNodeDeliveries"},
		{"delivery/read_projections.go", "CountConnectorRunNodeDeliveriesTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "ReadConnectorLatestNodeDeliveryDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "CountConnectorEntityDeadLettersTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "ReadConnectorLatestEntityDeadLetterDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"activityjournal/fork_observation.go", "CountConnectorRunActivityAttemptsTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"pipelinepersistence/connector_receipt_observation.go", "ReadConnectorLatestActivityReceiptDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"eventrecord/connector_storage_observation.go", "ReadChannelFirstActivitySourceRouteTx", "assertConfiguredChannelJournal"},
		{"activityjournal/fork_observation.go", "CountChannelSucceededToolAttemptsTx", "assertConfiguredChannelJournal"},
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", row.file))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), row.file, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		var queries []string
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Name.Name == row.function {
				queries = family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
			}
		}
		if len(queries) == 0 {
			t.Fatalf("missing closed read owner: %s/%s", row.file, row.function)
		}
		for _, query := range queries {
			found := false
			for _, before := range original[row.predecessor] {
				if query == before {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("closed owner changed historical SQL scope/order/NULL semantics: %s: %s", row.function, query)
			}
		}
	}
}

func family145ConnectorSQL(t testing.TB, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "sql-proof.go", source, parser.AllErrors)
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
		words := strings.Fields(value)
		if len(words) == 0 || !strings.EqualFold(words[0], "select") {
			return true
		}
		for i := 1; strings.Contains(value, "?"); i++ {
			value = strings.Replace(value, "?", "$"+strconv.Itoa(i), 1)
		}
		value = strings.ReplaceAll(value, "::uuid", "")
		var compact strings.Builder
		quoted := false
		for _, character := range value {
			if character == '\'' {
				quoted = !quoted
			}
			if !quoted && (character == ' ' || character == '\n' || character == '\t' || character == '\r') {
				continue
			}
			compact.WriteRune(character)
		}
		queries = append(queries, compact.String())
		return true
	})
	return queries
}

func TestFamily145ConnectorSQLCorrespondencePreservesQuotedCaseAndSpaces(t *testing.T) {
	original := family145ConnectorSQL(t, "package proof; const query = \"SELECT status FROM attempts WHERE status='succeeded'\"")[0]
	for _, hostile := range []string{"SUCCEEDED", "suc ceeded"} {
		query := family145ConnectorSQL(t, "package proof; const query = \"SELECT status FROM attempts WHERE status='"+hostile+"'\"")[0]
		if original == query {
			t.Fatalf("SQL correspondence hid changed quoted value: %s", hostile)
		}
	}
}

var family145ConnectorCalls = map[string][]string{
	"runTelegramConnectorSupportedSurfaceRoundTrip":                                   {"normalized inbound observer borrowed foreign evidence", "!errors.Is(err, sql.ErrNoRows) || id != \"\""},
	"startConfiguredChannelActivityNode":                                              {"func assertConfiguredChannelClaimedDispatch(", "storetest.ObserveSelectedPool(t, selected)", "if coordinator == nil || bus == nil", "len(nodes) != 0", "want claimed event dispatch only"},
	"TestTelegramConnectorBoundedIntegrationRoundTripThroughInboundGateway":           {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"TestSlackManagedCredentialConnectorPackRoundTripThroughActivityJournal":          {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"TestMicrosoftGraphClientCredentialsConnectorPackRoundTripThroughActivityJournal": {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"TestNotionManagedCredentialConnectorPackRoundTripThroughActivityJournal":         {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"TestGitHubAppIssueCommentConnectorPackRoundTripThroughActivityJournal":           {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"TestGitHubAppIssueWorkflowConnectorPackRoundTripThroughActivityJournal":          {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
	"countTelegramConnectorSupportedSurfaceFailureEventsForEvent":                     {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, failureEventType, inboundEventID)"},
	"countSlackManagedConnectorFailureEventsForSource":                                {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, failureEventType, sourceEventID)"},
	"countMicrosoftGraphFailureEventsForSource":                                       {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, failureEventType, sourceEventID)"},
	"countNotionManagedConnectorFailureEventsForSource":                               {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, failureEventType, sourceEventID)"},
	"countGitHubAppIssueCommentFailureEventsForSource":                                {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, failureEventType, sourceEventID)"},
	"countGitHubFailureEventsForSource":                                               {"storetest.CountConnectorNamedSourceEvent(backend.ctx, backend.eventStore, backend.runID, eventName, sourceEventID)"},
	"countTelegramConnectorSupportedSurfaceNodeDeliveries":                            {"storetest.CountTemplateNodeDelivery(backend.ctx, backend.eventStore, eventID, nodeID)"},
	"loadTelegramConnectorSupportedSurfaceInboundEventIDByRun":                        {"storetest.ReadConnectorNormalizedProviderEventID(backend.ctx, backend.eventStore, runID, backend.entityID, \"inbound.telegram.text_message\", providerEventID)"},
	"loadNormalizedProviderEventID":                                                   {"storetest.ReadConnectorNormalizedProviderEventID(backend.ctx, backend.eventStore, backend.runID, backend.entityID, eventName, providerEventID)"},
	"telegramConnectorSupportedSurfaceEventExists":                                    {"storetest.CountConnectorNamedEventIdentity(backend.ctx, backend.eventStore, eventID, eventType)", "return count == 1"},
	"slackManagedConnectorEventExists":                                                {"storetest.CountConnectorNamedEventIdentity(backend.ctx, backend.eventStore, eventID, eventType)", "return count == 1"},
	"loadTelegramConnectorSupportedSurfaceActivityRequestEvent":                       {"storetest.ReadCanonicalEventRecord(backend.ctx, backend.eventStore, eventID)", "event.Type() != events.EventType(\"platform.activity_requested\")", "return event"},
	"loadSlackManagedConnectorActivityRequestEvent":                                   {"storetest.ReadCanonicalEventRecord(backend.ctx, backend.eventStore, eventID)", "event.Type() != events.EventType(\"platform.activity_requested\")", "return event"},
	"startTelegramConnectorSupportedSurfaceCoordinator":                               {"assertConfiguredChannelClaimedDispatch(t, pc, bus, backend.eventStore)"},
	"startSlackManagedConnectorBusAndCoordinator":                                     {"assertConfiguredChannelClaimedDispatch(t, pc, bus, backend.eventStore)"},
	"telegramConnectorSupportedSurfaceDiagnostics":                                    {"\"inbound.telegram\"", "\"platform.activity_requested\"", "storetest.CountPhysicalRunEvents(backend.ctx, backend.eventStore, backend.runID)", "storetest.CountConnectorRunNodeDeliveries(backend.ctx, backend.eventStore, backend.runID, telegramConnectorSupportedSurfaceNodeKey(t))", "storetest.CountConnectorRunActivityAttempts(backend.ctx, backend.eventStore, backend.runID)", "storetest.CountConnectorEntityDeadLetters(backend.ctx, backend.eventStore, backend.entityID)", "storetest.ReadConnectorLatestNodeDeliveryDiagnostic(backend.ctx, backend.eventStore, backend.runID, telegramConnectorSupportedSurfaceNodeKey(t))", "storetest.ReadConnectorEventChronology(backend.ctx, backend.eventStore, backend.runID)", "storetest.ReadConnectorLatestActivityReceiptDiagnostic(backend.ctx, backend.eventStore, backend.runID)", "storetest.ReadConnectorLatestEntityDeadLetterDiagnostic(backend.ctx, backend.eventStore, backend.entityID)", "backend.persistence.LoadWorkflowInstance("},
	"telegramConnectorSupportedSurfaceScalarDiagnostic":                               {"errors.Is(err, sql.ErrNoRows)", "return \"<none>\"", "return \"ERR(\" + err.Error() + \")\""},
}

func family145ConnectorProofError(function, before, after string) error {
	if function == "startConfiguredChannelActivityNode" {
		// The immutable predecessor only checked zero background nodes and
		// returned a no-op. The successor asserts its original selected owner;
		// real coordinator/runtime teardown remains with the existing fixture.
		if !strings.Contains(strings.Join(strings.Fields(before), ""), "returnfunc(){}") || !strings.Contains(before, "len(nodes) != 0") {
			return fmt.Errorf("claimed-dispatch predecessor is not the reviewed no-op")
		}
		for _, call := range family145ConnectorCalls[function] {
			if !strings.Contains(after, call) {
				return fmt.Errorf("claimed-dispatch assertion lost: %s", call)
			}
		}
		return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
	}
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	index := 0
	for _, assertion := range got {
		if index < len(want) && assertion == want[index] {
			index++
		}
	}
	if index != len(want) {
		return fmt.Errorf("connector lost original assertion: %s", want[index])
	}
	for _, call := range family145ConnectorCalls[function] {
		if !strings.Contains(after, call) {
			return fmt.Errorf("connector lost exact owner/scope: %s", call)
		}
	}
	if strings.Contains(after, "eventtest.PersistedProjection(") || strings.Contains(after, "created.IsZero()") {
		return fmt.Errorf("connector replay reconstructs persisted identity or clock")
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func TestFamily145ConnectorChainPreservesAssertionsAndRejectsLostOwners(t *testing.T) {
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		calls, ok := family145ConnectorCalls[row.Function]
		if !ok {
			continue
		}
		if seen[row.Function] {
			t.Fatalf("duplicate connector transition: %s", row.Function)
		}
		seen[row.Function] = true
		if err := family145ConnectorProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, call := range calls {
			if err := family145ConnectorProofError(row.Function, row.Before, strings.ReplaceAll(row.After, call, "foreignOwnerCut(")); err == nil {
				t.Fatalf("lost connector owner accepted: %s/%s", row.Function, call)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		if row.Function == "startConfiguredChannelActivityNode" {
			// Its exact replacement checks are hostile-controlled above; the
			// removed DB nil gate is replaced by original-owner validation.
			continue
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if err := family145ConnectorProofError(row.Function, row.Before, strings.ReplaceAll(row.After, condition, "false")); err == nil {
				t.Fatalf("lost connector assertion accepted: %s/%s", row.Function, condition)
			}
		}
	}
	if len(seen) != len(family145ConnectorCalls) {
		t.Fatalf("connector inventory incomplete: %d/%d", len(seen), len(family145ConnectorCalls))
	}
}

func TestFamily145ConnectorEntireCallerChainRetiresPoolsAndNoOpLifetime(t *testing.T) {
	root := filepath.Join("..", "..", "..", "internal", "runtime")
	for _, path := range []string{
		"telegram_connector_supported_surface_test.go",
		"slack_connector_managed_credential_supported_surface_test.go",
		"microsoft_graph_connector_client_credentials_supported_surface_test.go",
		"notion_connector_managed_credential_supported_surface_test.go",
		"github_app_issue_comment_supported_surface_test.go",
		"github_app_issue_workflow_supported_surface_test.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := family145DescriptorReadinessEscape(data); err != nil {
			t.Fatalf("connector raw authority: %s: %v", path, err)
		}
		for _, retired := range []string{"backend.db", "backend.sqlite", "seedTelegramConnectorSupportedSurfaceWorkflowVersion", "startConfiguredChannelActivityNode", "eventtest.PersistedProjection", "json_set", "jsonb_set"} {
			if strings.Contains(string(data), retired) {
				t.Fatalf("connector retired escape: %s/%s", path, retired)
			}
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "channel_runtime_supported_surface_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"startConfiguredChannelActivityNode", "stopActivityNode"} {
		if strings.Contains(string(data), retired) {
			t.Fatalf("retired no-op lifetime survives: %s", retired)
		}
	}
	// The existing coordinator constructors retain their real joined close owner.
	for _, proof := range []string{"storetest.ObserveSelectedPool(t, selected)", "len(nodes) != 0", "want claimed event dispatch only"} {
		if !strings.Contains(string(data), proof) {
			t.Fatalf("claimed dispatch lost existing owner invariant: %s", proof)
		}
	}
	for _, escape := range []string{"db.QueryRowContext(ctx, query)", "storetest.AdmitPostgresRuntimeStore(t, db)", "storetest.Database(owner)", "(*sql.DB)(nil)"} {
		if family145DescriptorReadinessEscape([]byte("package probe; func escape() { _ = "+escape+" }")) == nil {
			t.Fatalf("raw connector resurrection accepted: %s", escape)
		}
	}
}
