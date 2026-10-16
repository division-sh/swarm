package main

import (
	"go/ast"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func family145MailboxCompletionCallerInverse(after string) string {
	return strings.NewReplacer(
		"mailboxCompletionRunEffects(t, rt.ReceiverStateReader, ", "mailboxCompletionRunEffects(t, rt, ",
		"mailboxCompletionRunEffects(t, f.rt.ReceiverStateReader, ", "mailboxCompletionRunEffects(t, f.rt, ",
		"waitPublicationSiteCompletion(t, rt.ReceiverStateReader, ", "waitPublicationSiteCompletion(t, rt, ",
		"waitPublicationSiteCompletion(t, f.rt.ReceiverStateReader, ", "waitPublicationSiteCompletion(t, f.rt, ",
	).Replace(after)
}

func TestFamily145PublicationDiagnosticsRetainExactElevenSectionDialectInventory(t *testing.T) {
	original := ""
	for _, row := range family145ServedDebugRows {
		if row.Function == "servedEventPublishDebugQuery" {
			original = row.Before
		}
	}
	if original == "" {
		t.Fatal("signed predecessor diagnostic query inventory missing")
	}
	physical := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_served_delivery_observation.go", "servedRunDebugSQL")
	delivery := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", "runDeliveryDebugQuery")
	lifecycle := family145ConversationForkOwnerSource(t, "internal/backend/runlifecycle/run_lifecycle_candidates.go", "readRunDebugStorageTx")
	want := family145A2ClaimLiteralQueries(t, original)
	got := family145A2ClaimLiteralQueries(t, physical+"\n"+delivery+"\n"+lifecycle)
	want = family145MailboxNormalizeDiagnosticQueries(want)
	got = family145MailboxNormalizeDiagnosticQueries(got)
	if len(want) != 22 || !reflect.DeepEqual(want, got) {
		t.Fatal("native diagnostics changed original full fields/LEFT joins/run scoping/dialect/limits/order")
	}
	for _, pair := range [][2]string{{"LIMIT 12", "LIMIT 5"}, {"LEFT JOIN", "JOIN"}, {"d.failure::text", "d.failure->>'class'"}, {"WHERE run_id = ?", "WHERE run_id = 'foreign'"}, {"SELECT instance_path, flow_template, status", "SELECT instance_path, flow_template"}} {
		hostile := strings.Replace(physical, pair[0], pair[1], 1)
		if hostile == physical {
			t.Fatalf("diagnostic hostile control lost its exact projection: %s", pair[0])
		}
		changed := family145MailboxNormalizeDiagnosticQueries(family145A2ClaimLiteralQueries(t, hostile+"\n"+delivery+"\n"+lifecycle))
		if reflect.DeepEqual(want, changed) {
			t.Fatalf("native diagnostic lost a physical section distinction: %s", pair[0])
		}
	}
}

func family145MailboxNormalizeDiagnosticQueries(queries []string) []string {
	for i, query := range queries {
		queries[i] = strings.Join(strings.Fields(query), "")
	}
	slices.Sort(queries)
	return queries
}

func family145MailboxCompletionRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	name := family145NodeString(call.Fun)
	if name != "mailboxCompletionRunEffects" && name != "waitPublicationSiteCompletion" {
		return
	}
	if len(call.Args) != 3 {
		t.Fatal("mailbox observation changed helper arity")
	}
	owner := family145NodeString(call.Args[1])
	if owner != "rt.ReceiverStateReader" && owner != "f.rt.ReceiverStateReader" && !(function == "mailboxCompletionRunEffects" && owner == "reader") {
		t.Fatal("mailbox/completion caller retained raw or foreign authority")
	}
	key := "mailbox-effects/"
	if name == "waitPublicationSiteCompletion" {
		key = "publication-completion/"
	}
	counts[key+function]++
}
