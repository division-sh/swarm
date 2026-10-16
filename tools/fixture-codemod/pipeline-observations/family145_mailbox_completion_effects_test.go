package main

import (
	"crypto/sha256"
	"fmt"
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

func family145MailboxCompletionReaderProofError(before, after string) error {
	const expected = `func mailboxCompletionRunEffects(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	waitPublicationSiteCompletion(t, reader, runID)
	var snapshot []string
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		snapshot, err = storetest.ReadMailboxCompletionRunEffects(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}`
	if after != expected || !strings.Contains(before, "waitPublicationSiteCompletion(t, rt, runID)") || !strings.Contains(before, "json.Marshal(values)") || !strings.Contains(before, `snapshot = append(snapshot, query+":"+string(raw))`) {
		return fmt.Errorf("mailbox snapshot changed original completion-before-read/encoding/selected owner/error cut")
	}
	return nil
}

func family145MailboxCompletionWaitInverse(after string) string {
	const nativeFailed = `reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			failed, err = storetest.CountRunDeadLetterDeliveries(ctx, reader, runID)
			return err
		})`
	const nativeIncomplete = `reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(ctx, reader, runID)
			return err
		})`
	const originalIncomplete = "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND\n\t\t\t(d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS\n\t\t\t(SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&incomplete)"
	return strings.NewReplacer(
		"reader receiverProofStateReader", "rt servedControlProofRuntime",
		nativeFailed, "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID).Scan(&failed)",
		nativeIncomplete, originalIncomplete,
		"servedInspectionRunDebugSummary(reader, runID)", "servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID)",
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

func family145MailboxCompletionCheckCaller(t *testing.T, function, before, after, normalized string) {
	t.Helper()
	normalized = family145MailboxCompletionCallerInverse(family145ServedDebugCallerInverse(function, after))
	if function == "requireTypedReceiverInitializationCases" {
		if err := family145ForkSourceDomainCallerProofError(function, before, after); err != nil {
			t.Fatal(err)
		}
	} else if err := family145MailboxCompletionCallerProofError(function, before, normalized); err != nil {
		t.Fatalf("mailbox propagation changed source/identity/assertions/waits/fault/cleanup cuts: %s", function)
	}
	finite := recipe{File: "caller.go", Function: function, Before: before, After: after}
	for _, cut := range []string{"ReceiverStateReader", "t.Fatal", "t.Errorf", "t.Fatalf", "reflect.DeepEqual"} {
		if !strings.Contains(after, cut) {
			continue
		}
		hostile := strings.Replace(after, cut, "foreignMailboxCut()", 1)
		if _, _, err := rewriteFunction(finite.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("mailbox propagation accepted foreign owner/assertion/temporal cut: %s/%s", function, cut)
		}
	}
}

func family145MailboxCompletionCallerProofError(function, before, normalized string) error {
	normalized = family145WriterStageCallerInverse(function, normalized)
	// Master's approved reporter serving-window assertions postdate the frozen
	// corpus. Pin their entire predecessor body at ee05cae31d, not the old subset.
	if function == "runIssue2394OriginalReporterHTTP" {
		if fmt.Sprintf("%x", sha256.Sum256([]byte(normalized))) != "23377f519af846666a4db08467fdbd3093337a147aba844cf1da9ef8e40186bb" {
			return fmt.Errorf("reporter wait propagation changed its approved whole predecessor")
		}
		return nil
	}
	if normalized != before {
		return fmt.Errorf("mailbox caller changed its complete original body")
	}
	return nil
}

func family145MailboxCompletionCheckHelper(t *testing.T, function, before, after string) {
	t.Helper()
	if function == "mailboxCompletionRunEffects" {
		if err := family145MailboxCompletionReaderProofError(before, after); err != nil {
			t.Fatal(err)
		}
		private := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_mailbox_completion_observation.go", "ReadMailboxCompletionRunEffectsForTest")
		delivery := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", "ReadMailboxCompletionDeliveryEffectsTx")
		original := family145A2ClaimLiteralQueries(t, before)
		queries := append(family145A2ClaimLiteralQueries(t, private), family145A2ClaimLiteralQueries(t, delivery)...)
		slices.Sort(original)
		slices.Sort(queries)
		if len(original) != 9 || len(queries) != 9 || !reflect.DeepEqual(original, queries) {
			t.Fatal("mailbox owner changed nine-table full projection/run predicates/diagnostic exclusion/order")
		}
	} else if family145MailboxCompletionWaitInverse(after) != before {
		t.Fatal("completion wait lost failed-first/OR/receipt/deadline/poll/stack/diagnostic cuts")
	}
	finite := recipe{File: "helper.go", Function: function, Before: before, After: after}
	for _, pair := range [][2]string{{"ORDER BY card_id", "ORDER BY run_id"}, {"event_class NOT IN", "event_class IN"}, {"run_id=$1", "run_id=$2"}, {"waitPublicationSiteCompletion(t, reader, runID)", "foreignCompletion()"}, {"status='dead_letter'", "status='delivered'"}, {"r.subscriber_id='pipeline'", "r.subscriber_id='foreign'"}} {
		for _, source := range []string{before, after} {
			hostile := strings.Replace(source, pair[0], pair[1], 1)
			if hostile != source {
				if _, _, err := rewriteFunction(finite.File, []byte("package proof\n"+hostile), finite); err == nil {
					t.Fatalf("mailbox transform accepted foreign shape/cut: %s", pair[0])
				}
			}
		}
	}
}

func family145CheckWholeObservationReader(t *testing.T, function, before, after string) bool {
	t.Helper()
	var err error
	switch function {
	case "readServedForkRecipientSourceDomain":
		err = family145ForkSourceDomainReaderProofError(before, after)
	case "lifecycleStoredSnapshot":
		err = family145LifecycleBusinessReaderProofError(before, after)
	case "mailboxCompletionRunEffects", "waitPublicationSiteCompletion":
		family145MailboxCompletionCheckHelper(t, function, before, after)
	default:
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
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
