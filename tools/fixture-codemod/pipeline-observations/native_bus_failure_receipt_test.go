package main

import (
	"strings"
	"testing"
)

const busFailureReceiptRawCut = `var outcome, failureClass, detailCode string
if err := db.QueryRowContext(ctx, ` + "`" + `
		SELECT outcome, COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	` + "`" + `, eventID).Scan(&outcome, &failureClass, &detailCode); err != nil {
	t.Fatalf("load pipeline receipt: %v", err)
}`

const busFailureReceiptOwnedCut = `evidence, err := storetest.ObserveSemanticEventFixtureEvidence(ctx, pg, eventBusTestRunID, eventID)
if err != nil {
	t.Fatalf("load pipeline receipt: %v", err)
}
if evidence.PipelineReceiptCount != 1 {
	t.Fatalf("pipeline receipt count = %d, want 1", evidence.PipelineReceiptCount)
}
outcome := evidence.PipelineReceiptOutcome
var failureClass, detailCode string
if evidence.PipelineReceiptFailure != nil {
	failureClass = string(evidence.PipelineReceiptFailure.Class)
	detailCode = evidence.PipelineReceiptFailure.Detail.Code
}`

func busFailureReceiptFragment(t *testing.T, source string) string {
	t.Helper()
	value, err := canonicalFunction("func cut(){" + source + "}")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(strings.TrimPrefix(value, "func cut() {\n"), "\n}")
}

func TestNativeBusFailureReceiptPreservesPostCommitErrorAndExactFailure(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-bus-postcommit-failure-receipt")
	before := normalizedNativePostCommitRoot(t, normalizedBusRunConstruction(t, row.Before))
	after := normalizedNativePostCommitRoot(t, normalizedBusRunConstruction(t, row.After))
	before = strings.Replace(before, busFailureReceiptFragment(t, busFailureReceiptRawCut), busFailureReceiptFragment(t, busFailureReceiptOwnedCut), 1)
	if before != after {
		t.Fatal("publication, real interceptor error, post-commit cut or canonical failure assertions changed")
	}
	for _, pair := range [][2]string{
		{"ctx, pg, eventBusTestRunID, eventID", "ctx, pg, eventBusTestRunID, otherEvent"},
		{"evidence.PipelineReceiptCount != 1", "false"},
		{"evidence.PipelineReceiptOutcome", "\"dead_letter\""},
		{"string(evidence.PipelineReceiptFailure.Class)", "string(runtimefailures.ClassInternalFailure)"},
		{"evidence.PipelineReceiptFailure.Detail.Code", "\"event_interceptor_failed\""},
		{"if !errors.Is(err, wantErr)", "if false"},
	} {
		mutant := strings.Replace(after, pair[0], pair[1], 1)
		if mutant == after || mutant == before {
			t.Fatalf("weakened failure receipt witness admitted: %v", pair)
		}
	}
}
