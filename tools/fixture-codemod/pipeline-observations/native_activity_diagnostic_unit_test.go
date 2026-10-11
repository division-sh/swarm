package main

import (
	"strings"
	"testing"
)

func TestActivityDiagnosticUnitRetiresPrivateTransactionMarkers(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-activity-diagnostic-authority-unit")
	before := strings.Replace(row.Before, "DoesNotUseAmbientPostCommitAuthority", "EmitsImmediateDiagnosticWithoutPublication", 1)
	before = strings.Replace(before, "\tpostCommit := []OwnerAction{}\n\trollbackActions := []OwnerAction{}\n\tctx := WithPipelinePostCommitActions(testAuthorActivityContext(t, context.Background()), &postCommit)\n\tctx = WithPipelineRollbackActions(ctx, &rollbackActions)\n\tctx = WithPipelineSQLTxContext(ctx, &sql.Tx{})\n", "\tctx := testAuthorActivityContext(t, context.Background())\n", 1)
	before = strings.Replace(before, "\tif got := len(postCommit); got != 0 {\n\t\tt.Fatalf(\"post-commit actions = %d, want none\", got)\n\t}\n", "\tif got := bus.outboxCount(); got != 0 {\n\t\tt.Fatalf(\"outbox intents = %d, want no durable publication\", got)\n\t}\n\tif got := bus.publishedCount(); got != 0 {\n\t\tt.Fatalf(\"published events = %d, want no dispatch\", got)\n\t}\n", 1)
	want, err := canonicalFunction(before)
	got, afterErr := canonicalFunction(row.After)
	actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, replacementFunctionName(t, row)))
	if err != nil || afterErr != nil || actualErr != nil || want != got || actual != got {
		t.Fatal("activity diagnostic lost exact intent/log contract or new authority assertions")
	}
	for _, raw := range []string{"OwnerAction", "WithPipelineSQL", "WithPipelinePostCommit", "WithPipelineRollback", "sql.Tx"} {
		if strings.Contains(row.After, raw) {
			t.Fatalf("diagnostic unit retains a fake transaction protocol: %s", raw)
		}
	}
	for _, cut := range []string{"writer.WriteActivityIntents", "bus.outboxCount()", "bus.publishedCount()", "got != 0", `logs[0].Action != "intent_persisted"`} {
		changed, mutantErr := canonicalFunction(strings.Replace(row.After, cut, "unreviewed", 1))
		if !strings.Contains(row.After, cut) || mutantErr == nil && changed == want {
			t.Fatalf("weakened activity unit accepted: %s", cut)
		}
	}
}
