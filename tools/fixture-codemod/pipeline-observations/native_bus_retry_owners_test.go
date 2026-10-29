package main

import (
	"strings"
	"testing"
)

func TestNativeBusRetryReceiptOwnerHasNoAdditionalOutcomeOrRunFilter(t *testing.T) {
	body := selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/owner_operations.go", "FixturePipelineReceiptCardinalityTx")
	if !strings.Contains(body, "`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`") ||
		!strings.Contains(body, "tx.QueryRowContext(ctx,") ||
		!strings.Contains(body, "eventID).Scan(&count)") ||
		strings.Contains(body, "LIMIT") {
		t.Fatal("receipt witness no longer preserves exact physical cardinality")
	}
}
