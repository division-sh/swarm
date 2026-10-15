package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

// Observe after the original timeout. Never print payload, SQL text, claim
// tokens, credentials or effect bodies, and never alter the settlement verdict.
func logNotifyAllChildrenDrainFailure(t *testing.T, selected notifyAllChildrenStore, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := storetest.ReadFanOutDrainDeliveryStorage(ctx, notifyAllChildrenNativeObservationOwner(selected), runID)
	if err != nil {
		t.Logf("drain delivery diagnostic: %v", err)
	} else {
		columns := []string{"delivery_id", "event_id", "event_name", "subscriber_type", "subscriber_id",
			"status", "claim_version", "retry_count", "next_eligible_at", "continuation_handoff_at",
			"updated_at", "attempt_closure", "lease_expires_at", "attempt_outcome"}
		count := 0
		for _, row := range rows {
			count++
			if count > 32 {
				t.Log("drain delivery diagnostic truncated after 32 rows")
				break
			}
			values := []storetest.FanOutDrainNullableText{
				row.DeliveryID, row.EventID, row.EventName, row.SubscriberType, row.SubscriberID,
				row.Status, row.ClaimVersion, row.RetryCount, row.NextEligibleAt, row.ContinuationHandoffAt,
				row.UpdatedAt, row.AttemptClosure, row.LeaseExpiresAt, row.AttemptOutcome,
			}
			evidence := map[string]string{"run_id": runID}
			for i, column := range columns {
				if values[i].Valid {
					evidence[column] = values[i].String
				}
			}
			raw, _ := json.Marshal(evidence)
			t.Logf("unsettled fan-out delivery: %s", raw)
		}
	}

	// Debug level one contains symbols rather than argument values. Keep only
	// Swarm frames so unrelated credentials and query text cannot enter evidence.
	var profile bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&profile, 1); err != nil {
		t.Logf("drain wait diagnostic: %v", err)
		return
	}
	count := 0
	for _, line := range strings.Split(profile.String(), "\n") {
		if !strings.HasPrefix(line, "#") || !strings.Contains(line, "github.com/division-sh/swarm/") {
			continue
		}
		count++
		if count > 128 {
			t.Log("drain wait diagnostic truncated after 128 frames")
			break
		}
		t.Logf("drain wait frame: %s", line)
	}
}
