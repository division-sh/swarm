package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// Observe after the original timeout. Never print payload, SQL text, claim
// tokens, credentials or effect bodies, and never alter the settlement verdict.
func logNotifyAllChildrenDrainFailure(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT CAST(d.delivery_id AS TEXT),
		CAST(d.event_id AS TEXT), e.event_name, d.subscriber_type, d.subscriber_id,
		d.status, CAST(d.claim_version AS TEXT), CAST(d.retry_count AS TEXT),
		CAST(d.next_eligible_at AS TEXT), CAST(d.continuation_handoff_at AS TEXT),
		CAST(d.updated_at AS TEXT), a.closure_kind, CAST(a.lease_expires_at AS TEXT),
		a.outcome FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
		LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id
		AND a.claim_version=d.current_attempt_version
		WHERE d.run_id=$1 AND d.status NOT IN ('delivered','dead_letter')
		ORDER BY d.created_at,d.delivery_id LIMIT 33`, runID)
	if err != nil {
		t.Logf("drain delivery diagnostic: %v", err)
	} else {
		columns := []string{"delivery_id", "event_id", "event_name", "subscriber_type", "subscriber_id",
			"status", "claim_version", "retry_count", "next_eligible_at", "continuation_handoff_at",
			"updated_at", "attempt_closure", "lease_expires_at", "attempt_outcome"}
		count := 0
		for rows.Next() {
			count++
			if count > 32 {
				t.Log("drain delivery diagnostic truncated after 32 rows")
				break
			}
			values, targets := make([]sql.NullString, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Logf("drain delivery diagnostic scan: %v", err)
				break
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
		if err := rows.Err(); err != nil {
			t.Logf("drain delivery diagnostic iteration: %v", err)
		}
		rows.Close()
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
