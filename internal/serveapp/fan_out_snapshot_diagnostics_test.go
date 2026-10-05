package serveapp

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Use the captured observations, never another query of the live store.
func issue2394SurfaceSnapshotDifference(before, after [][]sql.NullString) string {
	type projection struct {
		table   string
		columns []string
		keySize int
	}
	projections := []projection{
		{"fan_out_intents", []string{"triggering_delivery_id", "flow_path", "declaration_family", "semantic_path", "cardinality", "cursor", "status", "claim_owner", "claim_generation", "lease_expires_at", "blocked_reason", "retry_ready_at", "retry_failure", "updated_at"}, 4},
		{"fan_out_outcomes", []string{"triggering_delivery_id", "flow_path", "declaration_family", "semantic_path", "ordinal", "outcome_kind", "event_id", "failure", "created_at"}, 5},
		{"events", []string{"event_id", "event_name", "payload"}, 1},
	}
	fingerprint := func(values []sql.NullString) string {
		hash := sha256.New()
		for _, value := range values {
			fmt.Fprintf(hash, "%t:%d:%s;", value.Valid, len(value.String), value.String)
		}
		return fmt.Sprintf("%x", hash.Sum(nil))
	}
	summary := func(column string, value sql.NullString) string {
		result := fmt.Sprintf("valid=%t bytes=%d sha256=%s", value.Valid, len(value.String), fingerprint([]sql.NullString{value}))
		if value.Valid && strings.HasSuffix(column, "_at") {
			for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999 -0700 MST"} {
				if _, err := time.Parse(layout, value.String); err == nil {
					return result + fmt.Sprintf(" timestamp=%q", value.String)
				}
			}
		}
		return result
	}
	type observation struct {
		projection
		values []sql.NullString
	}
	index := func(snapshot [][]sql.NullString) map[string]observation {
		rows := map[string]observation{}
		occurrences := map[string]int{}
		for _, values := range snapshot {
			shape := projection{table: fmt.Sprintf("unknown_width_%d", len(values)), keySize: len(values)}
			for _, candidate := range projections {
				if len(values) == len(candidate.columns) {
					shape = candidate
					break
				}
			}
			key := shape.table + "/row_key_sha256=" + fingerprint(values[:shape.keySize])
			if len(shape.columns) != 0 {
				if id, err := uuid.Parse(values[0].String); values[0].Valid && err == nil {
					key += "/" + shape.columns[0] + "=" + id.String()
				}
			}
			occurrences[key]++
			key += fmt.Sprintf("/occurrence=%d", occurrences[key])
			rows[key] = observation{shape, values}
		}
		return rows
	}
	oldRows, newRows := index(before), index(after)
	keys := make([]string, 0, len(oldRows)+len(newRows))
	for key := range oldRows {
		keys = append(keys, key)
	}
	for key := range newRows {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	var output strings.Builder
	fmt.Fprintf(&output, "captured_at=%s before_rows=%d after_rows=%d\n", time.Now().UTC().Format(time.RFC3339Nano), len(before), len(after))
	for _, key := range keys {
		old, existed := oldRows[key]
		current, remains := newRows[key]
		if !existed || !remains {
			row, change := current, "added"
			if !remains {
				row, change = old, "removed"
			}
			fmt.Fprintf(&output, "%s change=%s row_sha256=%s\n", key, change, fingerprint(row.values))
			for i, column := range row.columns {
				fmt.Fprintf(&output, "  column=%s {%s}\n", column, summary(column, row.values[i]))
			}
			continue
		}
		for i, value := range old.values {
			if value == current.values[i] {
				continue
			}
			column := fmt.Sprintf("column_%d", i)
			if i < len(old.columns) {
				column = old.columns[i]
			}
			fmt.Fprintf(&output, "%s column=%s before={%s} after={%s}\n", key, column, summary(column, value), summary(column, current.values[i]))
		}
	}
	return output.String()
}

func TestIssue2394ServedSnapshotDiagnostics(t *testing.T) {
	value := func(text string) sql.NullString { return sql.NullString{String: text, Valid: true} }
	intent := make([]sql.NullString, 14)
	intent[0], intent[13] = value("d0f79448-8eec-46cc-8613-9d17d2278c2d"), value("2026-10-05T16:44:30.123456789Z")
	changed := slices.Clone(intent)
	changed[13] = value("2026-10-05T16:44:32.123456789Z")
	event := []sql.NullString{value("event-id"), value("event-name"), value(`{"secret":"never-print-original-payload"}`)}
	changedEvent := slices.Clone(event)
	changedEvent[2] = value(`{"secret":"never-print-changed-payload"}`)
	outcome := make([]sql.NullString, 9)
	outcome[0], outcome[7] = value("delivery-id"), value("never-print-failure-message")
	added := slices.Clone(outcome)
	added[0], added[8] = value("another-delivery"), value("2026-10-05T16:44:33Z")
	output := issue2394SurfaceSnapshotDifference([][]sql.NullString{intent, event, outcome}, [][]sql.NullString{added, changedEvent, changed})
	for _, required := range []string{"fan_out_intents/row_key_sha256=", "triggering_delivery_id=d0f79448-8eec-46cc-8613-9d17d2278c2d", "column=updated_at", "2026-10-05T16:44:30.123456789Z", "2026-10-05T16:44:32.123456789Z", "events/row_key_sha256=", "column=payload", "fan_out_outcomes/row_key_sha256=", "change=added", "change=removed", "column=failure", "sha256="} {
		if !strings.Contains(output, required) {
			t.Errorf("missing diagnostic %q: %s", required, output)
		}
	}
	if strings.Contains(output, "never-print-") || strings.Contains(output, "event-name") {
		t.Fatalf("snapshot diagnostics disclose raw non-timestamp values: %s", output)
	}
	if strings.Contains(issue2394SurfaceSnapshotDifference([][]sql.NullString{event}, [][]sql.NullString{event}), "column=") {
		t.Fatal("unchanged snapshot reported a column difference")
	}
	unknown := issue2394SurfaceSnapshotDifference(nil, [][]sql.NullString{{value("never-print-unknown-shape")}})
	if !strings.Contains(unknown, "unknown_width_1") || strings.Contains(unknown, "never-print-") {
		t.Fatalf("unknown snapshot shape is not safely captured: %s", unknown)
	}
}
