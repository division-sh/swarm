package schemastore

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

type retryAttemptColumns struct {
	closure   string
	outcome   *string
	completed *time.Time
	due       any
	open      bool
	duration  *int64
}

func TestRetryDecisionGeneratedSchemaCombinationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := bootstrappedForkPointSchemaDB(t, backend)
			runID, eventID, bundleHash := seedForkPointSchemaRunAndEvent(t, db)
			completed := time.Date(2026, 10, 15, 12, 0, 1, 123456000, time.UTC)
			due, earlier := completed.Add(time.Hour), completed.Add(-time.Microsecond)
			var duration int64
			for _, test := range []struct {
				name  string
				row   retryAttemptColumns
				field string
			}{
				{"retry_valid", retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), &completed, due, false, &duration}, ""},
				{"retry_null_due", retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), &completed, nil, false, &duration}, "retry_eligible_at"},
				{"retry_null_completion", retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), nil, due, false, &duration}, "completed_at"},
				{"settled_null_outcome_due_present", retryAttemptColumns{"settled", nil, &completed, due, false, &duration}, "outcome"},
				{"settled_null_outcome_due_null", retryAttemptColumns{"settled", nil, &completed, nil, false, &duration}, "outcome"},
				{"retry_due_equal", retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), &completed, completed, false, &duration}, "retry_eligible_at"},
				{"retry_due_earlier", retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), &completed, earlier, false, &duration}, "retry_eligible_at"},
				{"retry_wrong_closure", retryAttemptColumns{"parent_interrupted", retrySchemaText("retry_scheduled"), &completed, due, false, &duration}, "closure_kind"},
				{"open_null", retryAttemptColumns{"open", nil, nil, nil, true, nil}, ""},
				{"open_null_outcome_with_due", retryAttemptColumns{"open", nil, nil, due, true, nil}, "retry_eligible_at"},
				{"expired_null", retryAttemptColumns{"lease_expired", retrySchemaText("lease_expired"), &completed, nil, false, nil}, ""},
				{"expired_due", retryAttemptColumns{"lease_expired", retrySchemaText("lease_expired"), &completed, due, false, nil}, "retry_eligible_at"},
				{"interrupted_null", retryAttemptColumns{"parent_interrupted", retrySchemaText("terminalized"), &completed, nil, false, &duration}, ""},
				{"interrupted_due", retryAttemptColumns{"parent_interrupted", retrySchemaText("terminalized"), &completed, due, false, &duration}, "retry_eligible_at"},
				{"delivered_null", retryAttemptColumns{"settled", retrySchemaText("delivered"), &completed, nil, false, &duration}, ""},
				{"delivered_due", retryAttemptColumns{"settled", retrySchemaText("delivered"), &completed, due, false, &duration}, "retry_eligible_at"},
				{"dead_letter_null", retryAttemptColumns{"settled", retrySchemaText("dead_letter"), &completed, nil, false, &duration}, ""},
				{"dead_letter_due", retryAttemptColumns{"settled", retrySchemaText("dead_letter"), &completed, due, false, &duration}, "retry_eligible_at"},
				{"canceled_null", retryAttemptColumns{"settled", retrySchemaText("canceled"), &completed, nil, false, &duration}, ""},
				{"canceled_due", retryAttemptColumns{"settled", retrySchemaText("canceled"), &completed, due, false, &duration}, "retry_eligible_at"},
				{"terminalized_null", retryAttemptColumns{"settled", retrySchemaText("terminalized"), &completed, nil, false, &duration}, ""},
				{"terminalized_due", retryAttemptColumns{"settled", retrySchemaText("terminalized"), &completed, due, false, &duration}, "retry_eligible_at"},
			} {
				t.Run(test.name, func(t *testing.T) {
					tx, deliveryID := beginRetrySchemaCell(t, db, runID, eventID, bundleHash, test.row.open)
					err := insertRetrySchemaAttempt(tx, deliveryID, test.row)
					if test.field == "" {
						if err != nil {
							t.Fatalf("valid prerequisite/control rejected: %v", err)
						}
						if err := tx.Commit(); err != nil {
							t.Fatalf("valid control failed deferred constraints: %v", err)
						}
						return
					}
					if rollbackErr := tx.Rollback(); rollbackErr != nil {
						t.Fatalf("rollback invalid schema cell: %v (insert: %v)", rollbackErr, err)
					}
					requireRetrySchemaCheck(t, db, backend, err, test.field)
					requireRetrySchemaCellAbsent(t, db, deliveryID)
				})
			}
		})
	}
}

func TestRetryDecisionMalformedStoredDueIsNotRepairedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := bootstrappedForkPointSchemaDB(t, backend)
			runID, eventID, bundleHash := seedForkPointSchemaRunAndEvent(t, db)
			tx, deliveryID := beginRetrySchemaCell(t, db, runID, eventID, bundleHash, false)
			completed := time.Date(2026, 10, 15, 12, 0, 1, 123456000, time.UTC)
			var duration int64
			err := insertRetrySchemaAttempt(tx, deliveryID, retryAttemptColumns{"settled", retrySchemaText("retry_scheduled"), &completed, "not-a-time", false, &duration})
			if backend == "postgres" {
				var typed *pq.Error
				if !errors.As(err, &typed) || typed.Code != "22007" {
					t.Fatalf("malformed typed timestamp did not fail timestamp admission: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("SQLite TEXT control failed before canonical decoding: %v", err)
				}
				adapter, err := delivery.NewAdapter(delivery.DialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := adapter.Outcomes(context.Background(), tx, deliveryID); err == nil || !strings.Contains(err.Error(), "parse delivery timestamp") {
					t.Fatalf("malformed stored due was repaired or failed for another reason: %v", err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			requireRetrySchemaCellAbsent(t, db, deliveryID)
		})
	}
}

func beginRetrySchemaCell(t *testing.T, db *sql.DB, runID, eventID, bundleHash string, open bool) (*sql.Tx, string) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	deliveryID := uuid.NewString()
	started := time.Date(2026, 10, 15, 12, 0, 0, 123456000, time.UTC)
	route := "delivery-route-v2:sha256:" + strings.Repeat("a", 32) + strings.ReplaceAll(deliveryID, "-", "")
	if _, err := tx.ExecContext(ctx, `INSERT INTO event_deliveries
		(delivery_id,run_id,event_id,route_identity,subscriber_type,subscriber_id,
		agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,
		delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,
		execution_authority_kind,authority_bundle_hash,execution_authority_id,execution_authority_generation,
		status,retry_count,max_retries,next_eligible_at,claim_version,created_at,updated_at)
		VALUES ($1,$2,$3,$4,'node','schema-worker','','','','','','','{}','{}','{}','{}',
		'normal_runtime',$5,'schema-proof',1,'pending',0,3,$6,0,$6,$6)`, deliveryID, runID, eventID, route, bundleHash, started); err != nil {
		t.Fatalf("valid delivery prerequisite: %v", err)
	}
	if open {
		if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status='in_progress',next_eligible_at=NULL,
			claim_version=1,current_attempt_version=1,current_attempt_open=TRUE,started_at=$1 WHERE delivery_id=$2`, started, deliveryID); err != nil {
			t.Fatalf("valid open-attempt parent: %v", err)
		}
	}
	return tx, deliveryID
}

func insertRetrySchemaAttempt(tx *sql.Tx, deliveryID string, row retryAttemptColumns) error {
	started := time.Date(2026, 10, 15, 12, 0, 0, 123456000, time.UTC)
	var current, reason any
	if row.open {
		current = deliveryID
	}
	if row.outcome != nil && *row.outcome == "canceled" {
		reason = "terminate"
	}
	_, err := tx.ExecContext(context.Background(), `INSERT INTO event_delivery_attempts
		(delivery_id,claim_version,claim_token,started_at,lease_expires_at,current_delivery_id,open_marker,
		closure_kind,outcome,duration_ms,completed_at,retry_eligible_at,reason_code)
		VALUES ($1,1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, deliveryID, uuid.NewString(), started, started.Add(time.Minute),
		current, row.open, row.closure, row.outcome, row.duration, row.completed, row.due, reason)
	return err
}

func requireRetrySchemaCheck(t *testing.T, db *sql.DB, backend string, err error, field string) {
	t.Helper()
	if backend == "postgres" {
		var typed *pq.Error
		if !errors.As(err, &typed) || typed.Code != "23514" || typed.Table != "event_delivery_attempts" {
			t.Fatalf("expected attempt CHECK, not FK/type/prerequisite refusal: %v", err)
		}
		var definition string
		if readErr := db.QueryRowContext(context.Background(), `SELECT pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conrelid='event_delivery_attempts'::regclass AND conname=$1`, typed.Constraint).Scan(&definition); readErr != nil {
			t.Fatal(readErr)
		}
		if !strings.Contains(definition, field) {
			t.Fatalf("wrong CHECK refused field %s: constraint=%s definition=%s", field, typed.Constraint, definition)
		}
		return
	}
	var typed *sqlite.Error
	if !errors.As(err, &typed) || typed.Code() != 275 || !strings.Contains(err.Error(), field) {
		t.Fatalf("expected CHECK on %s, not FK/type/prerequisite refusal: %v", field, err)
	}
}

func requireRetrySchemaCellAbsent(t *testing.T, db *sql.DB, deliveryID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT
		(SELECT COUNT(*) FROM event_deliveries WHERE delivery_id=$1)+
		(SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1)`, deliveryID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled-back schema cell leaked rows: count=%d err=%v", count, err)
	}
}

func retrySchemaText(value string) *string { return &value }
