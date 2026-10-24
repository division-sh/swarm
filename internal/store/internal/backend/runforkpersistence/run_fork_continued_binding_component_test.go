package runforkpersistence

import (
	"context"

	"database/sql/driver"

	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"

	"github.com/google/uuid"
)

func TestRunForkFanOutReplayMembershipExactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE run_fork_delivery_event_replays (fork_run_id TEXT,source_run_id TEXT,source_event_id TEXT,
					source_delivery_id TEXT,fork_event_id TEXT,fork_delivery_id TEXT)`,
				`CREATE TABLE event_deliveries (delivery_id TEXT,run_id TEXT,event_id TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			sourceRun, childRun, sourceEvent := uuid.NewString(), uuid.NewString(), uuid.NewString()
			sourceA, sourceB, sourceExtra := uuid.NewString(), uuid.NewString(), uuid.NewString()
			childA, childB, childExtra := uuid.NewString(), uuid.NewString(), uuid.NewString()
			childEvent := deterministicRunForkReplayEventID(childRun, sourceEvent)
			for _, test := range []struct {
				name      string
				mutation  string
				args      []any
				sourceIDs []string
				accept    bool
			}{
				{name: "exact_two_recipients", sourceIDs: []string{sourceB, sourceA}, accept: true},
				{name: "missing_lineage", mutation: `DELETE FROM run_fork_delivery_event_replays WHERE source_delivery_id=$1`, args: []any{sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "extra_lineage", mutation: `INSERT INTO run_fork_delivery_event_replays VALUES ($1,$2,$3,$4,$5,$6)`, args: []any{childRun, sourceRun, sourceEvent, sourceExtra, childEvent, childExtra}, sourceIDs: []string{sourceA, sourceB}},
				{name: "duplicate_source", mutation: `INSERT INTO run_fork_delivery_event_replays VALUES ($1,$2,$3,$4,$5,$6)`, args: []any{childRun, sourceRun, sourceEvent, sourceA, childEvent, childExtra}, sourceIDs: []string{sourceA, sourceB}},
				{name: "wrong_source_run", mutation: `UPDATE run_fork_delivery_event_replays SET source_run_id=$1 WHERE source_delivery_id=$2`, args: []any{uuid.NewString(), sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "wrong_source_event", mutation: `UPDATE run_fork_delivery_event_replays SET source_event_id=$1 WHERE source_delivery_id=$2`, args: []any{uuid.NewString(), sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "wrong_fork_run", mutation: `UPDATE run_fork_delivery_event_replays SET fork_run_id=$1 WHERE source_delivery_id=$2`, args: []any{uuid.NewString(), sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "wrong_child_event", mutation: `UPDATE run_fork_delivery_event_replays SET fork_event_id=$1 WHERE source_delivery_id=$2`, args: []any{uuid.NewString(), sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "duplicate_child", mutation: `UPDATE run_fork_delivery_event_replays SET fork_delivery_id=$1 WHERE source_delivery_id=$2`, args: []any{childA, sourceB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "missing_child", mutation: `DELETE FROM event_deliveries WHERE delivery_id=$1`, args: []any{childB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "foreign_child_run", mutation: `UPDATE event_deliveries SET run_id=$1 WHERE delivery_id=$2`, args: []any{uuid.NewString(), childB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "foreign_child_event", mutation: `UPDATE event_deliveries SET event_id=$1 WHERE delivery_id=$2`, args: []any{uuid.NewString(), childB}, sourceIDs: []string{sourceA, sourceB}},
				{name: "unowned_child", mutation: `INSERT INTO event_deliveries VALUES ($1,$2,$3)`, args: []any{childExtra, childRun, childEvent}, sourceIDs: []string{sourceA, sourceB}},
				{name: "duplicate_expected_source", sourceIDs: []string{sourceA, sourceA}},
				{name: "omitted_expected_source", sourceIDs: []string{sourceA}},
				{name: "extra_expected_source", sourceIDs: []string{sourceA, sourceB, sourceExtra}},
				{name: "empty_expected_partition"},
			} {
				t.Run(test.name, func(t *testing.T) {
					ctx := context.Background()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					for i, sourceID := range []string{sourceA, sourceB} {
						childID := []string{childA, childB}[i]
						if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_delivery_event_replays VALUES ($1,$2,$3,$4,$5,$6)`,
							childRun, sourceRun, sourceEvent, sourceID, childEvent, childID); err != nil {
							t.Fatal(err)
						}
						if _, err := tx.ExecContext(ctx, `INSERT INTO event_deliveries VALUES ($1,$2,$3)`, childID, childRun, childEvent); err != nil {
							t.Fatal(err)
						}
					}
					if test.mutation != "" {
						if _, err := tx.ExecContext(ctx, test.mutation, test.args...); err != nil {
							t.Fatal(err)
						}
					}
					got, err := requireRunForkFanOutReplayMembershipTx(ctx, tx, sourceRun, childRun, runfork.RunForkFanOutPendingReplay{
						Ordinal: 0, SourceEventID: sourceEvent, SourceDeliveryIDs: test.sourceIDs,
					})
					if (err == nil) != test.accept || (test.accept && got != childEvent) {
						t.Fatalf("exact replay event=%q err=%v, accept=%t", got, err, test.accept)
					}
				})
			}
		})
	}
}

func continuedBindingComponentKey(deployment bool) fanoutobligation.IntentKey {
	key := fanoutobligation.IntentKey{RunID: uuid.NewString()}
	if deployment {
		key.DeploymentFeedID = uuid.NewString()
	} else {
		key.TriggeringDeliveryID = uuid.NewString()
		key.ElementRef = contracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "scatter/handler/items.ready/fan_out"}
	}
	return key
}

func TestRunForkDeploymentContinuedPendingReadbackRejectsUnownedAssociation(t *testing.T) {
	for _, test := range []struct {
		name      string
		omit      bool
		field     int
		value     driver.Value
		duplicate bool
		accept    bool
	}{
		{name: "prebinding_absence", omit: true, accept: true},
		{name: "unowned_continued", field: -1},
		{name: "wrong_child", field: 2, value: uuid.NewString()},
		{name: "wrong_source", field: 3, value: uuid.NewString()},
		{name: "lost_retained", field: 4, value: nil},
		{name: "wrong_retained", field: 4, value: "succeeded"},
		{name: "committed_both_identities", field: 1, value: "committed"},
		{name: "failure", field: 5, value: `{"class":"internal_failure"}`},
		{name: "duplicate_pending", omit: true, duplicate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			key := continuedBindingComponentKey(true)
			pending := runfork.RunForkFanOutPendingReplay{Ordinal: 0, SourceEventID: uuid.NewString(), InheritedDisposition: fanoutobligation.InheritedCanceled}
			carriage := forkDeploymentCarriage{inherit: true, cursor: 1, sourceRunID: uuid.NewString(),
				pendingDeployment: []runfork.RunForkDeploymentPendingEvent{{Ordinal: pending.Ordinal, SourceEventID: pending.SourceEventID,
					InheritedDisposition: pending.InheritedDisposition}},
			}
			childEvent := activityidentity.ForkLineageEventID(key.RunID, pending.SourceEventID)
			if test.duplicate {
				carriage.pendingDeployment = append(carriage.pendingDeployment, carriage.pendingDeployment[0])
			}
			rows := sqlmock.NewRows([]string{"ordinal", "kind", "child", "source", "retained", "failure", "created"})
			if !test.omit {
				row := []driver.Value{0, "continued", childEvent, pending.SourceEventID, "canceled", nil, time.Unix(200, 0).UTC()}
				if test.field >= 0 {
					row[test.field] = test.value
				}
				rows.AddRow(row...)
			}
			if !test.duplicate {
				mock.ExpectQuery(`SELECT ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at`).
					WithArgs(key.RunID, key.DeploymentFeedID).WillReturnRows(rows)
			}
			if test.omit && !test.duplicate || test.name == "unowned_continued" {
				mock.ExpectQuery(`FROM run_fork_selected_contract_executions`).WithArgs(key.RunID, pending.SourceEventID).
					WillReturnRows(sqlmock.NewRows([]string{"event", "source", "owner"}))
				mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM events`).WithArgs(key.RunID, childEvent, deterministicRunForkReplayEventID(key.RunID, pending.SourceEventID)).
					WillReturnRows(sqlmock.NewRows([]string{"orphan"}).AddRow(false))
			}
			err := requireRunForkDeploymentOutcomesTx(context.Background(), tx, false, key, durabledata.PinnedSource{RowCount: 1}, 1, carriage)
			if (err == nil) != test.accept {
				t.Fatalf("deployment continued association err=%v accept=%t", err, test.accept)
			}
		})
	}
}
