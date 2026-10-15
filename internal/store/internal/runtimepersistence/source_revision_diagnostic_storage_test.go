package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunLifecycleProjectionPayloadsRetainPhysicalScopeDuplicatesAndBytesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store.(diagnosticRuntimeLogFixtureStore), testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			sibling := runForkSnapshotFixtureEvent(t, ctx, selected)
			if rows, err := ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, root.RunID()); err != nil || len(rows) != 0 {
				t.Fatalf("ordinary root was treated as a runtime log: %q/%v", rows, err)
			}
			for _, run := range []string{root.RunID(), root.RunID(), sibling.RunID()} {
				payload := []byte(`{"log_level":"info", "message":"physical receipt", "details":{"outbox_id":"duplicate"}}`)
				event := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, run, "", events.EventEnvelope{}, time.Now().UTC())
				if err := commitDiagnosticRuntimeLogFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
			}
			physical, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := DecodeSelectedForkSnapshotRowsForTest(physical["events"])
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			for _, row := range stored {
				run, runErr := row["run_id"].Text()
				name, nameErr := row["event_name"].Text()
				payload, payloadErr := row["payload"].Text()
				if runErr != nil || nameErr != nil || payloadErr != nil {
					t.Fatalf("physical payload oracle: %v/%v/%v", runErr, nameErr, payloadErr)
				}
				if run == root.RunID() && name == string(events.EventTypePlatformRuntimeLog) {
					want = append(want, payload)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, root.RunID())
			if err != nil || len(rows) != 2 {
				t.Fatalf("scope or duplicate physical log rows changed: %q/%v", rows, err)
			}
			var got []string
			for _, row := range rows {
				var value struct {
					Details map[string]any `json:"details"`
				}
				if err := json.Unmarshal(row, &value); err != nil || value.Details["outbox_id"] != "duplicate" {
					t.Fatalf("physical payload details changed: %s/%v", row, err)
				}
				got = append(got, string(row))
			}
			sort.Strings(want)
			sort.Strings(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("native payload bytes reconstructed: got=%q want=%q", got, want)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("run log read escaped original coordinator: %+v", counts)
			}
			rows[0][0] = '!'
			again, err := ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, root.RunID())
			if err != nil || len(again) != 2 || again[0][0] == '!' {
				t.Fatalf("detached payload mutation escaped: %q/%v", again, err)
			}
		})
	}
}

func TestSourceRevisionDiagnosticObservationsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, selected any, runID string) {
		t.Helper()
		if rows, err := ReadSourceRevisionDiagnosticStorageForTest(ctx, selected, runID); err == nil || rows != nil {
			t.Fatalf("refused source receipts retained evidence: %+v/%v", rows, err)
		}
		if rows, err := ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, runID); err == nil || rows != nil {
			t.Fatalf("refused logs retained evidence: %q/%v", rows, err)
		}
	}
	for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		refuse(context.Background(), owner, uuid.NewString())
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			runID := uuid.NewString()
			for _, invalid := range []string{"", "bad", uuid.Nil.String()} {
				refuse(ctx, selected, invalid)
			}
			if rows, err := ReadSourceRevisionDiagnosticStorageForTest(ctx, selected, runID); err != nil || len(rows) != 0 {
				t.Fatalf("empty source receipt scope fabricated evidence: %+v/%v", rows, err)
			}
			if rows, err := ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, runID); err != nil || len(rows) != 0 {
				t.Fatalf("empty log scope fabricated evidence: %q/%v", rows, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			refuse(cancelled, selected, runID)
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(ctx, selected, runID)
		})
	}
}
