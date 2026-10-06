package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func runtimeConnectorActivityRows(ctx context.Context, selected any, runID, tool string) ([]storetest.ActivityAttemptStorageEvidence, error) {
	rows, err := storetest.ReadActivityAttemptStorage(ctx, selected, runID)
	if err != nil {
		return nil, err
	}
	var matches []storetest.ActivityAttemptStorageEvidence
	for _, row := range rows {
		if row.Tool == tool {
			matches = append(matches, row)
		}
	}
	return matches, nil
}

func TestRuntimeConnectorJournalWitnessesPreserveToolSourceAndEarliestAttemptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				storetest.RunFixtureStore
				pipeline.ActivityAttemptJournal
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t)
			}
			ctx := testAuthorActivityContext(context.Background())
			runID, otherRun, source, otherSource := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, run := range []string{runID, otherRun} {
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: run, Origin: storetest.ScenarioSetupOrigin()})
			}
			at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
			var first string
			for index, identity := range []struct{ run, tool, source string }{
				{runID, "other.tool", source}, {runID, "telegram.send_message", otherSource},
				{runID, "telegram.send_message", source}, {runID, "telegram.send_message", source},
				{runID, "telegram.send_message", ""}, {otherRun, "telegram.send_message", source},
			} {
				record := pipeline.ActivityAttemptRecord{RequestEventID: uuid.NewString(), RunID: identity.run,
					SourceEventID: identity.source, ExecutionMode: executionmode.Live, ActivityID: uuid.NewString(),
					Tool: identity.tool, EffectClass: "non_idempotent_write", Attempt: 1,
					SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "exact-storage-input",
					StartedAt: at.Add(time.Duration(index) * time.Minute)}
				stored, inserted, err := selected.StartActivityAttempt(ctx, record)
				if err != nil || !inserted {
					t.Fatalf("start original journal: inserted=%t err=%v", inserted, err)
				}
				if index == 2 {
					first = stored.RequestEventID
				}
			}
			rows, err := runtimeConnectorActivityRows(ctx, selected, runID, "telegram.send_message")
			if err != nil || len(rows) != 4 {
				t.Fatalf("run-wide tool cardinality: %#v %v", rows, err)
			}
			rows, err = runtimeConnectorActivityRowsForSource(ctx, selected, runID, "telegram.send_message", source)
			if err != nil || len(rows) != 2 || rows[0].RequestEventID != first {
				t.Fatalf("exact source/earliest ordering or duplicate collapse: %#v %v", rows, err)
			}
			stored, found, err := selected.LoadActivityAttempt(ctx, rows[0].RequestEventID)
			if err != nil || !found || stored.RunID != runID || stored.SourceEventID != source || stored.Tool != "telegram.send_message" {
				t.Fatalf("selected request lost its complete journal owner: %+v %t %v", stored, found, err)
			}
			if rows, err := runtimeConnectorActivityRowsForSource(ctx, selected, runID, "telegram.send_message", uuid.NewString()); err != nil || len(rows) != 0 {
				t.Fatalf("absent source fabricated attempt: %#v %v", rows, err)
			}
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				if rows, err := runtimeConnectorActivityRowsForSource(ctx, selected, runID, "telegram.send_message", invalid); err == nil || rows != nil {
					t.Fatalf("invalid source broadened journal selection: %#v %v", rows, err)
				}
			}
		})
	}
}

func runtimeConnectorActivityRowsForSource(ctx context.Context, selected any, runID, tool, sourceEventID string) ([]storetest.ActivityAttemptStorageEvidence, error) {
	id, err := uuid.Parse(sourceEventID)
	if err != nil || id == uuid.Nil || id.String() != sourceEventID {
		return nil, fmt.Errorf("connector journal readback requires an exact canonical source event")
	}
	rows, err := runtimeConnectorActivityRows(ctx, selected, runID, tool)
	if err != nil {
		return nil, err
	}
	var matches []storetest.ActivityAttemptStorageEvidence
	for _, row := range rows {
		if row.SourceEventID == sourceEventID {
			matches = append(matches, row)
		}
	}
	return matches, nil
}
