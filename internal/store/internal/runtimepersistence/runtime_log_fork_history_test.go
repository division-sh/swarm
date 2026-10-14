package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRuntimeLogForkHistoryExclusionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(interface {
				diagnosticRuntimeLogFixtureStore
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
				ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
			})
			ctx := testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, root.RunID()); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, root.RunID(), root.ID())
			if err != nil {
				t.Fatal(err)
			}
			original, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: root.RunID()})
			if err != nil || original.ForkPoint.EventID != root.ID() {
				t.Fatalf("original business cut: plan=%+v err=%v", original, err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			log := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "",
				[]byte(`{"log_level":"warn","message":"fork ledger exclusion","details":{"component":"issue2592","action":"proof"}}`),
				0, root.RunID(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitDiagnosticRuntimeLogFixture(ctx, store, log); err != nil {
				t.Fatal(err)
			}
			if got := probe.Snapshot(); got.Total.Revision.Finalizations != 0 {
				t.Errorf("diagnostic append finalized fork history: %+v", got)
			}
			after, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, root.RunID(), log.ID())
			if err != nil || !after.RecordFound || after.Record.EventName != string(events.EventTypePlatformRuntimeLog) || after.RunEventCount != before.RunEventCount+1 {
				t.Fatalf("physical diagnostic/count lost: before=%+v after=%+v err=%v", before, after, err)
			}
			if after.RevisionCount != before.RevisionCount {
				t.Errorf("diagnostic append advanced revision count: got=%d want=%d", after.RevisionCount, before.RevisionCount)
			}
			if err := commitDiagnosticRuntimeLogFixture(ctx, store, log); err != nil {
				t.Fatal(err)
			}
			replayed, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, root.RunID(), log.ID())
			if err != nil || !reflect.DeepEqual(replayed, after) {
				t.Fatalf("diagnostic replay changed physical or historical evidence: got=%+v want=%+v err=%v", replayed, after, err)
			}
			logs, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: root.RunID(), Limit: 10})
			if err != nil || len(logs.Logs) != 1 || logs.Logs[0].LogID != log.ID() {
				t.Fatalf("operator diagnostic readback: logs=%+v err=%v", logs, err)
			}
			// A later broad capture must not turn the physical log into a fork cut.
			if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, root.RunID()); err != nil {
				t.Fatal(err)
			}
			latest, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: root.RunID()})
			if err != nil || latest.ForkPoint != original.ForkPoint {
				t.Errorf("diagnostic became the latest cut: got=%+v want=%+v err=%v", latest.ForkPoint, original.ForkPoint, err)
			}
			if _, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: root.RunID(), At: log.ID()}); err == nil || !strings.Contains(err.Error(), "not found in revisioned source run") {
				t.Errorf("explicit diagnostic cut admitted or misclassified: %v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := commitDiagnosticRuntimeLogFixture(cancelled, store, log); err == nil {
				t.Error("cancelled diagnostic append succeeded")
			}
			preserved, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, root.RunID(), log.ID())
			if err != nil || !reflect.DeepEqual(preserved, after) {
				t.Errorf("broad capture/refusal changed diagnostic evidence: got=%+v want=%+v err=%v", preserved, after, err)
			}
		})
	}
}
