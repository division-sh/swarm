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
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRuntimeLogForkHistoryExclusionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, phase := range []string{"absent", "success", "dead_letter"} {
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				fixture := backend.open(t)
				store := fixture.store.(interface {
					diagnosticRuntimeLogFixtureStore
					PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
					MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
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
				if phase != "absent" {
					disposition := runtimepipelineobligation.Acknowledged("diagnostic_pipeline_fixture")
					if phase == "dead_letter" {
						failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "event_interceptor_failed", "runtime-log-test", "pipeline", nil), "runtime-log-test", "pipeline")
						disposition = runtimepipelineobligation.DeadLetter("event_interceptor_failed", &failure)
					}
					if err := runSelectedFixtureMutation(ctx, fixture.store, "diagnostic pipeline evidence", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						return commitRevisionedEventFixtureFactsForTest(txctx, fixture.store, attempt, log.ID(), runtimepipelineobligation.ScopeDirect, &disposition, log.CreatedAt())
					}); err != nil {
						t.Fatal(err)
					}
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
				if phase != "absent" && (!after.CommittedScopeFound || after.CommittedScope != string(runtimepipelineobligation.ScopeDirect) || after.PipelineReceiptCount != 1 || after.PipelineReceiptOutcome != phase) {
					t.Fatalf("physical diagnostic pipeline evidence lost: %+v", after)
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
				for _, pending := range latest.PendingWork {
					if pending.EventID == log.ID() {
						t.Errorf("diagnostic pipeline receipt became pending fork work: %+v", pending)
					}
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
				child, err := store.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
					SourceRunID: root.RunID(),
				})
				if err != nil || child.ForkRunID == "" || child.ForkPoint != original.ForkPoint {
					t.Fatalf("business cut could not materialize after diagnostic append: child=%+v err=%v", child, err)
				}
				childLogs, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: child.ForkRunID, Limit: 10})
				if err != nil || len(childLogs.Logs) != 0 {
					t.Fatalf("source diagnostics projected into child: logs=%+v err=%v", childLogs, err)
				}
				sourceLogs, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: root.RunID(), Limit: 10})
				if err != nil || !reflect.DeepEqual(sourceLogs, logs) {
					t.Errorf("child materialization changed source diagnostic readback: got=%+v want=%+v err=%v", sourceLogs, logs, err)
				}
			})
		}
	}
}
