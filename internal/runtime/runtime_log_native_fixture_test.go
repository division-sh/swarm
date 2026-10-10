package runtime

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type RuntimeLogNativeFixtureForTest struct {
	Persistence RuntimeLogPersistence
	Deliveries  deliverylifecycle.Store
	Runs        interface {
		runlifecycle.OperationOwner
		runlifecycle.CandidateStore
	}
	Context         context.Context
	RequireRun      func(context.Context, string) error
	RunSnapshot     func(context.Context, string) (runlifecycle.Snapshot, error)
	LatestLog       func(context.Context) (events.Event, error)
	StartupDecision func(context.Context) (events.Event, error)
	RunPresence     func(context.Context, string) (bool, error)
	RemoveArtifact  func(context.Context, string) (int64, error)
	AppendFault     func(context.Context, string, bool) error
	PublishSubject  func(context.Context, events.Event)
	PublishDelivery func(context.Context, events.Event, []events.DeliveryRoute, deliverylifecycle.ExecutionAuthority) events.Event
	Physical        func(context.Context) RuntimeLogPhysicalForTest
	FanOutCapacity  startupownership.FanOutCapacity
}

type RuntimeLogPhysicalForTest struct {
	Runs, Events, Deliveries, Entities, Receipts int
}

func requireNativeRuntimeLogRunForTest(t *testing.T, fixture RuntimeLogNativeFixtureForTest, ctx context.Context, runID string) runlifecycle.Snapshot {
	t.Helper()
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.RunSnapshot(ctx, runID)
	if err != nil || snapshot.RunID != runID || !snapshot.Origin.Equal(runlifecycle.ScenarioSetupRunOrigin()) {
		t.Fatalf("native log precondition lost exact scenario run: snapshot=%+v err=%v", snapshot, err)
	}
	return snapshot
}

func requireNativeRuntimeLogRunPreservedForTest(t *testing.T, fixture RuntimeLogNativeFixtureForTest, ctx context.Context, before runlifecycle.Snapshot) {
	t.Helper()
	after, err := fixture.RunSnapshot(ctx, before.RunID)
	if err != nil || after.RunID != before.RunID || !after.Origin.Equal(before.Origin) || after.BundleHash != before.BundleHash || after.State != before.State || !after.StartedAt.Equal(before.StartedAt) {
		t.Fatalf("diagnostic logging changed source/origin/status/start: before=%+v after=%+v err=%v", before, after, err)
	}
}

// The factory takes the already-admitted immutable source, not a database.
type RuntimeLogNativeOpenerForTest func(*testing.T, *sourceartifact.AdmittedSourceArtifact) RuntimeLogNativeFixtureForTest

func nativeRuntimeRecoveryLogFixtureForTest(t *testing.T, open RuntimeLogNativeOpenerForTest, source semanticview.Source) RuntimeLogNativeFixtureForTest {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("recovery log fixture requires its exact admitted workflow source")
	}
	return open(t, bundle.SourceArtifact)
}

func nativeRuntimeLogSourceFactForTest(t *testing.T, fixture RuntimeLogNativeFixtureForTest) correlation.SourceArtifactFact {
	t.Helper()
	fact, ok := correlation.SourceArtifactFactFromContext(fixture.Context)
	if !ok || fact.Validate() != nil {
		t.Fatal("native runtime log fixture requires its admitted source fact")
	}
	return fact
}

func exactNativeRuntimeLogRecordForTest(t *testing.T, runID string, mode executionmode.Mode, message string) RuntimeLogPersistenceRecord {
	t.Helper()
	record, _, err := EncodeRuntimeLogRecord(RuntimeLogFacts{
		EventID: uuid.NewString(), CreatedAt: time.Unix(1700000000, 123456000).UTC(), RunID: runID, ExecutionMode: mode,
	}, RuntimeLogEntry{Level: "info", Message: message, Component: "runtime", Action: "native_identity"})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.InExecutionMode(eventtest.DiagnosticDirect(record.EventID, events.EventTypePlatformRuntimeLog, "runtime", "", record.Payload, 0, runID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, record.CreatedAt), mode)
	record.PayloadAdmission, err = eventtest.PayloadAdmission(event, "", string(event.Type()))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func VerifyRuntimeLogNativeIdentityReplayAndConflictForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	for _, shape := range []string{"runless", "running", "cancelled"} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			t.Run(shape+"/"+string(mode), func(t *testing.T) {
				fixture := open(t, admittedRuntimeLogSourceArtifactForTest(t))
				ctx, runID := fixture.Context, ""
				var beforeRun runlifecycle.Snapshot
				if shape != "runless" {
					runID = uuid.NewString()
					beforeRun = requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
					if shape == "cancelled" {
						var err error
						beforeRun, _, err = fixture.Runs.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				record := exactNativeRuntimeLogRecordForTest(t, runID, mode, "exact durable diagnostic")
				before := fixture.Physical(ctx)
				if err := fixture.Persistence.PersistRuntimeLog(ctx, record); err != nil {
					t.Fatal(err)
				}
				stored, err := fixture.LatestLog(ctx)
				if err != nil || stored.ID() != record.EventID || !stored.CreatedAt().Equal(record.CreatedAt) || stored.RunID() != runID || stored.ExecutionMode() != mode || !bytes.Equal(stored.Payload(), record.Payload) || stored.AdmissionClass() != events.EventAdmissionDiagnosticDirect {
					t.Fatalf("native diagnostic replaced exact facts: stored=%+v record=%+v err=%v", stored, record, err)
				}
				after := fixture.Physical(ctx)
				want := before
				want.Events++
				if after != want {
					t.Fatalf("diagnostic created/routed work: before=%+v after=%+v", before, after)
				}
				if err := fixture.Persistence.PersistRuntimeLog(ctx, record); err != nil || fixture.Physical(ctx) != after {
					t.Fatalf("exact replay mutated storage: %v", err)
				}
				conflict := exactNativeRuntimeLogRecordForTest(t, runID, mode, "contradicting diagnostic")
				conflict.EventID = record.EventID
				if err := fixture.Persistence.PersistRuntimeLog(ctx, conflict); !errors.Is(err, events.ErrEventIdentityConflict) || fixture.Physical(ctx) != after {
					t.Fatalf("identity conflict lost fail-closed storage: %v", err)
				}
				if runID != "" {
					requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, beforeRun)
					current, err := fixture.RunSnapshot(ctx, runID)
					if err != nil || current.EventCount != beforeRun.EventCount+1 || !reflect.DeepEqual(current.EndedAt, beforeRun.EndedAt) {
						t.Fatalf("replay changed exact counter/terminal cut: before=%+v after=%+v err=%v", beforeRun, current, err)
					}
				}
			})
		}
	}
}

func VerifyRuntimeLoggerNativeArtifactLossCutsForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	for _, shape := range []string{"missing", "running", "cancelled", "runless"} {
		t.Run(shape, func(t *testing.T) {
			artifact := admittedRuntimeLogSourceArtifactForTest(t)
			fixture := open(t, artifact)
			ctx, runID := fixture.Context, ""
			var beforeRun runlifecycle.Snapshot
			if shape != "runless" {
				runID = uuid.NewString()
				ctx = correlation.WithRunID(ctx, runID)
				if shape != "missing" {
					beforeRun = requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
					if shape == "cancelled" {
						var err error
						beforeRun, _, err = fixture.Runs.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if n, err := fixture.RemoveArtifact(ctx, artifact.BundleHash()); err != nil || n != 1 {
				t.Fatalf("named original-writer source loss: %d/%v", n, err)
			}
			before := fixture.Physical(ctx)
			recorder := bus.NewEmittedEventsRecorder()
			ctx = bus.WithEmittedEventsRecorder(ctx, recorder)
			err := newTestRuntimeLogger(fixture.Persistence).Log(ctx, RuntimeLogEntry{Level: "info", Message: "diagnostic after source loss", Component: "runtime", Action: "source_loss"})
			if shape == "missing" {
				if !errors.Is(err, runlifecycle.ErrRunNotFound) || fixture.Physical(ctx) != before || len(recorder.SnapshotFlightRecorder()) != 0 {
					t.Fatalf("missing-run log resurrected work or acknowledged refusal: %v", err)
				}
			} else {
				want := before
				want.Events++
				if err != nil || fixture.Physical(ctx) != want || len(recorder.SnapshotFlightRecorder()) != 1 {
					t.Fatalf("existing/runless log re-admitted unavailable artifact: %v", err)
				}
				if runID != "" {
					requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, beforeRun)
				}
			}
			beforeCreate := fixture.Physical(ctx)
			fact, _ := correlation.SourceArtifactFactFromContext(ctx)
			_, err = fixture.Runs.CreateRun(ctx, runlifecycle.CreateRequest{RunID: uuid.NewString(), Source: fact, Origin: runlifecycle.ScenarioSetupRunOrigin(), StartedAt: time.Now().UTC()})
			if !errors.Is(err, runlifecycle.ErrSourceArtifactUnavailable) || fixture.Physical(ctx) != beforeCreate {
				t.Fatalf("new run crossed deleted-source admission: %v", err)
			}
		})
	}
}

func VerifyRuntimeLoggerNativeCancellationRefusesMutationForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	fixture := open(t, admittedRuntimeLogSourceArtifactForTest(t))
	before := fixture.Physical(fixture.Context)
	ctx, cancel := context.WithCancel(fixture.Context)
	cancel()
	recorder := bus.NewEmittedEventsRecorder()
	err := newTestRuntimeLogger(fixture.Persistence).Log(bus.WithEmittedEventsRecorder(ctx, recorder), RuntimeLogEntry{Level: "info", Message: "cancelled native diagnostic"})
	if !errors.Is(err, context.Canceled) || fixture.Physical(fixture.Context) != before || len(recorder.SnapshotFlightRecorder()) != 0 {
		t.Fatalf("cancelled native writer mutated storage or recorder: %v", err)
	}
	for _, read := range []func(context.Context) (events.Event, error){fixture.LatestLog, fixture.StartupDecision} {
		if event, err := read(ctx); !errors.Is(err, context.Canceled) || event.ID() != "" {
			t.Fatalf("cancelled native read returned partial evidence: %s/%v", event.ID(), err)
		}
	}
	runID := uuid.NewString()
	if snapshot, err := fixture.RunSnapshot(ctx, runID); !errors.Is(err, context.Canceled) || snapshot.RunID != "" {
		t.Fatalf("cancelled native lifecycle read returned evidence: %+v/%v", snapshot, err)
	}
	if n, err := fixture.RemoveArtifact(ctx, nativeRuntimeLogSourceFactForTest(t, fixture).BundleHash()); !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("cancelled source fault mutated storage: %d/%v", n, err)
	}
	if err := fixture.AppendFault(ctx, runID, true); !errors.Is(err, context.Canceled) || fixture.Physical(fixture.Context) != before {
		t.Fatalf("cancelled append fault escaped writer admission: %v", err)
	}
}

func VerifyRuntimeLoggerNativeAppendRollbackForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	fixture := open(t, admittedRuntimeLogSourceArtifactForTest(t))
	runID := uuid.NewString()
	ctx := correlation.WithRunID(fixture.Context, runID)
	beforeRun := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
	before := fixture.Physical(ctx)
	if err := fixture.AppendFault(ctx, runID, true); err != nil {
		t.Fatal(err)
	}
	installed := true
	t.Cleanup(func() {
		if installed {
			if err := fixture.AppendFault(context.WithoutCancel(ctx), runID, false); err != nil {
				t.Errorf("restore exact native append cut: %v", err)
			}
		}
	})
	recorder := bus.NewEmittedEventsRecorder()
	logger := newTestRuntimeLogger(fixture.Persistence)
	entry := RuntimeLogEntry{Level: "info", Message: "native append rollback", Component: "runtime", Action: "append_cut"}
	err := logger.Log(bus.WithEmittedEventsRecorder(ctx, recorder), entry)
	if err == nil || !strings.Contains(err.Error(), "runtime_log_exact_append_cut") || fixture.Physical(ctx) != before || len(recorder.SnapshotFlightRecorder()) != 0 {
		t.Fatalf("native post-append failure mutated storage/recorder or missed cut: %v", err)
	}
	afterRun, readErr := fixture.RunSnapshot(ctx, runID)
	if readErr != nil || !reflect.DeepEqual(afterRun, beforeRun) {
		t.Fatalf("native rollback changed lifecycle/counters: before=%+v after=%+v err=%v", beforeRun, afterRun, readErr)
	}
	if err := fixture.AppendFault(ctx, runID, false); err != nil {
		t.Fatal(err)
	}
	installed = false
	if err := logger.Log(bus.WithEmittedEventsRecorder(ctx, recorder), entry); err != nil || len(recorder.SnapshotFlightRecorder()) != 1 {
		t.Fatalf("restored native writer failed: %v", err)
	}
	want := before
	want.Events++
	if fixture.Physical(ctx) != want {
		t.Fatal("restored diagnostic created or routed work")
	}
}

func VerifyRuntimeLoggerNativeCrossRunLineageRefusalForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	fixture := open(t, admittedRuntimeLogSourceArtifactForTest(t))
	ctx := fixture.Context
	runID, otherRunID, subjectID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
	requireNativeRuntimeLogRunForTest(t, fixture, ctx, otherRunID)
	fixture.PublishSubject(ctx, eventtest.PersistedChildForProducer(subjectID, events.EventType("validation/validation.package_ready"), eventtest.Producer(events.EventProducerAgent, "runtime.run_fork.selected_contract_execution"), "", []byte(`{}`), 0, otherRunID, eventtest.UUID("diagnostic-subject-parent:"+subjectID), events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC()))
	if parent, err := fixture.Persistence.RuntimeLogLineageParentEventID(ctx, runID, "", subjectID); err != nil || parent != "" {
		t.Fatalf("lineage lookup leaked another run's subject: %q/%v", parent, err)
	}
	before := fixture.Physical(ctx)
	recorder := bus.NewEmittedEventsRecorder()
	ctx = bus.WithEmittedEventsRecorder(correlation.WithRunID(ctx, runID), recorder)
	ctx = correlation.WithRuntimeLineage(ctx, correlation.RuntimeLineage{Owner: "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage", RunID: runID, ParentEventID: subjectID, SubjectEventID: subjectID, SubjectEventType: "validation/validation.package_ready", RowCategory: correlation.RuntimeLineageRowCategoryDiagnostic})
	err := newTestRuntimeLogger(fixture.Persistence).Log(ctx, RuntimeLogEntry{Level: "info", Message: "cross-run lineage must refuse"})
	if err == nil || !strings.Contains(err.Error(), "causal source") || fixture.Physical(ctx) != before || len(recorder.SnapshotFlightRecorder()) != 0 {
		t.Fatalf("cross-run diagnostic crossed canonical lineage/zero-write gate: %v", err)
	}
}
