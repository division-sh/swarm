package genericschedule_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type forkJoinOccurrenceNativePreparer interface {
	PrepareGenericScheduleOccurrence(context.Context, runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparationCommit, error)
}

// Persistence-only preservation: preparation stamps an actual native occurrence,
// but this test installs no executor, publication, or firing evidence.
func TestForkJoinNativeOccurrenceCancellationChronologyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			preparer, ok := f.generic.(forkJoinOccurrenceNativePreparer)
			if !ok {
				t.Fatal("canonical generic owner lacks occurrence preparation")
			}
			for _, flow := range []string{".", "orders"} {
				for _, handle := range []struct {
					name    string
					timeout bool
				}{
					{name: "completion"},
					{name: "timeout", timeout: true},
				} {
					for _, disposition := range []string{"equal", "later", "without_occurrence"} {
						t.Run(flow+"/"+handle.name+"/"+disposition, func(t *testing.T) {
							ctx, request := forkJoinNativeRequest(t, f, flow, handle.timeout, false)
							child := forkJoinNativeRestore(t, f, ctx, request)
							ordinary := forkJoinInventoryOrdinaryChild(t, f, ctx, request, false)
							before := child
							if disposition != "without_occurrence" {
								prepared := forkJoinOccurrenceNativePrepare(t, f, preparer, ctx, child)
								before = prepared.Activation
							}
							at := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
							if disposition == "equal" {
								at = before.CurrentEventAdmittedAt
							} else if disposition == "later" && !at.After(before.CurrentEventAdmittedAt) {
								t.Fatal("actual cancellation clock did not advance beyond native occurrence admission")
							}
							forkJoinOccurrenceNativeCancel(t, f, preparer, ctx, request, before, at)
							forkJoinOccurrenceNativeUnchanged(t, f, correlation.WithRunID(ctx, request.Source.Command.RunID), request.Source)
							forkJoinOccurrenceNativeUnchanged(t, f, ctx, ordinary)
						})
					}
				}
			}
		})
	}
}

func TestForkJoinNativeBeforeDueCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			preparer, ok := f.generic.(forkJoinOccurrenceNativePreparer)
			if !ok {
				t.Fatal("canonical generic owner lacks occurrence preparation")
			}
			for _, flow := range []string{".", "orders"} {
				t.Run(flow, func(t *testing.T) {
					ctx, base := forkJoinNativeRequest(t, f, flow, true, false)
					request := forkJoinOccurrenceNativeFutureTimeout(t, f, ctx, base)
					child := forkJoinNativeRestore(t, f, ctx, request)
					ordinary := forkJoinInventoryOrdinaryChild(t, f, ctx, request, false)
					at := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
					if !at.Before(child.CurrentDueAt) || child.CurrentEventID != "" || !child.CurrentEventAdmittedAt.IsZero() {
						t.Fatal("before-due control lacks a genuinely future unpublished timeout")
					}
					forkJoinOccurrenceNativeCancel(t, f, preparer, ctx, request, child, at)
					sourceCtx := correlation.WithRunID(ctx, base.Source.Command.RunID)
					forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, base.Source)
					forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, request.Source)
					forkJoinOccurrenceNativeUnchanged(t, f, ctx, ordinary)
				})
			}
		})
	}
}

// Persistence-only failure admission through the canonical native owner. There
// is no executor, failed publication, or fabricated accepted-event evidence.
func TestForkJoinNativeOccurrenceFailureChronologyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			preparer, ok := f.generic.(forkJoinOccurrenceNativePreparer)
			if !ok {
				t.Fatal("canonical generic owner lacks occurrence preparation")
			}
			for _, flow := range []string{".", "orders"} {
				for _, handle := range []struct {
					name    string
					timeout bool
				}{
					{name: "completion"},
					{name: "timeout", timeout: true},
				} {
					for _, disposition := range []string{"equal", "later"} {
						t.Run(flow+"/"+handle.name+"/"+disposition, func(t *testing.T) {
							ctx, request := forkJoinNativeRequest(t, f, flow, handle.timeout, false)
							child := forkJoinNativeRestore(t, f, ctx, request)
							ordinary := forkJoinInventoryOrdinaryChild(t, f, ctx, request, false)
							prepared := forkJoinOccurrenceNativePrepare(t, f, preparer, ctx, child)
							before := prepared.Activation
							if !child.AdmittedAt.Before(before.CurrentEventAdmittedAt) {
								t.Fatal("actual child birth is not earlier than native occurrence admission")
							}
							rejected := forkJoinOccurrenceNativeFailureWrite(f, ctx, before, child.AdmittedAt)
							if rejected.Err() == nil || rejected.Acknowledged() || !strings.Contains(rejected.Err().Error(), "terminal disposition precedes its retained occurrence admission") {
								t.Fatalf("before-occurrence native failure was not refused by canonical chronology: acknowledged=%t err=%v", rejected.Acknowledged(), rejected.Err())
							}
							forkJoinOccurrenceNativeUnchanged(t, f, ctx, before)
							forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, f.pipeline, ctx, request.Child.RunID), before)
							sourceCtx := correlation.WithRunID(ctx, request.Source.Command.RunID)
							forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, request.Source)
							forkJoinOccurrenceNativeUnchanged(t, f, ctx, ordinary)
							at := before.CurrentEventAdmittedAt
							if disposition == "later" {
								at = runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
								if !at.After(before.CurrentEventAdmittedAt) {
									t.Fatal("actual failure clock did not advance beyond native occurrence admission")
								}
							}
							forkJoinOccurrenceNativeFailurePersist(t, f, preparer, ctx, request, before, at)
							forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, request.Source)
							forkJoinOccurrenceNativeUnchanged(t, f, ctx, ordinary)
						})
					}
				}
				t.Run(flow+"/timeout/without_occurrence_before_due", func(t *testing.T) {
					ctx, base := forkJoinNativeRequest(t, f, flow, true, false)
					request := forkJoinOccurrenceNativeFutureTimeout(t, f, ctx, base)
					child := forkJoinNativeRestore(t, f, ctx, request)
					ordinary := forkJoinInventoryOrdinaryChild(t, f, ctx, request, false)
					at := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
					if !at.Before(child.CurrentDueAt) || child.CurrentEventID != "" || !child.CurrentEventAdmittedAt.IsZero() {
						t.Fatal("before-due failure control lacks a genuinely future unpublished timeout")
					}
					forkJoinOccurrenceNativeFailurePersist(t, f, preparer, ctx, request, child, at)
					sourceCtx := correlation.WithRunID(ctx, base.Source.Command.RunID)
					forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, base.Source)
					forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, request.Source)
					forkJoinOccurrenceNativeUnchanged(t, f, ctx, ordinary)
				})
			}
		})
	}
}

func forkJoinOccurrenceNativeFailureWrite(f *forkJoinNativeFixture, ctx context.Context, before runtimegenericschedule.Activation, at time.Time) mutationprotocol.Result[runtimegenericschedule.Activation] {
	return f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		postgres := f.postgres != nil
		loaded, found, err := storegenericschedule.LoadActivationTx(ctx, attempt, postgres, before.ID)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		if !found || !reflect.DeepEqual(before.Canonical(), loaded.Canonical()) {
			return runtimegenericschedule.Activation{}, errors.New("native failure lost its exact current activation")
		}
		return storegenericschedule.FailActivationTx(ctx, attempt, postgres, loaded,
			"native_preservation", "native failure chronology preservation", at)
	})
}

func forkJoinOccurrenceNativeFailurePersist(t *testing.T, f *forkJoinNativeFixture, preparer forkJoinOccurrenceNativePreparer, ctx context.Context, request storegenericschedule.ForkJoinRequest, before runtimegenericschedule.Activation, at time.Time) {
	t.Helper()
	actual := forkJoinNativeAcknowledged(t, forkJoinOccurrenceNativeFailureWrite(f, ctx, before, at))
	expected := before
	expected.Status, expected.FailedAt = runtimegenericschedule.StatusFailed, at
	expected.Failure = runtimegenericschedule.Failure{Code: "native_preservation", Message: "native failure chronology preservation"}
	if err := actual.Validate(); err != nil {
		t.Fatalf("canonical chronology rejected a lawful native failure: %v", err)
	}
	if !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
		t.Fatal("native failure lost exact occurrence, cause, absolute due, or provenance")
	}
	atCut, err := request.Expected(actual.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := actual.ValidateForkJoinReplay(atCut); err != nil {
		t.Fatalf("shared replay owner rejected lawful native failure progress: %v", err)
	}
	forkJoinOccurrenceNativeUnchanged(t, f, ctx, actual)
	forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, f.pipeline, ctx, request.Child.RunID), actual)
	wakeup, err := runtimegenericschedule.NewWakeup(actual.ID, actual.CurrentDueAt)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := preparer.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || !terminal.Acknowledged || terminal.Result.Outcome != runtimegenericschedule.PrepareTerminal ||
		terminal.Result.Occurrence != (runtimegenericschedule.Occurrence{}) || !reflect.DeepEqual(actual.Canonical(), terminal.Result.Activation.Canonical()) {
		t.Fatalf("failed native schedule prepared or lost an occurrence: result=%+v err=%v", terminal, err)
	}
	forkJoinOccurrenceNativeUnchanged(t, f, ctx, actual)
}

func forkJoinOccurrenceNativePrepare(t *testing.T, f *forkJoinNativeFixture, preparer forkJoinOccurrenceNativePreparer, ctx context.Context, child runtimegenericschedule.Activation) runtimegenericschedule.PreparedOccurrence {
	t.Helper()
	wakeup, err := runtimegenericschedule.NewWakeup(child.ID, child.CurrentDueAt)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := preparer.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || !commit.Acknowledged || commit.Result.Outcome != runtimegenericschedule.PrepareReady {
		t.Fatalf("native occurrence preparation: result=%+v err=%v", commit, err)
	}
	prepared := commit.Result
	if err := prepared.Validate(); err != nil {
		t.Fatal(err)
	}
	if prepared.Occurrence.AdmittedAt.Before(child.AdmittedAt) || prepared.Occurrence.AdmittedAt.Before(child.CurrentDueAt) ||
		prepared.Activation.CurrentEventID != prepared.Occurrence.EventID || !prepared.Activation.CurrentEventAdmittedAt.Equal(prepared.Occurrence.AdmittedAt) {
		t.Fatal("native preparation lost exact occurrence identity or lawful admission chronology")
	}
	expected := child
	expected.CurrentEventID, expected.CurrentEventAdmittedAt = prepared.Occurrence.EventID, prepared.Occurrence.AdmittedAt
	if !reflect.DeepEqual(expected.Canonical(), prepared.Activation.Canonical()) {
		t.Fatal("native preparation changed more than occurrence identity and admission")
	}
	forkJoinOccurrenceNativeUnchanged(t, f, ctx, prepared.Activation)
	reused, err := preparer.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || !reused.Acknowledged || !reflect.DeepEqual(prepared, reused.Result) {
		t.Fatalf("native preparation did not reuse the exact occurrence: result=%+v err=%v", reused, err)
	}
	return prepared
}

func forkJoinOccurrenceNativeCancel(t *testing.T, f *forkJoinNativeFixture, preparer forkJoinOccurrenceNativePreparer, ctx context.Context, request storegenericschedule.ForkJoinRequest, before runtimegenericschedule.Activation, at time.Time) {
	t.Helper()
	if at.Before(before.AdmittedAt) || !before.CurrentEventAdmittedAt.IsZero() && at.Before(before.CurrentEventAdmittedAt) {
		t.Fatal("cancellation fixture precedes actual native admission")
	}
	handle, _, ok := timeridentity.ParseJoinHandle(before.Command.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("native cancellation lacks its canonical typed join handle")
	}
	cause := "join_stage_exit"
	if handle.Kind() == timeridentity.TimerHandleJoinTimeout {
		cause = "join_closed"
	}
	command := runtimegenericschedule.CancelCommand{ActivationID: before.ID, Cause: cause, CancelledAt: at}
	commit, err := f.generic.CancelGenericScheduleOutcome(ctx, command)
	if err != nil || !commit.Acknowledged || commit.Result.Outcome != runtimegenericschedule.CancelChanged {
		t.Fatalf("lawful native cancellation: result=%+v err=%v", commit, err)
	}
	expected := before
	expected.Status, expected.CancelCause, expected.CancelledAt = runtimegenericschedule.StatusCancelled, cause, at
	actual := commit.Result.Activation
	if err := actual.Validate(); err != nil {
		t.Fatalf("canonical chronology rejected a lawful native writer: %v", err)
	}
	if !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
		t.Fatal("native cancellation lost occurrence, absolute due, declaration, or provenance")
	}
	atCut, err := request.Expected(actual.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := actual.ValidateForkJoinReplay(atCut); err != nil {
		t.Fatalf("shared replay owner rejected lawful native cancellation progress: %v", err)
	}
	forkJoinOccurrenceNativeUnchanged(t, f, ctx, actual)
	read := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		inventory, err := f.pipeline.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, request.Child.RunID)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		if len(inventory) != 1 || !reflect.DeepEqual(actual.Canonical(), inventory[0].Canonical()) {
			t.Fatal("native inherited inventory lost the exact cancellation chronology")
		}
		return inventory[0], nil
	})
	forkJoinNativeAcknowledged(t, read)
	wakeup, err := runtimegenericschedule.NewWakeup(actual.ID, actual.CurrentDueAt)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := preparer.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || !stale.Acknowledged || stale.Result.Outcome != runtimegenericschedule.PrepareStaleCancelled ||
		stale.Result.Occurrence != (runtimegenericschedule.Occurrence{}) || !reflect.DeepEqual(actual.Canonical(), stale.Result.Activation.Canonical()) {
		t.Fatalf("canceled native schedule prepared or lost an occurrence: result=%+v err=%v", stale, err)
	}
	retried, err := f.generic.CancelGenericScheduleOutcome(ctx, command)
	if err != nil || !retried.Acknowledged || retried.Result.Outcome != runtimegenericschedule.CancelTerminal || !reflect.DeepEqual(actual.Canonical(), retried.Result.Activation.Canonical()) {
		t.Fatalf("terminal native cancellation changed retained history: result=%+v err=%v", retried, err)
	}
	forkJoinOccurrenceNativeUnchanged(t, f, ctx, actual)
}

func forkJoinOccurrenceNativeFutureTimeout(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, base storegenericschedule.ForkJoinRequest) storegenericschedule.ForkJoinRequest {
	t.Helper()
	_, sourceRef, ok := timeridentity.ParseJoinHandle(base.Source.Command.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("future source timeout lacks its canonical typed arrival reference")
	}
	_, childRef, ok := timeridentity.ParseJoinHandle(base.Child.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("future child timeout lacks its canonical typed arrival reference")
	}
	declaration, err := timeridentity.NewJoinRef(sourceRef.Node(), sourceRef.HandlerEvent(), sourceRef.Stage(), "before-due")
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err = declaration.BindStageEntry(sourceRef.StageEntry(), sourceRef.Generation())
	if err != nil {
		t.Fatal(err)
	}
	childRef, err = declaration.BindStageEntry(childRef.StageEntry(), childRef.Generation())
	if err != nil {
		t.Fatal(err)
	}
	armedAt := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
	join, err := joinruntime.NewActivation(sourceRef, []string{"member-a"}, nil, armedAt, armedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	sourceCommand, err := runtimegenericschedule.WorkflowJoinAdmission(join, base.Source.Command.ExecutionMode)
	if err != nil {
		t.Fatal(err)
	}
	sourceCtx := correlation.WithRunID(ctx, base.Source.Command.RunID)
	admission, err := f.generic.AdmitGenericScheduleOutcome(sourceCtx, sourceCommand)
	if err != nil || !admission.Acknowledged || admission.Result.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("actual future source timeout admission: result=%+v err=%v", admission, err)
	}
	childJoin, err := join.WithForkReference(childRef)
	if err != nil {
		t.Fatal(err)
	}
	childCommand, err := runtimegenericschedule.WorkflowJoinAdmission(childJoin, base.Child.ExecutionMode)
	if err != nil {
		t.Fatal(err)
	}
	request := base
	request.Source, request.Child = admission.Result.Activation, childCommand
	request.BornAt = runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	forkJoinOccurrenceNativeUnchanged(t, f, sourceCtx, request.Source)
	return request
}

func forkJoinOccurrenceNativeUnchanged(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, expected runtimegenericschedule.Activation) {
	t.Helper()
	actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, expected.ID)
	if err != nil || !found || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
		t.Fatalf("canonical native readback changed the exact activation: id=%s found=%t err=%v", expected.ID, found, err)
	}
}
