package genericschedule_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type forkJoinNativeInventoryReader interface {
	ReadRunForkArrivalJoinScheduleInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]runtimegenericschedule.Activation, error)
}

// An exact individual row witness does not prove an exhaustive child set.
// This is persistence evidence, not a demonstrated execution-admission defect.
func TestForkJoinNativeExpectedRowEvidenceIsNotInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			for _, flow := range []string{".", "orders"} {
				t.Run(flow, func(t *testing.T) {
					ctx, request := forkJoinNativeRequest(t, f, flow, false, false)
					wanted := forkJoinInventoryMaterialize(t, f, ctx, request)
					extraRequest := forkJoinInventoryAdditionalRequest(t, f, ctx, request, "unexpected-join")
					extra := forkJoinInventoryMaterialize(t, f, ctx, extraRequest)
					actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, wanted.ID)
					if err != nil || !found {
						t.Fatalf("wanted native row: found=%t err=%v", found, err)
					}
					expected, err := request.Expected(wanted.ID)
					if err != nil {
						t.Fatal(err)
					}
					wantDigest, err := expected.EvidenceDigest()
					if err != nil {
						t.Fatal(err)
					}
					actualDigest, err := actual.EvidenceDigest()
					if err != nil || actualDigest != wantDigest {
						t.Fatalf("individual exact-row evidence no longer matches: err=%v", err)
					}
					unexpected, found, err := f.generic.LoadGenericScheduleActivation(ctx, extra.ID)
					if err != nil || !found || wanted.ID == extra.ID || unexpected.Command.RunID != request.Child.RunID ||
						unexpected.ForkJoinOrigin == nil || !reflect.DeepEqual(unexpected.Canonical(), extra.Canonical()) {
						t.Fatalf("second real inherited row not demonstrated in the same child: found=%t err=%v", found, err)
					}
					t.Log("exact wanted-row digest matches while another known inherited child row exists; no whole-operation admission claim")
				})
			}
		})
	}
}

func TestForkJoinNativeInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			reader, ok := f.pipeline.(forkJoinNativeInventoryReader)
			if !ok {
				t.Fatal("canonical pipeline owner lacks arrival-join inventory")
			}
			for _, flow := range []string{".", "orders"} {
				for _, shape := range []struct {
					name      string
					timeout   bool
					cancelled bool
				}{
					{name: "active_completion"},
					{name: "cancelled_completion", cancelled: true},
					{name: "active_timeout", timeout: true},
					{name: "cancelled_timeout", timeout: true, cancelled: true},
				} {
					t.Run(flow+"/"+shape.name, func(t *testing.T) {
						ctx, request := forkJoinNativeRequest(t, f, flow, shape.timeout, shape.cancelled)
						sourceBefore := request.Source.Canonical()
						forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID))
						ordinary := forkJoinInventoryOrdinaryChild(t, f, ctx, request, shape.cancelled)
						forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID))
						wanted := forkJoinInventoryMaterialize(t, f, ctx, request)
						forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID), wanted)
						extraRequest := forkJoinInventoryAdditionalRequest(t, f, ctx, request, "unexpected-join")
						extra := forkJoinInventoryMaterialize(t, f, ctx, extraRequest)
						rows := forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID)
						forkJoinInventoryRequireRows(t, rows, wanted, extra)
						for _, row := range rows {
							if row.ID == ordinary.ID || row.ID == request.Source.ID || row.ID == extraRequest.Source.ID {
								t.Fatal("inventory included an ordinary child schedule or an unrelated source row")
							}
						}
						if !shape.cancelled {
							wanted = forkJoinInventoryCancelProgress(t, f, reader, ctx, request, wanted, extra)
						}
						forkJoinInventoryRollback(t, f, reader, ctx, request, wanted, extra)
						forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID), wanted, extra)
						for _, source := range []runtimegenericschedule.Activation{sourceBefore, extraRequest.Source} {
							actual, found, err := f.generic.LoadGenericScheduleActivation(correlation.WithRunID(ctx, source.Command.RunID), source.ID)
							if err != nil || !found || !reflect.DeepEqual(source.Canonical(), actual.Canonical()) {
								t.Fatalf("child inventory/progress/rollback changed source: found=%t err=%v", found, err)
							}
						}
						actualOrdinary, found, err := f.generic.LoadGenericScheduleActivation(ctx, ordinary.ID)
						if err != nil || !found || !reflect.DeepEqual(ordinary.Canonical(), actualOrdinary.Canonical()) {
							t.Fatalf("inventory changed excluded ordinary child work: found=%t err=%v", found, err)
						}
					})
				}
			}
			t.Run("frame_source_refusals", func(t *testing.T) {
				ctx, request := forkJoinNativeRequest(t, f, ".", false, false)
				forkJoinInventoryRefusals(t, f, reader, ctx, request.Child.RunID, request.Source.Command.RunID)
				forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID))
			})
		})
	}
}

func forkJoinInventoryMaterialize(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkJoinRequest) runtimegenericschedule.Activation {
	t.Helper()
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		return f.pipeline.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, request)
	})
	return forkJoinNativeAcknowledged(t, result)
}

func forkJoinInventoryRead(t *testing.T, f *forkJoinNativeFixture, reader forkJoinNativeInventoryReader, ctx context.Context, childRunID string) []runtimegenericschedule.Activation {
	t.Helper()
	before, err := f.generic.ListActiveGenericScheduleActivations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rows []runtimegenericschedule.Activation
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		var err error
		rows, err = reader.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, childRunID)
		return runtimegenericschedule.Activation{}, err
	})
	forkJoinNativeAcknowledged(t, result)
	after, err := f.generic.ListActiveGenericScheduleActivations(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("successful inventory read minted or changed active work: err=%v", err)
	}
	return rows
}

func forkJoinInventoryRequireRows(t *testing.T, actual []runtimegenericschedule.Activation, expected ...runtimegenericschedule.Activation) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("inventory census=%d want=%d", len(actual), len(expected))
	}
	byID := make(map[string]runtimegenericschedule.Activation, len(expected))
	for _, row := range expected {
		if _, duplicate := byID[row.ID]; duplicate {
			t.Fatal("expected inventory repeats a native activation")
		}
		byID[row.ID] = row.Canonical()
	}
	for _, row := range actual {
		if err := row.Validate(); err != nil {
			t.Fatal(err)
		}
		want, found := byID[row.ID]
		if !found || row.ForkJoinOrigin == nil || !reflect.DeepEqual(want, row.Canonical()) {
			t.Fatalf("inventory lost exact status, due, mode, command or full origin: row=%+v", row)
		}
		delete(byID, row.ID)
	}
}

func forkJoinInventoryAdditionalRequest(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, base storegenericschedule.ForkJoinRequest, joinID string) storegenericschedule.ForkJoinRequest {
	t.Helper()
	command := forkJoinInventoryChangeDeclaration(t, base.Source.Command, joinID)
	sourceCtx := correlation.WithRunID(ctx, base.Source.Command.RunID)
	admission, err := f.generic.AdmitGenericScheduleOutcome(sourceCtx, command)
	if err != nil || !admission.Acknowledged || admission.Result.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("additional real source admission: result=%+v err=%v", admission, err)
	}
	request := base
	request.Source = admission.Result.Activation
	request.Child = forkJoinInventoryChangeDeclaration(t, base.Child, joinID)
	request.BornAt = runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
	if request.BornAt.Before(request.Source.AdmittedAt) {
		request.BornAt = request.Source.AdmittedAt
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func forkJoinInventoryChangeDeclaration(t *testing.T, command runtimegenericschedule.AdmissionCommand, joinID string) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	handle, ref, valid := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !valid {
		t.Fatal("additional schedule lost its typed arrival handle")
	}
	declaration, err := timeridentity.NewJoinRef(ref.Node(), ref.HandlerEvent(), ref.Stage(), joinID)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = declaration.BindStageEntry(ref.StageEntry(), ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if handle.Kind() == timeridentity.TimerHandleJoinComplete {
		handle, err = timeridentity.JoinCompleteHandle(ref)
	} else {
		handle, err = timeridentity.JoinTimeoutHandle(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	command.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	command.ScheduleKey, command.TaskID, command.EventType = handle.TaskID(), handle.TaskID(), handle.EventType()
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	return command
}

func forkJoinInventoryOrdinaryChild(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, base storegenericschedule.ForkJoinRequest, cancelled bool) runtimegenericschedule.Activation {
	t.Helper()
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey: "ordinary-inventory-control", RunID: base.Child.RunID, EntityID: base.Child.EntityID, FlowInstance: base.Child.FlowInstance,
		OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "inventory-control", EventType: "poll.tick", Payload: semanticvalue.EmptyObject(),
		RoutingSource: base.Child.RoutingSource, ExecutionMode: base.Child.ExecutionMode, Due: base.Child.Due,
	}
	admission, err := f.generic.AdmitGenericScheduleOutcome(ctx, command)
	if err != nil || !admission.Acknowledged || admission.Result.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("ordinary child admission: result=%+v err=%v", admission, err)
	}
	row := admission.Result.Activation
	if cancelled {
		row = forkJoinInventoryCancel(t, f, ctx, row)
	}
	if row.ForkJoinOrigin != nil {
		t.Fatal("ordinary control unexpectedly contains inherited lineage")
	}
	return row
}

func forkJoinInventoryCancel(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, row runtimegenericschedule.Activation) runtimegenericschedule.Activation {
	t.Helper()
	at := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
	if at.Before(row.AdmittedAt) {
		at = row.AdmittedAt
	}
	cancel, err := f.generic.CancelGenericScheduleOutcome(ctx, runtimegenericschedule.CancelCommand{ActivationID: row.ID, Cause: "join_stage_exit", CancelledAt: at})
	if err != nil || !cancel.Acknowledged || cancel.Result.Outcome != runtimegenericschedule.CancelChanged {
		t.Fatalf("real native cancellation: result=%+v err=%v", cancel, err)
	}
	return cancel.Result.Activation
}

func forkJoinInventoryCancelProgress(t *testing.T, f *forkJoinNativeFixture, reader forkJoinNativeInventoryReader, ctx context.Context, request storegenericschedule.ForkJoinRequest, original, extra runtimegenericschedule.Activation) runtimegenericschedule.Activation {
	t.Helper()
	progressed := forkJoinInventoryCancel(t, f, ctx, original)
	forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, request.Child.RunID), progressed, extra)
	expected, err := request.Expected(original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := progressed.ValidateForkJoinReplay(expected); err != nil {
		t.Fatalf("canonical replay owner rejected lawful cancellation progress: %v", err)
	}
	wantDigest, err := expected.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	actualDigest, err := progressed.EvidenceDigest()
	if err != nil || actualDigest == wantDigest {
		t.Fatalf("strict at-cut evidence did not distinguish cancellation progress: err=%v", err)
	}
	return progressed
}

func forkJoinInventoryRollback(t *testing.T, f *forkJoinNativeFixture, reader forkJoinNativeInventoryReader, ctx context.Context, base storegenericschedule.ForkJoinRequest, wanted, extra runtimegenericschedule.Activation) {
	t.Helper()
	request := forkJoinInventoryAdditionalRequest(t, f, ctx, base, "rolled-back-join")
	abort := errors.New("abort after complete native inventory")
	var childID string
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		created, err := f.pipeline.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, request)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		childID = created.ID
		rows, err := reader.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, request.Child.RunID)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		forkJoinInventoryRequireRows(t, rows, wanted, extra, created)
		return runtimegenericschedule.Activation{}, abort
	})
	if !errors.Is(result.Err(), abort) || result.Acknowledged() || childID == "" {
		t.Fatalf("rollback did not follow real materialization/inventory: child=%q acknowledged=%t err=%v", childID, result.Acknowledged(), result.Err())
	}
	if _, found, err := f.generic.LoadGenericScheduleActivation(ctx, childID); err != nil || found {
		t.Fatalf("rolled-back inherited row survived: found=%t err=%v", found, err)
	}
	forkJoinInventoryRequireRows(t, forkJoinInventoryRead(t, f, reader, ctx, base.Child.RunID), wanted, extra)
	source, found, err := f.generic.LoadGenericScheduleActivation(correlation.WithRunID(ctx, request.Source.Command.RunID), request.Source.ID)
	if err != nil || !found || !reflect.DeepEqual(source.Canonical(), request.Source.Canonical()) {
		t.Fatalf("child rollback changed its real source row: found=%t err=%v", found, err)
	}
}

func forkJoinInventoryRefusals(t *testing.T, f *forkJoinNativeFixture, reader forkJoinNativeInventoryReader, ctx context.Context, childRunID, sourceRunID string) {
	t.Helper()
	if rows, err := reader.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, nil, childRunID); err == nil || len(rows) != 0 {
		t.Fatal("inventory accepted no native attempt")
	}
	foreignFact := sourceartifactfixture.FactFor(sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# foreign source\n")))
	missingRun := uuid.NewString()
	for _, refusal := range []struct {
		name string
		ctx  context.Context
		run  string
	}{
		{"wrong_context", correlation.WithRunID(ctx, sourceRunID), childRunID},
		{"wrong_source", correlation.WithSourceArtifactFact(ctx, foreignFact), childRunID},
		{"missing_run", correlation.WithRunID(ctx, missingRun), missingRun},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			before, err := f.generic.ListActiveGenericScheduleActivations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var rows []runtimegenericschedule.Activation
			result := f.run(refusal.ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
				var err error
				rows, err = reader.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, refusal.run)
				return runtimegenericschedule.Activation{}, err
			})
			if result.Err() == nil || result.Acknowledged() || len(rows) != 0 {
				t.Fatal("inventory acknowledged a foreign frame/source or missing run")
			}
			after, err := f.generic.ListActiveGenericScheduleActivations(ctx)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused inventory mutated or minted active work: err=%v", err)
			}
		})
	}
	var retired *mutationprotocol.Attempt
	var retiredCtx context.Context
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		retired, retiredCtx = attempt, ctx
		_, err := reader.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, childRunID)
		return runtimegenericschedule.Activation{}, err
	})
	forkJoinNativeAcknowledged(t, result)
	if rows, err := reader.ReadRunForkArrivalJoinScheduleInventoryTx(retiredCtx, retired, childRunID); err == nil || len(rows) != 0 {
		t.Fatal("inventory reused a retired native SQL frame")
	}
}
