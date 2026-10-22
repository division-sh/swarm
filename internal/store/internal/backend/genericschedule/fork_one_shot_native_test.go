package genericschedule_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestForkOneShotNativeRestoreBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			for _, delay := range []bool{false, true} {
				for _, status := range []runtimegenericschedule.Status{runtimegenericschedule.StatusActive, runtimegenericschedule.StatusCancelled, runtimegenericschedule.StatusFailed} {
					t.Run(fmt.Sprintf("delay=%t/%s", delay, status), func(t *testing.T) {
						ctx, request := forkOneShotNativeRequest(t, f, delay, status)
						before := request.Source.Canonical()
						child := forkOneShotNativeRestore(t, f, ctx, request)
						expected, err := request.Expected(child.ID)
						if err != nil || !reflect.DeepEqual(child.Canonical(), expected.Canonical()) || !child.InitialDueAt.Before(child.AdmittedAt) {
							t.Fatalf("native inherited delay/disposition: child=%+v expected=%+v err=%v", child, expected, err)
						}
						if replay := forkOneShotNativeRestore(t, f, ctx, request); !reflect.DeepEqual(child.Canonical(), replay.Canonical()) {
							t.Fatal("exact restoration replay changed child")
						}
						f.generic.(interface{ SetNowFnForTest(func() time.Time) }).SetNowFnForTest(func() time.Time { return request.BornAt.Add(24 * time.Hour) })
						ordinary, err := f.generic.AdmitGenericScheduleOutcome(ctx, request.Child)
						if err != nil || !ordinary.Acknowledged || ordinary.Result.Outcome != runtimegenericschedule.AdmissionExactReplay || !reflect.DeepEqual(child.Canonical(), ordinary.Result.Activation.Canonical()) {
							t.Fatalf("fresh ordinary retry rewrote relative intent: result=%+v err=%v", ordinary, err)
						}
						forkOneShotNativeRefusals(t, f, ctx, request, child)
						nextRun := uuid.NewString()
						nextCtx := forkJoinNativeRunContext(t, f, nextRun)
						next := storegenericschedule.ForkOneShotRequest{Source: child, Child: forkOneShotNativeChild(t, child.Command, nextRun),
							PointKind: forkpoint.DeploymentRevision, PointRevision: 11, BornAt: request.BornAt.Add(time.Hour)}
						refork := forkOneShotNativeRestore(t, f, nextCtx, next)
						if refork.ForkOneShotOrigin.SourceActivationID != child.ID || refork.ForkOneShotOrigin.SourceRunID != child.Command.RunID ||
							!refork.ForkOneShotOrigin.DueBasisAt.Equal(before.AdmittedAt) || !refork.CurrentDueAt.Equal(before.CurrentDueAt) {
							t.Fatal("native recursive fork restarted delay or lost immediate source")
						}
						forkOneShotNativeRollback(t, f, request)
						source, found, err := f.generic.LoadGenericScheduleActivation(ctx, before.ID)
						if err != nil || !found || !reflect.DeepEqual(source.Canonical(), before.Canonical()) {
							t.Fatalf("restoration changed source history: found=%t err=%v", found, err)
						}
					})
				}
			}
		})
	}
}

func TestForkOneShotNativeSharedInventorySeparatelyValidatesBothOrigins(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			ctx, joinRequest := forkJoinNativeRequest(t, f, ".", true, false)
			join := forkJoinNativeRestore(t, f, ctx, joinRequest)
			_, oneShotRequest := forkOneShotNativeRequest(t, f, true, runtimegenericschedule.StatusActive)
			oneShotRequest.Child = forkOneShotNativeChild(t, oneShotRequest.Source.Command, joinRequest.Child.RunID)
			result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
				oneShot, err := storegenericschedule.RestoreForkOneShotTx(ctx, attempt, f.postgres != nil, oneShotRequest)
				if err != nil {
					return runtimegenericschedule.Activation{}, err
				}
				rows, err := storegenericschedule.ReadForkJoinInventoryTx(ctx, attempt, f.postgres != nil, joinRequest.Child.RunID)
				if err != nil {
					return runtimegenericschedule.Activation{}, err
				}
				if len(rows) != 2 {
					return runtimegenericschedule.Activation{}, fmt.Errorf("shared inventory lost an inherited origin variant")
				}
				joinCount, oneShotCount := 0, 0
				for _, row := range rows {
					if row.ForkJoinOrigin != nil {
						joinCount++
						if !reflect.DeepEqual(row.Canonical(), join.Canonical()) {
							return runtimegenericschedule.Activation{}, fmt.Errorf("shared inventory weakened join projection")
						}
					}
					if row.ForkOneShotOrigin != nil {
						oneShotCount++
						if !reflect.DeepEqual(row.Canonical(), oneShot.Canonical()) {
							return runtimegenericschedule.Activation{}, fmt.Errorf("shared inventory weakened one-shot projection")
						}
					}
				}
				if joinCount != 1 || oneShotCount != 1 {
					return runtimegenericschedule.Activation{}, fmt.Errorf("shared inventory misclassified origin variants")
				}
				return oneShot, nil
			})
			forkJoinNativeAcknowledged(t, result)
		})
	}
}

func forkOneShotNativeRequest(t *testing.T, f *forkJoinNativeFixture, delay bool, status runtimegenericschedule.Status) (context.Context, storegenericschedule.ForkOneShotRequest) {
	t.Helper()
	sourceRun, childRun := uuid.NewString(), uuid.NewString()
	sourceCtx, childCtx := forkJoinNativeRunContext(t, f, sourceRun), forkJoinNativeRunContext(t, f, childRun)
	arm := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	f.generic.(interface{ SetNowFnForTest(func() time.Time) }).SetNowFnForTest(func() time.Time { return arm })
	routing, err := events.NewRootRoutingSource(sourceRun)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.FromGo(map[string]any{"message": "native original payload", "count": 3})
	if err != nil {
		t.Fatal(err)
	}
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey: "native-one-shot", TaskID: "native-one-shot", RunID: sourceRun, EntityID: sourceRun,
		OwnerKind: runtimegenericschedule.OwnerAgent, OwnerID: "agent-a",
		AgentIdentity: agentidentitytest.RootDeclaredForRun(t, sourceRun, "agent-a", "test/agents.yaml"),
		EventType:     "platform.generic_schedule_proof", Payload: payload, RoutingSource: routing,
		ExecutionMode: executionmode.Live, Due: runtimegenericschedule.AbsoluteDue(arm.Add(30 * time.Minute)),
	}
	if delay {
		command.Due = runtimegenericschedule.DelayDue(30 * time.Minute)
	}
	admitted, err := f.generic.AdmitGenericScheduleOutcome(sourceCtx, command)
	if err != nil || !admitted.Acknowledged || admitted.Result.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("source native admission: result=%+v err=%v", admitted, err)
	}
	source := admitted.Result.Activation
	if status == runtimegenericschedule.StatusCancelled {
		cancelled, err := f.generic.CancelGenericScheduleOutcome(sourceCtx, runtimegenericschedule.CancelCommand{
			ActivationID: source.ID, Cause: "retained_cancel", CancelledAt: arm.Add(time.Minute),
		})
		if err != nil || !cancelled.Acknowledged || cancelled.Result.Outcome != runtimegenericschedule.CancelChanged {
			t.Fatalf("source native cancellation: result=%+v err=%v", cancelled, err)
		}
		source = cancelled.Result.Activation
	}
	if status == runtimegenericschedule.StatusFailed {
		result := f.run(sourceCtx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
			loaded, found, err := storegenericschedule.LoadActivationTx(ctx, attempt, f.postgres != nil, source.ID)
			if err != nil || !found {
				return runtimegenericschedule.Activation{}, fmt.Errorf("failure source readback: found=%t err=%w", found, err)
			}
			return storegenericschedule.FailActivationTx(ctx, attempt, f.postgres != nil, loaded, "retained_failure", "source failure evidence", arm.Add(time.Minute))
		})
		source = forkJoinNativeAcknowledged(t, result)
	}
	request := storegenericschedule.ForkOneShotRequest{Source: source, Child: forkOneShotNativeChild(t, source.Command, childRun),
		PointKind: forkpoint.DeploymentRevision, PointRevision: 7, BornAt: arm.Add(time.Hour)}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return childCtx, request
}

func forkOneShotNativeChild(t *testing.T, source runtimegenericschedule.AdmissionCommand, childRun string) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	child := source
	var err error
	child.AgentIdentity, err = agentidentity.New(childRun, source.AgentIdentity.Name, source.AgentIdentity.Route)
	if err != nil {
		t.Fatal(err)
	}
	child.RoutingSource, err = runfork.ProjectProducerOwnership(source.RunID, childRun, source.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	if source.EntityID == source.RunID {
		child.EntityID = childRun
	}
	child.RunID = childRun
	return child
}

func forkOneShotNativeRestore(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkOneShotRequest) runtimegenericschedule.Activation {
	t.Helper()
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		child, err := storegenericschedule.RestoreForkOneShotTx(ctx, attempt, f.postgres != nil, request)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		rows, err := storegenericschedule.ReadForkJoinInventoryTx(ctx, attempt, f.postgres != nil, request.Child.RunID)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		if len(rows) != 1 || rows[0].ForkOneShotOrigin == nil || rows[0].ForkJoinOrigin != nil || !reflect.DeepEqual(rows[0].Canonical(), child.Canonical()) {
			return runtimegenericschedule.Activation{}, fmt.Errorf("native shared inventory lost exact one-shot row")
		}
		return child, nil
	})
	return forkJoinNativeAcknowledged(t, result)
}

func forkOneShotNativeRefusals(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, r storegenericschedule.ForkOneShotRequest, original runtimegenericschedule.Activation) {
	t.Helper()
	for _, variant := range []string{"cut", "command", "prepared", "recurring"} {
		wrong := r
		switch variant {
		case "cut":
			wrong.PointRevision++
		case "command":
			wrong.Source.Command.Due = runtimegenericschedule.AbsoluteDue(r.Source.InitialDueAt.Add(time.Second))
			wrong.Source.InitialDueAt, wrong.Source.CurrentDueAt = wrong.Source.Command.Due.Absolute, wrong.Source.Command.Due.Absolute
			wrong.Source.ImmutableHash, _ = wrong.Source.Command.ImmutableHash()
			wrong.Child.Due = wrong.Source.Command.Due
		case "prepared":
			wrong.Source.CurrentEventID = runtimegenericschedule.OccurrenceEventID(wrong.Source.ID, wrong.Source.CurrentDueAt)
			wrong.Source.CurrentEventAdmittedAt = wrong.Source.CurrentDueAt
		case "recurring":
			wrong.Source.Command.Due, wrong.Child.Due = runtimegenericschedule.EveryDue(30*time.Minute), runtimegenericschedule.EveryDue(30*time.Minute)
			wrong.Source.ImmutableHash, _ = wrong.Source.Command.ImmutableHash()
		}
		result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
			return storegenericschedule.RestoreForkOneShotTx(ctx, attempt, f.postgres != nil, wrong)
		})
		if result.Err() == nil || result.Acknowledged() || variant == "command" && !runtimegenericschedule.IsConflict(result.Err()) {
			t.Fatalf("%s restoration was not refused/rolled back: err=%v", variant, result.Err())
		}
		actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, original.ID)
		if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), original.Canonical()) {
			t.Fatalf("%s refusal changed child: found=%t err=%v", variant, found, err)
		}
	}
}

func forkOneShotNativeRollback(t *testing.T, f *forkJoinNativeFixture, r storegenericschedule.ForkOneShotRequest) {
	t.Helper()
	childRun := uuid.NewString()
	ctx := forkJoinNativeRunContext(t, f, childRun)
	r.Child = forkOneShotNativeChild(t, r.Source.Command, childRun)
	abort := errors.New("abort after native one-shot restoration")
	var childID string
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		child, err := storegenericschedule.RestoreForkOneShotTx(ctx, attempt, f.postgres != nil, r)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		childID = child.ID
		return runtimegenericschedule.Activation{}, abort
	})
	if !errors.Is(result.Err(), abort) || result.Acknowledged() || childID == "" {
		t.Fatalf("native restoration rollback: child=%q err=%v", childID, result.Err())
	}
	if _, found, err := f.generic.LoadGenericScheduleActivation(ctx, childID); err != nil || found {
		t.Fatalf("rolled-back child survived: found=%t err=%v", found, err)
	}
}
