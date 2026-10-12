package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type retainedJoinHeldSchedule struct {
	genericschedule.Store
	attempt  int
	prepared chan genericschedule.PreparationCommit
	release  chan struct{}
	holdOnce sync.Once
	freeOnce sync.Once
}

func (h *retainedJoinHeldSchedule) PrepareGenericScheduleOccurrence(ctx context.Context, wake genericschedule.Wakeup) (genericschedule.PreparationCommit, error) {
	prepared, err := h.Store.PrepareGenericScheduleOccurrence(ctx, wake)
	if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
		return prepared, err
	}
	payload, ok := prepared.Result.Activation.Command.Payload.Interface().(map[string]any)
	if !ok {
		return prepared, nil
	}
	_, ref, valid := timeridentity.ParseJoinHandle(payload)
	if !valid || prepared.Result.Activation.Command.EventType != "platform.join_complete" || ref.Generation().Attempt != h.attempt {
		return prepared, nil
	}
	held := false
	h.holdOnce.Do(func() {
		held = true
		h.prepared <- prepared
	})
	if held {
		select {
		case <-h.release:
		case <-ctx.Done():
			return prepared, ctx.Err()
		}
	}
	return prepared, nil
}

func (h *retainedJoinHeldSchedule) releaseHold() {
	h.freeOnce.Do(func() { close(h.release) })
}

func TestServedRetainedJoinOutcomeWaitHeldOccurrenceBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, separate := range []bool{false, true} {
			for _, attempt := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/separate_%t/attempt_%d", backend, separate, attempt), func(t *testing.T) {
					proveServedRetainedJoinOutcomeHeld(t, backend, separate, attempt)
				})
			}
		}
	}
}

func proveServedRetainedJoinOutcomeHeld(t *testing.T, backend servedparity.Backend, separate bool, attempt int) {
	t.Helper()
	held := &retainedJoinHeldSchedule{
		attempt: attempt, prepared: make(chan genericschedule.PreparationCommit, 1), release: make(chan struct{}),
	}
	defer held.releaseHold()
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		projection := previous(owner)
		held.Store = projection.deps.GenericScheduleStore
		projection.deps.GenericScheduleStore = held
		return projection
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	root := canonicalrouting.CopyForkLoopRetainedJoin(t)
	if separate {
		root = canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint(t)
	}
	entered := make(chan events.Event, 1)
	handlerRelease := make(chan struct{})
	var releaseOnce sync.Once
	unblockHandler := func() { releaseOnce.Do(func() { close(handlerRelease) }) }
	defer unblockHandler()
	rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root, func(ctx context.Context, _ string, event events.Event) error {
		if event.Type() != "platform.join_complete" {
			return nil
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload(), &payload); err != nil {
			return err
		}
		_, ref, valid := timeridentity.ParseJoinHandle(payload)
		if !valid || ref.Generation().Attempt != attempt {
			return nil
		}
		entered <- event.Clone()
		select {
		case <-handlerRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "work.requested", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{"token": "member-one"}, "idempotency_key": "held-join-start",
	})
	ctx, cancel := context.WithTimeout(t.Context(), servedProofPollDeadline)
	defer cancel()
	if attempt == 2 {
		waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
		waitForkReceiverSourceCompletion(t, rt, started.RunID)
		first, joins := readRetainedRootJoins(t, rt, started.RunID)
		if first.Attempt != 1 || len(joins) != 1 {
			t.Fatalf("first actual retained arm missing: loop=%+v joins=%+v", first, joins)
		}
		if err := waitServedRetainedJoinOutcome(ctx, started.RunID, joins[0].JoinRef(), func() (loopruntime.Activation, []joinruntime.Activation) {
			return readRetainedRootJoins(t, rt, started.RunID)
		}); err != nil {
			t.Fatal(err)
		}
		requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
			"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
			"payload": map[string]any{"revision_id": first.RevisionID, "token": "member-one"}, "idempotency_key": "held-join-repeat",
		})
	}
	var prepared genericschedule.PreparationCommit
	select {
	case prepared = <-held.prepared:
	case <-ctx.Done():
		t.Fatalf("native exact join preparation missing: %v", ctx.Err())
	}
	activation := prepared.Result.Activation
	handle, ref, valid := timeridentity.ParseJoinHandle(activation.Command.Payload.Interface().(map[string]any))
	if !valid || !prepared.Acknowledged || activation.Command.RunID != started.RunID || ref.Generation().Attempt != attempt || handle.Kind() != timeridentity.TimerHandleJoinComplete {
		t.Fatalf("held native preparation lost exact run/entry/generation: %+v", prepared)
	}
	// The original waits really return while this exact future occurrence is owed.
	waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
	waitForkReceiverSourceCompletion(t, rt, started.RunID)
	var selected any = rt.Postgres
	if rt.SQLite != nil {
		selected = rt.SQLite
	}
	if event, found, err := storetest.ReadCanonicalEventRecord(ctx, selected, prepared.Result.Occurrence.EventID); err != nil || found {
		t.Fatalf("held exact event already published: found=%t event=%+v err=%v", found, event, err)
	}
	stored, found, err := held.Store.LoadGenericScheduleActivation(ctx, activation.ID)
	if err != nil || !found || stored.Status != genericschedule.StatusActive || stored.CurrentEventID != prepared.Result.Occurrence.EventID {
		t.Fatalf("exact prepared occurrence not retained: found=%t row=%+v err=%v", found, stored, err)
	}

	type observation struct {
		loop    loopruntime.Activation
		joins   []joinruntime.Activation
		proceed chan struct{}
	}
	observed := make(chan observation)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- waitServedRetainedJoinOutcome(ctx, started.RunID, ref, func() (loopruntime.Activation, []joinruntime.Activation) {
			loop, joins := readRetainedRootJoins(t, rt, started.RunID)
			sample := observation{loop: loop, joins: joins, proceed: make(chan struct{})}
			select {
			case observed <- sample:
				select {
				case <-sample.proceed:
				case <-ctx.Done():
				}
			case <-ctx.Done():
			}
			return loop, joins
		})
	}()
	defer func() {
		held.releaseHold()
		unblockHandler()
		cancel()
		for range done {
		}
	}()
	awaitPending := func() observation {
		t.Helper()
		select {
		case sample := <-observed:
			complete, err := retainedJoinOutcomeComplete(started.RunID, ref, sample.loop, sample.joins)
			if err != nil || complete {
				t.Fatalf("unexecuted exact outcome reported complete: %t %v", complete, err)
			}
			return sample
		case err := <-done:
			t.Fatalf("exact wait returned before outcome: %v", err)
		case <-ctx.Done():
			t.Fatalf("exact waiter failed to observe pending arm: %v", ctx.Err())
		}
		return observation{}
	}
	firstRead := awaitPending()
	close(firstRead.proceed)
	secondRead := awaitPending()
	held.releaseHold()
	var published events.Event
	select {
	case published = <-entered:
	case <-ctx.Done():
		t.Fatalf("released exact occurrence did not reach real handler: %v", ctx.Err())
	}
	if published.ID() != prepared.Result.Occurrence.EventID || published.RunID() != started.RunID || published.TaskID() != handle.TaskID() {
		t.Fatalf("released occurrence identity changed: %+v", published)
	}
	stored, found, err = held.Store.LoadGenericScheduleActivation(ctx, activation.ID)
	if err != nil || !found || stored.Status != genericschedule.StatusFired || stored.CurrentEventID != published.ID() {
		t.Fatalf("released occurrence lacks native fired evidence: found=%t row=%+v err=%v", found, stored, err)
	}
	close(secondRead.proceed)
	thirdRead := awaitPending()
	// Publication is committed, but the owned handler has not fired the join outcome.
	unblockHandler()
	close(thirdRead.proceed)
	for {
		select {
		case sample := <-observed:
			close(sample.proceed)
		case err := <-done:
			if err != nil {
				t.Fatalf("released exact outcome failed: %v", err)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			loop, joins := readRetainedRootJoins(t, rt, started.RunID)
			complete, err := retainedJoinOutcomeComplete(started.RunID, ref, loop, joins)
			if err != nil || !complete {
				t.Fatalf("authoritative outcome reread failed: %t %v", complete, err)
			}
			t.Logf("exact_join_outcome run=%s occurrence=%s attempt=%d old_waits_early=true publication_alone_incomplete=true handler_outcome_fired=true", started.RunID, published.ID(), attempt)
			return
		case <-ctx.Done():
			t.Fatalf("exact released handler did not settle: %v", ctx.Err())
		}
	}
}
