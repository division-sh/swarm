package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// These are separate deterministic H2 mechanisms, not a replacement for the
// six-hub 900/600 served workloads. No carrier, revision, stage-entry, mutex or
// selected-store result is patched. Barriers delay actual selected-owner reads
// or commits while an actual competing node/timer executes.
type issue2564H2Barrier struct {
	reached chan string
	release chan struct{}
	once    sync.Once
	spent   atomic.Bool
}

func newIssue2564H2Barrier() *issue2564H2Barrier {
	return &issue2564H2Barrier{reached: make(chan string, 1), release: make(chan struct{})}
}

func (b *issue2564H2Barrier) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *issue2564H2Barrier) pause(ctx context.Context, label string) error {
	if !b.spent.CompareAndSwap(false, true) {
		return nil
	}
	b.reached <- label
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *issue2564H2Barrier) wait(t *testing.T) string {
	t.Helper()
	select {
	case label := <-b.reached:
		return label
	case <-time.After(10 * time.Second):
		t.Fatal("H2 actual selected-owner barrier was not reached")
		return ""
	}
}

type issue2564H2TimerCommitOwner struct {
	WorkflowEngineMutationOwner
	barrier *issue2564H2Barrier
	mu      sync.Mutex
	err     error
	ack     bool
}

func (o *issue2564H2TimerCommitOwner) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	event, found := correlation.InboundEventFromContext(ctx)
	timer := found && event.Type() == "platform.stage_timer"
	if timer {
		if err := o.barrier.pause(ctx, fmt.Sprintf("timer_commit event=%s evaluated_revision=%d", event.ID(), command.State.ExpectedRevision)); err != nil {
			return CommittedWorkflowEngineMutation{}, err
		}
	}
	result, err := o.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
	if timer {
		o.mu.Lock()
		o.err, o.ack = err, result.Committed
		o.mu.Unlock()
	}
	return result, err
}

type issue2564H2TimerR1Reader struct {
	WorkflowInstancePersistenceReader
	barrier *issue2564H2Barrier
	reads   atomic.Int32
}

func (r *issue2564H2TimerR1Reader) LoadWorkflowInstance(ctx context.Context, owner flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	instance, found, err := r.WorkflowInstancePersistenceReader.LoadWorkflowInstance(ctx, owner)
	event, inbound := correlation.InboundEventFromContext(ctx)
	// The first two reads authorize the exact activation and declaration. The
	// third is the timer transition's Rt1, returned before prepareMutation.
	if err == nil && found && inbound && event.Type() == "platform.stage_timer" && r.reads.Add(1) == 3 {
		if err := r.barrier.pause(ctx, fmt.Sprintf("timer_Rt1 event=%s stage=%s revision=%d count=%v", event.ID(), instance.CurrentState, instance.Revision, instance.Fields["count"])); err != nil {
			return WorkflowInstance{}, false, err
		}
	}
	return instance, found, err
}

type issue2564H2NodeR1Reader struct {
	WorkflowTargetPersistenceReader
	barrier *issue2564H2Barrier
}

func issue2564H2IsEngineEvaluationRead() bool {
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "pipelineEngineStateRepo.LoadState") {
			return true
		}
		if !more {
			return false
		}
	}
}

func (r issue2564H2NodeR1Reader) LoadWorkflowTargetPersistence(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entity identity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	result, err := r.WorkflowTargetPersistenceReader.LoadWorkflowTargetPersistence(ctx, owner, entity)
	event, inbound := correlation.InboundEventFromContext(ctx)
	_, claimed := runtimedelivery.ClaimFromContext(ctx)
	if err == nil && inbound && claimed && event.Type() == "hub.bump" && issue2564H2IsEngineEvaluationRead() {
		if err := r.barrier.pause(ctx, fmt.Sprintf("node_R1 event=%s stage=%s revision=%d fields=%s", event.ID(), result.State.CurrentState, result.State.Revision, result.State.Fields)); err != nil {
			return WorkflowTargetPersistenceRecord{}, err
		}
	}
	return result, err
}

// A completed competitor exposes the old unlocked interleaving. A real mutex
// waiter exposes repaired serialization. No elapsed-time "did not happen"
// assertion decides which schedule ran, and neither path changes assertions.
func issue2564H2Competitor(t *testing.T, frame string, done <-chan error) (blocked bool, completed error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			return false, err
		default:
		}
		n := runtime.Stack(stack, true)
		if n == len(stack) {
			stack = make([]byte, len(stack)*2)
			continue
		}
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, frame) && strings.Contains(goroutine, ".lockWorkflowEntity(") && strings.Contains(goroutine, "lockSlow(") {
				return true, nil
			}
		}
		runtime.Gosched()
	}
	t.Fatal("H2 competitor neither reached the actual entity gate nor completed")
	return false, nil
}

func issue2564H2Join(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("H2 native writer did not join after barrier release")
		return context.DeadlineExceeded
	}
}

func VerifyIssue2564H2BaselineMechanismsForTest(t *testing.T, factory WorkflowTimerCauseReplayFactoryForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mechanism := range []string{"H2a_timer_CAS_stranding", "H2b_timer_carrier_revert", "H2c_handler_stale_stage_entry"} {
			t.Run(backend+"/"+mechanism, func(t *testing.T) {
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: h2-mechanism-equivalent\nstages:\n  s1:\n    initial: true\n    timers: [{id: h2.s1_to_s2, after: 1s, advances_to: s2}]\n  s2:\n    timers: [{id: h2.s2_to_s1, after: 1s, advances_to: s1}]\n  closed: {terminal: true}\n",
					"entities.yaml": "hub:\n  count: integer\n  c1: integer\n  c2: integer\n",
					"events.yaml":   "hub.bump: {n: integer}\n",
					"nodes.yaml": `hub-node:
  execution_type: system_node
  subscribes_to: [hub.bump]
  event_handlers:
    hub.bump:
      data_accumulation:
        writes: [{target_field: count, value: entity.count + 1}]
      rules:
        - id: in_s1
          when: _entity.current_state == 's1'
          data_accumulation:
            writes: [{target_field: c1, value: entity.c1 + 1}]
        - id: in_s2
          else: true
          data_accumulation:
            writes: [{target_field: c2, value: entity.c2 + 1}]
`,
				})
				fixture := factory(t, backend, bundle)
				ctx, pc := fixture.Context, fixture.Coordinator
				run := correlation.RunIDFromContext(ctx)
				owner := testRunScopedWorkflowInstanceFromContext(ctx, run)
				at := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Second))
				fixture.CommitConstruction(ctx, owner, WorkflowInstance{InstanceID: run, StorageRef: run, EntityID: run, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(), Mode: "static", EntityType: "hub", CurrentState: "s1", StageDefined: true, Fields: map[string]any{"count": 0, "c1": 0, "c2": 0}, CreatedAt: at, EnteredStageAt: at}, at)
				rows := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, run)
				if len(rows) != 1 || rows[0].FireAt.Sub(rows[0].CreatedAt) != time.Second {
					t.Fatalf("H2 did not create its real one-second initial entry: %+v", rows)
				}
				barrier := newIssue2564H2Barrier()
				defer barrier.unblock()
				commitOwner := &issue2564H2TimerCommitOwner{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, barrier: barrier}
				switch mechanism {
				case "H2a_timer_CAS_stranding":
					pc.workflowStore.engineMutations = commitOwner
				case "H2b_timer_carrier_revert":
					pc.workflowStore.instanceReader = &issue2564H2TimerR1Reader{WorkflowInstancePersistenceReader: pc.workflowStore.instanceReader, barrier: barrier}
				case "H2c_handler_stale_stage_entry":
					pc.workflowStore.targetReader = issue2564H2NodeR1Reader{WorkflowTargetPersistenceReader: pc.workflowStore.targetReader, barrier: barrier}
				}
				bump := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "hub.bump", "operator", "", []byte(`{"n":1}`), 0, run, events.EnvelopeForEntityID(events.EventEnvelope{}, run), eventtest.RootRoutingSource(run), time.Now().UTC())
				bumpDone, timerDone := make(chan error, 1), make(chan error, 1)
				fire := func() {
					outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, rows[0])
					if outcome != WorkflowTimerFireCommitted {
						err = fmt.Errorf("H2 publication was not committed: %s: %w", outcome, err)
					}
					timerDone <- err
				}
				publish := func() { bumpDone <- fixture.Publish(correlation.WithInboundEvent(ctx, bump), bump) }
				var timerErr, bumpErr error
				var blocked bool
				if mechanism == "H2c_handler_stale_stage_entry" {
					go publish()
					t.Log(barrier.wait(t))
					go fire()
					blocked, timerErr = issue2564H2Competitor(t, ".handleWorkflowStageTimerFire(", timerDone)
					barrier.unblock()
					bumpErr = issue2564H2Join(t, bumpDone)
					if blocked {
						timerErr = issue2564H2Join(t, timerDone)
					}
				} else {
					go fire()
					t.Log(barrier.wait(t))
					go publish()
					blocked, bumpErr = issue2564H2Competitor(t, ".WithEntityLock(", bumpDone)
					if !blocked && bumpErr == nil {
						committed, found, err := pc.Load(ctx, owner)
						if err != nil || !found || fmt.Sprint(committed.Fields["count"]) != "1" {
							t.Fatalf("H2 competing actual node did not commit before timer continuation: %+v %v", committed, err)
						}
						t.Logf("H2 reverse interleaving: actual node acknowledged count=1 revision=%d while timer snapshot/commit held", committed.Revision)
					}
					barrier.unblock()
					timerErr = issue2564H2Join(t, timerDone)
					if blocked {
						bumpErr = issue2564H2Join(t, bumpDone)
					}
				}
				persisted, found, err := pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("H2 final native read: found=%v err=%v", found, err)
				}
				timers := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, run)
				active, fired := 0, 0
				for _, timer := range timers {
					if timer.Status == "active" {
						active++
					}
					if timer.Status == "fired" {
						fired++
					}
				}
				node := pipelineNode(t, ".", "hub-node")
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run})}
				proof, proofErr := pc.deliveryStore.ProveHandoff(ctx, bump.ID(), route)
				if proofErr != nil {
					t.Fatal(proofErr)
				}
				delivery, deliveryErr := pc.deliveryStore.Snapshot(ctx, proof.DeliveryID())
				outcomes, outcomesErr := pc.deliveryStore.Outcomes(ctx, proof.DeliveryID())
				t.Logf("%s actual_mutex_wait=%v count=%v c1=%v c2=%v stage=%s timer_rows=%d fired=%d active_successors=%d timer_error=%v bump_error=%v delivery_status=%s retry_count=%d attempt_count=%d failure=%+v", mechanism, blocked, persisted.Fields["count"], persisted.Fields["c1"], persisted.Fields["c2"], persisted.CurrentState, len(timers), fired, active, timerErr, bumpErr, delivery.Status, delivery.RetryCount, len(outcomes), delivery.Failure)
				if mechanism == "H2a_timer_CAS_stranding" {
					commitOwner.mu.Lock()
					failure, typed := failures.As(commitOwner.err)
					t.Logf("H2a selected-store commit acknowledgment=%v typed=%v failure=%+v", commitOwner.ack, typed, failure)
					commitOwner.mu.Unlock()
				}
				if timerErr != nil || bumpErr != nil || deliveryErr != nil || outcomesErr != nil || delivery.Status != runtimedelivery.StatusDelivered || delivery.RetryCount != 0 || len(outcomes) != 1 || fmt.Sprint(persisted.Fields["count"]) != "1" || persisted.CurrentState != "s2" || len(timers) != 2 || fired != 1 || active != 1 {
					t.Fatalf("%s lost a committed node field/timer transition or terminalized a first attempt", mechanism)
				}
				firedEvent := ""
				for _, record := range persisted.TransitionHistory {
					if record.From == "s1" && record.To == "s2" {
						if firedEvent != "" {
							t.Fatal("H2 duplicated its timer transition")
						}
						firedEvent = record.TriggerEventID
					}
				}
				if firedEvent == "" || fixture.Observe().Events != 2 {
					t.Fatalf("H2 must retain one exact timer event plus one node event: trigger=%s storage=%+v", firedEvent, fixture.Observe())
				}
				before := persisted
				if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, rows[0]); err != nil || outcome != WorkflowTimerFireTerminal {
					t.Fatalf("H2 old fired coordinate was requeued: %s %v", outcome, err)
				}
				after, _, err := pc.Load(ctx, owner)
				if err != nil || !reflect.DeepEqual(before, after) || fixture.Observe().Events != 2 {
					t.Fatal("H2 replay reminted event or changed acknowledged state")
				}
			})
		}
	}
}
