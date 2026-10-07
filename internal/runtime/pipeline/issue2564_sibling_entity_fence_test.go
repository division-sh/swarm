package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type siblingFenceObservation struct {
	lock            *sync.Mutex
	active          atomic.Bool
	reads           atomic.Int32
	postCommitReads atomic.Int32
	engineCommits   atomic.Int32
	cardCommits     atomic.Int32
	dispatches      atomic.Int32
	finalizations   atomic.Int32
	projections     atomic.Int32
	firstRead       sync.Once
	errors          chan error
}

func (o *siblingFenceObservation) checkLock(held bool, phase string) {
	if !o.active.Load() {
		return
	}
	available := o.lock.TryLock()
	if available {
		o.lock.Unlock()
	}
	if held == available {
		select {
		case o.errors <- fmt.Errorf("%s: entity lock held=%t, want %t", phase, !available, held):
		default:
		}
	}
}

func (o *siblingFenceObservation) read() {
	if o.active.Load() {
		o.reads.Add(1)
		o.firstRead.Do(func() { o.checkLock(true, "first mutable read") })
		if o.engineCommits.Load() > 0 {
			o.postCommitReads.Add(1)
			o.checkLock(false, "post-commit canonical reload")
		}
	}
}

type siblingFenceInstanceReader struct {
	WorkflowInstancePersistenceReader
	observation *siblingFenceObservation
}

func (r siblingFenceInstanceReader) LoadWorkflowInstance(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	r.observation.read()
	return r.WorkflowInstancePersistenceReader.LoadWorkflowInstance(ctx, owner)
}

type siblingFenceTargetReader struct {
	WorkflowTargetPersistenceReader
	observation *siblingFenceObservation
}

func (r siblingFenceTargetReader) LoadWorkflowTargetPersistence(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, entityID identity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	r.observation.read()
	return r.WorkflowTargetPersistenceReader.LoadWorkflowTargetPersistence(ctx, owner, entityID)
}

type siblingFenceEngineOwner struct {
	WorkflowEngineMutationOwner
	observation *siblingFenceObservation
}

func (o siblingFenceEngineOwner) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	o.observation.checkLock(true, "engine commit")
	result, err := o.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
	if result.Committed && o.observation.active.Load() {
		o.observation.engineCommits.Add(1)
	}
	return result, err
}

type siblingFenceCardOwner struct {
	DecisionCardMutationOwner
	observation *siblingFenceObservation
}

func (o siblingFenceCardOwner) AcquireDecisionCardMutation(ctx context.Context, request apiidempotency.Request, mutation DecisionCardMutation) (DecisionCardMutationLease, error) {
	lease, err := o.DecisionCardMutationOwner.AcquireDecisionCardMutation(ctx, request, mutation)
	if err != nil {
		return nil, err
	}
	return siblingFenceCardLease{DecisionCardMutationLease: lease, observation: o.observation}, nil
}

type siblingFenceCardLease struct {
	DecisionCardMutationLease
	observation *siblingFenceObservation
}

func (l siblingFenceCardLease) Commit(ctx context.Context, command DecisionCardMutationCommand) (CommittedDecisionCardMutation, error) {
	l.observation.checkLock(true, "card lease commit")
	result, err := l.DecisionCardMutationLease.Commit(ctx, command)
	if result.Acknowledged && l.observation.active.Load() {
		l.observation.cardCommits.Add(1)
	}
	return result, err
}

type siblingFenceBus struct {
	Bus
	EnginePublicationPlanner
	observation *siblingFenceObservation
	suppress    bool
}

func (b *siblingFenceBus) FinalizeEnginePublications(ctx context.Context, publications []runtimeengine.CommittedDurablePublication) error {
	b.observation.checkLock(false, "publication finalization")
	if b.observation.active.Load() {
		b.observation.finalizations.Add(1)
	}
	return b.EnginePublicationPlanner.FinalizeEnginePublications(ctx, publications)
}

func (b *siblingFenceBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return siblingFenceDispatcher{bus: b}
}

type siblingFenceDispatcher struct{ bus *siblingFenceBus }

func (d siblingFenceDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if d.bus.suppress {
		return nil
	}
	d.bus.observation.checkLock(false, "post-commit dispatch")
	if d.bus.observation.active.Load() {
		d.bus.observation.dispatches.Add(1)
	}
	return d.bus.Bus.EngineDispatcher().DispatchPostCommit(ctx, intents)
}

// Observe an actual mutex waiter, not elapsed time or a merely scheduled worker.
func waitForSiblingEntityMutex(t *testing.T, frame string, done <-chan error, observation *siblingFenceObservation) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("sibling completed before waiting on entity lock: %v", err)
		default:
		}
		length := runtime.Stack(stack, true)
		if length == len(stack) {
			stack = make([]byte, 2*len(stack))
			continue
		}
		for _, goroutine := range strings.Split(string(stack[:length]), "\n\n") {
			if strings.Contains(goroutine, frame) && strings.Contains(goroutine, ".lockWorkflowEntity(") && strings.Contains(goroutine, "lockSlow(") {
				if observation.reads.Load() != 0 || observation.engineCommits.Load() != 0 || observation.cardCommits.Load() != 0 {
					t.Fatal("sibling read or committed before acquiring the held entity gate")
				}
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("sibling did not reach the existing entity mutex within the bounded observation window")
}

func VerifyIssue2564SiblingEntityFenceForTest(t *testing.T, factory WorkflowTimerCauseReplayFactoryForTest, sibling string) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := gateLifecycleBundle(t)
			fixture := factory(t, backend, bundle)
			ctx, pc := fixture.Context, fixture.Coordinator
			runID := runtimeRunID(ctx)
			owner := testRunScopedWorkflowInstanceFromContext(ctx, runID)
			entityID := identity.NormalizeEntityID(runID)
			at := canonicalWorkflowTimerTime(time.Now())
			fixture.CommitConstruction(ctx, owner, WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: ".", WorkflowVersion: semanticview.Wrap(bundle).WorkflowVersion(),
				Mode: "static", EntityType: "test_entity", CurrentState: "awaiting_review", StageDefined: true,
				Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at,
			}, at)
			rows, _, err := pc.decisionCards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, EntityID: runID, Limit: 10})
			if err != nil || len(rows) != 1 {
				t.Fatalf("constructed gate cards=%+v error=%v", rows, err)
			}
			card, err := pc.decisionCards.GetDecisionCard(ctx, rows[0].CardID)
			if err != nil {
				t.Fatal(err)
			}
			before, found, err := pc.Load(ctx, owner)
			if err != nil || !found {
				t.Fatalf("load constructed gate: found=%v error=%v", found, err)
			}
			// Capture the existing mutex object; the sibling must use this same gate.
			unlock := pc.lockWorkflowEntity(runID)
			pc.entityLockMu.Lock()
			lock := pc.entityLocks[runID]
			pc.entityLockMu.Unlock()
			unlock()
			observation := &siblingFenceObservation{lock: lock, errors: make(chan error, 16)}
			bus := &siblingFenceBus{Bus: pc.bus, EnginePublicationPlanner: pc.bus.(EnginePublicationPlanner), observation: observation}
			pc.bus = bus
			pc.workflowStore.instanceReader = siblingFenceInstanceReader{WorkflowInstancePersistenceReader: pc.workflowStore.instanceReader, observation: observation}
			pc.workflowStore.targetReader = siblingFenceTargetReader{WorkflowTargetPersistenceReader: pc.workflowStore.targetReader, observation: observation}
			pc.workflowStore.engineMutations = siblingFenceEngineOwner{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, observation: observation}
			pc.workflowStore.cardMutations = siblingFenceCardOwner{DecisionCardMutationOwner: pc.workflowStore.cardMutations, observation: observation}
			pc.workflowTimers.testAfterWakeupLoad = func() {
				observation.checkLock(false, "post-commit lifecycle projection")
				if observation.active.Load() {
					observation.projections.Add(1)
				}
			}
			decisionID := uuid.NewString()
			now := at.Add(time.Second)
			principal, err := pc.decisionCards.(interface {
				EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
			}).EnsureOperatorPrincipal(ctx, at)
			if err != nil {
				t.Fatalf("admit actual operator principal: %v", err)
			}
			decision := decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal.ID,
				ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: decisionID, Now: now}
			request := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal.ID),
				ResourceID: card.CardID, RequestHash: "sibling-fence-" + decisionID, IdempotencyKey: uuid.NewString(), Now: now, TTL: time.Hour}
			var action func(context.Context) error
			var frame string
			var response json.RawMessage
			switch sibling {
			case "gate":
				// Keep the real card/header/publication commit, pausing only its dispatch
				// so the frozen route can be exercised as the contending sibling.
				bus.suppress = true
				if _, _, err := pc.CommitDecisionCardMutation(ctx, request, NewDecisionCardDecision(decision)); err != nil {
					t.Fatal(err)
				}
				bus.suppress = false
				card, err = pc.decisionCards.GetDecisionCard(ctx, card.CardID)
				if err != nil || card.Status != decisioncard.StatusDecided {
					t.Fatalf("freeze actual verdict: card=%+v error=%v", card, err)
				}
				route, err := pc.loadStageGateRoute(ctx, card)
				if err != nil {
					t.Fatal(err)
				}
				parent, err := decisionCardDecidedEvent(card, decision)
				if err != nil {
					t.Fatal(err)
				}
				emitted, err := workflowGateOutcomeEvent(card, parent, route)
				if err != nil || emitted == nil {
					t.Fatalf("frozen outcome=%+v error=%v", emitted, err)
				}
				before, found, err = pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatal(err)
				}
				frame = ".routeWorkflowGateDecisionAttempt("
				action = func(ctx context.Context) error {
					_, err := pc.routeWorkflowGateDecision(ctx, card, parent, route, emitted)
					return err
				}
			case "card":
				frame = ".commitDecisionCardMutation("
				action = func(ctx context.Context) error {
					var err error
					response, _, err = pc.CommitDecisionCardMutation(ctx, request, NewDecisionCardDecision(decision))
					return err
				}
			case "compatibility_card":
				frame = ".CommitDecision("
				action = func(ctx context.Context) error { return pc.CommitDecision(ctx, card, decisionID, now) }
			case "termination":
				frame = ".commitWorkflowTerminationAttempt("
				action = func(ctx context.Context) error { return pc.MarkTerminated(ctx, owner, entityID, now) }
			default:
				t.Fatalf("unknown sibling %s", sibling)
			}
			beforeCarrier, err := workflowInstanceStateCarrier(before)
			if err != nil {
				t.Fatal(err)
			}
			frozen, found, err := gateruntime.Load(beforeCarrier.StateBuckets, ".", "launch_review")
			if err != nil || !found {
				t.Fatalf("frozen gate missing before operation: %+v error=%v", frozen, err)
			}
			observation.active.Store(true)
			held := pc.lockWorkflowEntity(runID)
			workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			var finished atomic.Bool
			go func() {
				err := action(workerCtx)
				finished.Store(true)
				done <- err
			}()
			defer func() {
				if held != nil {
					held()
				}
				if !finished.Load() {
					cancel()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("sibling worker failed to join after releasing test gate")
					}
				}
			}()
			waitForSiblingEntityMutex(t, frame, done, observation)
			held()
			held = nil
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("sibling failed to commit/dispatch after entity gate release")
			}
			observation.active.Store(false)
			select {
			case err := <-observation.errors:
				t.Fatal(err)
			default:
			}
			if observation.reads.Load() == 0 || observation.engineCommits.Load() != 1 {
				t.Fatalf("no actual locked read/commit: reads=%d engine commits=%d", observation.reads.Load(), observation.engineCommits.Load())
			}
			if sibling == "card" && observation.cardCommits.Load() != 1 {
				t.Fatalf("public card request acknowledgments=%d, want one", observation.cardCommits.Load())
			}
			if sibling != "compatibility_card" && (observation.dispatches.Load() == 0 || observation.finalizations.Load() == 0) {
				t.Fatalf("post-commit dispatch/finalization not exercised: dispatches=%d finalizations=%d", observation.dispatches.Load(), observation.finalizations.Load())
			}
			if (sibling == "gate" || sibling == "card") && observation.projections.Load() == 0 {
				t.Fatal("post-commit lifecycle projection was not exercised")
			}
			if sibling == "termination" && observation.postCommitReads.Load() == 0 {
				t.Fatal("termination's unlocked canonical instance reload was not exercised")
			}
			wantRevision := before.Revision + 1
			if sibling == "card" {
				wantRevision++ // The atomic card decision is followed by its frozen route.
			}
			after, found, err := pc.Load(ctx, owner)
			if err != nil || !found || after.Revision != wantRevision {
				t.Fatalf("actual commit missing: before=%+v after=%+v error=%v", before, after, err)
			}
			carrier, err := workflowInstanceStateCarrier(after)
			if err != nil {
				t.Fatal(err)
			}
			gate, found, err := gateruntime.Load(carrier.StateBuckets, ".", "launch_review")
			if err != nil || !found {
				t.Fatalf("committed gate missing: %+v %v", gate, err)
			}
			if gate.ActivationID != frozen.ActivationID || gate.CardID != frozen.CardID || gate.RoutesJSON != frozen.RoutesJSON {
				t.Fatal("sibling replaced the admitted gate/card/frozen route identity")
			}
			switch sibling {
			case "gate", "card":
				if after.CurrentState != "operating" || gate.Status != gateruntime.StatusRouted || len(after.TransitionHistory) != 1 || gate.DecisionEventID != decisionID {
					t.Fatalf("frozen gate did not route once: instance=%+v gate=%+v", after, gate)
				}
				if sibling == "card" {
					replayedResponse, replayed, err := pc.CommitDecisionCardMutation(ctx, request, NewDecisionCardDecision(decision))
					if err != nil || !replayed || len(response) == 0 {
						t.Fatalf("atomic public completion replay failed: replayed=%v response=%s error=%v", replayed, replayedResponse, err)
					}
					var original, replayedValue map[string]any
					if err := json.Unmarshal(response, &original); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(replayedResponse, &replayedValue); err != nil || !reflect.DeepEqual(original, replayedValue) {
						t.Fatalf("public completion replay changed content: before=%s after=%s error=%v", response, replayedResponse, err)
					}
					reloaded, found, err := pc.Load(ctx, owner)
					if err != nil || !found || reloaded.Revision != after.Revision || len(reloaded.TransitionHistory) != 1 {
						t.Fatalf("public completion replay repeated the mutation: %+v error=%v", reloaded, err)
					}
				}
			case "compatibility_card":
				if after.CurrentState != "awaiting_review" || gate.Status != gateruntime.StatusDecisionCommitted || gate.DecisionEventID != decisionID {
					t.Fatalf("compatibility gate decision missing: instance=%+v gate=%+v", after, gate)
				}
			case "termination":
				if after.Status != "terminated" || !after.TerminatedAt.Equal(now) || gate.Status != gateruntime.StatusSuperseded {
					t.Fatalf("explicit termination missing: instance=%+v gate=%+v", after, gate)
				}
			}
		})
	}
}
