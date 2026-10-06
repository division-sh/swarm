package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type turnReactionProbe struct{ id string }

func (p turnReactionProbe) DurablePublicationEventID() string          { return p.id }
func (p turnReactionProbe) ValidateDurablePublicationPlan() error      { return nil }
func (p turnReactionProbe) CommittedDurablePublicationEventID() string { return p.id }
func (p turnReactionProbe) ValidateCommittedDurablePublication() error { return nil }

type canceledTurnConsumerStore struct {
	effects.Store
	commit func(context.Context, effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error)
}

type canceledTurnStartupStore struct {
	canceledTurnConsumerStore
	list func(context.Context, effects.RecoveryRequest) ([]effects.TurnExecutionResult, error)
}

func (*canceledTurnStartupStore) ReconcileExternalEffectAttempts(context.Context, effects.RecoveryRequest) (effects.RecoverySummary, error) {
	return effects.RecoverySummary{}, nil
}

func (s *canceledTurnStartupStore) ListCanceledTurnRecoveries(ctx context.Context, request effects.RecoveryRequest) ([]effects.TurnExecutionResult, error) {
	return s.list(ctx, request)
}

func (p *canceledTurnConsumerStore) CommitCanceledTurn(ctx context.Context, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
	return p.commit(ctx, command)
}

type canceledTurnConsumerBus struct {
	receiptOutcomeBus
	reaction   turnReactionProbe
	dispatch   func(context.Context, effects.CommittedTurnReaction) error
	releases   int
	prepareErr error
}

func (b *canceledTurnConsumerBus) PrepareTurnTimeoutReaction(_ context.Context, turn effects.TurnExecutionResult) (effects.TurnReactionPlan, error) {
	if turn.Cancellation.CauseEvent != b.reaction.id {
		return nil, errors.New("foreign reaction")
	}
	return b.reaction, b.prepareErr
}

func (b *canceledTurnConsumerBus) ReleaseTurnTimeoutReaction(context.Context, effects.TurnReactionPlan) error {
	b.releases++
	return nil
}

func (b *canceledTurnConsumerBus) DispatchTurnTimeoutReaction(ctx context.Context, committed effects.CommittedTurnReaction) error {
	return b.dispatch(ctx, committed)
}

func TestCanceledDeliveryConsumerKeepsExactCommitAndCleanupEvidence(t *testing.T) {
	for _, mode := range []string{"healthy", "captured", "foreign_capture", "prepare_failure", "unacknowledged", "foreign_origin", "missing_reaction", "commit_cleanup", "continuation_cleanup", "dispatch_cleanup"} {
		t.Run(mode, func(t *testing.T) {
			store := newManagerDeliveryTestStore(t)
			bus := &canceledTurnConsumerBus{reaction: turnReactionProbe{id: uuid.NewString()}}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: store})
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "source", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Time{})
			ctx := managerClaimedDeliveryContext(t, am, testAuthorActivityContext(context.Background()), event, "canceled-consumer")
			claim, _ := deliverylifecycle.ClaimFromContext(ctx)
			origin, err := effects.DeliveryCompletionOrigin(claim)
			if err != nil {
				t.Fatal(err)
			}
			turn := effects.TurnExecutionResult{Attempt: effects.Attempt{AttemptID: uuid.NewString(), Origin: origin}, Cancellation: effects.TurnCancellation{
				Committed: true, Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: bus.reaction.id, RequestedAt: time.Now().UTC(),
			}}
			first := time.Now().UTC()
			turn.Clock = &effects.LogicalTurnClock{Origin: origin, FirstAttempt: turn.Attempt.AttemptID, LaunchedAt: first,
				Timeout: &timeridentity.TurnTimeout{After: time.Minute, Emit: "work.aborted"}, DeadlineAt: first.Add(time.Minute), TimeoutEvent: bus.reaction.id}
			snapshot, err := store.Snapshot(ctx, claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Status, snapshot.ReasonCode, snapshot.SettledAt = deliverylifecycle.StatusCanceled, "turn_timeout", time.Now().UTC()
			snapshot.ClaimExpiresAt = time.Time{}
			cleanup := errors.New("independent cancellation cleanup failure")
			calls, dispatches := 0, 0
			am.roles.LifecycleEffects = &canceledTurnConsumerStore{commit: func(_ context.Context, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
				calls++
				if !command.Attempt.Origin.Same(origin) || command.Publication.DurablePublicationEventID() != bus.reaction.id {
					t.Fatal("Manager substituted origin or reaction")
				}
				intent := turn.Cancellation
				intent.OriginSettled = true
				result := effects.CanceledTurnCommit{Acknowledged: true, Origin: origin, Cancellation: intent, Delivery: snapshot, Publication: bus.reaction}
				switch mode {
				case "unacknowledged":
					result.Acknowledged = false
				case "foreign_origin":
					result.Origin.Directive.OperationID = uuid.NewString()
					result.Origin.Kind = effects.CompletionOriginDirective
				case "missing_reaction":
					result.Publication = nil
				case "commit_cleanup":
					return result, cleanup
				}
				return result, nil
			}}
			if mode == "prepare_failure" {
				bus.prepareErr = cleanup
			}
			if mode == "continuation_cleanup" {
				bus.failure = cleanup
			}
			bus.dispatch = func(postCtx context.Context, reaction effects.CommittedTurnReaction) error {
				dispatches++
				if len(bus.released) != 1 || bus.released[0] != claim.DeliveryID() || postCtx.Err() != nil {
					t.Fatal("reaction dispatched before exact continuation release or on canceled context")
				}
				if mode == "dispatch_cleanup" {
					return cleanup
				}
				return nil
			}
			heartbeat, err := deliverylifecycle.StartClaimHeartbeat(ctx, am.workOwner, store, claim)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = heartbeat.Stop() }()
			observed := effects.CompletionSettlementObservation{}
			if mode == "captured" || mode == "foreign_capture" {
				intent := turn.Cancellation
				observed = effects.CompletionSettlementObservation{AttemptID: turn.Attempt.AttemptID, Disposition: effects.CompletionSettlementDrained, Origin: origin, Cancellation: &intent}
				if mode == "foreign_capture" {
					intent.CauseEvent = uuid.NewString()
				}
			}
			result, err := am.settleCanceledDelivery(ctx, event, heartbeat, turn, observed)
			exact := mode != "prepare_failure" && mode != "unacknowledged" && mode != "foreign_origin" && mode != "missing_reaction" && mode != "foreign_capture"
			if exact && (dispatches != 1 || len(bus.released) != 1 || !result.Acknowledged) || !exact && (dispatches != 0 || len(bus.released) != 0 || err == nil) {
				t.Fatalf("canceled commit consumption: calls=%d dispatches=%d released=%v err=%v", calls, dispatches, bus.released, err)
			}
			if (mode == "healthy" || mode == "captured") && err != nil {
				t.Fatal(err)
			}
			if mode == "commit_cleanup" || mode == "continuation_cleanup" || mode == "dispatch_cleanup" || mode == "prepare_failure" {
				if !errors.Is(err, cleanup) {
					t.Fatalf("independent cleanup evidence lost: %v", err)
				}
			}
		})
	}
}

func TestStartupCanceledTurnCommitsBeforeAdmissionWithoutExecutingReaction(t *testing.T) {
	for _, mode := range []string{"healthy", "list_failure", "prepare_failure", "unacknowledged", "missing_reaction", "commit_cleanup", "continuation_cleanup"} {
		t.Run(mode, func(t *testing.T) {
			deliveries := newManagerDeliveryTestStore(t)
			bus := &canceledTurnConsumerBus{reaction: turnReactionProbe{id: uuid.NewString()}}
			bus.dispatch = func(context.Context, effects.CommittedTurnReaction) error {
				t.Fatal("startup dispatched a reaction before runtime admission")
				return nil
			}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveries})
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "source", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Time{})
			ctx := managerClaimedDeliveryContext(t, am, testAuthorActivityContext(context.Background()), event, "startup-canceled")
			claim, _ := deliverylifecycle.ClaimFromContext(ctx)
			origin, err := effects.DeliveryCompletionOrigin(claim)
			if err != nil {
				t.Fatal(err)
			}
			turn := effects.TurnExecutionResult{Attempt: effects.Attempt{AttemptID: uuid.NewString(), Origin: origin}, Cancellation: effects.TurnCancellation{
				Committed: true, Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: bus.reaction.id, RequestedAt: time.Now().UTC(),
			}}
			snapshot, err := deliveries.Snapshot(ctx, claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Status, snapshot.ReasonCode, snapshot.SettledAt = deliverylifecycle.StatusCanceled, "turn_timeout", time.Now().UTC()
			snapshot.ClaimExpiresAt = time.Time{}
			cleanup := errors.New("startup independent cleanup failure")
			pending, calls := true, 0
			store := &canceledTurnStartupStore{}
			store.list = func(context.Context, effects.RecoveryRequest) ([]effects.TurnExecutionResult, error) {
				if mode == "list_failure" {
					return nil, cleanup
				}
				if pending {
					return []effects.TurnExecutionResult{turn}, nil
				}
				return nil, nil
			}
			store.commit = func(_ context.Context, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
				calls++
				if !command.Attempt.Origin.Same(origin) || command.Publication.DurablePublicationEventID() != bus.reaction.id {
					t.Fatal("startup changed origin or reaction")
				}
				intent := turn.Cancellation
				intent.OriginSettled = true
				result := effects.CanceledTurnCommit{Acknowledged: true, Origin: origin, Cancellation: intent, Delivery: snapshot, Publication: bus.reaction}
				if mode == "unacknowledged" {
					result.Acknowledged = false
					return result, nil
				}
				if mode == "missing_reaction" {
					result.Publication = nil
					return result, nil
				}
				pending = false
				if mode == "commit_cleanup" {
					return result, cleanup
				}
				return result, nil
			}
			am.roles.EffectsRecovery, am.roles.LifecycleEffects = store, store
			if mode == "prepare_failure" {
				bus.prepareErr = cleanup
			}
			if mode == "continuation_cleanup" {
				bus.failure = cleanup
			}
			err = am.reconcileExternalEffectsForStartup(ctx)
			if mode == "healthy" {
				if err != nil || !am.startupEffectsReconciled || calls != 1 || bus.releases != 1 || len(bus.released) != 1 {
					t.Fatalf("startup did not commit/release exactly once: calls=%d plans=%d deliveries=%v err=%v", calls, bus.releases, bus.released, err)
				}
				if err := am.reconcileExternalEffectsForStartup(ctx); err != nil || calls != 1 {
					t.Fatalf("repeat startup replayed business work: calls=%d err=%v", calls, err)
				}
			} else {
				if err == nil || am.startupEffectsReconciled {
					t.Fatalf("failed startup marked recovery complete: %v", err)
				}
				if mode == "list_failure" || mode == "prepare_failure" || mode == "commit_cleanup" || mode == "continuation_cleanup" {
					if !errors.Is(err, cleanup) {
						t.Fatalf("startup lost cleanup evidence: %v", err)
					}
				}
			}
		})
	}
}

func TestCapturedCanceledTurnRetainsOnlyExactOriginSettlementContext(t *testing.T) {
	for _, mode := range []string{"current", "captured", "missing_intent", "foreign_origin", "foreign_intent", "foreign_cause", "foreign_time"} {
		t.Run(mode, func(t *testing.T) {
			origin, err := effects.DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			turn := effects.TurnExecutionResult{Attempt: effects.Attempt{AttemptID: uuid.NewString(), Origin: origin}, Cancellation: effects.TurnCancellation{
				Committed: true, Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: uuid.NewString(), RequestedAt: time.Now().UTC(),
			}}
			parent, cancel := context.WithCancel(context.Background())
			cancel()
			observed := effects.CompletionSettlementObservation{}
			if mode != "current" {
				intent := turn.Cancellation
				observed = effects.CompletionSettlementObservation{AttemptID: turn.Attempt.AttemptID, Disposition: effects.CompletionSettlementDrained, Origin: origin, Cancellation: &intent}
				switch mode {
				case "missing_intent":
					observed.Cancellation = nil
				case "foreign_origin":
					observed.Origin.Directive.OperationID = uuid.NewString()
				case "foreign_intent":
					intent.Origin.Directive.OperationID = uuid.NewString()
				case "foreign_cause":
					intent.CauseEvent = uuid.NewString()
				case "foreign_time":
					intent.RequestedAt = intent.RequestedAt.Add(time.Second)
				}
			}
			ctx, err := canceledTurnSettlementContext(parent, turn, observed)
			switch mode {
			case "current":
				if err != nil || ctx.Err() != context.Canceled {
					t.Fatalf("ordinary cancellation gained settlement context: %v", err)
				}
			case "captured":
				if err != nil || ctx.Err() != nil {
					t.Fatalf("exact captured settlement context rejected: %v", err)
				}
			default:
				if err == nil || ctx != nil {
					t.Fatalf("%s capture gained settlement context", mode)
				}
			}
		})
	}
}
