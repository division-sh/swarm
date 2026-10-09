package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type channelHintProof struct {
	subscription *render.ReconcileSubscription
	sequence     uint64
}

func TestChannelPostCommitActivityHintsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, linked := range []bool{false, true} {
			for _, operation := range []string{"start", "claim", "complete", "uncertain"} {
				name := "unlinked/"
				if linked {
					name = "linked/"
				}
				t.Run(backend+"/"+name+operation, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					cards, runID := decisionCardTestStore(t, backend)
					card, continuation := newRootProposedEffectTestCard(t, runID, time.Now().UTC())
					if linked {
						if err := cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(ctx, card, continuation); err != nil {
							t.Fatal(err)
						}
					}
					record := runtimepipeline.ActivityAttemptRecord{
						RequestEventID: continuation.RequestEventID, RunID: runID, ExecutionMode: continuation.ExecutionMode,
						SourceEventID: continuation.SourceEventID, EntityID: continuation.EntityID, FlowInstance: continuation.FlowInstance,
						NodeID: continuation.NodeID, HandlerEventKey: continuation.HandlerEventKey, ActivityID: continuation.ActivityID,
						Tool: continuation.Tool, EffectClass: string(continuation.EffectClass), Attempt: 1,
						SuccessEvent: continuation.SuccessEvent, FailureEvent: continuation.FailureEvent, InputHash: continuation.EffectContentHash,
					}
					proof := observeChannelHints(t, cards.(interface {
						SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
					}))
					want := render.ReconcileDemand(0)
					if linked {
						want = render.ReconcileOrdinary
					}
					journal := cards.(activityStoryJournal)
					write := journal.StartActivityAttempt
					var cleanupCause error
					switch operation {
					case "claim":
						write = journal.ClaimActivityAttemptForLoopGeneration
					case "complete", "uncertain":
						started, inserted, err := journal.StartActivityAttempt(ctx, record)
						if err != nil || !inserted {
							t.Fatalf("start before terminal write: %+v/%t %v", started, inserted, err)
						}
						proof.expect(t, want)
						status := runtimepipeline.ActivityAttemptStatusSucceeded
						if operation == "uncertain" {
							status = runtimepipeline.ActivityAttemptStatusUncertain
						}
						record = activityStoryTerminal(t, started, status)
						cleanupCause = errors.New("activity post-acknowledgment cleanup cut")
						faulted, err := activityJournalCleanupFaultForTest(cards, cleanupCause)
						if err != nil {
							t.Fatal(err)
						}
						write = faulted.CompleteActivityAttempt
						if operation == "uncertain" {
							write = faulted.MarkActivityAttemptUncertain
						}
					}
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if _, receipt, err := write(cancelled, record); receipt || !errors.Is(err, context.Canceled) || (cleanupCause != nil && errors.Is(err, cleanupCause)) {
						t.Fatalf("canceled write invented acknowledgment: receipt=%t error=%v", receipt, err)
					}
					proof.expect(t, 0)
					actual, receipt, err := write(ctx, record)
					if !receipt || (cleanupCause == nil && err != nil) || (cleanupCause != nil && !errors.Is(err, cleanupCause)) {
						t.Fatalf("actual write lost change/acknowledgment or cleanup: %+v receipt=%t error=%v", actual, receipt, err)
					}
					proof.expect(t, want)
					duplicate, receipt, err := write(ctx, record)
					wantReceipt := operation == "complete" || operation == "uncertain"
					if receipt != wantReceipt || !reflect.DeepEqual(duplicate, actual) || (cleanupCause == nil && err != nil) || (cleanupCause != nil && !errors.Is(err, cleanupCause)) {
						t.Fatalf("exact replay changed public receipt semantics: %+v receipt=%t error=%v", duplicate, receipt, err)
					}
					proof.expect(t, 0)
				})
			}
		}
	}
}

func observeChannelHints(t *testing.T, selected interface {
	SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
}) *channelHintProof {
	t.Helper()
	subscription, err := selected.SubscribeChannelReconciliation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(subscription.Close)
	_, mark := subscription.BeginPass()
	return &channelHintProof{subscription: subscription, sequence: mark.Sequence}
}

func (p *channelHintProof) expect(t *testing.T, demand render.ReconcileDemand) {
	t.Helper()
	if demand != 0 {
		p.sequence++
	}
	got, mark := p.subscription.BeginPass()
	if got != demand || mark.Sequence != p.sequence {
		t.Fatalf("hint=%d sequence=%d, want %d/%d", got, mark.Sequence, demand, p.sequence)
	}
}

func TestChannelPostCommitCardCreationAndTerminalHintsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"gate", "human_task", "proposed_effect"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				cards, runID := decisionCardTestStore(t, backend)
				proof := observeChannelHints(t, cards.(interface {
					SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
				}))
				now := time.Date(2026, 10, 9, 18, 0, 0, 0, time.UTC)
				var card decisioncard.Card
				var create func() error
				switch kind {
				case "gate":
					card = newDecisionCardTestCard(t, runID, now)
					create = func() error { return cards.CreateDecisionCard(ctx, card) }
				case "human_task":
					var continuation decisioncard.HumanTaskContinuation
					card, continuation = newHumanTaskDecisionCardTestFixture(t, runID, "hint", now, 1, now.Add(48*time.Hour))
					create = func() error {
						return cards.(decisioncard.HumanTaskStore).CreateHumanTaskCard(ctx, card, continuation)
					}
				case "proposed_effect":
					var continuation decisioncard.ProposedEffectContinuation
					card, continuation = newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
					create = func() error {
						return cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(ctx, card, continuation)
					}
				}
				if err := create(); err != nil {
					t.Fatal(err)
				}
				proof.expect(t, render.ReconcileOrdinary)
				if err := create(); err != nil {
					t.Fatalf("exact creation replay: %v", err)
				}
				proof.expect(t, 0)
				if _, err := markDecisionCardRunTerminal(ctx, cards, runID, "cancelled", now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				proof.expect(t, render.ReconcileOrdinary)
				terminal, err := cards.GetDecisionCard(ctx, card.CardID)
				if err != nil || terminal.Status != decisioncard.StatusSuperseded {
					t.Fatalf("terminal card=%#v error=%v", terminal, err)
				}
				if _, err := markDecisionCardRunTerminal(ctx, cards, runID, "cancelled", now.Add(time.Minute)); err != nil {
					t.Fatalf("exact terminal replay: %v", err)
				}
				proof.expect(t, 0)
				if _, err := markDecisionCardRunTerminal(ctx, cards, runID, "completed", now.Add(2*time.Minute)); err == nil {
					t.Fatal("conflicting terminal state accepted")
				}
				proof.expect(t, 0)
				if err := create(); err == nil {
					t.Fatal("terminal run admitted card creation")
				}
				proof.expect(t, 0)
			})
		}
	}
}

func TestChannelPostCommitBindingHintsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			fixture := openOperatorChannelContractFixture(t, backend)
			subscriber := fixture.store.(interface {
				SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
			})
			subscription, err := subscriber.SubscribeChannelReconciliation(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(subscription.Close)
			assertHint := func(want render.ReconcileDemand, sequence uint64) {
				t.Helper()
				got, mark := subscription.BeginPass()
				if got != want || mark.Sequence != sequence {
					t.Fatalf("hint=%d sequence=%d, want %d/%d", got, mark.Sequence, want, sequence)
				}
			}
			now := time.Date(2026, 10, 9, 17, 0, 0, 0, time.UTC)
			principal, err := fixture.store.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			assertHint(0, 0)
			identity := operatorChannelContractIdentity("post-commit-" + backend)
			var binding operatorchannel.Binding
			for index, kind := range []operatorchannel.OperationKind{operatorchannel.OperationConnect, operatorchannel.OperationReconnect, operatorchannel.OperationRebind} {
				operationID := uuid.NewString()
				op, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
					OperationID: operationID, Kind: kind, PrincipalID: principal.ID, Interface: identity,
					ExpectedRevision: binding.Revision, RequestKeyHash: operationID, RequestHash: operationID,
					ProviderAuthority: operatorChannelProviderAuthority(), RequestedAt: now,
					ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
				})
				if err != nil {
					t.Fatal(err)
				}
				account, conversation := "account", "conversation"
				if kind == operatorchannel.OperationRebind {
					account, conversation = "other-account", "other-conversation"
				}
				claim, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, account, conversation, uuid.NewString()), now.Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				assertHint(0, uint64(index))
				req := operatorchannel.ConfirmRequest{
					OperationID: operationID, PrincipalID: principal.ID, ExpectedRevision: claim.Operation.Revision,
					Approve: true, ProviderAuthorityCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
				}
				_, binding, err = fixture.store.ConfirmChannelBinding(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				assertHint(render.ReconcileOrdinary|render.ReconcileNative, uint64(index+1))
				_, replay, err := fixture.store.ConfirmChannelBinding(ctx, req)
				if err != nil || replay.Revision != binding.Revision {
					t.Fatalf("binding replay=%#v err=%v", replay, err)
				}
				assertHint(0, uint64(index+1))
				now = now.Add(3 * time.Second)
			}
			bad := operatorchannel.UnbindRequest{
				OperationID: uuid.NewString(), PrincipalID: principal.ID, Interface: identity,
				ExpectedRevision: binding.Revision + 1, RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), RequestedAt: now,
			}
			if _, _, err := fixture.store.UnbindOperatorChannel(ctx, bad); err == nil {
				t.Fatal("stale unbind was accepted")
			}
			assertHint(0, 3)
			bad.ExpectedRevision = binding.Revision
			_, retired, err := fixture.store.UnbindOperatorChannel(ctx, bad)
			if err != nil || retired.Revision != binding.Revision+1 {
				t.Fatalf("retirement=%#v err=%v", retired, err)
			}
			assertHint(render.ReconcileOrdinary|render.ReconcileNative, 4)
			if _, _, err := fixture.store.UnbindOperatorChannel(ctx, bad); err != nil {
				t.Fatal(err)
			}
			assertHint(0, 4)
		})
	}
}
