package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

func TestChannelActivityNativeAcknowledgementCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []string{"start", "claim", "complete", "uncertain"} {
			for _, phase := range []string{"entry_cancel", "callback_cancel", "commit_refused", "commit_ack_lost", "commit_admitted_cancel", "healthy"} {
				t.Run(backend+"/"+operation+"/"+phase, func(t *testing.T) {
					fixture, control := openCompletionOutcomeFixture(t, backend)
					ctx, cancel := context.WithCancel(testAuthorActivityContext())
					defer cancel()
					card, continuation := newRootProposedEffectTestCard(t, fixture.authority.Target.RunID, time.Now().UTC())
					cards := fixture.store.(decisioncard.ProposedEffectStore)
					if err := cards.CreateProposedEffectCard(ctx, card, continuation); err != nil {
						t.Fatal(err)
					}
					record := runtimepipeline.ActivityAttemptRecord{
						RequestEventID: continuation.RequestEventID, RunID: continuation.RunID, ExecutionMode: continuation.ExecutionMode,
						SourceEventID: continuation.SourceEventID, EntityID: continuation.EntityID, FlowInstance: continuation.FlowInstance,
						NodeID: continuation.NodeID, HandlerEventKey: continuation.HandlerEventKey, ActivityID: continuation.ActivityID,
						Tool: continuation.Tool, EffectClass: string(continuation.EffectClass), Attempt: 1,
						SuccessEvent: continuation.SuccessEvent, FailureEvent: continuation.FailureEvent, InputHash: continuation.EffectContentHash,
					}
					journal := fixture.store.(activityStoryJournal)
					write := journal.StartActivityAttempt
					var predecessor runtimepipeline.ActivityAttemptRecord
					switch operation {
					case "claim":
						write = journal.ClaimActivityAttemptForLoopGeneration
					case "complete", "uncertain":
						var inserted bool
						var err error
						predecessor, inserted, err = journal.StartActivityAttempt(ctx, record)
						if err != nil || !inserted {
							t.Fatalf("start before native terminal cut: inserted=%t error=%v", inserted, err)
						}
						status := runtimepipeline.ActivityAttemptStatusSucceeded
						write = journal.CompleteActivityAttempt
						if operation == "uncertain" {
							status = runtimepipeline.ActivityAttemptStatusUncertain
							write = journal.MarkActivityAttemptUncertain
						}
						record = activityStoryTerminal(t, predecessor, status)
					}
					proof := observeChannelHints(t, fixture.store.(interface {
						SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
					}))
					control.phase, control.cancel, control.failure = phase, cancel, errors.New("independent native activity COMMIT failure")
					control.statement = func(query string) bool {
						query = strings.Join(strings.Fields(strings.ToUpper(query)), " ")
						return strings.HasPrefix(query, "INSERT INTO ACTIVITY_ATTEMPTS (") || strings.HasPrefix(query, "UPDATE ACTIVITY_ATTEMPTS SET ")
					}
					control.enabled.Store(true)
					if phase == "entry_cancel" {
						cancel()
					}
					actual, acknowledged, err := write(ctx, record)
					control.enabled.Store(false)
					wantAck := phase == "healthy" || phase == "commit_admitted_cancel"
					if acknowledged != wantAck {
						t.Fatalf("native acknowledgment=%t want=%t error=%v", acknowledged, wantAck, err)
					}
					if wantAck {
						if err != nil || actual.RequestEventID != record.RequestEventID {
							t.Fatalf("acknowledged native result lost: %+v error=%v", actual, err)
						}
						proof.expect(t, render.ReconcileOrdinary)
					} else {
						if !reflect.DeepEqual(actual, runtimepipeline.ActivityAttemptRecord{}) {
							t.Fatalf("unacknowledged native result exposed attempt: %+v", actual)
						}
						proof.expect(t, 0)
						cause := control.failure
						if phase == "entry_cancel" || phase == "callback_cancel" {
							cause = context.Canceled
						}
						if !errors.Is(err, cause) {
							t.Fatalf("native failure cause lost: %v want=%v", err, cause)
						}
					}
					wantWrites := int32(1)
					if phase == "entry_cancel" {
						wantWrites = 0
					}
					if control.writes.Load() != wantWrites {
						t.Fatalf("native cut replayed or missed exact journal write: %d want=%d", control.writes.Load(), wantWrites)
					}
					stored, found, readErr := journal.LoadActivityAttempt(testAuthorActivityContext(), record.RequestEventID)
					if readErr != nil {
						t.Fatal(readErr)
					}
					if wantAck || phase == "commit_ack_lost" {
						wantStatus := "started"
						if operation == "complete" {
							wantStatus = "succeeded"
						} else if operation == "uncertain" {
							wantStatus = "uncertain"
						}
						if !found || stored.Status != wantStatus {
							t.Fatalf("native COMMIT evidence not durable: %+v found=%t", stored, found)
						}
					} else if predecessor.RequestEventID == "" {
						if found {
							t.Fatalf("rolled-back start survived: %+v", stored)
						}
					} else if !found || !reflect.DeepEqual(predecessor, stored) {
						t.Fatalf("rolled-back terminal cut changed predecessor: %+v found=%t", stored, found)
					}
				})
			}
		}
	}
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
