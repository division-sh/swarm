package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestMailboxPostCommitFaultsPreserveExactCompletionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"} {
					cuts := []string{"request_release"}
					if method == "mailbox.decide" || method == "mailbox.defer" {
						cuts = append(cuts, "finalize", "dispatch")
					}
					for _, cut := range cuts {
						t.Run(string(kind)+"/"+method+"/"+cut, func(t *testing.T) {
							f := mailboxCompletionFixtureInRuntime(t, rt, owner)
							params := mailboxPrincipalMutationParams(t, f, kind, method)
							req := mailboxPrincipalRequest(t, rt, method, params)
							fault := &mailboxPostCommitFault{cut: cut, err: fmt.Errorf("mailbox post-commit cut: %s", cut)}
							// Only the request coordinator is replaced. Source loading, card production,
							// publication planning, domain persistence, and served retry remain real.
							coordinator := mailboxFaultCoordinator(t, rt, fault)
							mutation, err := mailboxCardMutation(req, params)
							if err != nil {
								t.Fatal(err)
							}
							_, replayed, err := coordinator.CommitDecisionCardMutation(f.ctx, req, mutation)
							if !errors.Is(err, fault.err) || replayed || fault.hits.Load() != 1 {
								t.Fatalf("exact post-commit cut not reached: replay=%t hits=%d err=%v", replayed, fault.hits.Load(), err)
							}
							var stored string
							if err := rt.DB.QueryRow(`SELECT CAST(response AS TEXT) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&stored); err != nil {
								t.Fatalf("post-commit failure lost original response: %v", err)
							}
							var original map[string]any
							if err := json.Unmarshal([]byte(stored), &original); err != nil {
								t.Fatal(err)
							}
							card, err := owner.GetDecisionCard(f.ctx, req.ResourceID)
							if err != nil {
								t.Fatal(err)
							}
							var changes int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM decision_card_changes WHERE card_id=$1`, req.ResourceID).Scan(&changes); err != nil {
								t.Fatal(err)
							}
							for range 2 {
								var replay map[string]any
								requireServedJSONRPCResult(t, rt.Endpoint, method, params, &replay)
								if replay["idempotency_replayed"] != true {
									t.Fatalf("retry replanned committed mutation: %v", replay)
								}
								delete(replay, "idempotency_replayed")
								if !reflect.DeepEqual(original, replay) {
									t.Fatalf("retry differs from stored result: %s / %v", stored, replay)
								}
							}
							var count int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&count); err != nil || count != 1 {
								t.Fatalf("completion count=%d err=%v", count, err)
							}
							if fault.hits.Load() != 1 {
								t.Fatal("retry re-entered the failed post-commit operation")
							}
							after, err := owner.GetDecisionCard(f.ctx, req.ResourceID)
							if err != nil || !reflect.DeepEqual(card, after) {
								t.Fatalf("retry mutated committed card: err=%v before=%+v after=%+v", err, card, after)
							}
							var afterChanges int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM decision_card_changes WHERE card_id=$1`, req.ResourceID).Scan(&afterChanges); err != nil || afterChanges != changes {
								t.Fatalf("retry appended card changes: before=%d after=%d err=%v", changes, afterChanges, err)
							}
						})
					}
				}
			}
		})
	}
}

type mailboxPostCommitFault struct {
	cut          string
	err          error
	hits         atomic.Int32
	beforeCommit func(context.Context, runtimepipeline.DecisionCardMutationCommand) error
}

func (f *mailboxPostCommitFault) at(cut string) error {
	if f.cut != cut {
		return nil
	}
	f.hits.Add(1)
	return f.err
}

type mailboxFaultPublicationBus struct {
	*runtimebus.EventBus
	fault *mailboxPostCommitFault
}

func (b *mailboxFaultPublicationBus) FinalizeEnginePublications(ctx context.Context, publications []runtimeengine.CommittedDurablePublication) error {
	if err := b.fault.at("finalize"); err != nil {
		return err
	}
	return b.EventBus.FinalizeEnginePublications(ctx, publications)
}

func (b *mailboxFaultPublicationBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return b
}

func (b *mailboxFaultPublicationBus) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if err := b.fault.at("dispatch"); err != nil {
		return err
	}
	return b.EventBus.EngineDispatcher().DispatchPostCommit(ctx, intents)
}

type mailboxFaultPersistence struct {
	runtimepipeline.WorkflowPersistenceOwner
	fault *mailboxPostCommitFault
}

func (s *mailboxFaultPersistence) AcquireDecisionCardMutation(ctx context.Context, req apiidempotency.Request, mutation runtimepipeline.DecisionCardMutation) (runtimepipeline.DecisionCardMutationLease, error) {
	lease, err := s.WorkflowPersistenceOwner.AcquireDecisionCardMutation(ctx, req, mutation)
	if err != nil {
		return nil, err
	}
	return &mailboxFaultRequestLease{DecisionCardMutationLease: lease, fault: s.fault}, nil
}

type mailboxFaultRequestLease struct {
	runtimepipeline.DecisionCardMutationLease
	fault *mailboxPostCommitFault
}

func (l *mailboxFaultRequestLease) Commit(ctx context.Context, command runtimepipeline.DecisionCardMutationCommand) (runtimepipeline.CommittedDecisionCardMutation, error) {
	if l.fault.beforeCommit != nil {
		if err := l.fault.beforeCommit(ctx, command); err != nil {
			return runtimepipeline.CommittedDecisionCardMutation{}, err
		}
	}
	return l.DecisionCardMutationLease.Commit(ctx, command)
}

func (l *mailboxFaultRequestLease) Release(ctx context.Context) error {
	return errors.Join(l.DecisionCardMutationLease.Release(ctx), l.fault.at("request_release"))
}

func mailboxFaultCoordinator(t *testing.T, rt servedControlProofRuntime, fault *mailboxPostCommitFault) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	bus := &mailboxFaultPublicationBus{EventBus: rt.Runtime.Bus, fault: fault}
	opts := runtimepipeline.PipelineCoordinatorOptions{
		Module: rt.Runtime.Options.WorkflowModule, ExecutionPosture: rt.Runtime.ExecutionPosture,
		ReceiverExecution: eventreceiver.NormalExecution(), DeliveryRuntime: bus, FlowRoutes: bus,
		SourceArtifactFact: rt.Runtime.Options.SourceArtifactFact, WorkOwner: rt.Runtime.WorkOccurrence(),
	}
	if selected := rt.SQLite; selected != nil {
		opts.Persistence = runtimepipeline.NewWorkflowPersistence(&mailboxFaultPersistence{WorkflowPersistenceOwner: selected, fault: fault})
		opts.DeliveryStore, opts.DeadLetters, opts.PipelineObligations = selected, selected, selected.PipelineObligations()
		opts.DecisionCards, opts.ProposedEffects, opts.HumanTasks = selected, selected, selected
		opts.DecisionCardDraftExpiry, opts.HumanTaskExpiry, opts.RunLifecycle = selected, selected, selected
		opts.RunBundleAvailability = selected
	} else {
		selected := rt.Postgres
		opts.Persistence = runtimepipeline.NewWorkflowPersistence(&mailboxFaultPersistence{WorkflowPersistenceOwner: selected, fault: fault})
		opts.DeliveryStore, opts.DeadLetters, opts.PipelineObligations = selected, selected, selected.PipelineObligations()
		opts.DecisionCards, opts.ProposedEffects, opts.HumanTasks = selected, selected, selected
		opts.DecisionCardDraftExpiry, opts.HumanTaskExpiry, opts.RunLifecycle = selected, selected, selected
		opts.RunBundleAvailability = selected
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, opts)
	if coordinator == nil {
		t.Fatal("real selected-store fault coordinator was not admitted")
	}
	return coordinator
}
