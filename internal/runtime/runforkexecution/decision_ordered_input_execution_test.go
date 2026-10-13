package runforkexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642ForkOrderedGateInputUsesOwnCurrentGenerationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s := storetest.StartPostgresRuntimeStore(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(provider.Close)
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo,
				SourceRoot: canonicalrouting.CopyNestedPendingActivityReplyWithOrderedInputGate(t, provider.URL), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			ctx := runForkTestContext(t)
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			source := uuid.NewString()
			ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), source)
			ctx = effects.WithExecutionMode(ctx, executionmode.Live)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
			at := time.Now().UTC()
			bundle, _ := semanticview.Bundle(loaded.Source)
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: source,
				BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at})
			rt, _ := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.Live)
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
				Instance: flowidentity.Stored(loaded.Source, ".", source, source, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct ordered-input source: created=%t error=%v", created, err)
			}
			store := selected.(pendingReplyExecutionStore)
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.started", []byte(`{"account_id":"input-account","request_id":"input-request"}`))
			sourceCard := requirePendingOrderedGate(t, ctx, store, source)
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.note", []byte(`{"note":"open-input-cut"}`))
			page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: source, EventName: "request.noted"}, Limit: 2})
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("ordered-input cut: %+v error=%v", page, err)
			}
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.note", []byte(`{"note":"independently-active-source"}`))
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, source)
			if err != nil {
				t.Fatal(err)
			}
			result := activatePendingDecisionFork(t, ctx, owner, source, page.Events[0].EventID, loader, loaded)
			child := result.Materialization.ForkRunID
			card := requirePendingOrderedGate(t, ctx, store, child)
			principal, err := store.EnsureOperatorPrincipal(ctx, at)
			if err != nil {
				t.Fatal(err)
			}
			domain := storetest.DecisionCardDomain(selected)
			for _, mode := range []string{"text", "skip"} {
				t.Run(mode, func(t *testing.T) {
					request := forkCardControlRequest(card, principal.ID, "mailbox.begin_input")
					mutation := pipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "edit", PrincipalID: principal.ID, Now: request.Now}, card.CardContentHash)
					raw, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation)
					var draft struct {
						ID string `json:"input_draft_id"`
					}
					if err != nil || replayed || !isSelected || json.Unmarshal(raw, &draft) != nil || draft.ID == "" {
						t.Fatalf("begin ordered child input: raw=%s selected=%t replay=%t error=%v", raw, isSelected, replayed, err)
					}
					use, isSelected, err := owner.AcquireSelectedDecisionCardControl(ctx, child, runfork.ControlMailboxDecide)
					if err != nil || !isSelected || use == nil {
						t.Fatalf("acquire exact ordered input turn: selected=%t error=%v", isSelected, err)
					}
					t.Cleanup(func() {
						if err := use.Close(); err != nil {
							t.Error(err)
						}
					})
					for index, field := range []string{"first", "second"} {
						var progress decisioncard.InputFieldProgress
						if mode == "text" {
							progress, err = domain.AdvanceInputTextForTest(use.WorkContext(), draft.ID, principal.ID, "own-"+field, time.Now().UTC())
						} else {
							progress, err = domain.AdvanceInputSkipForTest(use.WorkContext(), draft.ID, principal.ID, time.Now().UTC())
						}
						if err != nil || progress.AcceptedField != field || progress.NextFieldIndex != index+1 || progress.Complete != (index == 1) || progress.Skipped != (mode == "skip") {
							t.Fatalf("ordered %s %s write: %+v error=%v", mode, field, progress, err)
						}
						if mode == "text" {
							value, present := progress.Fields.Lookup(field)
							if !present || value.Interface() != "own-"+field || progress.Fields.Len() != index+1 {
								t.Fatalf("ordered text discarded a field: %+v", progress)
							}
						} else if progress.Fields.Len() != 0 {
							t.Fatalf("optional skip invented field data: %+v", progress)
						}
						preview, previewErr := domain.PreviewInputTextForTest(use.WorkContext(), draft.ID, principal.ID, "next", time.Now().UTC())
						if index == 0 && (previewErr != nil || preview.AcceptedField != "second" || preview.NextFieldIndex != 2 || !preview.Complete) {
							t.Fatalf("partial progress was not durable: %+v error=%v", preview, previewErr)
						}
						if index == 1 && (previewErr == nil || !strings.Contains(previewErr.Error(), "no next field")) {
							t.Fatalf("complete ordered draft did not retain exact terminal progress: %v", previewErr)
						}
						if mode == "text" && index == 1 {
							value, present := progress.Fields.Lookup("first")
							if !present || value.Interface() != "own-first" {
								t.Fatal("final input replaced the earlier committed field")
							}
						}
					}
					if err := use.Close(); err != nil {
						t.Fatal(err)
					}
					request = forkCardControlRequest(card, principal.ID, "mailbox.cancel_input")
					mutation = pipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{CardID: card.CardID, InputDraftID: draft.ID, PrincipalID: principal.ID, Now: request.Now})
					if _, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation); err != nil || replayed || !isSelected {
						t.Fatalf("cancel only the child's completed draft: selected=%t replay=%t error=%v", isSelected, replayed, err)
					}
				})
			}
			actualSourceCard, err := store.GetDecisionCard(ctx, sourceCard.CardID)
			after, domainErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, source)
			actualChild, childErr := store.GetDecisionCard(ctx, card.CardID)
			if err != nil || domainErr != nil || childErr != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sourceCard, actualSourceCard) ||
				actualChild.Status != decisioncard.StatusPending || calls.Load() != 0 {
				t.Fatalf("input progress decided a card, ran the provider or changed the source: calls=%d errors=%v/%v/%v", calls.Load(), err, domainErr, childErr)
			}
			changes, err := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			progressCount := 0
			for _, change := range changes {
				if change.CardID == card.CardID && change.ChangeType == decisioncard.ChangeDraftProgressed {
					if change.RunID != child {
						t.Fatal("child draft progress leaked across runs")
					}
					progressCount++
				}
			}
			if progressCount != 4 {
				t.Fatalf("ordered input progress facts=%d, want4", progressCount)
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requirePendingOrderedGate(t *testing.T, ctx context.Context, store pendingReplyExecutionStore, runID string) decisioncard.Card {
	t.Helper()
	cards, cursor, err := store.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, AnchorKind: string(decisioncard.AnchorKindStageGate), Limit: 2})
	if err != nil || cursor != "" || len(cards) != 1 || cards[0].Status != decisioncard.StatusPending {
		t.Fatalf("exact ordered-gate inventory: %+v cursor=%q error=%v", cards, cursor, err)
	}
	card, err := store.GetDecisionCard(ctx, cards[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	return card
}
