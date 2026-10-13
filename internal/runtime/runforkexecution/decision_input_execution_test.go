package runforkexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
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
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642ForkDecisionInputControlsRequireCurrentGenerationBothStores(t *testing.T) {
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
				SourceRoot: canonicalrouting.CopyNestedPendingActivityReply(t, provider.URL), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
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
				t.Fatalf("construct control source: created=%t error=%v", created, err)
			}
			store := selected.(pendingReplyExecutionStore)
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.started", []byte(`{"account_id":"control-account","request_id":"control-request"}`))
			sourceCard := requirePendingReplyCard(t, ctx, store, source)
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.note", []byte(`{"note":"open-cut"}`))
			page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: source, EventName: "request.noted"}, Limit: 2})
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("control cut: %+v error=%v", page, err)
			}
			// Advance the ordinary source so the earlier fork cut cannot freeze it.
			publishPendingReplyInput(t, ctx, rt.Bus, source, "request.note", []byte(`{"note":"source-still-owns-its-card"}`))
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: source, At: page.Events[0].EventID,
				AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: owner.ports.contexts.capability}})
			if err != nil || !result.Activation.Activated || result.Activation.SourceFrozen {
				t.Fatalf("activate independent decision controls: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			card := requirePendingReplyCard(t, ctx, store, child)
			principal, err := store.EnsureOperatorPrincipal(ctx, at)
			if err != nil {
				t.Fatal(err)
			}
			request := forkCardControlRequest(card, principal.ID, "mailbox.begin_input")
			mutation := pipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", PrincipalID: principal.ID, Now: request.Now}, card.CardContentHash)
			raw, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation)
			var draft struct {
				ID string `json:"input_draft_id"`
			}
			if err != nil || replayed || !isSelected || json.Unmarshal(raw, &draft) != nil || draft.ID == "" {
				t.Fatalf("fork begin_input: raw=%s selected=%t replay=%t error=%v causes=%v", raw, isSelected, replayed, err, pendingReplyDecisionCauses(err))
			}
			if replay, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation); err != nil || !replayed || !isSelected || string(replay) != string(raw) {
				t.Fatalf("fork input replay changed identity: selected=%t replay=%t raw=%s error=%v", isSelected, replayed, replay, err)
			}
			beforeCard, err := store.GetDecisionCard(ctx, card.CardID)
			beforeChanges, changeErr := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
			if err != nil || changeErr != nil {
				t.Fatalf("read control boundary: %v/%v", err, changeErr)
			}
			use, isSelected, err := owner.AcquireSelectedDecisionCardControl(ctx, child, runfork.ControlMailboxDecide)
			if err != nil || !isSelected || use == nil {
				t.Fatalf("acquire actual input control: selected=%t error=%v", isSelected, err)
			}
			t.Cleanup(func() {
				if err := use.Close(); err != nil {
					t.Error(err)
				}
			})
			domain := storetest.DecisionCardDomain(selected)
			owned := use.WorkContext()
			evidence, present := startupownership.GenerationGrantEvidenceFromContext(owned)
			if !present {
				t.Fatal("retained input control lost its admitted grant")
			}
			evidence.StateVersion++
			stale := startupownership.WithGenerationGrantEvidence(owned, evidence)
			if _, err := domain.ApplyDeferralForTest(stale, decisioncard.DeferRequest{CardID: card.CardID, PrincipalID: principal.ID, Until: at.Add(time.Hour), Now: time.Now().UTC()}); err == nil {
				t.Fatal("a descriptor differing from the current native grant deferred a card")
			}
			if _, err := domain.BeginInputForTest(owned, decisioncard.BeginInputRequest{CardID: sourceCard.CardID, Verdict: "revise", PrincipalID: principal.ID, Now: time.Now().UTC()}); err == nil {
				t.Fatal("fork execution acquired its ordinary source's input draft")
			}
			canceled, cancel := context.WithCancel(owned)
			cancel()
			if _, err := domain.CancelInputForTest(canceled, decisioncard.CancelInputRequest{CardID: card.CardID, InputDraftID: draft.ID, PrincipalID: principal.ID, Now: time.Now().UTC()}); err == nil {
				t.Fatal("canceled input control mutated its draft")
			}
			if err := use.Close(); err != nil {
				t.Fatal(err)
			}
			afterCard, err := store.GetDecisionCard(ctx, card.CardID)
			afterChanges, changeErr := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
			if err != nil || changeErr != nil || !reflect.DeepEqual(beforeCard, afterCard) || !reflect.DeepEqual(beforeChanges, afterChanges) {
				t.Fatalf("refused controls changed card/history: %v/%v", err, changeErr)
			}
			request = forkCardControlRequest(card, principal.ID, "mailbox.cancel_input")
			mutation = pipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{CardID: card.CardID, InputDraftID: draft.ID, PrincipalID: principal.ID, Now: request.Now})
			if _, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation); err != nil || replayed || !isSelected {
				t.Fatalf("fork cancel_input: selected=%t replay=%t error=%v", isSelected, replayed, err)
			}
			request = forkCardControlRequest(card, principal.ID, "mailbox.defer")
			until := request.Now.Add(10 * time.Minute)
			mutation = pipeline.NewDecisionCardDeferral(decisioncard.DeferRequest{CardID: card.CardID, PrincipalID: principal.ID, Until: until, Now: request.Now})
			if _, replayed, isSelected, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation); err != nil || replayed || !isSelected {
				t.Fatalf("fork defer: selected=%t replay=%t error=%v", isSelected, replayed, err)
			}
			deferred, err := store.GetDecisionCard(ctx, card.CardID)
			original, sourceErr := store.GetDecisionCard(ctx, sourceCard.CardID)
			if err != nil || sourceErr != nil || !deferred.DeferredUntil.Equal(decisioncard.CanonicalTimestamp(until)) ||
				deferred.Status != decisioncard.StatusPending || !reflect.DeepEqual(sourceCard, original) || calls.Load() != 0 {
				t.Fatalf("input/defer authorized an effect or a source decision: child=%+v source=%+v calls=%d errors=%v/%v", deferred, original, calls.Load(), err, sourceErr)
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
			if use, _, err := owner.AcquireSelectedDecisionCardControl(ctx, child, runfork.ControlMailboxDefer); err == nil {
				_ = use.Close()
				t.Fatal("retired execution reopened a decision turn")
			}
		})
	}
}

func forkCardControlRequest(card decisioncard.Card, principal, method string) apiidempotency.Request {
	return apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal), ResourceID: card.CardID,
		RequestHash: "own-input-" + uuid.NewString(), IdempotencyKey: uuid.NewString(), Now: time.Now().UTC(), TTL: time.Hour}
}
