package runforkexecution

import (
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
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642SourceAndForkDecideOwnPendingEffectsBothStores(t *testing.T) {
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
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var input map[string]string
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Errorf("read branch-owned provider input: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"account_id": input["account_id"], "request_id": input["request_id"], "result": "fork-approved-only"})
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
			sourceRun := uuid.NewString()
			ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), sourceRun)
			ctx = effects.WithExecutionMode(ctx, executionmode.Live)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
			at := time.Now().UTC()
			bundle, _ := semanticview.Bundle(loaded.Source)
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: sourceRun,
				BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at})
			rt, _ := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.Live)
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
				Instance: flowidentity.Stored(loaded.Source, ".", sourceRun, sourceRun, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct branch source: created=%t error=%v", created, err)
			}
			store := selected.(pendingReplyExecutionStore)
			publishPendingReplyInput(t, ctx, rt.Bus, sourceRun, "request.started", []byte(`{"account_id":"own-account","request_id":"own-request"}`))
			sourceCard := requirePendingReplyCard(t, ctx, store, sourceRun)
			publishPendingReplyInput(t, ctx, rt.Bus, sourceRun, "request.note", []byte(`{"note":"both-branches-start-pending"}`))
			page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: sourceRun, EventName: "request.noted"}, Limit: 2})
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" || calls.Load() != 0 {
				t.Fatalf("exact open-card cut: %+v calls=%d error=%v", page, calls.Load(), err)
			}
			principal, err := store.EnsureOperatorPrincipal(ctx, at)
			if err != nil {
				t.Fatal(err)
			}
			request, mutation := branchCardDecision(sourceCard, principal.ID, "reject")
			if _, replayed, err := rt.Pipeline.CommitDecisionCardMutation(ctx, request, mutation); err != nil || replayed {
				t.Fatalf("source's own rejection: replay=%t error=%v causes=%v", replayed, err, pendingReplyDecisionCauses(err))
			}
			sourceRejected, err := store.GetDecisionCard(ctx, sourceCard.CardID)
			sourceEffect, effectErr := store.LoadProposedEffectContinuation(ctx, sourceCard.CardID)
			sourceRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
			before, domainErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil || effectErr != nil || rowsErr != nil || domainErr != nil || sourceRejected.Verdict != "reject" ||
				sourceRejected.DecidedBy != principal.ID || sourceEffect.State != decisioncard.ProposedEffectOutcomeDispatched || calls.Load() != 0 {
				t.Fatalf("source rejection did not own its effect: card=%+v effect=%+v calls=%d errors=%v/%v/%v/%v",
					sourceRejected, sourceEffect, calls.Load(), err, effectErr, rowsErr, domainErr)
			}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: sourceRun, At: page.Events[0].EventID,
				AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: owner.ports.contexts.capability}})
			if err != nil || !result.Activation.Activated || result.Activation.SourceFrozen {
				t.Fatalf("fork at the earlier open card inherited or froze the later rejection: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			card := requirePendingReplyCard(t, ctx, store, child)
			rows, err := storetest.ReadReplyContextRecords(ctx, selected, child)
			if err != nil || len(rows) != 2 || card.CardID == sourceCard.CardID || card.DecidedBy != "" || card.Verdict != "" {
				t.Fatalf("fork inherited approval or lost its own open returns: card=%+v rows=%+v error=%v", card, rows, err)
			}
			request, mutation = branchCardDecision(card, principal.ID, "approve")
			if _, replayed, selectedRun, err := owner.CommitSelectedDecisionCardMutation(ctx, child, request, mutation); err != nil || replayed || !selectedRun {
				t.Fatalf("fork's own approval: selected=%t replay=%t error=%v causes=%v execution=%v", selectedRun, replayed, err,
					pendingReplyDecisionCauses(err), pendingReplyExecutionCauses(owner, child))
			}
			waitPendingReplyReturn(t, ctx, selected, store, child, rows)
			childDecided, err := store.GetDecisionCard(ctx, card.CardID)
			afterCard, cardErr := store.GetDecisionCard(ctx, sourceCard.CardID)
			afterEffect, effectErr := store.LoadProposedEffectContinuation(ctx, sourceCard.CardID)
			afterRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
			after, domainErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil || cardErr != nil || effectErr != nil || rowsErr != nil || domainErr != nil || calls.Load() != 1 ||
				childDecided.Verdict != "approve" || childDecided.DecidedBy != principal.ID || !reflect.DeepEqual(sourceRejected, afterCard) ||
				!reflect.DeepEqual(sourceEffect, afterEffect) || !reflect.DeepEqual(sourceRows, afterRows) || !reflect.DeepEqual(before, after) {
				t.Fatalf("branch decisions crossed runs: child=%+v source=%+v calls=%d errors=%v/%v/%v/%v/%v", childDecided, afterCard,
					calls.Load(), err, cardErr, effectErr, rowsErr, domainErr)
			}
		})
	}
}

func branchCardDecision(card decisioncard.Card, principal, verdict string) (apiidempotency.Request, pipeline.DecisionCardMutation) {
	at, eventID := time.Now().UTC(), uuid.NewString()
	request := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal), ResourceID: card.CardID,
		RequestHash: "branch-owned-" + eventID, IdempotencyKey: uuid.NewString(), Now: at, TTL: time.Hour}
	mutation := pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: card.CardID, Verdict: verdict, PrincipalID: principal,
		ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: eventID, Now: at})
	return request, mutation
}
