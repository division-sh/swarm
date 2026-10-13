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
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type pendingReplyExecutionStore interface {
	decisioncard.Store
	decisioncard.ProposedEffectStore
	pipeline.ActivityAttemptJournal
	replycontext.Store
	bus.PreparedPublishEventReader
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
}

func TestIssue642FreshPendingEffectReturnsOwnNestedReplyBothStores(t *testing.T) {
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
					t.Errorf("read approved child input: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"account_id": input["account_id"], "request_id": input["request_id"], "result": "child-approved-only"})
			}))
			t.Cleanup(provider.Close)
			root := canonicalrouting.CopyNestedPendingActivityReply(t, provider.URL)
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			ctx := runForkTestContext(t)
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), runID)
			ctx = effects.WithExecutionMode(ctx, executionmode.Live)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
			at := time.Now().UTC()
			bundle, _ := semanticview.Bundle(loaded.Source)
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
				BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at})
			contexts := make(chan context.Context, 1)
			owner.ports.busDurable.RunLifecycle = nestedDeadlineServingContext{OperationOwner: owner.ports.busDurable.RunLifecycle, sourceRun: runID, contexts: contexts}
			rt, _ := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.Live)
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
				Instance: flowidentity.Stored(loaded.Source, ".", runID, runID, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct pending reply source: created=%t error=%v", created, err)
			}
			publishPendingReplyInput(t, ctx, rt.Bus, runID, "request.started", []byte(`{"account_id":"original-account","request_id":"original-request"}`))
			store := selected.(pendingReplyExecutionStore)
			original := requirePendingReplyCard(t, ctx, store, runID)
			originalEffect, err := store.LoadProposedEffectContinuation(ctx, original.CardID)
			if err != nil || originalEffect.ReplyContextID == "" || calls.Load() != 0 {
				t.Fatalf("source proposal executed or lost its reply: %+v calls=%d error=%v", originalEffect, calls.Load(), err)
			}
			publishPendingReplyInput(t, ctx, rt.Bus, runID, "request.note", []byte(`{"note":"approval-still-pending"}`))
			page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "request.noted"}, Limit: 2})
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("pending approval lacks exact committed note cut: %+v error=%v", page, err)
			}
			cut := page.Events[0].EventID
			sourceRows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
			if err != nil || len(sourceRows) != 2 {
				t.Fatalf("source lacks two actual open returns: %+v error=%v", sourceRows, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			options := SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: owner.ports.contexts.capability}
			prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{SourceRunID: runID, At: cut, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			}()
			request, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			staged, err := owner.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatalf("stage pending work without granting event-cut execution: %v", err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			stagedCard := requirePendingReplyCard(t, ctx, store, staged.ForkRunID)
			startPlan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: staged.ForkRunID, AtStart: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("pending context-only source: run=%s start=%s card_created=%s cut_revision=%d entities=%d", staged.ForkRunID,
				startPlan.ForkPoint.Timestamp.Format(time.RFC3339Nano), stagedCard.CreatedAt.Format(time.RFC3339Nano), startPlan.ForkPoint.Revision, len(startPlan.Entities))
			stagedBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, staged.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: staged.ForkRunID, AtStart: true, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
				ForkOperation: &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "pending-reply-proof", IdempotencyKey: uuid.NewString(),
					TransportHash: "pending-reply-proof", SourceRunID: staged.ForkRunID, AtStart: true, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(),
					AllowSourceFreeze: true, ContractSelection: selection}})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("activate pending effect child: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			projected := requirePendingReplyCard(t, ctx, store, child)
			pending, err := store.LoadProposedEffectContinuation(ctx, projected.CardID)
			childRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, child)
			if err != nil || rowsErr != nil || len(childRows) != 2 || pending.ReplyContextID == originalEffect.ReplyContextID ||
				pending.ReplyContextID == "" || !pending.Input.Equal(originalEffect.Input) || pending.State != decisioncard.ProposedEffectPending || calls.Load() != 0 {
				t.Fatalf("child proposal borrowed source authority or changed its input: pending=%+v rows=%+v calls=%d errors=%v/%v", pending, childRows, calls.Load(), err, rowsErr)
			}
			wait, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			var serving context.Context
			select {
			case serving = <-contexts:
			case <-wait.Done():
				t.Fatal("pending child did not acquire current selected execution")
			}
			var childPipeline *pipeline.PipelineCoordinator
			owner.ports.contexts.mu.Lock()
			for _, entry := range owner.ports.contexts.entries {
				if entry.binding.ForkRunID == child && entry.retained != nil && entry.retained.retainedRuntime != nil {
					childPipeline, _ = entry.retained.retainedRuntime.pipeline.(*pipeline.PipelineCoordinator)
				}
			}
			owner.ports.contexts.mu.Unlock()
			if childPipeline == nil {
				t.Fatal("pending child lacks its actual retained pipeline")
			}
			approvePendingReplyCard(t, serving, store, childPipeline, projected)
			waitPendingReplyReturn(t, serving, selected, store, child, childRows)
			if calls.Load() != 1 {
				t.Fatalf("fresh child approval provider calls=%d, want1", calls.Load())
			}
			stagedAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, staged.ForkRunID)
			stagedActual, stagedErr := store.GetDecisionCard(ctx, stagedCard.CardID)
			if err != nil || stagedErr != nil || !reflect.DeepEqual(stagedBefore, stagedAfter) || !reflect.DeepEqual(stagedCard, stagedActual) {
				t.Fatalf("approved child changed its immediate staged source: errors=%v/%v", err, stagedErr)
			}
			actualCard, err := store.GetDecisionCard(ctx, original.CardID)
			actualEffect, effectErr := store.LoadProposedEffectContinuation(ctx, original.CardID)
			afterRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, runID)
			after, domainErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, runID)
			if err != nil || effectErr != nil || rowsErr != nil || domainErr != nil || !reflect.DeepEqual(original, actualCard) ||
				!reflect.DeepEqual(originalEffect, actualEffect) || !reflect.DeepEqual(sourceRows, afterRows) || !reflect.DeepEqual(before, after) {
				t.Fatalf("child approval/return changed source pending work: errors=%v/%v/%v/%v", err, effectErr, rowsErr, domainErr)
			}
		})
	}
}

func publishPendingReplyInput(t *testing.T, ctx context.Context, eventBus *bus.EventBus, runID, name string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), events.EventType(name), "pending-reply-proof", "", payload,
		0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC(), executionmode.Live)
	if err := eventBus.PublishAndWait(ctx, input); err != nil {
		t.Fatalf("publish actual %s input: %v", name, err)
	}
}

func requirePendingReplyCard(t *testing.T, ctx context.Context, store pendingReplyExecutionStore, runID string) decisioncard.Card {
	t.Helper()
	cards, cursor, err := store.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || cursor != "" || len(cards) != 1 || cards[0].Status != decisioncard.StatusPending {
		t.Fatalf("exact pending-card inventory: %+v cursor=%q error=%v", cards, cursor, err)
	}
	card, err := store.GetDecisionCard(ctx, cards[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	return card
}

func approvePendingReplyCard(t *testing.T, ctx context.Context, store pendingReplyExecutionStore, coordinator *pipeline.PipelineCoordinator, card decisioncard.Card) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	at, decisionID := time.Now().UTC(), uuid.NewString()
	principal, err := store.EnsureOperatorPrincipal(ctx, at)
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: card.CardID,
		RequestHash: "pending-reply-" + decisionID, IdempotencyKey: uuid.NewString(), Now: at, TTL: time.Hour}
	mutation := pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal.ID,
		ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: decisionID, Now: at})
	if _, replayed, err := coordinator.CommitDecisionCardMutation(ctx, request, mutation); err != nil || replayed {
		t.Fatalf("approve only the fresh child effect: replay=%t error=%v", replayed, err)
	}
}

func waitPendingReplyReturn(t *testing.T, ctx context.Context, selected any, store pendingReplyExecutionStore, child string, rows []replycontext.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		closed := 0
		for _, row := range rows {
			actual, err := store.LoadReplyContext(ctx, row.ID)
			if err != nil || !actual.SameIdentity(row) {
				t.Fatalf("approved return changed its own row: %+v error=%v", actual, err)
			}
			if actual.State == replycontext.StateTerminal && actual.AcceptedReplyEventID != "" {
				returned, found, err := store.LoadPreparedPublishEvent(ctx, actual.AcceptedReplyEventID)
				if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != actual.Origin ||
					returned.DeliveryRoutes[0].Context.ReplyContextID() != actual.EnclosingReplyContextID {
					t.Fatalf("approved nested result borrowed another return binding: %+v found=%t error=%v", returned, found, err)
				}
				closed++
			}
		}
		finished, err := storetest.ReadLifecycleEventCardinality(ctx, selected, child, "request.finished")
		if err != nil || finished > 1 {
			t.Fatalf("approved final requester cardinality=%d error=%v", finished, err)
		}
		incomplete, err := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, child)
		if err != nil {
			t.Fatal(err)
		}
		if closed == 2 && finished == 1 && incomplete == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("approved child did not finish its two own returns: closed=%d finished=%d pending=%d cause=%v", closed, finished, incomplete, context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
