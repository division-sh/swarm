package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type postCommitFaultHumanTaskStore struct {
	decisioncard.HumanTaskAcknowledgedCreationStore
	fault  error
	reject bool
	writes int
}

type humanTaskRuntimeLogBus struct {
	logs []runtimepipeline.RuntimeLogEntry
}

func (*humanTaskRuntimeLogBus) Publish(context.Context, events.Event) error { return nil }
func (*humanTaskRuntimeLogBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*humanTaskRuntimeLogBus) PublishDirectRoutes(context.Context, events.Event, []events.DeliveryRoute) error {
	return nil
}
func (b *humanTaskRuntimeLogBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.logs = append(b.logs, entry)
	return nil
}

func (s *postCommitFaultHumanTaskStore) CreateHumanTaskCardOutcome(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) (decisioncard.HumanTaskCreationResult, error) {
	if s.reject {
		return decisioncard.HumanTaskCreationResult{}, s.fault
	}
	result, err := s.HumanTaskAcknowledgedCreationStore.CreateHumanTaskCardOutcome(ctx, card, continuation)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	s.writes++
	return result, s.fault
}

func TestAskHumanAcknowledgedPostCommitErrorKeepsCardWithoutDuplicateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected humanTaskToolStore
			if backend == "sqlite" {
				selected = newSQLiteRuntimeToolStoreForTest(t)
			} else {
				selected = newPostgresHumanTaskToolStoreForTest(t)
			}
			const flowPath = "gateway/provider"
			actor := models.AgentConfig{
				ExecutionMode: "live", ID: "requester", Role: "worker",
				FlowID: "provider", FlowPath: flowPath, EntityID: uuid.NewString(),
				Tools: []string{"ask_human"}, Permissions: []string{"ask_human"},
			}
			bundle := loadWave1EntityToolBundle(t, actor, "provider", "provider_record", "", "provider_record:\n  status: text\n")
			bundle.FlowTree.ByID["provider"].Path = flowPath
			source := semanticview.Wrap(bundle)
			declarations := semanticview.AgentDeclarations(source)
			if len(declarations) != 1 {
				t.Fatalf("requester declarations = %d, want one", len(declarations))
			}
			plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
			if err != nil {
				t.Fatal(err)
			}
			actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, flowPath, "provider", flowPath)
			ctx, _, _ := seedReplyToolContext(t, selected)
			ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "human-task-ack-turn")
			ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, "tool_call:1:0:ask_human")
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, authorActivityTestSourceArtifactFact)
			ctx = runtimetools.WithActor(ctx, actor)
			fault := errors.Join(errors.New("post-commit cleanup failed"), errors.New("SQL password=private-token"))
			wrapped := &postCommitFaultHumanTaskStore{HumanTaskAcknowledgedCreationStore: selected, fault: fault}
			bus := &humanTaskRuntimeLogBus{}
			exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
				Config: &config.Config{}, HumanTaskStore: wrapped,
				AuthorityProvider: allowHumanTaskAuthority{}, WorkflowSource: source,
			})
			input := map[string]any{"scope": "flow", "category": "review", "description": "Review provider response"}
			out, err := exec.Execute(ctx, "ask_human", input)
			if err != nil {
				t.Fatalf("acknowledged creation reported retryable error: %v", err)
			}
			response, ok := out.(map[string]any)
			if !ok || response["status"] != "committed_with_post_commit_error" || response["card_status"] != decisioncard.StatusPending || response["write_committed"] != true || response["retry_write"] != false || response["post_commit_error_code"] != "human_task_create_post_commit_failure" {
				t.Fatalf("acknowledged response = %#v", out)
			}
			cardID, ok := response["card_id"].(string)
			if !ok || cardID == "" {
				t.Fatalf("missing committed card id: %#v", response)
			}
			encoded, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "private-token") || strings.Contains(string(encoded), "post-commit cleanup") {
				t.Fatalf("tool response exposed backend fault: %s", encoded)
			}
			card, err := selected.GetDecisionCard(ctx, cardID)
			if err != nil || card.CardID != cardID || card.Status != decisioncard.StatusPending {
				t.Fatalf("committed card readback = %+v, %v", card, err)
			}
			continuation, err := selected.LoadHumanTaskContinuation(ctx, cardID)
			if err != nil || continuation.CardID != cardID || continuation.State != decisioncard.HumanTaskContinuationPending {
				t.Fatalf("committed continuation readback = %+v, %v", continuation, err)
			}
			var diagnosticFound bool
			for _, entry := range bus.logs {
				if entry.Action != "human_task_create_post_commit_failure" {
					continue
				}
				detail, ok := entry.Detail.(map[string]any)
				if !ok || detail["post_commit_error"] != fault.Error() || detail["card_id"] != cardID {
					t.Fatalf("internal post-commit diagnostic = %+v", entry)
				}
				diagnosticFound = true
			}
			if !diagnosticFound {
				t.Fatalf("missing internal post-commit diagnostic: %+v", bus.logs)
			}

			replayed, err := exec.Execute(ctx, "ask_human", input)
			if err != nil || replayed.(map[string]any)["card_id"] != cardID {
				t.Fatalf("same-operation replay = %#v, %v", replayed, err)
			}
			var cards, continuations int
			query := "SELECT COUNT(*) FROM decision_cards WHERE card_id = ?"
			continuationQuery := "SELECT COUNT(*) FROM human_task_continuations WHERE card_id = ?"
			if backend == "postgres" {
				query = "SELECT COUNT(*) FROM decision_cards WHERE card_id = $1"
				continuationQuery = "SELECT COUNT(*) FROM human_task_continuations WHERE card_id = $1"
			}
			db := storetest.DatabaseForTest(selected)
			if err := db.QueryRowContext(ctx, query, cardID).Scan(&cards); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, continuationQuery, cardID).Scan(&continuations); err != nil {
				t.Fatal(err)
			}
			if cards != 1 || continuations != 1 || wrapped.writes != 2 {
				t.Fatalf("replayed creation: cards=%d continuations=%d writes=%d", cards, continuations, wrapped.writes)
			}

			wrapped.reject = true
			refusedCtx := runtimeeffects.WithLogicalOperationIdentity(ctx, "refused-human-task-turn")
			refused, err := exec.Execute(refusedCtx, "ask_human", input)
			if refused != nil || err == nil {
				t.Fatalf("unacknowledged refusal = %#v, %v", refused, err)
			}
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'human_task'").Scan(&cards); err != nil {
				t.Fatal(err)
			}
			if cards != 1 {
				t.Fatalf("unacknowledged refusal created another card: %d", cards)
			}
		})
	}
}
