package runtime

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	runtimeagents "empireai/internal/runtime/agents"
	runtimeactor "empireai/internal/runtime/actorctx"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestRuntimeToolExecutor_HumanTaskRequest_InsertsAndEmits(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	reqCh := bus.Subscribe("coordinator", events.EventType("human_task.requested"))

	exec := runtimetools.NewExecutor(bus, nil, nil)
	exec.SetSQLDB(db)
	exec.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				CategoriesEnabled: []string{"verification"},
			},
		},
	})

	mock.ExpectQuery("INSERT INTO human_tasks").
		WithArgs(
			"agent-1",
			"v1",
			"verification",
			"call supplier",
			sqlmock.AnyArg(), // talking_points json
			"",
			"high",
			sqlmock.AnyArg(), // deadline sql.NullTime
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("task-123"))

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})

	out, err := exec.Execute(ctx, "human_task_request", map[string]any{
		"category":       "verification",
		"description":    "call supplier",
		"talking_points": []string{"ask about lead times"},
		"priority":       "high",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["task_id"] != "task-123" {
		t.Fatalf("expected task_id task-123, got %#v", out)
	}

	select {
	case evt := <-reqCh:
		if evt.SourceAgent != "agent-1" {
			t.Fatalf("unexpected source: %s", evt.SourceAgent)
		}
		if string(evt.Type) != "human_task.requested" {
			t.Fatalf("unexpected type: %s", evt.Type)
		}
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("payload json: %v", err)
		}
		if payload["task_id"] != "task-123" {
			t.Fatalf("expected payload.task_id task-123, got %#v", payload["task_id"])
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected human_task.requested event")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestRuntimeToolExecutor_HumanTaskDecide_ApprovalBudgetExhaustedForcesDeferral(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	// Create channel for requesting agent without subscribing to any patterns.
	reqAgentCh := bus.Subscribe("agent-req")
	coordCh := bus.Subscribe("coordinator", events.EventType("human_task.*"))

	exec := runtimetools.NewExecutor(bus, nil, nil)
	exec.SetSQLDB(db)
	exec.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
			},
		},
	})

	// Not requeued.
	mock.ExpectQuery("SELECT COALESCE\\(requeue_count, 0\\) FROM human_tasks").
		WithArgs("task-1").
		WillReturnRows(sqlmock.NewRows([]string{"requeue_count"}).AddRow(0))

	// Approved this week already at cap.
	mock.ExpectQuery("SELECT COALESCE\\(count\\(\\*\\), 0\\)\\s+FROM human_tasks\\s+WHERE reviewed_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Update: should set status=deferred and return requesting_agent + vertical_id.
	mock.ExpectQuery("UPDATE human_tasks\\s+SET status = \\$2,").
		WithArgs("task-1", "deferred", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"requesting_agent", "vertical_id"}).AddRow("agent-req", "v1"))

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{
		ID:         "coordinator-1",
		Role:       "empire-coordinator",
		Mode:       "holding",
		VerticalID: "",
	})

	out, err := exec.Execute(ctx, "human_task_decide", map[string]any{
		"task_id":  "task-1",
		"decision": "approved",
		"reason":   "ok",
	})
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["status"] != "deferred" {
		t.Fatalf("expected deferred, got %#v", out)
	}

	// Must emit deferred and deliver to coordinator subscribers AND requesting agent (forced).
	gotDeferred := false
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) && !gotDeferred {
		select {
		case evt := <-coordCh:
			if string(evt.Type) == "human_task.deferred" {
				gotDeferred = true
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !gotDeferred {
		t.Fatal("expected coordinator to receive human_task.deferred")
	}

	select {
	case evt := <-reqAgentCh:
		if string(evt.Type) != "human_task.deferred" {
			t.Fatalf("expected requesting agent to receive deferred, got %s", evt.Type)
		}
	default:
		t.Fatal("expected requesting agent to receive forced outcome event")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLLMAgent_InjectsHumanTaskOutcomeAsAsyncToolResult(t *testing.T) {
	rt := &humanTaskOutcomeRuntime{}
	te := noopToolExecForAgentTest{}

	agent := runtimeagents.NewLLMAgent(models.AgentConfig{
		ID:   "agent-req",
		Type: "worker",
		Role: "pm-agent",
		Mode: "operating",
	}, rt, te, nil)

	evtPayload := mustJSON(map[string]any{
		"task_id":          "task-9",
		"requesting_agent": "agent-req",
		"vertical_id":      "v1",
		"result":           "done",
	})
	evt := events.Event{
		ID:          "evt-1",
		Type:        events.EventType("human_task.completed"),
		SourceAgent: "coordinator",
		VerticalID:  "v1",
		Payload:     evtPayload,
		CreatedAt:   time.Now(),
	}
	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}

	// The injected async tool result should exist in the conversation session messages.
	if agent.Conversation() == nil || agent.Conversation().Session == nil {
		t.Fatal("expected conversation session to be initialized")
	}
	found := false
	for _, m := range agent.Conversation().Session.Messages {
		if m.Role != "tool" {
			continue
		}
		// Should be a JSON array with one entry.
		var arr []map[string]any
		if err := json.Unmarshal([]byte(m.Content), &arr); err != nil {
			continue
		}
		if len(arr) != 1 {
			continue
		}
		if arr[0]["name"] == "human_task_request" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected injected tool-result style message in session, got %+v", agent.Conversation().Session.Messages)
	}
}

type noopToolExecForAgentTest struct{}

func (noopToolExecForAgentTest) Execute(context.Context, string, any) (any, error) { return nil, nil }

type humanTaskOutcomeRuntime struct{}

func (h *humanTaskOutcomeRuntime) StartSession(_ context.Context, agentID, _ string, _ []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "sess-ht", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (h *humanTaskOutcomeRuntime) ContinueSession(_ context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ack"}}, nil
}

// sqlmock will compare sql.NullTime values via driver.Valuer; ensure it doesn't panic.
var _ driver.Valuer
