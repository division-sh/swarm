package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecontracts "swarm/internal/runtime/contracts"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

func TestBudgetTracker_KeepsTerminalStatesInstanceOwned(t *testing.T) {
	trackerA := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done"},
		},
	}))
	trackerB := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"closed"},
		},
	}))

	if got, want := trackerA.TerminalInstanceStates(), []string{"done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerA.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
	if got, want := trackerB.TerminalInstanceStates(), []string{"closed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerB.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
}

func TestBudgetTracker_RecordLLMUsagePersistsUsageAccounting(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	tracker := &BudgetTracker{db: db}
	ctx := context.Background()

	mock.ExpectExec("INSERT INTO spend_ledger").
		WithArgs("", "flow/1", "agent-1", "claude-3-5-sonnet", 11, 7, sqlmock.AnyArg(), "api", "exact", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := tracker.RecordLLMUsage(ctx, "", "agent-1", "api", llm.UsageTokens{
		Model:        "claude-3-5-sonnet",
		InputTokens:  11,
		OutputTokens: 7,
	}, true, map[string]any{"flow_instance": "flow/1"}); err != nil {
		t.Fatalf("RecordLLMUsage exact: %v", err)
	}

	mock.ExpectExec("INSERT INTO spend_ledger").
		WithArgs("", "flow/1", "agent-1", "claude-cli-sonnet", 13, 5, sqlmock.AnyArg(), "cli_test", "estimated", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := tracker.RecordLLMUsage(ctx, "", "agent-1", "cli_test", llm.UsageTokens{
		Model:        "claude-cli-sonnet",
		InputTokens:  13,
		OutputTokens: 5,
	}, false, map[string]any{"flow_instance": "flow/1"}); err != nil {
		t.Fatalf("RecordLLMUsage estimated: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
