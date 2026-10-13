package runtimepersistence

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type completion2270ReadStore interface {
	ListOperatorConversationTurns(context.Context, operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(context.Context, string, string) (operatorread.OperatorPublicConversationTurnDetail, error)
	LoadLatestPublicConversationTurn(context.Context, string) (*operatorread.OperatorPublicConversationTurn, error)
	LoadOperatorAgentUsage(context.Context, agentidentity.Identity, operatorread.OperatorAgentUsageOptions) (operatorread.OperatorAgentUsage, error)
}

func TestCompletion2270SettlementBothStores(t *testing.T) {
	for _, shape := range []string{"terminal", "multiple", "missing", "reported-zero", "request-conflict", "reconstructed-request-conflict"} {
		t.Run(shape, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				store := selected.selected.(completionSettlementTestStore)
				if selected.postgres {
					store = admitTestPostgresStore(t, selected.db)
				}
				fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
				ctx := fixture.context
				handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "completion-2270:"+shape)
				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "", "")
				settlement.ProviderHead = nil
				switch shape {
				case "multiple":
					settlement.Usage.Evidence.Models = []runtimeeffects.CompletionModelObservation{{Model: "a", Source: runtimeeffects.CompletionModelCLIUsage}, {Model: "b", Source: runtimeeffects.CompletionModelCLIUsage}}
				case "missing":
					settlement.Usage.Evidence.Models = nil
				case "reported-zero":
					zero := 0.0
					settlement.Usage.ProviderReportedCostUSD, settlement.Usage.Evidence.ProviderReportedCostUSD = &zero, &zero
					settlement.Usage.Evidence.CostProvenance = runtimeeffects.CompletionCostReported
					settlement.Spend.CostUSD = 0
				case "request-conflict", "reconstructed-request-conflict":
					settlement.Usage.Evidence.Request.RequestedModel = "changed-after-admission"
					settlement.Spend.Model = "changed-after-admission"
				}
				settlement.Usage.ResolvedModel = settlement.Usage.Evidence.ResolvedModel()
				settlement.Spend.ResolvedModel = settlement.Usage.ResolvedModel
				if shape == "reconstructed-request-conflict" {
					attempt := handle.Attempt()
					attempt.Authority.CompletionRequest = settlement.Usage.Evidence.Request
					if result, err := store.SettleCompletion(ctx, attempt, settlement); err == nil || result.Committed {
						t.Fatalf("native request fence accepted reconstructed authority: %+v, %v", result, err)
					}
					requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 1)
					return
				}
				if shape == "request-conflict" {
					if result, err := handle.SettleCompletion(ctx, settlement); err == nil || result.Committed {
						t.Fatalf("accepted altered request: %+v, %v", result, err)
					}
					return
				}
				frozen := settlement.Clone()
				settlement.Usage.Evidence.Models = append(settlement.Usage.Evidence.Models, runtimeeffects.CompletionModelObservation{Model: "caller-mutation", Source: runtimeeffects.CompletionModelCLIUsage})
				*settlement.Usage.InputTokens = 999
				result, err := handle.SettleCompletion(ctx, frozen)
				if err != nil || !result.Committed || !result.SpendRecorded {
					t.Fatalf("settle completion: %+v, %v", result, err)
				}
				requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 1, 1, 0)
				proveCompletion2270Readback(t, ctx, store.(completion2270ReadStore), fixture, frozen)
			})
		})
	}
}

func proveCompletion2270Readback(t *testing.T, ctx context.Context, reads completion2270ReadStore, fixture completionSettlementFixture, settlement runtimeeffects.CompletionSettlement) {
	t.Helper()
	want := settlement.Usage.Evidence.Canonical()
	detail, err := reads.LoadOperatorPublicConversationTurn(ctx, fixture.sessionID, settlement.AgentTurn.TurnID)
	if err != nil {
		t.Fatalf("public detail: %v", err)
	}
	if !reflect.DeepEqual(detail.Turn.CompletionEvidence, want) || detail.Turn.ResolvedModel != settlement.Usage.ResolvedModel || detail.Turn.Tokens == nil || detail.Turn.Tokens.Input != 12 {
		t.Fatalf("public detail lost immutable evidence: %+v", detail.Turn)
	}
	page, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 1})
	if err != nil || len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].CompletionEvidence, want) {
		t.Fatalf("public page lost evidence: %+v, %v", page, err)
	}
	latest, err := reads.LoadLatestPublicConversationTurn(ctx, fixture.sessionID)
	if err != nil || latest == nil || !reflect.DeepEqual(latest.CompletionEvidence, want) {
		t.Fatalf("latest lost evidence: %+v, %v", latest, err)
	}
	usage, err := reads.LoadOperatorAgentUsage(ctx, fixture.authority.Target.AgentIdentity, operatorread.OperatorAgentUsageOptions{})
	if err != nil || len(usage.Breakdown) != 1 {
		t.Fatalf("usage read: %+v, %v", usage, err)
	}
	row := usage.Breakdown[0]
	if row.CompletionEvidence == nil || !reflect.DeepEqual(*row.CompletionEvidence, want) || row.ResolvedModel != settlement.Usage.ResolvedModel || row.Totals.CostUSD != settlement.Spend.CostUSD || row.Totals.InputTokens != 12 {
		t.Fatalf("ledger grouping lost evidence or changed accounting: %+v", row)
	}
	if want.CostProvenance == runtimeeffects.CompletionCostReported && row.Totals.EstimatedCostUSD != 0 {
		t.Fatal("provider-reported zero was classified as a local estimate")
	}
}
