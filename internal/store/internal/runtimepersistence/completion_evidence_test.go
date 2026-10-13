package runtimepersistence

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type completion2270ReadStore interface {
	ListOperatorConversationTurns(context.Context, operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(context.Context, string, string) (operatorread.OperatorPublicConversationTurnDetail, error)
	LoadLatestPublicConversationTurn(context.Context, string) (*operatorread.OperatorPublicConversationTurn, error)
	LoadOperatorAgentUsage(context.Context, agentidentity.Identity, operatorread.OperatorAgentUsageOptions) (operatorread.OperatorAgentUsage, error)
	LoadOperatorAgent(context.Context, agentidentity.Identity) (operatorread.OperatorAgentDetail, error)
}

func TestCompletion2270ForkChatReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkChatCompletionAuthorityFixture(t, backend == "sqlite")
			prepared := prepareForkChatCompletionGroup(t, fixture, "2270-rounds", "inspect independent model evidence")
			want := make([]operatorread.OperatorConversationCompletion, 0, 2)
			for ordinal := 1; ordinal <= 2; ordinal++ {
				ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, ordinal, fmt.Sprintf("2270-round-%d", ordinal))
				launchAndObserveForkChatCompletion(t, ctx, handle, "2270")
				input, output := int64(ordinal*3), int64(ordinal*2)
				settlement := runtimeeffects.CompletionSettlement{
					Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled},
					Usage:      runtimeeffects.CompletionUsage{Exactness: runtimeeffects.CompletionUsageExact, InputTokens: &input, OutputTokens: &output},
					Spend:      runtimeeffects.CompletionSpend{FlowInstance: fixture.fork.ForkID, AgentID: fixture.fork.SourceAgentID, CostUSD: 0.01, InvocationType: "anthropic"},
					Now:        fixture.now.Add(time.Duration(ordinal+1) * time.Second),
				}
				bindCompletionEvidenceForTest(&settlement, handle.Attempt().Authority.CompletionRequest, []runtimeeffects.CompletionModelObservation{{Model: fmt.Sprintf("reported-round-%d", ordinal), Source: runtimeeffects.CompletionModelResponse}})
				if ordinal == 2 {
					reported := 0.0000000004
					settlement.Usage.ProviderReportedCostUSD, settlement.Usage.Evidence.ProviderReportedCostUSD = &reported, &reported
					settlement.Usage.Evidence.CostProvenance, settlement.Spend.CostUSD = runtimeeffects.CompletionCostReported, reported
				}
				if result, err := handle.SettleCompletion(ctx, settlement); err != nil || !result.Committed || !result.SpendRecorded {
					t.Fatalf("settle fork round %d: %+v, %v", ordinal, result, err)
				}
				want = append(want, operatorread.OperatorConversationCompletion{AttemptID: handle.Attempt().AttemptID, Ordinal: ordinal, ExecutionMode: string(handle.Attempt().Authority.ExecutionMode), Usage: settlement.Usage.Clone()})
			}
			result, err := fixture.store.RecordOperatorConversationForkChat(testAuthorActivityContext(), runfork.ConversationForkChatRecordRequest{
				ForkID: fixture.fork.ForkID, Prepared: prepared, Message: "inspect independent model evidence", ActorTokenID: prepared.ActorTokenID,
				Execution: runfork.ConversationForkChatExecution{AssistantMessage: "completed", ExecutionOwner: prepared.ExecutionOwner, FenceGeneration: prepared.FenceGeneration}, Now: fixture.now.Add(4 * time.Second),
			})
			if err != nil || !reflect.DeepEqual(result.Turn.Completions, want) {
				t.Fatalf("immediate fork result lost ordered child evidence: %+v, %v", result.Turn.Completions, err)
			}
			reads := fixture.store.(interface {
				LoadOperatorConversationFork(context.Context, string) (runfork.OperatorConversationForkSession, error)
			})
			for repeat := 0; repeat < 2; repeat++ {
				reloaded, err := reads.LoadOperatorConversationFork(testAuthorActivityContext(), fixture.fork.ForkID)
				if err != nil || len(reloaded.Turns) != 1 || !reflect.DeepEqual(reloaded.Turns[0].Completions, want) {
					t.Fatalf("fork repeat %d changed child evidence: %+v, %v", repeat, reloaded.Turns, err)
				}
			}
			requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 2, 2)
		})
	}
}

func TestCompletion2270SettlementBothStores(t *testing.T) {
	for _, shape := range []string{"terminal", "multiple", "missing", "reported-zero", "drained-copy", "price-conflict", "request-conflict", "reconstructed-request-conflict"} {
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
				case "price-conflict":
					reported := 0.1
					settlement.Usage.ProviderReportedCostUSD, settlement.Usage.Evidence.ProviderReportedCostUSD = &reported, &reported
					settlement.Usage.Evidence.CostProvenance = runtimeeffects.CompletionCostReported
				case "drained-copy":
					cost := 0.1
					settlement.Usage.Evidence.Models = append(settlement.Usage.Evidence.Models, runtimeeffects.CompletionModelObservation{Model: "contributor", Source: runtimeeffects.CompletionModelCLIUsage, InputTokens: settlement.Usage.InputTokens, ProviderReportedCostUSD: &cost})
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
				if shape == "request-conflict" || shape == "price-conflict" {
					if result, err := handle.SettleCompletion(ctx, settlement); err == nil || result.Committed {
						t.Fatalf("accepted altered request: %+v, %v", result, err)
					}
					return
				}
				frozen := settlement.Clone()
				if shape == "drained-copy" {
					drained, err := runtimeeffects.ProjectDrainedCompletionSettlement(handle.Attempt(), settlement)
					if err != nil {
						t.Fatal(err)
					}
					frozen = drained.CompletionSettlement()
					for i := range drained.Usage.Evidence.Models {
						model := &drained.Usage.Evidence.Models[i]
						model.Model = "mutated-after-drain-copy"
						if model.InputTokens != nil {
							*model.InputTokens = 888
						}
						if model.ProviderReportedCostUSD != nil {
							*model.ProviderReportedCostUSD = 999
						}
					}
				}
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
	if fixture.authority.Target.Memory.Enabled {
		first, err := reads.LoadOperatorAgent(ctx, fixture.authority.Target.AgentIdentity)
		if err != nil || first.LastTurnRef == nil || !reflect.DeepEqual(first.LastTurnRef.CompletionEvidence, want) {
			t.Fatalf("stateful last-turn evidence: %+v, %v", first, err)
		}
		if len(first.LastTurnRef.CompletionEvidence.Models) > 0 {
			first.LastTurnRef.CompletionEvidence.Models[0].Model = "mutable-readback"
		}
		again, err := reads.LoadOperatorAgent(ctx, fixture.authority.Target.AgentIdentity)
		if err != nil || again.LastTurnRef == nil || !reflect.DeepEqual(again.LastTurnRef.CompletionEvidence, want) {
			t.Fatalf("stateful last-turn evidence aliased: %+v, %v", again, err)
		}
	}
}

func requireCompletion2270UncertainRecoveryEvidence(t *testing.T, fixture completionSettlementFixture, attempt runtimeeffects.Attempt) {
	t.Helper()
	reads := fixture.store.(completion2270ReadStore)
	detail, err := reads.LoadOperatorPublicConversationTurn(testAuthorActivityContext(), fixture.sessionID, attempt.Authority.Target.ID)
	if err != nil {
		t.Fatalf("recovered completion detail: %v", err)
	}
	evidence := detail.Turn.CompletionEvidence
	if evidence.Request != attempt.Authority.CompletionRequest || len(evidence.Models) != 0 || evidence.ProviderReportedCostUSD != nil || detail.Turn.ResolvedModel != "" || detail.Turn.Tokens != nil {
		t.Fatalf("uncertain recovery invented observed model, tokens or price: %+v", detail.Turn)
	}
	if evidence.CostProvenance != runtimeeffects.CompletionCostReservation && evidence.CostProvenance != runtimeeffects.CompletionCostUnavailable {
		t.Fatalf("recovery changed exhaustion qualification: %+v", evidence)
	}
}

func requireCompletion2270FrozenNativeEvidence(t *testing.T, fixture completionSettlementFixture, attemptID string) {
	t.Helper()
	frame, err := agentframe.DecodeDurable(loadCompletionFrameBytes(t, fixture, "runtime_external_effect_attempts", "attempt_id", attemptID))
	if err != nil {
		t.Fatal(err)
	}
	bound, err := runtimeeffects.BindCompletionRequest(fixture.authority, runtimeeffects.AuthorizeRequest{AgentFrame: &frame})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := fixture.store.(completion2270ReadStore).LoadLatestPublicConversationTurn(testAuthorActivityContext(), fixture.sessionID)
	if err != nil || latest == nil {
		t.Fatalf("native completion readback: %+v, %v", latest, err)
	}
	if err := latest.CompletionEvidence.ValidateRequest(bound.CompletionRequest, latest.ResolvedModel); err != nil {
		t.Fatalf("native settlement/recovery lost frozen request: %v", err)
	}
}
