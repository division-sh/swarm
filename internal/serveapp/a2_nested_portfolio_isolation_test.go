package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestA2NestedPortfolioSamePeriodAndMemberIsolationBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInBarrier)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyExample(t, canonicalrouting.FanInBarrier))
			const periodKey = "same-period"
			portfolios := []string{"portfolio-left", "portfolio-right"}
			parents := make([]operatorread.OperatorEntityFull, 2)
			periods := make([]operatorread.OperatorEntityFull, 2)
			runID := ""
			for index, portfolio := range portfolios {
				params := map[string]any{"event_name": "portfolio.setup", "idempotency_key": "a2-nested-setup-" + portfolio,
					"payload": map[string]any{"portfolio_id": portfolio, "period_id": periodKey, "expected_operating_ids": []string{"op-a", "op-b"}}}
				if runID == "" {
					params["bundle_hash"] = rt.BundleHash
				} else {
					params["run_id"] = runID
				}
				setup := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if runID != "" && setup.RunID != runID {
					t.Fatalf("sibling portfolio setup changed run: %+v", setup)
				}
				runID = setup.RunID
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
				parents[index] = requireA2PortfolioKeyedEntity(t, rt, runID, "portfolio", "portfolio_id", portfolio)
				payload := map[string]any{"portfolio_id": portfolio, "period_id": periodKey, "expected_operating_ids": []any{"op-a", "op-b"}}
				forwarded := requireA2PortfolioEmission(t, rt, runID, parents[index].Entity.FlowInstance+"/period.setup", setup.EventID, payload)
				periods[index] = requireA2PortfolioEntity(t, rt, runID, forwarded.Deliveries[0].Target.FlowInstance)
				requireA2PortfolioDelivery(t, forwarded, "portfolio/period", "portfolio-collector", periods[index], "materializing_entity")
				if periods[index].Fields["portfolio_id"] != portfolio || periods[index].Fields["period_id"] != periodKey {
					t.Fatalf("nested constructor selected wrong parent: %+v", periods[index])
				}
				arm := requireA2PortfolioJoin(t, rt, periods[index], 0, false, nil)
				if arm.Completed() != 0 {
					t.Fatalf("new sibling reused an existing arm: %+v", arm)
				}
			}
			if periods[0].Entity.EntityID == periods[1].Entity.EntityID || periods[0].Entity.FlowInstance == periods[1].Entity.FlowInstance {
				t.Fatal("identical period keys in different portfolios collapsed")
			}
			initial := append([]operatorread.OperatorEntityFull(nil), periods...)
			for _, arrival := range []struct {
				owner   int
				member  string
				revenue int
			}{{0, "op-b", 22}, {1, "op-a", 111}, {0, "op-a", 11}, {1, "op-b", 222}} {
				index, portfolio := arrival.owner, portfolios[arrival.owner]
				otherBefore := requireA2PortfolioEntity(t, rt, runID, periods[1-index].Entity.FlowInstance)
				trigger := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"run_id": runID, "event_name": "operating.report.triggered", "idempotency_key": "a2-nested-" + portfolio + "-" + arrival.member,
					"payload": map[string]any{"portfolio_id": portfolio, "period_id": periodKey, "operating_id": arrival.member, "revenue": arrival.revenue},
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
				payload := map[string]any{"portfolio_id": portfolio, "period_id": periodKey, "operating_id": arrival.member, "revenue": float64(arrival.revenue)}
				requested, operating := requireA2PortfolioOperatingCreation(t, rt, trigger, payload, "operating_instance_id")
				report := map[string]any{"portfolio_id": portfolio, "period_id": periodKey, "operating_id": arrival.member, "operating_instance_id": requested.EventID, "revenue": float64(arrival.revenue)}
				reported := requireA2PortfolioEmission(t, rt, runID, operating.Entity.FlowInstance+"/operating.reported", requested.EventID, report)
				requireA2PortfolioDelivery(t, reported, "portfolio", "portfolio-router", parents[index], "existing_entity")
				forwarded := requireA2PortfolioEmission(t, rt, runID, parents[index].Entity.FlowInstance+"/period.reported", reported.EventID, payload)
				requireA2PortfolioDelivery(t, forwarded, "portfolio/period", "portfolio-collector", periods[index], "existing_entity")
				if arrival.member == "op-a" && index == 0 || arrival.member == "op-b" && index == 1 {
					periods[index] = waitA2PortfolioComplete(t, rt, runID, periods[index].Entity.FlowInstance)
				} else {
					periods[index] = requireA2PortfolioEntity(t, rt, runID, periods[index].Entity.FlowInstance)
				}
				if !reflect.DeepEqual(otherBefore, requireA2PortfolioEntity(t, rt, runID, periods[1-index].Entity.FlowInstance)) {
					t.Fatal("same period/member key in another portfolio changed sibling state or arm")
				}
			}
			for index, expected := range [][]any{{int64(11), int64(22)}, {int64(111), int64(222)}} {
				join := requireA2PortfolioJoin(t, rt, periods[index], 2, true, expected)
				results, err := join.Results()
				if periods[index].Entity.CurrentState != "complete" || !join.OutcomeFired || join.OutcomePending || err != nil || !reflect.DeepEqual(results, expected) ||
					join.JoinRef().StageEntry().InstancePath != initial[index].Entity.FlowInstance {
					t.Fatalf("nested ordered completion changed owner/results: join=%+v results=%#v err=%v", join, results, err)
				}
			}
		})
	}
}
