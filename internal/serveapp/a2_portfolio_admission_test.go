package serveapp

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestA2PortfolioPublicTypedIngressRefusesBeforeMutationBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, fixture := range []canonicalrouting.ArtifactID{canonicalrouting.FanInStream, canonicalrouting.FanInBarrier} {
			t.Run(string(backend)+"/"+string(fixture), func(t *testing.T) {
				root := canonicalrouting.CopyExample(t, fixture)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				event, field := "operating.report.triggered", "period_id"
				valid := map[string]any{"period_id": "period-one", "revenue": 100}
				if fixture == canonicalrouting.FanInBarrier {
					event, field = "portfolio.setup", "portfolio_id"
					valid = map[string]any{"portfolio_id": "portfolio-one", "period_id": "period-one", "expected_operating_ids": []string{"op-a", "op-b"}}
				}
				before := a2PortfolioAdmissionDomainCounts(t, rt.ReceiverStateReader)
				for _, method := range []string{"event.publish", "run.start"} {
					for _, malformed := range []struct {
						name    string
						present bool
						value   any
					}{{"missing", false, nil}, {"null", true, nil}, {"integer", true, 1}, {"boolean", true, true}, {"array", true, []string{"period-one"}}, {"object", true, map[string]any{"value": "period-one"}}} {
						t.Run(method+"/"+malformed.name, func(t *testing.T) {
							payload := make(map[string]any, len(valid))
							for name, value := range valid {
								payload[name] = value
							}
							delete(payload, field)
							if malformed.present {
								payload[field] = malformed.value
							}
							response := requestServedJSONRPC(t, rt.Endpoint, method, map[string]any{"bundle_hash": rt.BundleHash, "event_name": event, "payload": payload, "idempotency_key": "a2-refusal-" + method + "-" + malformed.name})
							if response.Error == nil || response.Error.Data["code"] != apiv1.PayloadValidationFailedCode {
								t.Fatalf("typed %s ingress refusal: %+v", field, response)
							}
							if after := a2PortfolioAdmissionDomainCounts(t, rt.ReceiverStateReader); !reflect.DeepEqual(after, before) {
								t.Fatalf("invalid keyed ingress mutated domain state: before=%v after=%v", before, after)
							}
						})
					}
				}
			})
		}
	}
}

func a2PortfolioAdmissionDomainCounts(t *testing.T, selectedProofReceiverStateReader receiverProofStateReader) map[string]int {
	t.Helper()
	counts := readServedA2DomainCounts(t, selectedProofReceiverStateReader)
	var businessEvents int
	if err := func() error {
		observed, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
			return storetest.CountGlobalBusinessEventsStorage(ctx, selectedProofReceiverStateReader)
		})
		businessEvents = observed
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	counts["events"] = businessEvents
	return counts
}
