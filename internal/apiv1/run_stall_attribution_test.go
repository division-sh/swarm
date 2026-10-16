package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestRunDiagnosisUsesOneCoherentReportAndPreservesMixedInstanceAttribution(t *testing.T) {
	const runID = "run-1"
	turn := operatorread.RunTurnAttribution{FlowInstance: "workers/idle", AgentID: "worker", EventID: "request", EventType: "work.requested", DeliveryID: "delivery", LogicalTurnID: "logical", ProviderTurns: []string{"provider"}, Emission: operatorread.RunEmissionNone}
	reader := &fakeRunReadStore{
		headers: map[string]operatorread.RunHeader{runID: {
			RunID: runID, Status: "running", Origin: mustEventRunOrigin(t, "request", "work.requested"), StartedAt: time.Unix(1, 0).UTC(),
		}},
		reports: map[string]operatorread.RunDebugReport{runID: {
			RunID: runID, RunTableStatus: "running", FanOut: fanoutobligation.RunSummary{RunID: runID},
			OperationalFacts: &operatorread.RunOperationalFacts{Complete: true, Instances: []operatorread.RunInstanceFacts{
				{FlowInstance: "workers/active", Complete: true, Revision: 1, Work: operatorread.RunInstanceWork{Deliveries: 1}},
				{FlowInstance: "workers/idle", Complete: true, Revision: 1, LastTurn: &turn},
			}},
		}},
	}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Runs: reader})})
	response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnosis","method":"run.diagnose","params":{"run_id":"run-1"}}`)
	if response.Error != nil {
		t.Fatalf("public diagnosis: %#v", response.Error)
	}
	result := asMap(t, response.Result)
	if reader.headerReads != 0 || reader.reportReads != 1 || result["operational_state"] != "running" {
		t.Fatalf("diagnosis reread or reinterpreted a sibling: report=%d header=%d result=%#v", reader.reportReads, reader.headerReads, result)
	}
	instances, ok := result["instances"].([]any)
	if !ok || len(instances) != 2 || asMap(t, instances[0])["disposition"] != "active" || asMap(t, instances[1])["disposition"] != "stalled" {
		t.Fatalf("public instance dispositions: %#v", result)
	}
	attributions, ok := result["attributions"].([]any)
	if !ok || len(attributions) != 1 {
		t.Fatalf("public attributions: %#v", result)
	}
	attribution := asMap(t, attributions[0])
	if attribution["flow_instance"] != "workers/idle" || attribution["agent_id"] != "worker" || attribution["delivery_id"] != "delivery" || attribution["event_id"] != "request" || attribution["logical_turn_id"] != "logical" || attribution["emission_evidence"] != "none" {
		t.Fatalf("public origin changed: %#v", attribution)
	}
}
