package serveapp

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestScenarioSetupSemanticNumericParity(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyScenarioRootSetup(t))
			run, key := uuid.NewString(), uuid.NewString()
			var first json.RawMessage
			for _, number := range []string{"5", "5.0", "5e0"} {
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"setup","method":"test.setup_entities","params":{"bundle_hash":%q,"run_id":%q,"idempotency_key":%q,"entities":[{"alias":"subject","entity_id":%q,"entity_type":"widget","current_state":"waiting","fields":{"score":%s}}]}}`, rt.BundleHash, run, key, run, number)
				result := semanticNumericRPC(t, rt.Endpoint, "http", body)
				if result.Error != nil {
					t.Fatalf("setup %s: %#v", number, result.Error)
				}
				if first == nil {
					first = result.Result
				} else {
					requireSemanticReplay(t, first, result)
				}
			}
			var stored string
			if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE CAST(run_id AS TEXT)=$1`, run).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes([]byte(stored), &fields); err != nil {
				t.Fatal(err)
			}
			projected, err := workflowexpr.ProjectCELValue(fields)
			if err != nil || projected.(map[string]any)["score"] != int64(5) {
				t.Fatalf("stored setup %#v: %v", projected, err)
			}
			var public struct {
				Fields map[string]any `json:"fields"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": run, "entity_id": run}, &public)
			if public.Fields["score"] != float64(5) {
				t.Fatalf("public fields %#v", public)
			}
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash,
				"run_id": run, "event_name": "widget.scored", "idempotency_key": uuid.NewString(),
				"payload": map[string]any{"delta": public.Fields["score"]}})
			if published.RunID != run {
				t.Fatalf("wrong run %#v", published)
			}
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, run, run, "done")
			requireServedEntityReadback(t, rt.Endpoint, run, run, "done")
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": run, "entity_id": run}, &public)
			if public.Fields["score"] != float64(10) {
				t.Fatalf("republication lost numeric agreement: %#v", public)
			}
			requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, run, servedparity.Scenario{
				ID: "numeric-setup-republication", Postconditions: []servedparity.Postcondition{servedparity.PostconditionNoNonTerminalDeliveries, servedparity.PostconditionNoPendingPipelineEvents, servedparity.PostconditionNoUnfiredDueTimers}})
		})
	}
}
