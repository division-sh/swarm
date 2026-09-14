package serveapp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestWorkflowGateSemanticNumericOutcomeExecutionParity(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := semanticNumericIngressFixture(t)
			schema := `name: numeric-gate
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: numeric_review
      outcomes:
        approve:
          input:
            score: {type: integer, required: true}
          advances_to: approved
          emit:
            event: numeric.approved
            fields:
              value: decision.score
  approved: {}
  done: {terminal: true}
pins:
  inputs:
    events: [numeric.requested]
`
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte(schema), 0600); err != nil {
				t.Fatal(err)
			}
			eventsPath := filepath.Join(root, "events.yaml")
			rawEvents, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(eventsPath, append(rawEvents, []byte("numeric.approved:\n  value: integer\n")...), 0600); err != nil {
				t.Fatal(err)
			}
			nodesPath := filepath.Join(root, "nodes.yaml")
			raw, err := os.ReadFile(nodesPath)
			if err != nil {
				t.Fatal(err)
			}
			nodes := strings.ReplaceAll(string(raw), "numeric.requested", "numeric.approved")
			nodes = strings.ReplaceAll(nodes, "payload.nested.numbers[?0].value()", "payload.value")
			nodes = strings.ReplaceAll(nodes, "payload.nested.fraction", "7.5")
			nodes = strings.ReplaceAll(nodes, "      create_entity: true\n", "")
			nodes += `requester:
  execution_type: system_node
  subscribes_to: [numeric.requested]
  event_handlers:
    numeric.requested:
      create_entity: true
      advances_to: review
`
			if err := os.WriteFile(nodesPath, []byte(nodes), 0600); err != nil {
				t.Fatal(err)
			}
			rt, restart := startSemanticNumericLiveRuntime(t, backend, root)
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "numeric.requested", "idempotency_key": uuid.NewString(), "payload": map[string]any{"value": 7, "nested": map[string]any{"numbers": []any{7}, "fraction": 7.5}}})
			cardID := waitLifecycleGateCard(t, rt, published.RunID)
			params := lifecycleDecisionParamsForCard(t, rt, cardID, "approve")
			hash := params["observed_content_hash"].(string)
			key := uuid.NewString()
			body := func(number string) string {
				return fmt.Sprintf(`{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":%q,"observed_content_hash":%q,"verdict":"approve","fields":{"score":%s},"idempotency_key":%q}}`, cardID, hash, number, key)
			}
			first := semanticNumericRPC(t, rt.Endpoint, "http", body("7.0"))
			if first.Error != nil {
				t.Fatalf("gate decision: %#v", first.Error)
			}
			semanticNumericOutput(t, rt.Endpoint, rt.DB, published.RunID)
			proveReplay := func(spelling string) {
				t.Helper()
				replayed := semanticNumericRPC(t, rt.Endpoint, "http", body(spelling))
				if replayed.Error != nil {
					t.Fatalf("gate replay: %#v", replayed.Error)
				}
				var original, repeated map[string]json.RawMessage
				if err := json.Unmarshal(first.Result, &original); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(replayed.Result, &repeated); err != nil {
					t.Fatal(err)
				}
				if string(original["idempotency_replayed"]) != "false" || string(repeated["idempotency_replayed"]) != "true" {
					t.Fatalf("gate replay receipt markers: %s -> %s", first.Result, replayed.Result)
				}
				// Mailbox receipts intentionally mark replay; every domain result stays exact.
				original["idempotency_replayed"] = json.RawMessage("true")
				want, err := json.Marshal(original)
				if err != nil {
					t.Fatal(err)
				}
				requireSemanticReplay(t, want, replayed)
				semanticNumericOutput(t, rt.Endpoint, rt.DB, published.RunID)
			}
			proveReplay("7e0")
			rt = restart()
			proveReplay("7")
		})
	}
}
