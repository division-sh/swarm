package serveapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
			nodes = strings.ReplaceAll(nodes, "payload.nested.numbers[0]", "payload.value")
			nodes = strings.ReplaceAll(nodes, "payload.nested.numbers[1]", "7.5")
			nodes = strings.ReplaceAll(nodes, "      create_entity: true\n", "")
			nodes += `requester:
  id: requester
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
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "numeric.requested", "idempotency_key": uuid.NewString(), "payload": map[string]any{"value": 7, "nested": map[string]any{"numbers": []any{7, 7.5}}}})
			var cardID, hash string
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				var listed map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": published.RunID, "status": "pending"}, &listed)
				items, _ := listed["items"].([]any)
				for _, item := range items {
					entry := servedAnyMap(t, item)
					if entry["kind"] != "decision_card" {
						continue
					}
					card := servedAnyMap(t, entry["decision_card"])
					cardID, _ = card["card_id"].(string)
					hash, _ = card["content_hash"].(string)
				}
				if cardID != "" && hash != "" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if cardID == "" || hash == "" {
				t.Fatal("real workflow gate did not produce a decidable card")
			}
			key := uuid.NewString()
			body := func(number string) string {
				return fmt.Sprintf(`{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":%q,"observed_content_hash":%q,"verdict":"approve","fields":{"score":%s},"idempotency_key":%q}}`, cardID, hash, number, key)
			}
			first := semanticNumericRPC(t, rt.Endpoint, "http", body("7.0"))
			if first.Error != nil {
				t.Fatalf("gate decision: %#v", first.Error)
			}
			semanticNumericOutput(t, rt.Endpoint, rt.DB, published.RunID)
			requireSemanticReplay(t, first.Result, semanticNumericRPC(t, rt.Endpoint, "http", body("7e0")))
			semanticNumericOutput(t, rt.Endpoint, rt.DB, published.RunID)
		})
	}
}
