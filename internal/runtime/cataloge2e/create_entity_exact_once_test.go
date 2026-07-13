package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCatalogRejectsStaticCreateEntityHandlerFixture(t *testing.T) {
	fixtureRoot := writeCreateEntityExactOnceFixture(t)
	bundle := loadFixtureBundle(t, fixtureRoot)
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})

	if !catalogCreateEntityFindingContains(report.Errors(), "flow_boundary_create_entity_validation", "static multi-row entity ownership is retired") {
		t.Fatalf("expected retired static create_entity validation error, got %#v", report.Errors())
	}
}

func writeCreateEntityExactOnceFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_static_create_entity_retirement proof=TestCatalogRejectsStaticCreateEntityHandlerFixture
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, root, "package.yaml", `
name: exact-once-catalog
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`)
	writeFixtureFile(t, root, "schema.yaml", "name: exact-once-catalog\n")
	writeFixtureFile(t, root, filepath.Join("flows", "validation", "schema.yaml"), `
name: validation
mode: static
initial_state: new
terminal_states: [done]
states: [new, done]
pins:
  inputs:
    events: [thing.created]
  outputs:
    events: [thing.emitted]
`)
	writeFixtureFile(t, root, filepath.Join("flows", "validation", "entities.yaml"), `
widget:
  amount:
    type: integer
    initial: 0
  who:
    type: text
    initial: ""
  counter:
    type: integer
    initial: 0
`)
	writeFixtureFile(t, root, filepath.Join("flows", "validation", "events.yaml"), `
thing.created:
  swarm:
    source: external
  amount: integer
  who: text
thing.emitted:
  amount: integer
  who: text
`)
	writeFixtureFile(t, root, filepath.Join("flows", "validation", "nodes.yaml"), `
w-node:
  id: w-node
  execution_type: system_node
  subscribes_to: [thing.created]
  produces: [thing.emitted]
  event_handlers:
    thing.created:
      create_entity: true
      data_accumulation:
        source_event: thing.created
        writes:
          - source_field: amount
            target_field: amount
          - source_field: who
            target_field: who
          - target_field: counter
            value:
              cel: entity.counter + 1
      advances_to: done
      emit:
        event: thing.emitted
        broadcast: true
        fields:
          amount:
            cel: entity.amount
          who:
            cel: entity.who
`)
	return root
}

func catalogCreateEntityFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if finding.CheckID != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}

func writeFixtureFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture path %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("write fixture file %s: %v", rel, err)
	}
}

func assertCatalogMutationCount(t *testing.T, h *runtimeHarness, eventID, field, writerID, handlerStep string, want int) {
	t.Helper()
	var got int
	if err := h.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE caused_by_event = $1::uuid
		  AND field = $2
		  AND writer_id = $3
		  AND handler_step = $4
	`, eventID, field, writerID, handlerStep).Scan(&got); err != nil {
		t.Fatalf("count entity_mutations: %v", err)
	}
	if got != want {
		t.Fatalf("mutation count field=%s writer=%s step=%s = %d, want %d", field, writerID, handlerStep, got, want)
	}
}

func assertCatalogReceiptCount(t *testing.T, h *runtimeHarness, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	if err := h.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if got != want {
		t.Fatalf("event receipt count = %d, want %d", got, want)
	}
}

func assertCatalogDeliveryStatusCount(t *testing.T, h *runtimeHarness, eventID, nodeID, status string, want int) {
	t.Helper()
	var got int
	if err := h.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND status = $3
	`, eventID, nodeID, status).Scan(&got); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if got != want {
		t.Fatalf("delivery status %s count = %d, want %d", status, got, want)
	}
}
