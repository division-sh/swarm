package cataloge2e

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	t.Helper()
	return canonicalrouting.CopyLegacyStaticCreate(t, false)
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
