package store_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/store"
)

func testAuthorActivityContext() context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		"bundle-v1:sha256:"+strings.Repeat("a", 64),
	))
}

func registerExternalTestAuthorActivityCatalog(t *testing.T, target interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}) {
	t.Helper()
	eventTypes := []string{
		"budget.alert", "operating/child/grandchild/opco.launched", "review.ready", "scoring/a/b",
		"test.concurrent_retry_upsert", "test.delivery_receipt.invariant.dead_letter_leaves_all_pending_surfaces",
		"test.delivery_receipt.invariant.in_progress_delivery_remains_pending_everywhere",
		"test.delivery_receipt.invariant.legacy_receipt_only",
		"test.delivery_receipt.invariant.pending_delivery_remains_pending_everywhere",
		"test.delivery_receipt.invariant.processed_delivery_leaves_all_pending_surfaces",
		"test.delivery_receipt.invariant.retryable_aged_failure_remains_pending_everywhere",
		"test.delivery_receipt.invariant.retryable_fresh_failure_stays_out_of_pending_surfaces_until_backoff_elapses",
		"test.direct_dead_letter_delivery", "test.immediate_terminal_delivery",
		"test.pending_details.dead", "test.pending_details.delivered", "test.pending_details.failed",
		"test.pending_details.in_progress", "test.pending_details.legacy_receipt_only", "test.pending_details.pending",
		"test.pending_direct", "test.pending_facts.dead", "test.pending_facts.failed", "test.pending_facts.full_horizon",
		"test.pending_facts.in_progress", "test.pending_facts.legacy_receipt_only", "test.pending_facts.pending",
		"test.pending_in_progress", "test.pending_legacy_retry_owner.direct", "test.pending_legacy_retry_owner.subscribed",
		"test.pending_subscribed", "test.receipt_delivery_atomicity", "test.retry_alignment.delivery_backed",
		"test.retry_claim.failed", "test.retry_delivery_status", "test.retry_legacy_receipt_only", "test.retry_upsert",
	}
	sort.Strings(eventTypes)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: eventType, Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(testAuthorActivityContext())
	if !ok {
		t.Fatal("test author activity scope is unavailable")
	}
	lease, err := target.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatalf("register external test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func registerExternalTestPostgresStore(t *testing.T, pg *store.PostgresStore) *store.PostgresStore {
	t.Helper()
	registerExternalTestAuthorActivityCatalog(t, pg)
	return pg
}
