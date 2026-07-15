package store

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testAuthorActivityContext() context.Context {
	return testAuthorActivityContextForBundle(authorActivityTestBundleHash)
}

func testAuthorActivityRuntimeContext() context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.RuntimeScope(
		authorActivityTestRuntimeInstanceID,
	))
}

func testAuthorActivityContextForBundle(bundleHash string) context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		bundleHash,
	))
}

func testAuthorActivityBundleSourceContext() context.Context {
	ctx := runtimecorrelation.WithBundleSourceFact(testAuthorActivityContext(), runtimecorrelation.BundleSourceFact{
		BundleHash:   authorActivityTestBundleHash,
		BundleSource: storerunlifecycle.BundleSourceEphemeral,
	})
	return ctx
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerTestAuthorActivityCatalog(t *testing.T, target testAuthorActivityCatalogRegistrar) {
	t.Helper()
	registerTestAuthorActivityCatalogForContext(t, target, testAuthorActivityContext())
}

func registerTestAuthorActivityCatalogForContext(t *testing.T, target testAuthorActivityCatalogRegistrar, ctx context.Context) {
	t.Helper()
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("test author activity scope is unavailable")
	}
	eventTypes := []string{
		"child.event", "child/output.done", "company.scanned", "example.started", "fork.ready",
		"deadletter.test", "inbound.alert", "inbound.child", "inbound.root", "inbound.test", "item.failed", "item.received",
		"github.push.normalized", "inbound.github.push",
		"human_task.approved", "human_task.expired", "human_task.rejected",
		"launch.completed", "legacy.filled", "legacy.followup", "legacy.requested", "mailbox.card_superseded", "mailbox.review_requested", "parent.event", "pin.output",
		"phrase.completed", "review.requested", "scan.completed", "scan.dev", "scan.followup", "scan.requested", "scoring.requested",
		"subscription.visible", "support_reply.rejected", "support_reply.revision_requested", "system.directive", "system.parent", "system.started", "task.completed",
		"test.delivery_receipt", "test.delivery_requested", "test.direct_dead_letter", "test.event", "test.started", "test.terminal_admission", "test.terminal_delivery",
		"trace.visible", "validation/validation.package_ready", "workflow.executable",
	}
	sort.Strings(eventTypes)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptor := runtimeauthoractivity.EventDescriptor{EventType: eventType, Disposition: runtimeauthoractivity.StoryDifferent}
		if eventType == "test.delivery_receipt" {
			descriptor.Disposition = runtimeauthoractivity.StoryAuthored
			descriptor.AuthorSummaryField = "text"
		}
		descriptors = append(descriptors, descriptor)
	}
	lease, err := target.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatalf("register test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func newTestPostgresStore(t *testing.T, db *sql.DB) *PostgresStore {
	t.Helper()
	pg := &PostgresStore{DB: db}
	registerTestAuthorActivityCatalog(t, pg)
	return pg
}
