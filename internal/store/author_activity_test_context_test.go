package store

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type storeTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var storeTestWorkFixtures sync.Map

func storeTestWorkOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := storeTestWorkFixtures.Load(t); ok {
		return existing.(*storeTestWorkFixture).runtime
	}
	fixture := &storeTestWorkFixture{process: worklifetime.NewProcess()}
	owner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        authorActivityTestBundleHash,
	})
	if err != nil {
		t.Fatalf("create store test work owner: %v", err)
	}
	fixture.runtime = owner
	actual, loaded := storeTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		_, _ = owner.RetireAndWait(context.Background())
		return actual.(*storeTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer storeTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire store test work owner: %v", err)
			return
		}
		fixture.process.Retire()
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join store test process owner: %v", err)
		}
	})
	return owner
}

func storeTestWorkContext(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	return worklifetime.WithOccurrence(ctx, storeTestWorkOwner(t))
}

func ownStoreTestAgentManager(t *testing.T, manager *runtimemanager.AgentManager) *runtimemanager.AgentManager {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown store test manager: %v", err)
		}
	})
	return manager
}

func newStoreTestEventBus(t *testing.T, store runtimebus.EventStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	var opts runtimebus.EventBusOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = storeTestWorkOwner(t)
	}
	return runtimebus.NewEventBusWithOptions(store, opts)
}

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
		"contract.child", "contract.control", "contract.diagnostic", "contract.operator", "contract.replay", "contract.root", "contract.selected_fork",
		"deadletter.test", "inbound.alert", "inbound.child", "inbound.root", "inbound.test", "item.failed", "item.received",
		"foreign/task.ready",
		"github.push.normalized", "inbound.github.push",
		"human_task.approved", "human_task.expired", "human_task.rejected",
		"launch.completed", "legacy.filled", "legacy.followup", "legacy.requested", "mailbox.card_superseded", "mailbox.review_requested", "parent.event", "pin.output",
		"first.event", "second.event", "phrase.completed", "review.requested", "review/inst-1/task.ready", "scan.completed", "scan.dev", "scan.followup", "scan.requested", "scoring.requested", "scoring/scoring.requested",
		"subscription.visible", "support_reply.rejected", "support_reply.revision_requested", "system.directive", "system.parent", "system.started", "task.completed",
		"custom.stop", "quiescence.active_delivery", "quiescence.missing_pipeline_receipt", "quiescence.ready",
		"scan.finished", "scan.progressed", "scan.replayed", "selected.test", "standing.unsettled", "standing.work",
		"task.canonical_entity", "task.dead", "task.dead_letter", "task.delivered", "task.failed", "task.failed.new", "task.failed.old", "task.in_progress", "task.other", "task.other_agent", "task.payload_only", "task.pending",
		"trace.event_only", "trace.failed", "trace.late_delivered", "trace.second_delivered", "trace.task_audit", "trace.tie",
		"test.delivery_receipt", "test.delivery_requested", "test.direct_dead_letter", "test.event", "test.receipts.typed_identity", "test.started", "test.terminal_admission", "test.terminal_delivery",
		"atomic.selected", "atomic.source", "batch.contract", "duplicate.base", "test.node_emitted",
		"trace.visible", "validation/validation.package_ready", "workflow.executable",
	}
	sort.Strings(eventTypes)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptor := runtimeauthoractivity.EventDescriptor{EventType: eventType, Disposition: runtimeauthoractivity.StoryDifferent}
		if eventType == "test.delivery_receipt" {
			descriptor.Disposition = runtimeauthoractivity.StoryAuthored
			descriptor.AuthorSummaryField = "text"
		} else if eventType == "test.node_emitted" || eventType == "atomic.selected" {
			descriptor.Disposition = runtimeauthoractivity.StoryAuthored
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
	pg := admitTestPostgresStore(t, db)
	registerTestAuthorActivityCatalog(t, pg)
	return pg
}

func admitTestPostgresStore(t testing.TB, db *sql.DB) *PostgresStore {
	t.Helper()
	pg := &PostgresStore{DB: db}
	bootstrapTestPostgresStore(t, pg)
	return pg
}

func bootstrapTestPostgresStore(t testing.TB, pg *PostgresStore) {
	t.Helper()
	if err := pg.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
}

func failureEnvelopesEqual(got, want runtimefailures.Envelope) bool {
	return reflect.DeepEqual(got, want)
}
