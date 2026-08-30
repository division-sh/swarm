package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = sourceartifactfixture.BundleHash

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

type storeTestDurableEventBusStore interface {
	sourceartifactfixture.Writer
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.SelectedRunTargetOwnerLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimepipeline.StandingRestartDispositionReader
	PipelineObligations() runtimepipelineobligation.Store
}

func newStoreTestEventBus(t *testing.T, store storeTestDurableEventBusStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	var opts runtimebus.EventBusOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if opts.SourceArtifactFact.Validate() != nil {
		opts.SourceArtifactFact = sourceartifactfixture.Require(t, context.Background(), store)
	} else if opts.SourceArtifactFact.BundleHash() == sourceartifactfixture.BundleHash {
		sourceartifactfixture.Require(t, context.Background(), store)
	}
	if opts.RuntimeInstanceID == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = storeTestWorkOwner(t)
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.SourceArtifactFact,
			opts.RuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
		}
		opts.DeliveryAuthority = authority
	}
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = store.PipelineObligations()
	}
	opts.Durable = runtimebus.DurableDependencies{
		ReplyContext: store, RunLifecycle: store, DeliveryLifecycle: store,
		FlowRoutes: store, FlowRouteRecords: store, FlowRouteSets: store, FlowRouteTopology: store, FlowRouteRollback: store,
		ActiveAgents: store, ActiveFlows: store, TargetOwners: store, WorkflowInstances: store, PreparedEvents: store,
		TargetFailureRecorder: store, RunOrigins: store, StandingRestarts: store,
	}
	bus, err := runtimebus.NewEventBusWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(false),
	); err != nil {
		return nil, err
	}
	return bus, nil
}

func testAuthorActivityContext() context.Context {
	return testAuthorActivityContextForBundle(authorActivityTestBundleHash)
}

func requireDefaultSourceArtifactForTest(t testing.TB, ctx context.Context, selected any) {
	t.Helper()
	writer, ok := selected.(sourceartifactfixture.Writer)
	if !ok {
		t.Fatalf("default source artifact fixture requires source writer, got %T", selected)
	}
	sourceartifactfixture.Require(t, ctx, writer)
}

func testAuthorActivityRuntimeContext() context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.RuntimeScope(
		authorActivityTestRuntimeInstanceID,
	))
}

func testAuthorActivityContextForBundle(bundleHash string) context.Context {
	ctx := runtimecorrelation.WithSourceArtifactFact(
		context.Background(),
		mustStoreTestSourceArtifactFact(bundleHash),
	)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		bundleHash,
	))
}

func testAuthorActivitySourceArtifactContext() context.Context {
	return testAuthorActivityContext()
}

func mustStoreTestSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func storeTestSourceArtifact(label string) *sourceartifact.AdmittedSourceArtifact {
	return sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# "+label+"\n"))
}

func requireStoreTestPersistedBundle(t *testing.T, db *sql.DB, bundleHash string) {
	t.Helper()
	persisted := sourceartifact.Persisted{BundleHash: bundleHash, CreatedAt: time.Now().UTC()}
	err := db.QueryRowContext(testAuthorActivityContextForBundle(bundleHash), `
		SELECT source_blob, member_count, total_bytes
		FROM source_artifacts
		WHERE bundle_hash = $1
	`, bundleHash).Scan(&persisted.SourceBlob, &persisted.MemberCount, &persisted.TotalBytes)
	if err == nil {
		if err := persisted.Validate(); err != nil {
			t.Fatalf("validate persisted source artifact %s: %v", bundleHash, err)
		}
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read persisted source artifact %s: %v", bundleHash, err)
	}
	if bundleHash != sourceartifactfixture.BundleHash {
		t.Fatalf("persisted source fixture %s requires an exact admitted artifact", bundleHash)
	}
	seedStoreTestPersistedArtifact(t, db, sourceartifactfixture.Artifact())
}

func seedStoreTestPersistedArtifact(t *testing.T, db *sql.DB, artifact *sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		t.Fatalf("project persisted source artifact fixture: %v", err)
	}
	if _, err := db.ExecContext(testAuthorActivityContextForBundle(persisted.BundleHash), `
		INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (bundle_hash) DO NOTHING
	`, persisted.BundleHash, persisted.SourceBlob, persisted.MemberCount, persisted.TotalBytes); err != nil {
		t.Fatalf("seed persisted source artifact %s: %v", persisted.BundleHash, err)
	}
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
		"run_fork.root_gate_approved", "scan.finished", "scan.progressed", "scan.replayed", "selected.test", "standing.unsettled", "standing.work",
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
	pg := NewPostgresStoreForTest(db)
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
