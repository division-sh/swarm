package pipeline_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Encode only the active union branch; unused normal-agent generations refuse
// JSON encoding. This is preserved evidence for a negative fence proof only.
type publishedBarrierCrashAuthority struct {
	Kind            effects.AuthorityKind
	ID              string
	SelectedFork    effects.SelectedContractForkAuthority
	ExecutionOwner  string
	LeaseExpiresAt  time.Time
	FenceGeneration uint64
	Target          effects.UsageTarget
	BudgetScopes    []effects.BudgetAdmissionScope
	ExecutionMode   effects.ExecutionMode
}

func (p publishedBarrierCrashAuthority) native() effects.Authority {
	return effects.Authority{Kind: p.Kind, ID: p.ID, SelectedFork: p.SelectedFork, ExecutionOwner: p.ExecutionOwner,
		LeaseExpiresAt: p.LeaseExpiresAt, FenceGeneration: p.FenceGeneration, Target: p.Target,
		BudgetScopes: p.BudgetScopes, ExecutionMode: p.ExecutionMode}
}

func TestIssue642PublishedBarrierCrashProcessHelper(t *testing.T) {
	if os.Getenv("SWARM_PUBLISHED_BARRIER_CRASH_BACKEND") == "" {
		return
	}
	TestIssue642PublishedFanOutBarrierContinuesFinalConsumerBothStores(t)
}

func TestIssue642UnpublishedBarrierCrashProcessHelper(t *testing.T) {
	if os.Getenv("SWARM_UNPUBLISHED_BARRIER_CRASH_BACKEND") == "" {
		return
	}
	TestIssue642UnpublishedTransferredBarrierReforkBothStores(t)
}

func publishBarrierCrashCheckpoint(t *testing.T, ctx context.Context, selected *storeselected.Owner, result runforkexecution.SelectedContractExecutionResult, original string, observed forkEngineObservation, sourceRevision int) {
	t.Helper()
	claim, valid := deliverylifecycle.ClaimFromContext(observed.ctx)
	authority, granted := effects.AuthorityFromContext(observed.ctx)
	fact, pinned := correlation.SourceArtifactFactFromContext(ctx)
	if !valid || !granted || !pinned || !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork {
		t.Fatal("published barrier crash requires its actual native claim and selected authority")
	}
	preserved := publishedBarrierCrashAuthority{Kind: authority.Kind, ID: authority.ID, SelectedFork: authority.SelectedFork,
		ExecutionOwner: authority.ExecutionOwner, LeaseExpiresAt: authority.LeaseExpiresAt, FenceGeneration: authority.FenceGeneration,
		Target: authority.Target, BudgetScopes: authority.BudgetScopes, ExecutionMode: authority.ExecutionMode}
	if !reflect.DeepEqual(authority, preserved.native()) {
		t.Fatal("crash proof must preserve the exact active authority without another union branch")
	}
	child, err := selected.Entities().LoadOperatorEntity(ctx, claim.RunID(), claim.RunID())
	if err != nil || child.Fields["completed_total"] != float64(0) || child.Entity.Revision != 1 {
		t.Fatalf("crash crossed the unpublished final-consumer write: %+v err=%v", child, err)
	}
	fixture := &mixedFanOutCrashFixture{}
	fixture.publishCheckpoint(t, mixedFanOutCrashCheckpoint{
		Proof:      forkEngineExecutionProofRequest(t, result, original, observed.id, claim),
		BundleHash: fact.BundleHash(), SourceEntityID: result.Materialization.SourceRunID,
		ChildEntityID: claim.RunID(), SourceRevision: sourceRevision, ChildRevision: child.Entity.Revision, Authority: &preserved,
	})
}

// SIGKILL occurs after acknowledged activation/publication and the exact child
// claim, before any handler or graceful retirement can settle that occurrence.
func TestIssue642PublishedBarrierCrashRestartBothStores(t *testing.T) {
	verifyBarrierCrashRestartBothStores(t, false)
}

func TestIssue642UnpublishedBarrierCrashSuccessorBothStores(t *testing.T) {
	verifyBarrierCrashRestartBothStores(t, true)
}

func verifyBarrierCrashRestartBothStores(t *testing.T, pendingSource bool) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			address := filepath.Join(t.TempDir(), "published-barrier-crash.db")
			request := storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: address}}
			if backend == "postgres" {
				address = testutil.StartPostgresDSN(t)
				request.Selection = storebackend.Selection{Backend: storebackend.BackendPostgres}
				request.PostgresDSN = address
			}
			helper, prefix := "TestIssue642PublishedBarrierCrashProcessHelper", "SWARM_PUBLISHED_BARRIER_CRASH_"
			actor, key, transport := "published-barrier-crash", "pending-cut", "published-barrier-crash-transport"
			if pendingSource {
				helper, prefix = "TestIssue642UnpublishedBarrierCrashProcessHelper", "SWARM_UNPUBLISHED_BARRIER_CRASH_"
				actor, key, transport = "pending-barrier-refork", "parent-start", "pending-barrier-refork-transport"
			}
			checkpoint := killSelectedReceiverAtClaim(t, helper, []string{prefix + "BACKEND=" + backend, prefix + "ADDRESS=" + address})
			selected, err := storeselected.OpenRuntime(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = selected.CloseUnactivated() })
			repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
			if err != nil {
				t.Fatal(err)
			}
			_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, canonicalrouting.CopyForkFanOutBarrierReceiver(t), filepath.Join(repo, "platform-spec.yaml"))
			if err != nil || bundle.SourceArtifact.BundleHash() != checkpoint.BundleHash {
				t.Fatalf("barrier successor requires the exact original source artifact: %v", err)
			}
			plans, err := cliapp.StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.Schema().BootstrapSchema(t.Context(), store.SchemaBootstrapRequest{
				PlatformPlans: plans.Platform, StatePlans: plans.State,
				Origin: store.RuntimeStoreOrigin{SwarmVersion: "fork-engine-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC()},
			}); err != nil {
				t.Fatal(err)
			}
			runtimeID := uuid.NewString()
			fact, err := correlation.NewSourceArtifactFact(checkpoint.BundleHash)
			if err != nil {
				t.Fatal(err)
			}
			ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(t.Context(), runtimeID), fact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
			process := worklifetime.NewProcess()
			ctx = worklifetime.WithProcess(ctx, process)
			capability, err := selected.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
				OwnerID: "published-barrier-restart", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
			})
			if err != nil {
				t.Fatal(err)
			}
			family, found := selected.RunFork()
			if !found {
				t.Fatal("barrier successor requires the existing selected fork owner")
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := family.RetireSelectedContexts(cleanup); err != nil {
					t.Error(err)
				}
				process.Retire()
				if _, err := process.Join(cleanup); err != nil {
					t.Error(err)
				}
				if err := capability.Release(cleanup); err != nil {
					t.Error(err)
				}
			})
			if err := family.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			baseline := process.ActiveCount()
			wait, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			original, found, err := family.Operations().LoadForkOperation(wait, actor, key, transport)
			if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.ForkRunID != checkpoint.Proof.ChildRunID || original.Result == nil || original.Request.ResolvedPoint == nil {
				t.Fatalf("barrier crash lost its permanent cut/activation acknowledgment: %+v found=%t err=%v", original, found, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(wait, selected.RuntimeDeps().DeliveryStore, checkpoint.Proof.SourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			var pending *fanoutbarrier.TransferredOccurrence
			var ancestorBefore map[string]storetest.SelectedForkStorageTableSnapshot
			if pendingSource {
				cut, err := family.Plan(wait, runfork.RunForkPlanRequest{SourceRunID: checkpoint.Proof.SourceRunID, AtStart: true})
				if err != nil || len(cut.FanOutObligations) != 1 || cut.FanOutObligations[0].Barrier == nil || cut.FanOutObligations[0].Barrier.TransferredOccurrence == nil {
					t.Fatalf("pending crash source lost its admitted barrier: %+v error=%v", cut, err)
				}
				pending = cut.FanOutObligations[0].Barrier.TransferredOccurrence
				if pending.Publication.EventID != checkpoint.Proof.SourceEventID || cut.FanOutObligations[0].Barrier.Status != fanoutbarrier.StatusClosedPending ||
					original.Request.ResolvedPoint.Kind != runfork.RunForkPointRunStart || !original.Result.SourceFrozen {
					t.Fatal("pending crash source changed its obligation or start cut")
				}
				if _, physical := cut.HistoricalJoinPublication(pending.Publication.EventID); physical {
					t.Fatal("pending crash source invented a physical parent event")
				}
				ancestorBefore, err = storetest.ReadSelectedForkSourceDomain(wait, selected.RuntimeDeps().DeliveryStore, pending.Publication.CommittedCause.RunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			producer, err := identity.AdmitExecutableNodeDeclaration("producer", "producer")
			if err != nil {
				t.Fatal(err)
			}
			controller, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
			if err != nil {
				t.Fatal(err)
			}
			probe := &publishedBarrierClaimProbe{Probe: lifecycleprobe.New(), owner: producer.Key(), claim: make(chan forkEngineObservation, 1), resume: make(chan struct{}), completed: make(chan string, 1)}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(probe.resume) }) })
			options := runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability}
			options.AgentManagerOptions.TestLifecycleProbe = probe
			recovered, err := family.RecoverSelectedForkContexts(wait, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{
				SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.SourceArtifactStore()}, AgentRuntime: options,
			})
			if err != nil {
				t.Fatalf("published barrier native successor: %+v err=%v", recovered, err)
			}
			want := 1
			if pendingSource {
				want = 2
			}
			if len(recovered) != want {
				t.Fatalf("barrier successor lost a source boundary: %+v want=%d", recovered, want)
			}
			for _, entry := range recovered {
				if pendingSource && entry.RunID == checkpoint.Proof.SourceRunID {
					if entry.Disposition != runfork.SelectedForkRecoveryTerminal || entry.ExecutionID != "" || entry.Continuation != nil {
						t.Fatalf("materialized-only source acquired execution authority: %+v", entry)
					}
					continue
				}
				if entry.RunID != checkpoint.Proof.ChildRunID || entry.ExecutionID != checkpoint.Proof.ExecutionID || entry.Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("barrier native successor changed its acknowledged occurrence: %+v", entry)
				}
			}
			var observed forkEngineObservation
			select {
			case observed = <-probe.claim:
			case <-wait.Done():
				t.Fatal("successor did not acquire the exact published barrier delivery")
			}
			claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
			admission, admitted := managedexecution.FromContext(observed.ctx)
			if !claimed || !admitted || observed.id != checkpoint.Proof.ChildEventID || claim.DeliveryID() != checkpoint.Proof.DeliveryID ||
				uint64(claim.Version()) <= checkpoint.Proof.ClaimVersion || admission.Generation <= checkpoint.Proof.Generation || admission.ExecutionAuthorityID == checkpoint.Proof.ExecutionID {
				t.Fatalf("barrier successor borrowed the old grant or replaced owed work: event=%s claim=%+v admission=%+v checkpoint=%+v", observed.id, claim, admission, checkpoint.Proof)
			}
			old, ok := selected.RuntimeDeps().DeliveryStore.(interface {
				HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, effects.Authority, time.Duration) error
			})
			if !ok || checkpoint.Authority == nil || !checkpoint.Authority.native().Valid() || old.HeartbeatRunForkSelectedContractRuntimeExecution(wait, checkpoint.Authority.native(), time.Minute) == nil || wait.Err() != nil {
				t.Fatal("successor failed to fence the actual predecessor execution authority")
			}
			release.Do(func() { close(probe.resume) })
			if _, err := probe.Wait(wait, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: observed.id, SubscriberID: producer.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			var output string
			select {
			case output = <-probe.completed:
			case <-wait.Done():
				t.Fatal("recovered barrier never published its real business output")
			}
			if _, err := probe.Wait(wait, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: output, SubscriberID: controller.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			waitPublishedBarrierPipelineReceipts(t, wait, selected.RuntimeDeps().DeliveryStore, observed.id, output)
			child, err := selected.Entities().LoadOperatorEntity(wait, checkpoint.Proof.ChildRunID, checkpoint.Proof.ChildRunID)
			if err != nil || child.Fields["completed_total"] != float64(1) || child.Entity.Revision != checkpoint.ChildRevision+1 {
				t.Fatalf("barrier successor lost the once-only final consumer: %+v err=%v", child, err)
			}
			final, err := family.Plan(wait, runfork.RunForkPlanRequest{SourceRunID: checkpoint.Proof.ChildRunID, At: output})
			if err != nil || len(final.FanOutObligations) != 1 || final.FanOutObligations[0].Barrier == nil {
				t.Fatalf("barrier successor lost complete settled history: %+v err=%v", final, err)
			}
			barrier := final.FanOutObligations[0].Barrier
			if barrier.Status != fanoutbarrier.StatusFired || barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 1, Succeeded: 1}) || barrier.ScheduleActivationID != "" ||
				barrier.TransferredOccurrence == nil || barrier.TransferredOccurrence.Publication.EventID != observed.id || barrier.TransferredOccurrence.Publication.SourceEventID != checkpoint.Proof.SourceEventID {
				t.Fatalf("native barrier recovery changed retained lineage/summary or invented a timer: %+v", barrier)
			}
			if pendingSource {
				transferred := barrier.TransferredOccurrence
				if transferred.Publication.SourceRunID != checkpoint.Proof.SourceRunID || transferred.Publication.CommittedCause != pending.Publication.CommittedCause || !transferred.DueAt.Equal(pending.DueAt) {
					t.Fatal("pending barrier recovery flattened its parent or changed cause/due")
				}
				event := storetest.LoadCanonicalEventRecord(t, wait, selected.RuntimeDeps().DeliveryStore, observed.id)
				lineage, valid := event.SelectedForkLineage()
				if !valid || lineage.SourceRunID() != checkpoint.Proof.SourceRunID || lineage.SourceEventID() != pending.Publication.EventID || !event.CreatedAt().Equal(pending.DueAt) {
					t.Fatal("pending barrier successor lost exact event lineage or due")
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected.RuntimeDeps().DeliveryStore, checkpoint.Proof.SourceRunID, "platform.join_complete"); err != nil || count != 0 {
					t.Fatalf("pending barrier successor invented EB: count=%d error=%v", count, err)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(wait, selected.RuntimeDeps().DeliveryStore, pending.Publication.CommittedCause.RunID)
				if err != nil || !reflect.DeepEqual(ancestorBefore, after) {
					t.Fatalf("pending barrier successor changed ancestor cause/business facts: %v", err)
				}
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected.RuntimeDeps().DeliveryStore, checkpoint.Proof.ChildRunID, "platform.join_complete"); err != nil || count != 1 {
				t.Fatalf("native successor duplicated its published occurrence: count=%d err=%v", count, err)
			}
			settlement, err := selected.RuntimeDeps().DeliveryStore.Snapshot(wait, claim.DeliveryID())
			if err != nil || settlement.Status != deliverylifecycle.StatusDelivered || !settlement.MatchesSettlementClaim(claim) {
				t.Fatalf("native successor did not settle the exact fresh claim: %+v err=%v", settlement, err)
			}
			again, found, err := family.Operations().LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(original, again) {
				t.Fatalf("native recovery changed the immutable operation acknowledgment: %+v err=%v", again, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(wait, selected.RuntimeDeps().DeliveryStore, checkpoint.Proof.SourceRunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("native barrier recovery changed source business facts: %v", err)
			}
			cleanup, stop := context.WithTimeout(context.WithoutCancel(wait), 10*time.Second)
			defer stop()
			if err := family.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("native barrier recovery did not join exact owned resources: work=%d baseline=%d err=%v", process.ActiveCount(), baseline, err)
			}
		})
	}
}
