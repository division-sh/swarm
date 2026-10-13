package runtimepersistence

import (
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// This consumes the real revision-1 construction through selected preparation,
// materialization and activation. The one-hour timer remains pending; firing
// and crash/recovery are the separate retained C3 controls.
func TestC4F40SelectedStartTimerChildBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := newP16RaceStore(t, backend)
			ctx, creation := c4DeploymentStartCommand(t, selected, true)
			created, err := selected.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, creation)
			if err != nil || !created.Acknowledged || created.Replay || len(creation.Root.Plan.Lifecycle.Timers) != 1 {
				t.Fatalf("real timer-bearing deployment creation: %+v err=%v", created, err)
			}
			sourceRun, bundleHash := creation.RunCreation.RunID, creation.RunCreation.BundleHash
			sourcePlan := c4RequireExclusiveStart(t, ctx, selected, sourceRun, bundleHash, "")
			c4RequireRevisionCount(t, ctx, selected, sourceRun, 1)
			if len(sourcePlan.WorkflowTimers) != 1 {
				t.Fatalf("real original start lost its initial timer: %+v", sourcePlan.WorkflowTimers)
			}
			sourceTimer, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(sourcePlan.WorkflowTimers[0])
			if err != nil || !reflect.DeepEqual(sourceTimer.Canonical(), creation.Root.Plan.Lifecycle.Timers[0].Activation.Canonical()) {
				t.Fatalf("revision-1 capture changed initial timer: %+v err=%v", sourceTimer, err)
			}
			reader := selected.(interface {
				LoadWorkflowTimerActivation(context.Context, string) (pipeline.WorkflowTimerActivation, bool, error)
			})
			sourceDomain, err := ReadSelectedForkSourceDomainForTest(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			sourceStorage, err := ObserveWorkflowTimerReplayStorageForTest(ctx, selected, sourceRun, sourceTimer.EntityID)
			if err != nil || sourceStorage != (WorkflowTimerReplayStorageObservation{Timers: 1, ActiveTimers: 1, TimerRevisionFacts: 1}) {
				t.Fatalf("initial timer source cardinality: %+v err=%v", sourceStorage, err)
			}
			requireSourceUnchanged := func() {
				t.Helper()
				actual, found, err := reader.LoadWorkflowTimerActivation(ctx, sourceTimer.Ref.ActivationID)
				if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), sourceTimer.Canonical()) {
					t.Fatalf("selected child changed source timer: %+v found=%t err=%v", actual, found, err)
				}
				domain, err := ReadSelectedForkSourceDomainForTest(ctx, selected, sourceRun)
				if err != nil || !reflect.DeepEqual(domain, sourceDomain) {
					t.Fatalf("selected child changed source business rows: %v", err)
				}
				storage, err := ObserveWorkflowTimerReplayStorageForTest(ctx, selected, sourceRun, sourceTimer.EntityID)
				if err != nil || storage != sourceStorage {
					t.Fatalf("selected child changed source timer/publication history: %+v err=%v", storage, err)
				}
			}

			workValue, _ := storeTestWorkFixtures.Load(t)
			work := workValue.(*storeTestWorkFixture)
			ctx = worklifetime.WithProcess(ctx, work.process)
			// The real source constructor already possesses this capability. Do
			// not acquire a competing process or synthesize an execution authority.
			capability, err := agentfixture.ProcessCapability(t, ctx, selected.(agentfixture.Store))
			if err != nil {
				t.Fatal(err)
			}
			fork := selected.(runforkexecution.SelectedContractForkLifecycle)
			owner := selectedStorePreparationOwnerWithForkForTest(t, selected, fork)
			if err := owner.BindSelectedProcess(ctx, work.process, capability); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil || len(recovered) != 0 {
				t.Fatalf("fresh selected process has unexpected recovery debt: %+v err=%v", recovered, err)
			}
			baseline := work.process.ActiveCount()
			selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: bundleHash}
			operation := &runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "c4-f40-start-timer", IdempotencyKey: uuid.NewString(), TransportHash: "c4-f40-start-timer",
				SourceRunID: sourceRun, AtStart: true, ResolvedPoint: &sourcePlan.ForkPoint, TargetBundleHash: bundleHash,
				AllowSourceFreeze: true, ContractSelection: selection,
			}
			if _, _, err := operation.Canonical(); err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			executionRequest := runforkexecution.SelectedContractExecutionRequest{
				Owner: owner, SourceRunID: sourceRun, AtStart: true, ForkOperation: operation, AllowSourceFreeze: true,
				ContractSelection: selection, ExpectedBundleHash: bundleHash,
				SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
					RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(runforkexecution.SourceArtifactSelectedContractSourceStore),
				},
				AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
					ProcessCapability: capability, ExecutionPosture: executionposture.Live,
					Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
				},
			}
			prepared, err := owner.Prepare(ctx, executionRequest)
			if err != nil {
				t.Fatalf("compose exact selected start preparation: %v", err)
			}
			t.Cleanup(func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			})
			request, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			// Prepare owns admitted coordinates/readiness; the durable request is
			// still caller intent, as in the existing prepare/materialize controls.
			request.ForkOperation = operation
			if request.Preparation.ForkPoint != sourcePlan.ForkPoint || request.Preparation.ForkEventID != "" || request.At != "" ||
				request.SourceArtifactFact.BundleHash() != bundleHash || request.Preparation.Coordinates.BundleHash != bundleHash {
				t.Fatalf("selected preparation changed exact start selector/schema source: %+v", request.Preparation)
			}
			identity, err := request.Readiness.ExecutionIdentity()
			if err != nil || identity.BundleHash != bundleHash {
				t.Fatalf("selected timer readiness elected another schema source: %+v err=%v", identity, err)
			}
			sourceFingerprint, err := runfork.SelectedPreparationSourceFingerprint(request.EffectiveSourceIdentity)
			if err != nil || sourceFingerprint != request.Preparation.Coordinates.SourceFingerprint {
				t.Fatalf("selected timer preparation lost its stored source hash: %v", err)
			}
			fingerprint, err := runfork.SelectedPreparationPlanFingerprint(sourcePlan, request.FrontierAdmission, request.RecipientPlanning, request.Preparation.DeclarationPlanFingerprint)
			if err != nil || fingerprint != request.Preparation.Coordinates.AdmittedPlanFingerprint {
				t.Fatalf("selected preparation lost real fixed-plan timer hash: %v", err)
			}
			withoutTimer := sourcePlan
			withoutTimer.WorkflowTimers = nil
			missingHash, err := runfork.SelectedPreparationPlanFingerprint(withoutTimer, request.FrontierAdmission, request.RecipientPlanning, request.Preparation.DeclarationPlanFingerprint)
			if err != nil || missingHash == fingerprint {
				t.Fatalf("selected preparation hash did not bind initial timer: %v", err)
			}
			staged, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || staged.ForkRunID == "" || staged.SourceRunID != sourceRun || staged.ForkPoint != sourcePlan.ForkPoint || staged.ForkRunStatus != runfork.RunForkMaterializedStatus {
				t.Fatalf("native selected start timer child: %+v err=%v", staged, err)
			}
			binding := staged.SelectedContractBinding
			if binding == nil || binding.ValidateExecutionBinding(staged.ForkRunID) != nil || binding.SourceRunID != sourceRun ||
				binding.ForkPoint != sourcePlan.ForkPoint || binding.ForkEventID != "" || binding.ContractSelection != selection {
				t.Fatalf("native selected start child lacks exact staged binding: %+v", binding)
			}
			admission := staged.ReplayResumeAdmission
			if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner || !admission.ReplayResumeFactsPresent ||
				len(staged.UnsupportedBlockers) != 0 || len(admission.UnsupportedBlockers) != 0 {
				t.Fatalf("native selected start child lacks complete staged admission: %+v", staged)
			}
			var timerCertificate string
			for _, disposition := range admission.Dispositions {
				if disposition.Disposition == runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != "" {
					t.Fatalf("native selected start child retained a staged blocker: %+v", disposition)
				}
				if disposition.Fact != runfork.RunForkReplayResumeFactTimerHistory {
					continue
				}
				if timerCertificate != "" || disposition.Disposition != runfork.RunForkReplayResumeDispositionReconstruct || disposition.Owner != "store.run_fork.timer_history_materialization" {
					t.Fatalf("native selected start timer has incomplete or duplicate readback: %+v", disposition)
				}
				timerCertificate = disposition.Classification
			}
			digest, certified := strings.CutPrefix(timerCertificate, "timer_history_native_readback:sha256:")
			certificateHash, err := hex.DecodeString(digest)
			if !certified || err != nil || len(certificateHash) != 32 {
				t.Fatalf("native selected start timer lacks its complete readback certificate: %q err=%v", timerCertificate, err)
			}
			// Staged timer certification is not a current execution grant. The
			// real owner must still activate and retain the pending child below.
			header, err := selected.(runOriginReadStore).LoadRunHeader(ctx, staged.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			origin, err := sourcePlan.ForkPoint.MaterializationRunOrigin(sourceRun)
			if err != nil || !header.Origin.Equal(origin) || header.EventCount != 0 {
				t.Fatalf("timer child invented creating event or wrong origin: %+v err=%v", header, err)
			}
			bornAt := runlifecycle.CanonicalTimestamp(header.StartedAt)
			projection, err := runfork.ProjectWorkflowTimerRecord(sourceTimer.PersistenceRecord(), sourceTimer.Ref, staged.ForkRunID, sourcePlan.ForkPoint, nil, bornAt)
			if err != nil {
				t.Fatal(err)
			}
			selectedRecord, err := request.Readiness.SelectInheritedWorkflowTimerRecord(projection)
			if err != nil || selectedRecord == nil {
				t.Fatalf("real selected declaration removed initial timer: %v", err)
			}
			desired, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(*selectedRecord)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projection)
			if err != nil {
				t.Fatal(err)
			}
			expected.Ref.DeclarationRevision = desired.Ref.DeclarationRevision
			expected.OwnerAgent, expected.EventType = desired.OwnerAgent, desired.EventType
			requireChildTimer := func() {
				t.Helper()
				actual, found, err := reader.LoadWorkflowTimerActivation(ctx, expected.Ref.ActivationID)
				if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), expected.Canonical()) ||
					actual.ForkedFromPointKind != runfork.RunForkPointRunStart || actual.ForkedFromPointRevision != 1 || actual.ForkedFromEventID != "" ||
					actual.SourceTimerID != sourceTimer.Ref.ActivationID || actual.ForkedFromRunID != sourceRun ||
					!actual.SourceArmedAt.Equal(sourceTimer.CreatedAt) || !actual.FireAt.Equal(sourceTimer.FireAt) || !actual.CreatedAt.Equal(bornAt) {
					t.Fatalf("native selected timer lost original due/arm/birth/typed lineage: %+v found=%t err=%v", actual, found, err)
				}
				decoded, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(actual.PersistenceRecord())
				if err != nil || !reflect.DeepEqual(decoded.Canonical(), actual.Canonical()) {
					t.Fatalf("native selected timer codec changed schema/lineage: %v", err)
				}
				storage, err := ObserveWorkflowTimerReplayStorageForTest(ctx, selected, staged.ForkRunID, expected.EntityID)
				if err != nil || storage.Timers != 1 || storage.ActiveTimers != 1 || storage.TimerRevisionFacts != 1 {
					t.Fatalf("selected timer retry rearmed or duplicated history: %+v err=%v", storage, err)
				}
				fired, err := ReadLifecycleEventCardinalityForTest(ctx, selected, staged.ForkRunID, expected.EventType)
				if err != nil || fired != 0 {
					t.Fatalf("one-hour selected timer unexpectedly fired: count=%d err=%v", fired, err)
				}
			}
			childStart := c4RequireExclusiveStart(t, ctx, selected, staged.ForkRunID, bundleHash, "")
			if len(childStart.WorkflowTimers) != 1 || !reflect.DeepEqual(childStart.WorkflowTimers[0], expected.PersistenceRecord()) {
				t.Fatal("native child creation did not atomically retain its exact inherited timer")
			}
			for pass := 0; pass < 2; pass++ {
				replayed, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
				if err != nil || replayed.ForkRunID != staged.ForkRunID || replayed.ForkPoint != staged.ForkPoint {
					t.Fatalf("exact selected start materialization retry: %+v err=%v", replayed, err)
				}
				c4RequireRevisionCount(t, ctx, selected, staged.ForkRunID, 1)
				requireChildTimer()
				requireSourceUnchanged()
			}
			if err := prepared.Close(); err != nil || work.process.ActiveCount() != baseline {
				t.Fatalf("staged preparation did not release only its own work: leases=%d baseline=%d err=%v", work.process.ActiveCount(), baseline, err)
			}
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			materializedOperation, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || materializedOperation.Validate() != nil || materializedOperation.Status != runfork.ForkOperationMaterialized ||
				materializedOperation.ForkRunID != staged.ForkRunID || materializedOperation.Request.OperationID != operation.OperationID ||
				!materializedOperation.Request.AtStart || materializedOperation.Request.ResolvedPoint == nil ||
				*materializedOperation.Request.ResolvedPoint != sourcePlan.ForkPoint || materializedOperation.Request.ForkEventID != "" || materializedOperation.Request.TargetBundleHash != bundleHash {
				t.Fatalf("native retry lost the durable exact-start request: %+v found=%t err=%v", materializedOperation, found, err)
			}

			activated, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, executionRequest)
			if err != nil || !activated.Activation.Activated || activated.Materialization.ForkRunID != staged.ForkRunID ||
				activated.Activation.ForkPoint != sourcePlan.ForkPoint || activated.ExecutedEventCount != 0 ||
				activated.ForkLocalRuntimeContainer == nil || activated.ForkLocalRuntimeContainer.RuntimeExecutionID == "" ||
				activated.ForkLocalRuntimeContainer.ForkEventID != "" || activated.ForkLocalRuntimeContainer.ForkPoint != sourcePlan.ForkPoint || len(activated.ForkLocalRuntimeContainer.SourceEventIDs) != 0 {
				t.Fatalf("real owner failed pending exact-start timer activation: %+v err=%v", activated, err)
			}
			activatedOperation, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || activatedOperation.Validate() != nil || activatedOperation.Status != runfork.ForkOperationActivated ||
				activatedOperation.Request.OperationID != materializedOperation.Request.OperationID || activatedOperation.ForkRunID != staged.ForkRunID ||
				activatedOperation.Result == nil || activatedOperation.Result.ForkPoint != sourcePlan.ForkPoint ||
				activatedOperation.Result.ForkEventID != "" || activatedOperation.Result.BundleHash != bundleHash || activatedOperation.Result.ExecutedEventCount != 0 {
				t.Fatalf("native activation lost exact typed-start completion: %+v found=%t err=%v", activatedOperation, found, err)
			}
			pending, err := ReadSelectedExecutionStorageForTest(ctx, selected, staged.ForkRunID)
			if err != nil || pending.State != "running" || pending.RunStatus != runfork.RunForkActivatedStatus || pending.Occurrences != 1 || work.process.ActiveCount() <= baseline {
				t.Fatalf("timer child lacks current retained owner: %+v leases=%d baseline=%d err=%v", pending, work.process.ActiveCount(), baseline, err)
			}
			if err := capability.ProveCurrent(ctx); err != nil {
				t.Fatal(err)
			}
			requireChildTimer()
			requireSourceUnchanged()
			if err := owner.RetireSelectedContexts(context.Background()); err != nil || work.process.ActiveCount() != baseline {
				t.Fatalf("pending timer owner did not retire/join locally: leases=%d baseline=%d err=%v", work.process.ActiveCount(), baseline, err)
			}
			if next, err := owner.Prepare(ctx, executionRequest); next != nil || !errors.Is(err, worklifetime.ErrRetired) {
				if next != nil {
					_ = next.Close()
				}
				t.Fatalf("retired owner admitted another selected context: %v", err)
			}
			// Local retirement is not terminal timer settlement or crash recovery.
			// Keep the durable pending row, original due and source untouched.
			requireChildTimer()
			requireSourceUnchanged()
			retiredOperation, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(retiredOperation, activatedOperation) {
				t.Fatalf("local retirement rewrote permanent start acknowledgment: found=%t err=%v", found, err)
			}
		})
	}
}
