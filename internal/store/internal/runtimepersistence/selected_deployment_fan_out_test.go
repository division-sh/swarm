package runtimepersistence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedDeploymentServingRequiresExactLiveGrantBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store, db, sqlite := fixture.store, fixture.db, backend.name == "sqlite"
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, !sqlite)
			request.ForkOperation = &runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "selected-barrier-proof", IdempotencyKey: uuid.NewString(),
				TransportHash: "selected-barrier-proof", SourceRunID: request.SourceRunID, ForkEventID: request.At,
				TargetBundleHash: request.SourceArtifactFact.BundleHash(), TargetRootReference: request.RootAdmission.Reference(), ContractSelection: request.ContractSelection,
			}
			materialized, err := store.(forkWorkflowOwnershipStore).MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, forkRun := materialized.SourceRunID, materialized.ForkRunID
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{
				RepoRoot: canonicalrouting.RepoRoot(t), Store: store.(runforkexecution.SourceArtifactSelectedContractSourceStore),
			}
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
				Responsibility: runforkexecution.OriginalRunSource(sourceRun), Selection: request.ContractSelection,
			})
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Cleanup != nil {
				t.Cleanup(func() {
					if err := loaded.Cleanup(); err != nil {
						t.Error(err)
					}
				})
			}
			servingCtx, bound, _ := admitSelectedForkNativePhaseForTest(t, ctx, fixture, loaded.Source, materialized, request)
			original, err := startupownership.CaptureMutationAuthority(servingCtx)
			if err != nil {
				t.Fatal(err)
			}
			evidence := original.GrantEvidence()
			servingCtx = startupownership.WithGenerationGrantEvidence(servingCtx, evidence)
			serving := store.(interface {
				BindSelectedFanOutGrant(startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error)
				ListSelectedDeploymentFeeds(context.Context, startupownership.GrantEvidence) ([]fanoutobligation.Intent, error)
			})
			owner, ok := bound.(pipeline.SelectedFanOutObligationOwner)
			if !ok {
				t.Fatalf("exact selected bind: owner=%T", bound)
			}
			if activations, err := owner.ReconcileFanOutBarriers(servingCtx); err != nil || len(activations) != 0 {
				t.Fatalf("empty exact child barrier recovery: activations=%v err=%v", activations, err)
			}
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			for _, runID := range []string{sourceRun, forkRun} {
				barrier := seedFanOutOwnerChildFixture(t, ctx, db, store, !sqlite, fanOutOwnerFixture{
					runID: runID, flowPath: ".", bundleHash: evidence.BundleHash, plan: fanOutOwnerTypedPlanFixture(t),
				}, 0, base)
				seedFanOutDeliveryBarrier(t, ctx, db, barrier, base)
			}
			var activationID string
			for restart := range 2 {
				bound, err = serving.BindSelectedFanOutGrant(evidence)
				if err != nil {
					t.Fatal(err)
				}
				owner, ok = bound.(pipeline.SelectedFanOutObligationOwner)
				if !ok {
					t.Fatalf("selected barrier recovery owner=%T", bound)
				}
				activations, err := owner.ReconcileFanOutBarriers(servingCtx)
				if err != nil || len(activations) != 1 {
					t.Fatalf("selected barrier recovery %d: activations=%v err=%v", restart, activations, err)
				}
				if restart == 0 {
					activationID = activations[0].ID()
				} else if activations[0].ID() != activationID {
					t.Fatal("selected restart replaced the committed barrier activation")
				}
			}
			assertFanOutBarrierTimerCount(t, ctx, db, forkRun, 1)
			assertFanOutBarrierTimerCount(t, ctx, db, sourceRun, 0)
			feedKey := fanoutobligation.IntentKey{RunID: forkRun, DeploymentFeedID: uuid.NewString()}
			claim := pipeline.FanOutClaimRequest{Owner: "feed-worker", BundleHash: evidence.BundleHash, Candidate: &feedKey, Now: time.Now().UTC(), Lease: time.Minute}
			_, _, found, err := owner.ClaimFanOutIntent(ctx, claim)
			if err != nil || found {
				t.Fatalf("exact selected claim admission without a feed: found=%v err=%v", found, err)
			}
			handlerKey := fanoutobligation.IntentKey{RunID: forkRun, TriggeringDeliveryID: uuid.NewString(), ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "nodes.scatter"}}
			claim.Candidate = &handlerKey
			if _, _, found, err := owner.ClaimFanOutIntent(ctx, claim); err != nil || found {
				t.Fatalf("exact selected handler admission without an intent: found=%v err=%v", found, err)
			}
			foreignHandler := handlerKey
			foreignHandler.RunID = uuid.NewString()
			claim.Candidate = &foreignHandler
			if _, _, _, err := owner.ClaimFanOutIntent(ctx, claim); err == nil {
				t.Fatal("selected serving owner admitted another run's handler")
			}
			feeds, err := serving.ListSelectedDeploymentFeeds(ctx, evidence)
			if err != nil || len(feeds) != 0 {
				t.Fatalf("empty selected feed census: feeds=%#v err=%v", feeds, err)
			}
			ref, err := durabledata.ParseDeclarationRef(".", "items.ready")
			if err != nil {
				t.Fatal(err)
			}
			compiled, defects := durabledata.CompileJSONL(ref, map[string]any{
				"type": "object", "required": []string{"name"},
				"properties": map[string]any{"name": map[string]any{"type": "string"}},
			}, "", []byte("{\"name\":\"selected\"}\n"))
			if len(defects) != 0 {
				t.Fatalf("compile selected source: %#v", defects)
			}
			manifest, err := json.Marshal(compiled.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if _, err := db.ExecContext(ctx, `INSERT INTO resource_declarations (flow_path,event_name,admitted_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, ref.FlowPath, ref.EventName, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO resource_versions (
				version_id,flow_path,event_name,sequence_alias,schema_digest,canonical_schema_bytes,
				business_key_field,content_digest,row_codec,row_count,manifest_json,canonical_jsonl,created_at
			) VALUES ($1,$2,$3,1,$4,$5,NULL,$6,$7,$8,$9,$10,$11)`,
				compiled.VersionID, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest,
				compiled.CanonicalSchema, compiled.Manifest.ContentDigest, compiled.Manifest.RowCodec,
				len(compiled.Rows), string(manifest), compiled.CanonicalJSONL, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO fan_out_intents (
				run_id,origin_kind,deployment_feed_id,bundle_hash,source_kind,
				source_resource_flow_path,source_resource_event_name,source_resource_version_id,
				deployment_schema_digest,cardinality,cursor,status,next_chunk_size,created_at,updated_at
			) VALUES ($1,'deployment',$2,$3,'resource_version',$4,$5,$6,$7,1,0,'open',$8,$9,$9)`,
				forkRun, feedKey.DeploymentFeedID, evidence.BundleHash, ref.FlowPath, ref.EventName,
				compiled.VersionID, compiled.Manifest.SchemaDigest, fanoutobligation.InitialChunkSize, now); err != nil {
				t.Fatal(err)
			}
			feeds, err = serving.ListSelectedDeploymentFeeds(ctx, evidence)
			if err != nil || len(feeds) != 1 || feeds[0].Request.Key != feedKey || feeds[0].Request.Deployment == nil ||
				feeds[0].Request.Deployment.BundleHash != evidence.BundleHash || feeds[0].Request.Source.VersionID != compiled.VersionID ||
				feeds[0].Status != fanoutobligation.StatusOpen || feeds[0].Cursor != 0 {
				t.Fatalf("exact selected feed census: feeds=%#v err=%v", feeds, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET bundle_hash=$1 WHERE run_id=$2 AND deployment_feed_id=$3`,
				"bundle-v2:sha256:"+strings.Repeat("6", 64), forkRun, feedKey.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, evidence); err == nil {
				t.Fatal("selected feed census accepted contradictory bundle ownership")
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET bundle_hash=$1 WHERE run_id=$2 AND deployment_feed_id=$3`,
				evidence.BundleHash, forkRun, feedKey.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			ordinary := evidence
			ordinary.SelectedFork = nil
			if _, err := serving.BindSelectedFanOutGrant(ordinary); err == nil {
				t.Fatal("selected binder borrowed ordinary grant")
			}
			wrong := evidence
			wrongBinding := *evidence.SelectedFork
			wrongBinding.FenceGeneration++
			wrong.SelectedFork = &wrongBinding
			if _, err := serving.BindSelectedFanOutGrant(wrong); err == nil {
				t.Fatal("selected binder accepted wrong execution fence")
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, wrong); err == nil {
				t.Fatal("selected feed listing accepted wrong execution fence")
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fence_generation=fence_generation+1 WHERE execution_id=$1`, evidence.SelectedFork.ExecutionID); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fence_generation=$1 WHERE execution_id=$2`, evidence.SelectedFork.FenceGeneration, evidence.SelectedFork.ExecutionID); err != nil {
					t.Error(err)
				}
			}()
			if _, err := serving.BindSelectedFanOutGrant(evidence); err == nil {
				t.Fatal("stale selected grant retained bind authority")
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, evidence); err == nil {
				t.Fatal("stale selected grant retained feed-list authority")
			}
			claim.Candidate = &feedKey
			if _, _, _, err := owner.ClaimFanOutIntent(ctx, claim); err == nil {
				t.Fatal("bound selected owner retained stale execution fence")
			}
			if _, err := owner.ReconcileFanOutBarriers(servingCtx); err == nil {
				t.Fatal("selected barrier recovery retained stale execution fence")
			}
		})
	}
}
