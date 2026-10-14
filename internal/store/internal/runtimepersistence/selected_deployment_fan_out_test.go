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
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestSelectedDeploymentServingRequiresExactLiveGrantBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			fixture.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
			fixture.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
			fixture.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "feed-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := store.(interface {
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
			}).RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			process, err := fixture.process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			request := startupownership.SelectedForkGrantRequest{
				RuntimeInstanceID: process.RuntimeInstanceID,
				Binding: startupownership.SelectedForkGrantBinding{
					BindingID: binding.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
					ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration,
					ExecutionOwner: authority.ExecutionOwner, AdmissionFingerprint: issued.AdmissionFingerprint,
					ContainerPlanFingerprint: issued.ContainerPlanFingerprint, ActorCensusFingerprint: issued.ActorCensusFingerprint,
					EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
					DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint,
					PreparationFingerprint:     issued.PreparationFingerprint,
				},
			}
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&request.BundleHash); err != nil {
				t.Fatal(err)
			}
			grant, err := fixture.process.IssueSelectedForkGenerationGrant(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			evidence, err := grant.AdmitExecution(ctx)
			if err != nil {
				t.Fatal(err)
			}
			serving := store.(interface {
				BindSelectedDeploymentFanOutGrant(startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error)
				ListSelectedDeploymentFeeds(context.Context, startupownership.GrantEvidence) ([]fanoutobligation.Intent, error)
			})
			owner, err := serving.BindSelectedDeploymentFanOutGrant(evidence)
			if err != nil || owner == nil {
				t.Fatalf("exact selected bind: owner=%T err=%v", owner, err)
			}
			feedKey := fanoutobligation.IntentKey{RunID: fixture.forkRun, DeploymentFeedID: uuid.NewString()}
			claim := pipeline.FanOutClaimRequest{Owner: "feed-worker", BundleHash: evidence.BundleHash, Candidate: &feedKey, Now: time.Now().UTC(), Lease: time.Minute}
			_, _, found, err := owner.ClaimFanOutIntent(ctx, claim)
			if err != nil || found {
				t.Fatalf("exact selected claim admission without a feed: found=%v err=%v", found, err)
			}
			handlerKey := fanoutobligation.IntentKey{RunID: fixture.forkRun, TriggeringDeliveryID: uuid.NewString(), ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "nodes.scatter"}}
			claim.Candidate = &handlerKey
			if _, _, _, err := owner.ClaimFanOutIntent(ctx, claim); err == nil {
				t.Fatal("selected serving owner admitted handler intent")
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
				fixture.forkRun, feedKey.DeploymentFeedID, evidence.BundleHash, ref.FlowPath, ref.EventName,
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
				"bundle-v2:sha256:"+strings.Repeat("6", 64), fixture.forkRun, feedKey.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, evidence); err == nil {
				t.Fatal("selected feed census accepted contradictory bundle ownership")
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET bundle_hash=$1 WHERE run_id=$2 AND deployment_feed_id=$3`,
				evidence.BundleHash, fixture.forkRun, feedKey.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			ordinary := evidence
			ordinary.SelectedFork = nil
			if _, err := serving.BindSelectedDeploymentFanOutGrant(ordinary); err == nil {
				t.Fatal("selected binder borrowed ordinary grant")
			}
			wrong := evidence
			wrongBinding := *evidence.SelectedFork
			wrongBinding.FenceGeneration++
			wrong.SelectedFork = &wrongBinding
			if _, err := serving.BindSelectedDeploymentFanOutGrant(wrong); err == nil {
				t.Fatal("selected binder accepted wrong execution fence")
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, wrong); err == nil {
				t.Fatal("selected feed listing accepted wrong execution fence")
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fence_generation=fence_generation+1 WHERE execution_id=$1`, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			if _, err := serving.BindSelectedDeploymentFanOutGrant(evidence); err == nil {
				t.Fatal("stale selected grant retained bind authority")
			}
			if _, err := serving.ListSelectedDeploymentFeeds(ctx, evidence); err == nil {
				t.Fatal("stale selected grant retained feed-list authority")
			}
			claim.Candidate = &feedKey
			if _, _, _, err := owner.ClaimFanOutIntent(ctx, claim); err == nil {
				t.Fatal("bound selected owner retained stale execution fence")
			}
		})
	}
}
