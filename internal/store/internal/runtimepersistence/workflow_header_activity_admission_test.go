package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// The loop buckets are controlled decoder inputs on an actually constructed
// header. This proves native activity admission/result persistence, not a served
// loop journey or the compiler's production of those inputs.
func TestActivityAdmissionConsumesConstructedHeaderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"fieldless", "fields"} {
			for _, cell := range []string{"current", "stale", "wrong_entity", "wrong_path", "wrong_header_flow", "missing_header", "malformed_bucket"} {
				t.Run(backend+"/"+shape+"/"+cell, func(t *testing.T) {
					files := map[string]string{"schema.yaml": "name: activity-header\nstages:\n  review: {}\n"}
					if shape == "fields" {
						files["entities.yaml"] = "default:\n  title: text\n  accumulator:\n    type: text?\n    initial: 'business-only'\n"
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
					run := correlation.RunIDFromContext(f.ctx)
					req := sqliteFlowActivationRequest(f.bundle, ".", run, "", run)
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !committed.Created || !committed.Acknowledged {
						t.Fatalf("canonical construction: %+v %v", committed, err)
					}
					at := time.Now().UTC()
					loop, err := loopruntime.New(run, req.Instance.EntityID, ".", "revision", "revision_id", uuid.NewString(), "review", 3, at)
					if err != nil {
						t.Fatal(err)
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, loop); err != nil {
						t.Fatal(err)
					}
					raw, err := json.Marshal(buckets)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.Exec(`UPDATE flow_instances SET accumulator=$3 WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID, string(raw)); err != nil {
						t.Fatal(err)
					}
					owner, err := plan.Readiness.FlowIdentity()
					if err != nil {
						t.Fatal(err)
					}
					stored, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || stored.EntityID != req.Instance.EntityID || stored.CurrentState != "review" ||
						(shape == "fields" && stored.Fields["accumulator"] != "business-only") || (shape == "fieldless" && len(stored.Fields) != 0) {
						t.Fatalf("independent activity header/business witness: found=%t instance=%+v err=%v", found, stored, err)
					}
					carrier, err := engine.StateCarrierFromPersisted(stored.Fields, stored.Bookkeeping, stored.Gates, stored.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					ownedLoop, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
					if err != nil || !found || !ownedLoop.Generation().Equal(loop.Generation()) {
						t.Fatalf("business field replaced header loop evidence: found=%t loop=%+v err=%v", found, ownedLoop, err)
					}
					record := pipeline.ActivityAttemptRecord{
						RequestEventID: uuid.NewString(), RunID: run, EntityID: req.Instance.EntityID, FlowInstance: req.Instance.InstancePath,
						NodeID: activityidentity.MustNodeOwner(mustPersistenceNode(".", "writer")).Key(), HandlerEventKey: "request", ActivityID: "write", Tool: "provider.write",
						EffectClass: "non_idempotent_write", Attempt: 1, ExecutionMode: executionmode.Live,
						SuccessEvent: "write.succeeded", FailureEvent: "write.failed", InputHash: "exact-input", Generation: loop.Generation(), LoopStage: "review",
					}
					if cell == "current" || cell == "stale" {
						card, continuation := newRootProposedEffectTestCard(t, run, at)
						continuation.BundleHash, continuation.WorkflowVersion = f.bundle.SourceArtifact.BundleHash(), f.bundle.WorkflowVersion()
						card.BundleHash, card.WorkflowVersion = continuation.BundleHash, continuation.WorkflowVersion
						continuation.NodeID, continuation.HandlerEventKey = record.NodeID, record.HandlerEventKey
						continuation.ActivityID, continuation.Tool = record.ActivityID, record.Tool
						continuation.SuccessEvent, continuation.FailureEvent = record.SuccessEvent, record.FailureEvent
						continuation.Generation, continuation.LoopStage = loop.Generation(), record.LoopStage
						effect, err := continuation.EffectValue()
						if err != nil {
							t.Fatal(err)
						}
						continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
						if err != nil {
							t.Fatal(err)
						}
						card.Anchor, err = decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
							RequestEventID: continuation.RequestEventID, ActivityID: record.ActivityID, Decision: "support_reply",
							Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: run, EntityID: run}, Source: eventtest.RootRoutingSource(run),
						})
						if err != nil {
							t.Fatal(err)
						}
						card.EffectContentHash = continuation.EffectContentHash
						card, err = decisioncard.New(card)
						if err != nil {
							t.Fatal(err)
						}
						if err := f.store.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(f.ctx, card, continuation); err != nil {
							t.Fatal(err)
						}
						record.RequestEventID = continuation.RequestEventID
					}
					switch cell {
					case "stale":
						record.Generation.RevisionID = uuid.NewString()
					case "wrong_entity":
						record.EntityID = uuid.NewString()
					case "wrong_path":
						record.FlowInstance = "foreign/one"
					case "wrong_header_flow":
						if _, err := f.db.Exec(`UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					case "missing_header":
						if _, err := f.db.Exec(`DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					case "malformed_bucket":
						if _, err := f.db.Exec(`UPDATE flow_instances SET accumulator='[]' WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					hints := observeChannelHints(t, f.store.(interface {
						SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
					}))
					journal := f.store.(activityStoryJournal)
					actual, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(f.ctx, record)
					if cell != "current" {
						if err == nil || inserted {
							t.Fatalf("invalid header/generation admitted: %+v %t %v", actual, inserted, err)
						}
						if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("refused activity mutated durable state")
						}
						hints.expect(t, 0)
						return
					}
					if err != nil || !inserted || actual.Status != pipeline.ActivityAttemptStatusStarted {
						t.Fatalf("canonical activity admission: %+v %t %v", actual, inserted, err)
					}
					hints.expect(t, render.ReconcileOrdinary)
					if repeated, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(f.ctx, record); err != nil || inserted || !reflect.DeepEqual(repeated, actual) {
						t.Fatalf("exact loop claim replay: %+v inserted=%t error=%v", repeated, inserted, err)
					}
					hints.expect(t, 0)
					completed, _, err := journal.CompleteActivityAttempt(f.ctx, activityStoryTerminal(t, actual, "succeeded"))
					if err != nil || completed.Status != pipeline.ActivityAttemptStatusSucceeded || !completed.Generation.Equal(loop.Generation()) {
						t.Fatalf("result persistence: %+v %v", completed, err)
					}
					hints.expect(t, render.ReconcileOrdinary)
					loaded, found, err := journal.LoadActivityAttempt(f.ctx, record.RequestEventID)
					if err != nil || !found || !reflect.DeepEqual(loaded, completed) {
						t.Fatalf("persisted result: %+v %t %v", loaded, found, err)
					}
					var fields int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID).Scan(&fields); err != nil || (shape == "fieldless" && fields != 0) || (shape == "fields" && fields != 1) {
						t.Fatalf("activity changed declared field presence: count=%d error=%v", fields, err)
					}
					after, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(after.Fields, stored.Fields) {
						t.Fatalf("activity changed business fields: found=%t instance=%+v err=%v", found, after, err)
					}
				})
			}
		}
	}
}
