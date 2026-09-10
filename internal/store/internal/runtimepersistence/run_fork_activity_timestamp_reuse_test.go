package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type activityTimestampJournal interface {
	StartActivityAttempt(context.Context, runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error)
	CompleteActivityAttempt(context.Context, runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, error)
	MarkActivityAttemptUncertain(context.Context, runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, error)
	LoadActivityAttempt(context.Context, string) (runtimepipeline.ActivityAttemptRecord, bool, error)
}

func TestRunForkActivityTimestampRecordedReuseBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, approved := range []bool{false, true} {
				policy := "ordinary"
				if approved {
					policy = "approved_effect"
				}
				for _, status := range []string{"succeeded", "failed", "uncertain", "started"} {
					t.Run(policy+"/"+status, func(t *testing.T) {
						ctx := testAuthorActivityContext()
						child, event, sourceRecord := seedActivityTimestampReuse(t, fixture, backend.name == "postgres", approved, status)
						store := fixture.store.(selectedActivityProjectionStore)
						journal := fixture.store.(activityTimestampJournal)
						before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						loaded, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID()))
						wantErr := ""
						if status == "started" {
							wantErr = "recorded evidence is not terminal"
						} else if approved && status == "uncertain" {
							wantErr = "ambiguous dispatch evidence"
						}
						if wantErr != "" {
							if err == nil || !strings.Contains(err.Error(), wantErr) {
								t.Fatalf("source-loader rejection = %v, want %q", err, wantErr)
							}
							if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
								t.Fatal("rejected source evidence changed persisted rows")
							}
							return
						}
						if err != nil || len(loaded) != 1 {
							t.Fatalf("materialized source-loader reuse: count=%d err=%v", len(loaded), err)
						}
						var payload runForkActivityRequestPayload
						if err := json.Unmarshal(loaded[0].Payload, &payload); err != nil {
							t.Fatal(err)
						}
						owner, err := activityidentity.ParseOwnerKey(payload.NodeID)
						if err != nil {
							t.Fatal(err)
						}
						fact := activityidentity.Fact{RunID: child.ForkRunID, SourceEventID: payload.SourceEventID, ParentEventID: payload.ParentEventID,
							EntityID: payload.EntityID, Owner: owner, ExecutionFlowID: payload.FlowID, HandlerEventKey: payload.HandlerEventKey,
							ActivityID: payload.ActivityID, Tool: payload.Tool, Attempt: 1, RevisionID: payload.Generation.RevisionID}
						copied, found, err := journal.LoadActivityAttempt(ctx, activityidentity.RequestEventID(fact))
						if err != nil || !found {
							t.Fatalf("read copied terminal attempt: found=%v err=%v", found, err)
						}
						if copied.RunID != child.ForkRunID || copied.Status != status || copied.ExecutionMode != sourceRecord.ExecutionMode || copied.InputHash != sourceRecord.InputHash || copied.ResultEventType != sourceRecord.ResultEventType || copied.ResultEventID != activityidentity.ResultEventID(fact, sourceRecord.ResultEventType) || copied.ResultEventID == sourceRecord.ResultEventID {
							t.Fatalf("copied attempt lost exact identity/terminal evidence: %#v", copied)
						}
						if !copied.StartedAt.Equal(sourceRecord.StartedAt) || !copied.UpdatedAt.Equal(sourceRecord.UpdatedAt) || copied.CompletedAt == nil || sourceRecord.CompletedAt == nil || !copied.CompletedAt.Equal(*sourceRecord.CompletedAt) {
							t.Fatalf("timestamp precision lost: source=%#v child=%#v", sourceRecord, copied)
						}
						if !reflect.DeepEqual(copied.ResultPayload, sourceRecord.ResultPayload) || !reflect.DeepEqual(copied.Failure, sourceRecord.Failure) {
							t.Fatalf("result/failure changed: source=%#v child=%#v", sourceRecord, copied)
						}
						wantEntity, wantFlow := sourceRecord.EntityID, sourceRecord.FlowInstance
						if approved {
							wantEntity, wantFlow = child.ForkRunID, child.ForkRunID
						}
						if payload.SourceRunID != child.ForkRunID || copied.EntityID != wantEntity || copied.FlowInstance != wantFlow || copied.Generation.Valid() || payload.Generation.Valid() || (approved && payload.ForkPolicy != string(runtimecontracts.ActivityForkRequireConfirmation)) {
							t.Fatalf("source/child owner or approval policy changed: payload=%#v copied=%#v", payload, copied)
						}
						sourceAfter, found, err := journal.LoadActivityAttempt(ctx, sourceRecord.RequestEventID)
						if err != nil || !found || !reflect.DeepEqual(sourceAfter, sourceRecord) {
							t.Fatalf("source journal changed: found=%v err=%v", found, err)
						}
						afterFirst := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						again, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID()))
						if err != nil || !reflect.DeepEqual(loaded, again) {
							t.Fatalf("repeat changed prepared event: err=%v", err)
						}
						if afterRepeat := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(afterFirst, afterRepeat) {
							t.Fatal("repeated reuse changed persisted rows")
						}
						// Exercise the real duplicate-attempt gate, not a connector or
						// runtime-container provider invocation.
						restarted, inserted, err := journal.StartActivityAttempt(ctx, copied)
						if err != nil || inserted || !reflect.DeepEqual(restarted, copied) {
							t.Fatalf("copied terminal request restarted: inserted=%v err=%v got=%#v want=%#v", inserted, err, restarted, copied)
						}
						afterStart := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						if !reflect.DeepEqual(afterFirst, afterStart) {
							var changed []string
							for table, rows := range afterStart {
								if !reflect.DeepEqual(afterFirst[table], rows) {
									changed = append(changed, table)
								}
							}
							sort.Strings(changed)
							t.Fatalf("duplicate copied request changed persisted tables: %v", changed)
						}
						for _, op := range []string{"start", "claim", "complete", "uncertain"} {
							t.Run("copied_noop/"+op, func(t *testing.T) {
								request := activityStoryRequest(t, copied, op)
								spy := &activityStorySpy{}
								got, inserted, err := activityStoryKernel(t, fixture, backend.name == "postgres", op, request, spy)
								if err != nil || inserted || !reflect.DeepEqual(got, copied) || len(spy.drafts) != 0 {
									t.Fatalf("copied no-op kernel: err=%v inserted=%v drafts=%d got=%#v", err, inserted, len(spy.drafts), got)
								}
								got, inserted, err = activityStoryOuter(fixture, op, request)
								if err != nil || inserted || !reflect.DeepEqual(got, copied) {
									t.Fatalf("copied no-op outer: err=%v inserted=%v got=%#v", err, inserted, got)
								}
								assertActivityStoryTablesUnchanged(t, fixture, backend.name == "postgres", afterFirst)
							})
						}
					})
				}
			}
		})
	}
}

func TestRunForkActivityTimestampRejectsIncompleteReuseBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, column := range []string{"started_at", "completed_at", "updated_at"} {
				t.Run(column, func(t *testing.T) {
					child, event, sourceRecord := seedActivityTimestampReuse(t, fixture, backend.name == "postgres", false, "succeeded")
					if _, err := fixture.db.Exec(`UPDATE activity_attempts SET `+column+`=$1 WHERE request_event_id=$2`, time.Time{}, sourceRecord.RequestEventID); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					_, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, fixture.store, event.RunID()))
					if err == nil || !strings.Contains(err.Error(), column+" is required") {
						t.Fatalf("incomplete recorded timestamp: got %v, want %s is required", err, column)
					}
					if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
						t.Fatal("incomplete source evidence changed persisted rows")
					}
				})
			}
		})
	}
}

func TestRunForkActivityTimestampCopyPreservesStoredPrecisionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			child, event, source := seedActivityTimestampReuse(t, fixture, postgres, false, "succeeded")
			fraction := 123456789
			if postgres {
				fraction = 123456000
			}
			start := time.Date(2026, 8, 1, 12, 0, 0, fraction, time.FixedZone("offset", 90*60))
			completed, updated := start.Add(17*time.Second), start.Add(19*time.Second)
			// The source is writer-created. This explicit representation probe
			// substitutes store-supported fractional/offset values; it does not
			// claim SQLite CURRENT_TIMESTAMP emits fractional seconds itself.
			if _, err := fixture.db.Exec(`UPDATE activity_attempts SET started_at=$1, completed_at=$2, updated_at=$3 WHERE request_event_id=$4`, start.Format(time.RFC3339Nano), completed.Format(time.RFC3339Nano), updated.Format(time.RFC3339Nano), source.RequestEventID); err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext()
			journal := fixture.store.(activityTimestampJournal)
			original, found, err := journal.LoadActivityAttempt(ctx, source.RequestEventID)
			if err != nil || !found || !original.StartedAt.Equal(start) || original.CompletedAt == nil || !original.CompletedAt.Equal(completed) || !original.UpdatedAt.Equal(updated) {
				t.Fatalf("source precision fixture: %#v err=%v", original, err)
			}
			store := fixture.store.(selectedActivityProjectionStore)
			if _, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID())); err != nil {
				t.Fatal(err)
			}
			var copiedID string
			if err := fixture.db.QueryRow(`SELECT request_event_id FROM activity_attempts WHERE run_id=$1`, child.ForkRunID).Scan(&copiedID); err != nil {
				t.Fatal(err)
			}
			copied, found, err := journal.LoadActivityAttempt(ctx, copiedID)
			if err != nil || !found || !copied.StartedAt.Equal(start) || copied.CompletedAt == nil || !copied.CompletedAt.Equal(completed) || !copied.UpdatedAt.Equal(updated) {
				t.Fatalf("copy lost stored precision: %#v err=%v", copied, err)
			}
			if !postgres {
				var a, b, c string
				if err := fixture.db.QueryRow(`SELECT started_at,completed_at,updated_at FROM activity_attempts WHERE request_event_id=$1`, copiedID).Scan(&a, &b, &c); err != nil {
					t.Fatal(err)
				}
				if a != start.UTC().Format(time.RFC3339Nano) || b != completed.UTC().Format(time.RFC3339Nano) || c != updated.UTC().Format(time.RFC3339Nano) {
					t.Fatalf("noncanonical copied representations: %q %q %q", a, b, c)
				}
			}
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
			if _, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID())); err != nil {
				t.Fatal(err)
			}
			if _, inserted, err := journal.StartActivityAttempt(ctx, copied); err != nil || inserted {
				t.Fatalf("precision retry: inserted=%v err=%v", inserted, err)
			}
			assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
		})
	}
}

// This composes the existing source-loaded activity fixture with the real
// journal writers and fixed-event materializer, rather than inserting attempts.
func seedActivityTimestampReuse(t *testing.T, fixture authorActivityReceiptFixture, postgres, approved bool, status string) (runfork.RunForkMaterialization, events.Event, runtimepipeline.ActivityAttemptRecord) {
	return seedActivityEvidenceReuse(t, fixture, postgres, approved, status, 0)
}

// Loop state is explicit fixture evidence; attempt writes use the real journal.
// Ordinary loop execution is proved separately by the served/container journeys.
func seedActivityEvidenceReuse(t *testing.T, fixture authorActivityReceiptFixture, postgres, approved bool, status string, loopAttempt int) (runfork.RunForkMaterialization, events.Event, runtimepipeline.ActivityAttemptRecord) {
	t.Helper()
	runID, parentID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	declarations := selectedActivityProducerSourceWithLoops(t, false, loopAttempt > 0)
	ctx := seedSelectedActivitySourceRun(t, fixture, runID, declarations)
	flow, instance := "flow-a", "flow-a"
	node := mustPersistenceNode(flow, "writer")
	owner := activityidentity.MustNodeOwner(node)
	policy := runtimecontracts.ActivityForkReuseRecordedResult
	activity, tool, handler, success, failure := "commit", "provider.write", "review.accepted", "flow-a/commit.succeeded", "flow-a/commit.failed"
	var card decisioncard.Card
	var continuation decisioncard.ProposedEffectContinuation
	var source events.RoutingSource
	if approved {
		card, continuation = newDeclaredRootActivityCard(t, runID, at, declarations)
		parentID, entityID, flow, instance = continuation.SourceEventID, runID, continuation.FlowID, runID
		var err error
		owner, err = activityidentity.ParseOwnerKey(continuation.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		policy = continuation.ForkPolicy
		activity, tool, handler, success, failure = continuation.ActivityID, continuation.Tool, continuation.HandlerEventKey, continuation.SuccessEvent, continuation.FailureEvent
		anchor, err := card.Anchor.ProposedEffect()
		if err != nil {
			t.Fatal(err)
		}
		source = anchor.Source
	} else {
		var err error
		source, err = pinrouting.AdmitNodeExecutionRoutingSource(selectedActivityProducerSource(t), node, flow, events.RouteIdentity{FlowID: flow, FlowInstance: instance, EntityID: entityID})
		if err != nil {
			t.Fatal(err)
		}
	}
	parent := eventtest.ExistingRunRootIngress(parentID, "activity.seeded", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,'lifecycle_state','','null','"pending"',$3,'platform','activity-timestamp-fixture','seed',$4)`, runID, entityID, parentID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1,$2,$3,'default','pending','{}','{}','{}','{}',1,$4,$5,$6)`, runID, entityID, instance, at, at, at); err != nil {
		t.Fatal(err)
	}
	var generation attemptgeneration.Generation
	loopStage := ""
	if loopAttempt > 0 {
		if approved {
			t.Fatal("this fixture's loop is declared on flow-a, not the root approval")
		}
		activation, err := loopruntime.New(runID, entityID, flow, "revision", "revision_id", parentID, "review", 3, at)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 1; attempt < loopAttempt; attempt++ {
			if escaped, err := activation.Repeat("review", uuid.NewString(), at.Add(time.Duration(attempt)*time.Second)); err != nil || escaped {
				t.Fatalf("fixture repeat escaped=%v err=%v", escaped, err)
			}
		}
		generation, loopStage = activation.Generation(), "review"
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET accumulator=$3 WHERE run_id=$1 AND entity_id=$2`, runID, entityID, forkTestJSON(t, buckets)); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,'accumulator','handler_loops','null',$3,$4,'platform','activity-evidence-fixture','seed',$5)`, runID, entityID, forkTestJSON(t, buckets[loopruntime.BucketKey]), parentID, at); err != nil {
			t.Fatal(err)
		}
	}
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	fact := activityidentity.Fact{RunID: runID, SourceEventID: parentID, EntityID: entityID, Owner: owner, ExecutionFlowID: flow, HandlerEventKey: handler, ActivityID: activity, Tool: tool, Attempt: 1, RevisionID: generation.RevisionID}
	requestID := activityidentity.RequestEventID(fact)
	var input any = map[string]any{"value": "recorded"}
	decisionID := ""
	if approved {
		requestID, input = continuation.RequestEventID, continuation.Input.Interface()
		cards := fixture.store.(decisioncard.ProposedEffectStore)
		if err := cards.CreateProposedEffectCard(ctx, card, continuation); err != nil {
			t.Fatal(err)
		}
		decisionID = uuid.NewString()
		if _, err := fixture.store.(decisioncard.Store).DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: decisionID, Now: at.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := json.Marshal(map[string]any{"activity_id": activity, "tool": tool, "input": input, "effect_class": "non_idempotent_write", "fork_policy": string(policy),
		"success_event": success, "failure_event": failure, "attempt": 1, "entity_id": entityID, "node_id": owner.Key(), "flow_id": flow, "flow_instance": instance,
		"handler_event_key": handler, "source_run_id": runID, "source_event_id": parentID, "loop_generation": generation, "loop_stage": loopStage})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ChildForProducerWithRoutingSource(requestID, "platform.activity_requested", eventtest.Producer(events.EventProducerPlatform, "workflow"), "", payload, 1,
		events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live}, events.EventEnvelope{}, source, at.Add(2*time.Minute))
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
		t.Fatal(err)
	}
	if approved {
		if _, err := fixture.store.(decisioncard.ProposedEffectStore).CompleteProposedEffectRoute(ctx, card.CardID, decisionID, at.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	journal := fixture.store.(activityTimestampJournal)
	record, inserted, err := journal.StartActivityAttempt(ctx, runtimepipeline.ActivityAttemptRecord{
		RequestEventID: requestID, RunID: runID, SourceEventID: parentID, ExecutionMode: executionmode.Live,
		EntityID: entityID, FlowInstance: instance, NodeID: owner.Key(), HandlerEventKey: handler, ActivityID: activity, Tool: tool,
		EffectClass: "non_idempotent_write", Attempt: 1, SuccessEvent: success, FailureEvent: failure, InputHash: "recorded-input-hash",
		Generation: generation, LoopStage: loopStage,
	})
	if err != nil || !inserted {
		t.Fatalf("canonical journal start: inserted=%v err=%v", inserted, err)
	}
	if status != "started" {
		record.Status, record.ResultEventType = status, success
		record.ResultPayload = map[string]any{"activity_id": activity, "result": map[string]any{"ok": true}}
		if status != "succeeded" {
			record.ResultEventType = failure
			class := runtimefailures.ClassDependencyUnavailable
			if status == "uncertain" {
				class = runtimefailures.ClassOutcomeUncertain
			}
			envelope, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(class, "activity_timestamp_test", "activity-runtime", "execute", nil))
			if !ok {
				t.Fatal("canonical activity failure envelope is required")
			}
			record.Failure = &envelope
			record.ResultPayload = map[string]any{"activity_id": activity, "failure": map[string]any{"code": "activity_timestamp_test"}}
		}
		record.ResultEventID = activityidentity.ResultEventID(fact, record.ResultEventType)
		if generation.Valid() {
			record.ResultPayload[generation.RevisionField] = generation.RevisionID
		}
		if status == "uncertain" {
			record, err = journal.MarkActivityAttemptUncertain(ctx, record)
		} else {
			record, err = journal.CompleteActivityAttempt(ctx, record)
		}
		if err != nil {
			t.Fatalf("canonical journal terminal write: %v", err)
		}
	}
	fixture.advance()
	child := materializeSelectedActivityFixture(t, ctx, fixture.store.(selectedActivityProjectionStore), runID, event.ID())
	if child.MaterializedEntityCount != 1 {
		t.Fatalf("producer materialization count=%d", child.MaterializedEntityCount)
	}
	return child, event, record
}

func newDeclaredRootActivityCard(t *testing.T, runID string, at time.Time, declarations semanticview.Source) (decisioncard.Card, decisioncard.ProposedEffectContinuation) {
	t.Helper()
	card, continuation := newRootProposedEffectTestCard(t, runID, at)
	bundle, ok := semanticview.Bundle(declarations)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("proposed activity requires its admitted declaration artifact")
	}
	continuation.BundleHash, continuation.FlowID = bundle.SourceArtifact.BundleHash(), "."
	continuation.SuccessEvent, continuation.FailureEvent = "send_support_reply.succeeded", "send_support_reply.failed"
	continuation.RevisionEvent, continuation.RejectedEvent = "send_support_reply.revision_requested", "send_support_reply.rejected"
	owner, err := activityidentity.ParseOwnerKey(continuation.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	continuation.RequestEventID = activityidentity.RequestEventID(activityidentity.Fact{
		RunID: runID, SourceEventID: continuation.SourceEventID, EntityID: runID, Owner: owner,
		ExecutionFlowID: continuation.FlowID, HandlerEventKey: continuation.HandlerEventKey,
		ActivityID: continuation.ActivityID, Tool: continuation.Tool, Attempt: 1,
	})
	continuation.CardID = decisioncard.ProposedEffectCardID(continuation.RequestEventID, "support_reply")
	effect, err := continuation.EffectValue()
	if err != nil {
		t.Fatal(err)
	}
	continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: continuation.RequestEventID, ActivityID: continuation.ActivityID, Decision: "support_reply",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: runID, EntityID: runID}, Source: eventtest.RootRoutingSource(runID),
	})
	if err != nil {
		t.Fatal(err)
	}
	card.CardID, card.BundleHash, card.Anchor = continuation.CardID, continuation.BundleHash, anchor
	card.EffectContentHash, card.CardContentHash = continuation.EffectContentHash, ""
	card, err = decisioncard.New(card)
	if err != nil {
		t.Fatal(err)
	}
	return card, continuation
}
