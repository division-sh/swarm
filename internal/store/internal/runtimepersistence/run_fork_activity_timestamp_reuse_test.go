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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
						loaded, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()})
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
						again, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()})
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
					_, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), event.RunID(), child.ForkRunID, []string{event.ID()})
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

// This composes the existing source-loaded activity fixture with the real
// journal writers and fixed-event materializer, rather than inserting attempts.
func seedActivityTimestampReuse(t *testing.T, fixture authorActivityReceiptFixture, postgres, approved bool, status string) (runfork.RunForkMaterialization, events.Event, runtimepipeline.ActivityAttemptRecord) {
	t.Helper()
	ctx := testAuthorActivityContext()
	runID, parentID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	flow, instance := "flow-a", "flow-a"
	node := mustPersistenceNode(flow, "writer")
	owner := activityidentity.MustNodeOwner(node)
	policy := runtimecontracts.ActivityForkReuseRecordedResult
	activity, tool, handler, success, failure := "commit", "provider.write", "review.accepted", "write.succeeded", "write.failed"
	var card decisioncard.Card
	var continuation decisioncard.ProposedEffectContinuation
	var source events.RoutingSource
	if approved {
		card, continuation = newRootProposedEffectTestCard(t, runID, at)
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
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	fact := activityidentity.Fact{RunID: runID, SourceEventID: parentID, EntityID: entityID, Owner: owner, ExecutionFlowID: flow, HandlerEventKey: handler, ActivityID: activity, Tool: tool, Attempt: 1}
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
		"handler_event_key": handler, "source_run_id": runID, "source_event_id": parentID})
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewChildEvent(events.ChildEventInput{
		Facts:   events.EventFacts{ID: requestID, Type: "platform.activity_requested", Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "workflow"}, Payload: payload, ChainDepth: 1, RoutingSource: source, CreatedAt: at.Add(2 * time.Minute)},
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatal(err)
	}
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
