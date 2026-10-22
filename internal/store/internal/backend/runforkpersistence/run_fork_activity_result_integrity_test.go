package runforkpersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestPhysicalActivityResultImmediateSourceIntegrityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			if _, err := db.Exec(`CREATE TABLE run_fork_selected_contract_executions (
				fork_run_id TEXT, source_run_id TEXT, source_event_id TEXT, fork_event_id TEXT,
				event_name TEXT, selection_authority TEXT, created_at TIMESTAMP, source_obligation_event_id TEXT)`); err != nil {
				t.Fatal(err)
			}
			for _, withLoop := range []bool{false, true} {
				t.Run(map[bool]string{false: "ordinary", true: "loop"}[withLoop], func(t *testing.T) {
					a, _, evidence, _ := activityPublicationCutFixture(t)
					cut, err := newRunForkReplayCut(a.snapshot)
					if err != nil {
						t.Fatal(err)
					}
					parent, err := cut.event(evidence.ResultEventID)
					if err != nil {
						t.Fatal(err)
					}
					var activation loopruntime.Activation
					if withLoop {
						activation, err = loopruntime.New(parent.RunID(), parent.RoutingSource().Route().EntityID, ".", "review", "opaque_revision", uuid.NewString(), "review", 3, parent.CreatedAt())
						if err != nil {
							t.Fatal(err)
						}
						payload := changedActivityResultField(t, parent.Payload(), "opaque_revision", activation.RevisionID)
						parent = eventtest.ChildForProducerWithRoutingSource(parent.ID(), parent.Type(), parent.Producer(), parent.TaskID(), payload, parent.ChainDepth(),
							events.EventLineage{RunID: parent.RunID(), ParentEventID: parent.ParentEventID(), ExecutionMode: parent.ExecutionMode()}, parent.Envelope(), parent.RoutingSource(), parent.CreatedAt())
						parent, err = eventtest.AdmitPayload(parent, ".", string(parent.Type()))
						if err != nil {
							t.Fatal(err)
						}
					}
					for hop := 0; hop < 2; hop++ {
						binding := runfork.RunForkSelectedContractBinding{SourceRunID: parent.RunID(), ForkRunID: uuid.NewString()}
						projected, err := runfork.ProjectSelectedContractSourceEvent(binding.SourceRunID, binding.ForkRunID,
							runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()), ExecutionMode: parent.ExecutionMode(), RoutingSource: parent.RoutingSource(), Payload: parent.Payload()})
						if err != nil {
							t.Fatal(err)
						}
						var reference *loopruntime.ForkChildReference
						if withLoop {
							correspondence, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{activation}, binding.ForkRunID, projected.RoutingSource.Route().EntityID)
							if err != nil {
								t.Fatal(err)
							}
							source, err := correspondence.AdmitSource(activation.Generation())
							if err != nil {
								t.Fatal(err)
							}
							child, err := correspondence.Bind(source)
							if err != nil {
								t.Fatal(err)
							}
							reference = &child
							activation = correspondence.ProjectedActivations()[0]
						}
						payload, err := projectForkRevisionPayload(parent.Payload(), reference)
						if err != nil {
							t.Fatal(err)
						}
						var exact events.Event
						for _, fault := range []string{"exact", "business", "activity", "revision", "routing", "crossed_run", "crossed_event", "missing_lineage"} {
							t.Run(fault, func(t *testing.T) {
								id := activityidentity.ForkLineageEventID(binding.ForkRunID, parent.ID())
								stamp, at := "selected-owner", time.Now().UTC().Truncate(time.Microsecond)
								if fault != "missing_lineage" {
									if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_executions (fork_run_id,source_run_id,source_event_id,fork_event_id,event_name,selection_authority,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, binding.ForkRunID, parent.RunID(), parent.ID(), id, string(parent.Type()), stamp, at); err != nil {
										t.Fatal(err)
									}
								}
								sourceRun, sourceEvent, raw, route := parent.RunID(), parent.ID(), payload, projected.RoutingSource
								switch fault {
								case "business":
									raw = changedActivityResultField(t, raw, "result", map[string]any{"value": "CHANGED"})
								case "activity":
									raw = changedActivityResultField(t, raw, "activity_id", "foreign")
								case "revision":
									raw = changedActivityResultField(t, raw, "opaque_revision", "foreign")
								case "routing":
									route = events.RoutingSource{}
								case "crossed_run":
									sourceRun = uuid.NewString()
								case "crossed_event":
									sourceEvent = uuid.NewString()
								}
								lineage, err := events.NewSelectedForkLineage(binding.ForkRunID, sourceRun, sourceEvent, stamp, "", parent.ExecutionMode())
								if err != nil {
									t.Fatal(err)
								}
								physical, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Lineage: lineage, Facts: events.EventFacts{
									ID: id, Type: parent.Type(), Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: stamp}, Payload: raw,
									RoutingSource: route, Envelope: events.EventEnvelope{Source: route.Route()}, CreatedAt: at, ExecutionMode: parent.ExecutionMode()}})
								if err != nil {
									t.Fatal(err)
								}
								physical, err = eventtest.AdmitPayload(physical, ".", string(parent.Type()))
								if err != nil {
									t.Fatal(err)
								}
								if _, err := events.AdmitForPersistence(physical, events.AdmissionOptions{RequirePersistentUUIDIdentity: true}); err != nil {
									t.Fatal(err)
								}
								tx, err := db.BeginTx(t.Context(), nil)
								if err != nil {
									t.Fatal(err)
								}
								err = requireSelectedPublicationCutLineage(context.Background(), tx, binding, physical, parent, reference)
								_ = tx.Rollback()
								if fault != "missing_lineage" {
									if _, deleteErr := db.Exec(`DELETE FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`, binding.ForkRunID); deleteErr != nil {
										t.Fatal(deleteErr)
									}
								}
								if fault == "exact" {
									if err != nil {
										t.Fatalf("exact immediate-source projection: %v", err)
									}
									exact = physical
								} else if err == nil {
									t.Fatalf("changed %s accepted at hop %d", fault, hop)
								}
							})
						}
						parent = exact
					}
				})
			}
		})
	}
}

func changedActivityResultField(t *testing.T, raw json.RawMessage, key string, value any) json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	fields[key] = encoded
	encoded, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
