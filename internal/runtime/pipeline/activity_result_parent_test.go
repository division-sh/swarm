package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

// C's f023c07529 control: the canonical emitter and lineage owner must choose
// one exact parent for ordinary requests and selected-root replay alike.
func TestActivityResultEmitterAndLineageAgreeOnExactParent(t *testing.T) {
	for _, selected := range []bool{false, true} {
		name := "ordinary"
		if selected {
			name = "selected_root"
		}
		t.Run(name, func(t *testing.T) {
			intent := testActivityIntent("https://example.invalid/recorded")
			intent.SourceRunID, intent.SourceEventID, intent.ParentEventID = uuid.NewString(), uuid.NewString(), uuid.NewString()
			request, err := activityRequestEmitIntentFromAdmittedSource(intent)
			if err != nil {
				t.Fatal(err)
			}
			lineage := ActivityRequestLineage{request: request.Event, intent: intent}
			if selected {
				selectedLineage, err := events.NewSelectedForkLineage(intent.SourceRunID, uuid.NewString(), uuid.NewString(), "selected-parent-control", intent.SourceTaskID, intent.ExecutionMode)
				if err != nil {
					t.Fatal(err)
				}
				request.Event, err = events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
					Facts: events.EventFacts{
						ID: intent.SourceEventID, Type: request.Event.Type(), Producer: events.ProducerClaim{Type: request.Event.ProducerType(), ID: request.Event.Producer().ID()},
						TaskID: intent.SourceTaskID, Payload: request.Event.Payload(), ChainDepth: request.Event.ChainDepth(),
						Envelope: request.Event.Envelope(), RoutingSource: request.Event.RoutingSource(), CreatedAt: time.Now().UTC(),
					},
					Lineage: selectedLineage,
				})
				if err != nil {
					t.Fatal(err)
				}
				lineage, err = SelectedActivityRequestLineage(request.Event)
				if err != nil {
					t.Fatal(err)
				}
			}
			emissions := &pipelineEmissionPlan{}
			dispatcher := pipelineActivityDispatcher{emissions: emissions}
			if err := dispatcher.publishActivitySuccess(context.Background(), intent, map[string]any{"recorded": 7}); err != nil {
				t.Fatal(err)
			}
			result := emissions.immutableEvents()[0]
			if result.ParentEventID() != intent.SourceEventID {
				t.Fatalf("canonical emitter parent = %q, want exact source request %q", result.ParentEventID(), intent.SourceEventID)
			}
			if err := lineage.ValidateResult(result); err != nil {
				t.Errorf("canonical emitter result refused: %v", err)
			}
			siblingIntent := intent
			siblingIntent.SourceEventID = uuid.NewString()
			sibling, err := activityRequestEmitIntentFromAdmittedSource(siblingIntent)
			if err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []string{"empty_parent", "ancestor_parent", "sibling_parent", "arbitrary_parent", "wrong_id", "wrong_run", "wrong_route", "wrong_mode", "wrong_task", "wrong_depth", "wrong_producer", "wrong_payload"} {
				t.Run(invalid, func(t *testing.T) {
					facts := events.EventFacts{
						ID: result.ID(), Type: result.Type(), Producer: events.ProducerClaim{Type: result.ProducerType(), ID: result.Producer().ID()}, TaskID: result.TaskID(),
						Payload: result.Payload(), ChainDepth: result.ChainDepth(), Envelope: result.Envelope(),
						RoutingSource: result.RoutingSource(), CreatedAt: result.CreatedAt(),
					}
					frame := events.EventLineage{RunID: result.RunID(), ParentEventID: result.ParentEventID(), TaskID: result.TaskID(), ExecutionMode: result.ExecutionMode()}
					switch invalid {
					case "empty_parent":
						frame.ParentEventID = ""
					case "ancestor_parent":
						frame.ParentEventID = intent.ParentEventID
					case "sibling_parent":
						frame.ParentEventID = sibling.Event.ID()
					case "arbitrary_parent":
						frame.ParentEventID = uuid.NewString()
					case "wrong_id":
						facts.ID = uuid.NewString()
					case "wrong_run":
						frame.RunID = uuid.NewString()
					case "wrong_route":
						facts.RoutingSource = events.NewPlatformControlRoutingSource()
						facts.Envelope = events.EventEnvelope{}
					case "wrong_mode":
						frame.ExecutionMode = executionmode.Mock
					case "wrong_task":
						facts.TaskID, frame.TaskID = "foreign-task", "foreign-task"
					case "wrong_depth":
						facts.ChainDepth++
					case "wrong_producer":
						facts.Producer.ID = "foreign"
					case "wrong_payload":
						facts.Payload = []byte(`{"activity_id":"foreign","tool":"foreign","result":7}`)
					}
					var altered events.Event
					var err error
					if frame.ParentEventID == "" {
						root, rootErr := events.NewSelectedForkLineage(frame.RunID, uuid.NewString(), uuid.NewString(), "invalid-empty-parent", frame.TaskID, frame.ExecutionMode)
						if rootErr != nil {
							t.Fatal(rootErr)
						}
						altered, err = events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: facts, Lineage: root})
					} else {
						altered, err = events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: frame})
					}
					if err != nil {
						t.Fatalf("invalid frame must reach lineage validator: %v", err)
					}
					if err := lineage.ValidateResult(altered); err == nil {
						t.Fatal("foreign result frame accepted")
					}
				})
			}
		})
	}
}
